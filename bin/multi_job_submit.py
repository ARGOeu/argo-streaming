#!/usr/bin/env python
import sys
import os
import argparse
from snakebite.client import Client

from datetime import datetime, date, timedelta
import logging
from urllib.parse import urlparse
from utils.argo_mongo import ArgoMongoClient
from utils.common import cmd_to_string, date_rollback, flink_job_submit, hdfs_check_path, get_log_conf, get_config_paths
from utils.update_profiles import ArgoProfileManager
from utils.argo_config import ArgoConfig


log = logging.getLogger(__name__)


def compose_hdfs_commands(year, month, day, args, config):
    """Checks hdfs for available files back in time and prepares the correct hdfs arguments

    Args:
        year (int): year part of the date to check for hdfs files
        month (int): month part of the date to check for hdfs files
        day (int): day part of the date to check for hdfs files
        config (obj.): argo configuration object


    Returns:
        list: A list of all hdfs arguments to be used in flink job submission
    """

    # set up the hdfs client to be used in order to check the files
    namenode = config.get("HDFS", "namenode")
    client = Client(namenode.hostname, namenode.port, use_trash=False)

    # hdfs sync  path for the tenant

    hdfs_user = config.get("HDFS", "user")
    tenant = args.tenant
    hdfs_sync = config.get("HDFS", "path_sync")
    hdfs_sync = hdfs_sync.fill(namenode=namenode.geturl(
    ), hdfs_user=hdfs_user, tenant=tenant).geturl()

    hdfs_metric = config.get("HDFS", "path_metric")

    hdfs_metric = hdfs_metric.fill(
        namenode=namenode.geturl(), hdfs_user=hdfs_user, tenant=tenant).geturl()

    hdfs_tenants = config.get("HDFS", "path_tenants")
    hdfs_tenants = hdfs_tenants.fill(
        namenode=namenode.geturl(), hdfs_user=hdfs_user
    ).geturl()

    # dictionary holding all the commands with their respective arguments' name
    hdfs_commands = dict()

    # file location of previous day's metric data (local or hdfs)
    hdfs_commands["--pdata"] = hdfs_check_path(
        hdfs_metric + "/" + str(date(year, month, day) - timedelta(1)), client)

    # file location of target day's metric data (local or hdfs)
    hdfs_commands["--mdata"] = hdfs_check_path(
        hdfs_metric+"/"+args.date,  client)
    
    # if job will run in combined data mode then we should add the hdfs tenants path as --basispath to the jar
    # to find out if the tenant will run in combined data mode we check if --source-data 
    # is provided as argument and has value other than tenant

    if ("source_data" in args and args.source_data != "tenant"):
        hdfs_commands["--basispath"] = hdfs_check_path(hdfs_tenants, client)

    return hdfs_commands


def compose_command(config, args,  hdfs_commands, dry_run=False):
    """Composes a command line execution string for submitting a flink job. Also calls mongodb
       clean up procedure before composing the command

    Args:
        config (obj.): argo configuration object
        args (dict): command line arguments of this script
        hdfs_commands (list): a list of hdfs related arguments to be passed in flink job
        dry_run (bool, optional): signifies a dry-run execution context, if yes no mongodb clean-up is perfomed.
                                  Defaults to False.

    Returns:
        list: A list of all command line arguments for performing the flink job submission
    """

    # job sumbission command
    cmd_command = []

    if args.sudo is True:
        cmd_command.append("sudo")

    # flink executable
    cmd_command.append(config.get("FLINK", "path"))

    cmd_command.append("run")

    cmd_command.append("-c")

    # Job's class inside the jar
    cmd_command.append(config.get("CLASSES", "batch-multi"))

    # jar to be sumbitted to flink
    cmd_command.append(config.get("JARS", "batch-multi"))

    # date the report will run for
    cmd_command.append("--run.date")
    cmd_command.append(args.date)

    #  MongoDB uri for outputting the results to (e.g. mongodb://localhost:21017/example_db)
    cmd_command.append("--mongo.uri")
    group_tenant = "TENANTS:" + args.tenant
    mongo_endpoint = config.get("MONGO", "endpoint").geturl()
    mongo_uri = config.get(group_tenant, "mongo_uri").fill(
        mongo_endpoint=mongo_endpoint, tenant=args.tenant)
    cmd_command.append(mongo_uri.geturl())

    # if args.method == "insert" and dry_run == False:
    #     argo_mongo_client = ArgoMongoClient(args, config, ["status_metrics", "status_endpoints", "status_services",
    #                                                        "status_endpoint_groups"])

    #     argo_mongo_client.mongo_clean_status(mongo_uri, dry_run)

    # MongoDB method to be used when storing the results, either insert or upsert
    cmd_command.append("--mongo.method")
    cmd_command.append(args.method)

    # add the hdfs commands
    for command in hdfs_commands:
        cmd_command.append(command)
        cmd_command.append(hdfs_commands[command])

    # get the api endpoint
    api_endpoint = config.get("API","endpoint")
    if api_endpoint:
        cmd_command.append("--api.endpoint")
        cmd_command.append(api_endpoint.hostname)

    # get the api token
    cmd_command.append("--api.token")
    cmd_command.append(config.get("API",args.tenant + "_key"))

    cmd_command.append("--report.id")
    cmd_command.append(config.get("TENANTS:"+args.tenant,"report_"+args.report))

    # get optional api proxy
    proxy = config.get("API", "proxy")
    if proxy is not None:
        cmd_command.append("--api.proxy")
        cmd_command.append(proxy.geturl())

    # check clear
    if args.clear_results is not None:
        cmd_command.append("--clearMongo")
        cmd_command.append("true")

    # check what to compute
    if args.calculate is not None:
        if "ar" not in args.calculate:
            cmd_command.append("--calcAR")
            cmd_command.append("OFF")
        if "status" not in args.calculate:
            cmd_command.append("--calcStatus")
            cmd_command.append("OFF")
        if "trends" not in args.calculate:
            cmd_command.append("--calcTrends")
            cmd_command.append("OFF")

    # check if sources are defined
    if args.source_data:
        cmd_command.append("--source-data")
        cmd_command.append(args.source_data)

    if args.source_topo:
        cmd_command.append("--source-topo")
        cmd_command.append(args.source_topo)

    return cmd_command


def main(args=None):

    # Get configuration paths
    conf_paths = get_config_paths(args.config)

    # Get logger config file
    get_log_conf(conf_paths['log'])

    # Get main configuration and schema
    config = ArgoConfig(conf_paths["main"], conf_paths["schema"])

    # check if configuration for the given tenant exists
    if not config.has("TENANTS:"+args.tenant):
        log.fatal("Tenant: "+args.tenant+" doesn't exist.")
        sys.exit(1)

    year, month, day = [int(x) for x in args.date.split("-")]

    # dictionary containing the argument's name and the command associated with each name
    hdfs_commands = compose_hdfs_commands(year, month, day, args, config)

    cmd_command = compose_command(config, args, hdfs_commands, args.dry_run)

    # submit the script's command
    flink_job_submit(config, cmd_command, None, args.dry_run)


if __name__ == "__main__":

    today = datetime.today().strftime('%Y-%m-%d')

    parser = argparse.ArgumentParser(
        description="Batch Status Job submit script")
    parser.add_argument(
        "-t", "--tenant", metavar="STRING", help="Name of the tenant", required=True, dest="tenant")
    parser.add_argument(
        "-r", "--report", metavar="STRING", help="Name of the report", required=True, dest="report")
    parser.add_argument(
        "-d", "--date", metavar="DATE(YYYY-MM-DD)", help="Date to run the job for", required=False, dest="date", default=today)
    parser.add_argument(
        "-m", "--method", metavar="KEYWORD(insert|upsert)", help="Insert or Upsert data in mongoDB", required=False, dest="method", default="insert")
    parser.add_argument(
        "-x", "--calculate", metavar="STRING", help="Comma separated list of what to calculate (ar,status,trends)", required=False, dest="calculate", default=None)
    parser.add_argument(
        "-c", "--config", metavar="PATH", help="Path for the config file", dest="config")
    parser.add_argument(
        "-u", "--sudo", help="Run the submit job as superuser",  action="store_true")
    parser.add_argument("--clear-prev-results", help="Clear previous results from datastore",
                        action="store_true", dest="clear_results")
    parser.add_argument("--dry-run", help="Runs in test mode without actually submitting the job",
                        action="store_true", dest="dry_run")
    parser.add_argument(
        "-s", "--source-data", metavar="STRING", help="Define source of data to be used (possible values: tenant, feeds, all)", 
        dest="source_data", required=False)
    parser.add_argument(
        "-S", "--source-topo", metavar="STRING", help="Define source of topology to be used (possible values: tenant, all)", 
        dest="source_topo", required=False)
    # Pass the arguments to main method
    sys.exit(main(parser.parse_args()))
