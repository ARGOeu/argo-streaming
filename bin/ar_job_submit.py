#!/usr/bin/env python

import sys
import os
import argparse
import datetime
from snakebite.client import Client
import ConfigParser
import logging
from urlparse import urlparse
from utils.argo_log import ArgoLogger
from utils.argo_mongo import ArgoMongoClient
from utils.common import cmd_toString, date_rollback, flink_job_submit, hdfs_check_path
from utils.update_profiles import ArgoProfileManager

def compose_hdfs_commands(year, month, day, args, config, logger):

    # set up the hdfs client to be used in order to check the files
    client = Client(config.get("HDFS", "hdfs_host"), config.getint("HDFS", "hdfs_port"), use_trash=False)

    # hdfs sync  path for the tenant
    hdfs_sync = config.get("HDFS", "hdfs_sync")
    hdfs_sync = hdfs_sync.replace("{{hdfs_host}}", config.get("HDFS", "hdfs_host"))
    hdfs_sync = hdfs_sync.replace("{{hdfs_port}}", config.get("HDFS", "hdfs_port"))
    hdfs_sync = hdfs_sync.replace("{{hdfs_user}}", config.get("HDFS", "hdfs_user"))
    hdfs_sync = hdfs_sync.replace("{{tenant}}", args.Tenant)

    # hdfs metric path for the tenant
    hdfs_metric = config.get("HDFS", "hdfs_metric")
    hdfs_metric = hdfs_metric.replace("{{hdfs_host}}", config.get("HDFS", "hdfs_host"))
    hdfs_metric = hdfs_metric.replace("{{hdfs_port}}", config.get("HDFS", "hdfs_port"))
    hdfs_metric = hdfs_metric.replace("{{hdfs_user}}", config.get("HDFS", "hdfs_user"))
    hdfs_metric = hdfs_metric.replace("{{tenant}}", args.Tenant)

    # dictionary holding all the commands with their respective arguments' name
    hdfs_commands = {}

    # file location of previous day's metric data (local or hdfs)
    hdfs_commands["--pdata"] = hdfs_check_path(hdfs_metric+"/"+str(datetime.date(year, month, day) - datetime.timedelta(1)), logger, client)

    # file location of target day's metric data (local or hdfs)
    hdfs_commands["--mdata"] = hdfs_check_path(hdfs_metric+"/"+args.Date, logger, client)

    # file location of report configuration json file (local or hdfs)
    hdfs_commands["--conf"] = hdfs_check_path(hdfs_sync+"/"+args.Tenant+"_"+args.Report+"_cfg.json", logger, client)

    # file location of metric profile (local or hdfs)
    hdfs_commands["--mps"] = date_rollback(hdfs_sync+"/"+args.Report+"/"+"metric_profile_"+"{{date}}"+".avro", year, month, day, config, logger, client)

    # file location of operations profile (local or hdfs)
    hdfs_commands["--ops"] = hdfs_check_path(hdfs_sync+"/"+args.Tenant+"_ops.json", logger, client)

    # file location of aggregations profile (local or hdfs)
    hdfs_commands["--apr"] = hdfs_check_path(hdfs_sync+"/"+args.Tenant+"_"+args.Report+"_ap.json", logger, client)

    #  file location of endpoint group topology file (local or hdfs)
    hdfs_commands["-egp"] = date_rollback(hdfs_sync+"/"+args.Report+"/"+"group_endpoints_"+"{{date}}"+".avro", year, month, day, config, logger, client)

    # file location of group of groups topology file (local or hdfs)
    hdfs_commands["-ggp"] = date_rollback(hdfs_sync+"/"+args.Report+"/"+"group_groups_"+"{{date}}"+".avro", year, month, day, config, logger, client)

    # file location of weights file (local or hdfs)
    hdfs_commands["--weights"] = date_rollback(hdfs_sync+"/"+args.Report+"/weights_"+"{{date}}"+".avro", year, month, day, config, logger, client)

    # file location of downtimes file (local or hdfs)
    hdfs_commands["--downtimes"] = hdfs_check_path(hdfs_sync+"/"+args.Report+"/downtimes_"+str(datetime.date(year, month, day))+".avro", logger, client)

    # file location of recomputations file (local or hdfs)
    # first check if there is a recomputations file for the given date
    if client.test(urlparse(hdfs_sync+"/recomp_"+args.Date+".json").path, exists=True):
        hdfs_commands["--rec"] = hdfs_sync+"/recomp_"+args.Date+".json"
    else:
        hdfs_commands["--rec"] = hdfs_check_path(hdfs_sync+"/recomp.json", logger, client)

    return hdfs_commands


def compose_command(config, args,  hdfs_commands, logger=None):

    # job sumbission command
    cmd_command = []

    if args.Sudo is True:
        cmd_command.append("sudo")

    # create a simple stream_handler whenever tetsing
    if logger is None:
        logger = ArgoLogger()

    # flink executable
    cmd_command.append(config.get("FLINK", "path"))

    cmd_command.append("run")

    cmd_command.append("-c")

    # Job's class inside the jar
    cmd_command.append(config.get("CLASSES", "batch-ar"))

    # jar to be sumbitted to flink
    cmd_command.append(config.get("JARS", "batch-ar"))

    # date the report will run for
    cmd_command.append("--run.date")
    cmd_command.append(args.Date)

    #  MongoDB uri for outputting the results to (e.g. mongodb://localhost:21017/example_db)
    cmd_command.append("--mongo.uri")
    mongo_tenant = "TENANTS:"+args.Tenant+":MONGO"
    mongo_uri = config.get(mongo_tenant, "mongo_uri")
    mongo_uri = mongo_uri.replace("{{mongo_host}}", config.get(mongo_tenant, "mongo_host"))
    mongo_uri = mongo_uri.replace("{{mongo_port}}", config.get(mongo_tenant, "mongo_port"))
    cmd_command.append(mongo_uri)

    if args.Method == "insert":
        argo_mongo_client = ArgoMongoClient(args, config, logger, ["service_ar", "endpoint_group_ar"])
        argo_mongo_client.mongo_clean_ar(mongo_uri)

    # MongoDB method to be used when storing the results, either insert or upsert
    cmd_command.append("--mongo.method")
    cmd_command.append(args.Method)

    # add the hdfs commands
    for command in hdfs_commands:
        cmd_command.append(command)
        cmd_command.append(hdfs_commands[command])

    # ams proxy
    if config.getboolean("AMS", "proxy_enabled"):
        cmd_command.append("--ams.proxy")
        cmd_command.append(config.get("AMS", "ams_proxy"))

    # ssl verify
    cmd_command.append("--ams.verify")
    if config.getboolean("AMS", "ssl_enabled"):
        cmd_command.append("true")
    else:
        cmd_command.append("false")

    return cmd_command


def main(args=None):

    # make sure the argument are in the correct form
    args.Tenant = args.Tenant.upper()
    args.Method = args.Method.lower()

    year, month, day = [int(x) for x in args.Date.split("-")]

    # set up the config parser
    config = ConfigParser.ConfigParser()

    # check if config file has been given as cli argument else
    # check if config file resides in /etc/argo-streaming/ folder else
    # check if config file resides in local folder
    if args.ConfigPath is None:
        if os.path.isfile("/etc/argo-streaming/conf/conf.cfg"):
            config.read("/etc/argo-streaming/conf/conf.cfg")
        else:
            config.read("../conf/conf.cfg")
    else:
        config.read(args.ConfigPath)

    # set up the logger
    logger = ArgoLogger(log_name="batch-ar", config=config)

    # check if configuration for the given tenant exists
    if not config.has_section("TENANTS:"+args.Tenant):
        logger.print_and_log(logging.CRITICAL, "Tenant: "+args.Tenant+" doesn't exist.", 1)

    # call update profiles
    profile_mgr = ArgoProfileManager(args.ConfigPath)
    profile_mgr.profile_update_check(args.Tenant, args.Report)


    # dictionary containing the argument's name and the command assosciated with each name
    hdfs_commands = compose_hdfs_commands(year, month, day, args, config, logger)

    cmd_command = compose_command(config, args, hdfs_commands, logger)

    logger.print_and_log(logging.INFO, "Getting ready to submit job")
    logger.print_and_log(logging.INFO, cmd_toString(cmd_command)+"\n")

    # submit the script's command
    flink_job_submit(config, logger, cmd_command)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Batch A/R Job submit script")
    parser.add_argument(
        "-t", "--Tenant", type=str, help="Name of the tenant", required=True)
    parser.add_argument(
        "-r", "--Report", type=str, help="Report status", required=True)
    parser.add_argument(
        "-d", "--Date", type=str, help="Date to run the job for", required=True)
    parser.add_argument(
        "-m", "--Method", type=str, help="Insert or Upsert data in mongoDB", required=True)
    parser.add_argument(
        "-c", "--ConfigPath", type=str, help="Path for the config file")
    parser.add_argument(
        "-u", "--Sudo", help="Run the submition as superuser",  action="store_true")

    # Pass the arguments to main method
    sys.exit(main(parser.parse_args()))
