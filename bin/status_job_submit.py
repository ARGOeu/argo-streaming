#!/usr/bin/env python
import sys
import os
import argparse
import datetime
from snakebite.client import Client
import ConfigParser
import logging
import subprocess
from subprocess import check_call
from utils.argo_log import ArgoLogger
from utils.common import cmd_toString
from utils.argo_mongo import ArgoMongoClient
from utils.common import date_rollback

def compose_hdfs_commands(year, month, day, args, config, logger):

    # set up the hdfs client to be used in order to check the files
    client = Client(config.get("HDFS","hdfs_host"), config.getint("HDFS","hdfs_port"), use_trash=False)
    
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
    hdfs_commands["--pdata"] = hdfs_metric+"/"+str(datetime.date(year, month, day) - datetime.timedelta(1))

    # file location of target day's metric data (local or hdfs)
    hdfs_commands["--mdata"] = hdfs_metric+"/"+args.Date

    # file location of report configuration json file (local or hdfs)
    hdfs_commands["--conf"] = hdfs_sync+"/"+args.Tenant+"_"+args.Report+"_cfg.json"
    
    # file location of metric profile (local or hdfs)
    hdfs_commands["--mps"] = date_rollback(hdfs_sync+"/"+args.Report+"/"+"metric_profile_"+"{{date}}"+".avro", year, month, day, config, logger, client)

    # file location of operations profile (local or hdfs)
    hdfs_commands["--ops"] = hdfs_sync+"/"+args.Tenant+"_ops.json"

    # file location of aggregations profile (local or hdfs)
    hdfs_commands["--apr"] = hdfs_sync+"/"+args.Tenant+"_"+args.Report+"_ap.json"

    #  file location of endpoint group topology file (local or hdfs)
    hdfs_commands["-egp"] = date_rollback(hdfs_sync+"/"+args.Report+"/"+"group_endpoints_"+"{{date}}"+".avro", year, month, day, config, logger, client)

    # file location of group of groups topology file (local or hdfs)
    hdfs_commands["-ggp"] = date_rollback(hdfs_sync+"/"+args.Report+"/"+"group_groups_"+"{{date}}"+".avro", year, month, day, config, logger, client)

    # file location of recomputations file (local or hdfs)
    hdfs_commands["--rec"] = hdfs_sync+"/recomp.json"

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
    cmd_command.append(config.get("CLASSES", "batch-status"))

    # jar to be sumbitted to flink
    cmd_command.append(config.get("JARS", "batch-status"))

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
        argo_mongo_client = ArgoMongoClient(args, config, logger, ["status_metrics","status_endpoints","status_services","status_endpoint_groups"])
        argo_mongo_client.mongo_clean_status(mongo_uri)
                                            
    
    # MongoDB method to be used when storing the results, either insert or upsert
    cmd_command.append("--mongo.method")
    cmd_command.append(args.Method)
    
    # add the hdfs commands
    for command in hdfs_commands:
        cmd_command.append(command)
        cmd_command.append(hdfs_commands[command])

    return cmd_command


def main(args=None):

    # make sure the argument are in the correct form
    args.Report = args.Report.capitalize()
    args.Tenant = args.Tenant.upper()
    args.Method = args.Method.lower()

    year, month, day =[int(x) for x in  args.Date.split("-")]

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
    logger = ArgoLogger(log_name="batch-status", config=config)

    # check if configuration for the given tenant exists
    if not config.has_section("TENANTS:"+args.Tenant):
        logger.print_and_log(logging.CRITICAL, "Tenant: "+args.Tenant+" doesn't exist.", 1)

    # dictionary containing the argument's name and the command assosciated with each name
    hdfs_commands = compose_hdfs_commands(year, month, day, args, config, logger)
    
    cmd_command = compose_command(config, args, hdfs_commands, logger)

    logger.print_and_log(logging.INFO, "Getting ready to submit job")
    logger.print_and_log(logging.INFO, cmd_toString(cmd_command)+"\n")

    # submit the script's command
    try:
        check_call(cmd_command)
    except subprocess.CalledProcessError as esp:
        logger.print_and_log(logging.CRITICAL, "Job was not submited. Error exit code: "+str(esp.returncode), 1)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Batch Status Job submit script")
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
