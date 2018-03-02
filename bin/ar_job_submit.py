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
from urlparse import urlparse, urlsplit, urlunsplit
from utils.argo_log import ArgoLogger
from utils.common import cmd_toString
from utils.argo_mongo import ArgoMongoClient

def date_rollback(path, year, month, day, config, logger, client):
    """Method that checks if a file(inside the hdfs) that is described by a date exists.
       If it doesn't exist, it will rollback the date up to a number days specified by the conf file and see if the file exists
       for any of the dates.If a file is not found it will exit with the appropriate message.
    Attributes:
              path(str): the hdfs filepath
              year(int): the year of the starting date
              month(int): the month of the starting date
              day(int) : the day of the starting date
              config(ConfigParser): script's configuration
              logger(ArgoLogger): logger
              client(snakebite.Client): Client for hdfs
    Returns:
           (str): the hdfs file path of the latest date
    """
    snakebite_path = urlparse(path).path

    # maximun number of days it should rollback
    days = config.getint("HDFS", "rollback_days")

    # number of day to check
    day_to_check= 0
    
    starting_date = datetime.date(year, month, day)
    
    if client.test(snakebite_path.replace("{{date}}", str(starting_date)), exists=True):
        return path.replace("{{date}}", str(starting_date))
    else:
        logger.print_and_log(logging.WARNING, "- File : "+snakebite_path.replace("{{date}}", str(starting_date))+" was not found. Beggining rollback proccess for: "+str(days)+" days")
        day_to_check = 1
        # while the number of maximum days we can rollback is greater than the number of days we have already rolledback, continue searching
        while days >= day_to_check:
            logger.print_and_log(logging.WARNING, "Trying: "+snakebite_path.replace("{{date}}", str(starting_date-datetime.timedelta(day_to_check))))
            if client.test(snakebite_path.replace("{{date}}", str(starting_date-datetime.timedelta(day_to_check))), exists=True):
               logger.print_and_log(logging.WARNING, "File found after: "+str(day_to_check)+" days.\n")
               snakebite_path = snakebite_path.replace("{{date}}", str(starting_date-datetime.timedelta(day_to_check)))
               # decompose the original path into a list, where scheme, netlock, path, query, fragment are the list indices from 0 to 4 respectively
               path_decomposed = list(urlsplit(path))
               # update the path with the snakebite path containg the date the file was found
               path_decomposed[2] = snakebite_path
               # recompose the path and return it
               return urlunsplit(path_decomposed)

            # if the file was not found, rollback one more day              
            day_to_check += 1

    logger.print_and_log(logging.INFO, "No file found after "+str(day_to_check-1)+" days.", 1)

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

    # file location of weights file (local or hdfs)
    hdfs_commands["--weights"] = date_rollback(hdfs_sync+"/"+args.Report+"/weights_"+"{{date}}"+".avro", year, month, day, config, logger, client)

    # file location of downtimes file (local or hdfs)
    hdfs_commands["--downtimes"] = hdfs_sync+"/"+args.Report+"/downtimes_"+str(datetime.date(year, month, day))+".avro"

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
        argo_mongo_client = ArgoMongoClient(args, config, logger, ["service_ar","endpoint_group_ar"])
        argo_mongo_client.mongo_clean_ar(mongo_uri)
                                            
    
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
    logger = ArgoLogger(log_name="batch-ar", config=config)

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
        pass
    except subprocess.CalledProcessError as esp:
        logger.print_and_log(logging.CRITICAL, "Job was not submited. Error exit code: "+str(esp.returncode), 1)


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
