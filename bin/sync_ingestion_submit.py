#! /usr/bin/env python

import sys
import os
import subprocess
import argparse
import requests
import json
import ConfigParser
from subprocess import check_call
import logging
from utils.argo_log import ArgoLogger
from utils.common import cmd_toString

def compose_command(config, args, sudo, logger=None):

    # job_namespace
    job_namespace = config.get("JOB-NAMESPACE", "ingest-sync-namespace")

    # job sumbission command
    cmd_command = []

    if sudo is True:
        cmd_command.append("sudo")

    # create a simple stream_handler whenever tetsing
    if logger is None:
        logger = ArgoLogger()

    # check if configuration for the given tenant exists
    try:
        # tenant the job will run for
        tenant = "TENANTS:"+args.Tenant.upper()
        tenant_job = tenant+":ingest-sync"
        config.get(tenant, "ams_project")
        logger.print_and_log(logging.INFO, "Starting building the submit command for tenant: " + args.Tenant.upper())
    except ConfigParser.NoSectionError as e:
        logger.print_and_log(logging.CRITICAL, str(e), 1)

    # flink executable
    cmd_command.append(config.get("FLINK", "path"))

    cmd_command.append("run")

    cmd_command.append("-c")

    # Job's class inside the jar
    cmd_command.append(config.get("CLASSES", "ams-ingest-sync"))

    # jar to be sumbitted to flink
    cmd_command.append(config.get("JARS", "ams-ingest-sync"))

    # ams endpoint
    cmd_command.append("--ams.endpoint")
    cmd_command.append(config.get("AMS", "ams_endpoint"))
    job_namespace = job_namespace.replace("{{ams_endpoint}}", config.get("AMS", "ams_endpoint"))

    # ams port
    cmd_command.append("--ams.port")
    cmd_command.append(config.get("AMS", "ams_port"))
    job_namespace = job_namespace.replace("{{ams_port}}", config.get("AMS", "ams_port"))

    # tenant token
    cmd_command.append("--ams.token")
    cmd_command.append(config.get(tenant, "ams_token"))

    # project/tenant
    cmd_command.append("--ams.project")
    cmd_command.append(config.get(tenant, "ams_project"))
    job_namespace = job_namespace.replace("{{project}}",  config.get(tenant, "ams_project"))

    # ams subscription
    cmd_command.append("--ams.sub")
    cmd_command.append(config.get(tenant_job, "ams_sub"))
    job_namespace = job_namespace.replace("{{ams_sub}}", config.get(tenant_job, "ams_sub"))

    # hdfs path for the tenant
    hdfs_metric = config.get("HDFS", "hdfs_sync")
    hdfs_metric = hdfs_metric.replace("{{hdfs_host}}", config.get("HDFS", "hdfs_host"))
    hdfs_metric = hdfs_metric.replace("{{hdfs_port}}", config.get("HDFS", "hdfs_port"))
    hdfs_metric = hdfs_metric.replace("{{hdfs_user}}", config.get("HDFS", "hdfs_user"))
    hdfs_metric = hdfs_metric.replace("{{tenant}}", args.Tenant.upper())
    cmd_command.append("--hdfs.path")
    cmd_command.append(hdfs_metric)

    # num of messages to be retrieved from AMS per request
    cmd_command.append("--ams.batch")
    cmd_command.append(config.get(tenant_job, "ams_batch"))

    # interval in ms betweeb AMS service requests
    cmd_command.append("--ams.interval")
    cmd_command.append(config.get(tenant_job, "ams_interval"))

    return cmd_command, job_namespace


def main(args=None):

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
    logger = ArgoLogger(log_name="ingest-sync", config=config)

    cmd_command, job_namespace = compose_command(config, args, args.Sudo, logger)

    logger.print_and_log(logging.INFO, "Getting ready to submit job")
    logger.print_and_log(logging.INFO, cmd_toString(cmd_command)+"\n")

    # check if flink is up and running
    try:
        flink_response = requests.get(config.get("FLINK", "job_manager")+"/joboverview/running")
        # if the job's already running then exit, else sumbit the command
        for job in json.loads(flink_response.text)["jobs"]:
            if job["name"] == job_namespace:
                logger.print_and_log(logging.CRITICAL, "\nJob: "+"'"+job_namespace+"' is already running", 1)

        logger.print_and_log(logging.INFO, "Everything is ok")
        try:
            check_call(cmd_command)
        except subprocess.CalledProcessError as esp:
            logger.print_and_log(logging.CRITICAL, "Job was not submitted. Error exit code: "+str(esp.returncode), 1)
    except requests.exceptions.ConnectionError:
        logger.print_and_log(logging.CRITICAL, "Flink is not currently running. Tried to communicate with job manager at: " + config.get("FLINK", "job_manager"), 1)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="AMS Sync Ingestion submission script")
    parser.add_argument(
        "-t", "--Tenant", type=str, help="Name of the tenant", required=True)
    parser.add_argument(
        "-c", "--ConfigPath", type=str, help="Path for the config file")
    parser.add_argument(
        "-u", "--Sudo", help="Run the submition as superuser",  action="store_true")
    sys.exit(main(parser.parse_args()))
