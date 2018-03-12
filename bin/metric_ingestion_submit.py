#!/usr/bin/env python

import sys
import os
import argparse
import ConfigParser
import logging
from utils.argo_log import ArgoLogger
from utils.common import cmd_toString, flink_job_submit

def compose_command(config, args, sudo, logger=None):

    # job_namespace
    job_namespace = config.get("JOB-NAMESPACE", "ingest-metric-namespace")

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
        tenant_job = tenant+":ingest-metric"
        config.get(tenant, "ams_project")
        logger.print_and_log(logging.INFO, "Starting building the submit command for tenant: " + args.Tenant.upper())
    except ConfigParser.NoSectionError as e:
        logger.print_and_log(logging.CRITICAL, str(e), 1)

    # flink executable
    cmd_command.append(config.get("FLINK", "path"))

    cmd_command.append("run")

    cmd_command.append("-c")

    # Job's class inside the jar
    cmd_command.append(config.get("CLASSES", "ams-ingest-metric"))

    # jar to be sumbitted to flink
    cmd_command.append(config.get("JARS", "ams-ingest-metric"))

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
    hdfs_metric = config.get("HDFS", "hdfs_metric")
    hdfs_metric = hdfs_metric.replace("{{hdfs_host}}", config.get("HDFS", "hdfs_host"))
    hdfs_metric = hdfs_metric.replace("{{hdfs_port}}", config.get("HDFS", "hdfs_port"))
    hdfs_metric = hdfs_metric.replace("{{hdfs_user}}", config.get("HDFS", "hdfs_user"))
    hdfs_metric = hdfs_metric.replace("{{tenant}}", args.Tenant.upper())
    cmd_command.append("--hdfs.path")
    cmd_command.append(hdfs_metric)

    # path to store flink checkpoints
    cmd_command.append("--check.path")
    cmd_command.append(config.get(tenant_job, "checkpoint_path"))

    # interval for checkpont in ms
    cmd_command.append("--check.interval")
    cmd_command.append(config.get(tenant_job, "checkpoint_interval"))

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
    logger = ArgoLogger(log_name="ingest-metric", config=config)

    cmd_command, job_namespace = compose_command(config, args, args.Sudo, logger)

    logger.print_and_log(logging.INFO, "Getting ready to submit job")
    logger.print_and_log(logging.INFO, cmd_toString(cmd_command)+"\n")

    # submit script's command
    flink_job_submit(config, logger, cmd_command, job_namespace)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="AMS Metric Ingestion submission script")
    parser.add_argument(
        "-t", "--Tenant", type=str, help="Name of the tenant", required=True)
    parser.add_argument(
        "-c", "--ConfigPath", type=str, help="Path for the config file")
    parser.add_argument(
        "-u", "--Sudo", help="Run the submition as superuser",  action="store_true")
    sys.exit(main(parser.parse_args()))
