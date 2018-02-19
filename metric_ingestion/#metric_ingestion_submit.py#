#!/usr/bin/env python

import sys
import subprocess
import argparse
import requests
import json
import ConfigParser
from subprocess import check_call
import logging
import logging.handlers


def cmd_toString(cmd):
    """ Take as input a list containing the job sumbission command
        and return a string representation
    Attributes:
              cmd(list): list containing the submition command
    Returns:
           (str): String representation of the submition command
    """

    return " ".join(x for x in cmd)


def main(args=None):

    # set up the config parser
    config = ConfigParser.ConfigParser()
    config.read("../conf/conf.cfg")

    # set up the logger
    log = logging.getLogger(__name__)
    log.setLevel(logging.INFO)
    sys_log = logging.handlers.SysLogHandler(config.get("LOGS", "handler_path"))
    sys_format = logging.Formatter('%(name)s[%(process)d]: %(levelname)s %(message)s')
    sys_log.setFormatter(sys_format)

    log.addHandler(sys_log)

    # job_namespace
    job_namespace = config.get("JOB-NAMESPACE", "ingest-metric-namespace")

    # job sumbission command
    cmd_command = ["sudo"]

    # check if configuration for the given tenant exists
    try:
        # tenant the job will run for
        tenant = "TENANTS:"+args.Tenant.upper()
        tenant_job = tenant+":ingest-metric"
        config.get(tenant, "ams_project")
        log.info("Starting building the submit command for tenant: " + args.Tenant.upper())
    except ConfigParser.NoSectionError as e:
        log.critical(str(e))
        sys.exit(str(e))

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

    print("Getting ready to submit job...\n")
    log.info("Getting ready to submit job")
    log.info(cmd_toString(cmd_command))
    print(cmd_toString(cmd_command))

    # if the job's already running then exit, else sumbit the command
    # check if flink is up and running
    try:
        flink_response = requests.get("http://localhost:8081/joboverview/running")
        for job in json.loads(flink_response.text)["jobs"]:
            if job["name"] == job_namespace:
                log.critical("\nJob: "+"'"+job_namespace+"' is already running")
                sys.exit("\nJob: "+"'"+job_namespace+"' is already running")
            else:
                log.info("Everything is ok")
                try:
                    check_call(cmd_command)
                except subprocess.CalledProcessError as esp:
                    log.critical("Job was not submited. Error exit code: "+str(esp.returncode))
                    sys.exit("Job was not submited. Error exit code: "+str(esp.returncode))
    except requests.exceptions.ConnectionError:
        log.critical("Flink is not currently running")
        sys.exit("Flink is not currently running")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="AMS Metric Ingestion submition script")
    parser.add_argument(
        "-t", "--Tenant", type=str, help="Name of the tenant", required=True)
    sys.exit(main(parser.parse_args()))
