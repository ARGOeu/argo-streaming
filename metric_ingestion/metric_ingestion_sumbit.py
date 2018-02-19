#!/usr/bin/env python

import sys
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
              cmd(list): list containing the sumbition command
    Returns:
           (str): String representation of the sumbition command
    """

    return " ".join(x for x in cmd)


def main(args=None):
    # set up the logger
    # log = logging.getLogger(__name__)
    # log.setLevel(logging.INFO)
    # sys_log = logging.handlers.SysLogHandler("/dev/log")
    # sys_format = logging.Formatter('%(name)s[%(process)d]: %(levelname)s %(message)s')
    # sys_log.setFormatter(sys_format)

    # log.addHandler(sys_log)

    # job_namespace
    job_namespace = "Ingesting  metric data from "

    # job sumbission command
    cmd_command = ["sudo"]

    # tenant the job will run for
    tenant = args.Tenant

    config = ConfigParser.ConfigParser()
    config.read("conf.cfg")

    # flink executable
    cmd_command.append(config.get("FLINK", "path"))

    cmd_command.append("run")

    cmd_command.append("-c")

    # job's class inside the jar
    cmd_command.append(config.get("CLASSES", "ams-ingest-metric"))

    # jar to be sumbitted to flink
    cmd_command.append(config.get("JARS", "ams-ingest-metric"))

    # ams endpoint
    cmd_command.append("--ams.endpoint")
    cmd_command.append(config.get("AMS", "endpoint"))
    job_namespace += config.get("AMS", "endpoint")
    job_namespace += ":"

    # ams port
    cmd_command.append("--ams.port")
    cmd_command.append(config.get("AMS", "port"))
    job_namespace += config.get("AMS", "port")
    job_namespace += "/v1/projects/"

    # project/tenant
    cmd_command.append("--ams.project")
    cmd_command.append(tenant)
    job_namespace += tenant
    job_namespace += "/subscriptions/"

    # ams subscription
    cmd_command.append("--ams.sub")
    cmd_command.append(config.get("AMS", "sub"))
    job_namespace += config.get("AMS", "sub")

    # hdfs path for the tenant
    cmd_command.append("--hdfs.path")
    cmd_command.append(config.get("HDFS", "path").replace("{{tenant}}", tenant))

    # path to store flink checkpoints
    cmd_command.append("--check.path")
    cmd_command.append(config.get("CHECKPOINTS", "path"))

    # interval for checkpont in ms
    cmd_command.append("--check.interval")
    cmd_command.append(config.get("CHECKPOINTS", "interval"))

    # num of messages to be retrieved from AMS per request
    cmd_command.append("--ams.batch")
    cmd_command.append(config.get("AMS", "batch"))

    # interval in ms betweeb AMS service requests
    cmd_command.append("--ams.interval")
    cmd_command.append(config.get("AMS", "interval"))

    print("Getting ready to sumbit job...\n")
    print(cmd_toString(cmd_command))

    # if the job's already running then exit, else sumbit the command
    # check if flink is up and running
    try:
        flink_response = requests.get("http://localhost:8081/joboverview/running")
        for job in json.loads(flink_response.text)["jobs"]:
            if job["name"] == job_namespace:
                sys.exit("\nJob: "+"'"+job_namespace+"' is already running")
            else:
                check_call(cmd_command)
    except requests.exceptions.ConnectionError:
        sys.exit("Flink is not currently running")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="AMS Metric Ingestion sumbit script")
    parser.add_argument(
        "-t", "--Tenant", type=str, help="Name of the tenant", required=True)
    sys.exit(main(parser.parse_args()))
