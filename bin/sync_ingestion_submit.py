#! /usr/bin/env python

import sys
import argparse
import logging
from utils.common import cmd_to_string, flink_job_submit, get_log_conf, get_config_paths
from utils.argo_config import ArgoConfig

log = logging.getLogger(__name__)


def compose_command(config, args):
    """Composes a command line execution string for submitting a flink job. 
    
    Args:
        config (obj.): argo configuration object
        args (dict): command line arguments of this script
    
    Returns:
        list: A list of all command line arguments for performing the flink job submission
    """

    # job submission command
    cmd_command = []

    if args.sudo is True:
        cmd_command.append("sudo")

    # tenant the job will run for
    section_tenant = "TENANTS:" + args.tenant
    section_tenant_job = section_tenant + ":ingest-sync"
    config.get(section_tenant, "ams_project")

    # get needed config params
    job_namespace = config.get("JOB-NAMESPACE", "ingest-sync-namespace")
    ams_endpoint = config.get("AMS", "endpoint")

    ams_project = config.get(section_tenant, "ams_project")
    ams_sub = config.get(section_tenant_job, "ams_sub")

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
    cmd_command.append(ams_endpoint.hostname)

    # ams port
    ams_port = 443
    if ams_endpoint.port is not None:
	ams_port = ams_endpoint.port
    cmd_command.append("--ams.port")
    cmd_command.append(str(ams_port))

    # tenant token
    cmd_command.append("--ams.token")
    cmd_command.append(config.get(section_tenant, "ams_token"))

    # project/tenant
    cmd_command.append("--ams.project")
    cmd_command.append(ams_project)

    # ams subscription
    cmd_command.append("--ams.sub")
    cmd_command.append(ams_sub)

    # fill job_namespace template
    job_namespace = job_namespace.fill(ams_endpoint=ams_endpoint.hostname, ams_port=ams_port,
                                       ams_project=ams_project, ams_sub=ams_sub)

    # set up the hdfs client to be used in order to check the files
    namenode = config.get("HDFS", "namenode")
    hdfs_user = config.get("HDFS", "user")

    hdfs_sync = config.get("HDFS", "path_sync")
    hdfs_sync.fill(namenode=namenode.geturl(), hdfs_user=hdfs_user, tenant=args.tenant)

    hdfs_sync = hdfs_sync.fill(namenode=namenode.geturl(), hdfs_user=hdfs_user, tenant=args.tenant).geturl()

    # append hdfs sync base path to the submit command
    cmd_command.append("--hdfs.path")
    cmd_command.append(hdfs_sync)

    # num of messages to be retrieved from AMS per request
    cmd_command.append("--ams.batch")
    cmd_command.append(str(config.get(section_tenant_job, "ams_batch")))

    # interval in ms betweeb AMS service requests
    cmd_command.append("--ams.interval")
    cmd_command.append(str(config.get(section_tenant_job, "ams_interval")))

    # get optional ams proxy
    proxy = config.get("AMS", "proxy")
    if proxy is not None:
        cmd_command.append("--ams.proxy")
        cmd_command.append(proxy.geturl())

    # ssl verify
    cmd_command.append("--ams.verify")
    ams_verify = config.get("AMS", "verify")
    if ams_verify is not None:
        cmd_command.append(str(ams_verify).lower())
    else:
        # by default assume ams verify is always true
        cmd_command.append("true")

    return cmd_command, job_namespace


def main(args=None):
    # Get configuration paths
    conf_paths = get_config_paths(args.config)

    # Get logger config file
    get_log_conf(conf_paths['log'])

    # Get main configuration and schema
    config = ArgoConfig(conf_paths["main"], conf_paths["schema"])

    # check if configuration for the given tenant exists
    if not config.has("TENANTS:" + args.tenant):
        log.info("Tenant: " + args.tenant + " doesn't exist.")
        sys.exit(1)

    cmd_command, job_namespace = compose_command(config, args)

    # submit the job
   
    flink_job_submit(config, cmd_command, job_namespace, args.dry_run)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="AMS Sync Ingestion submission script")
    parser.add_argument(
        "-t", "--tenant", metavar="STRING", help="Name of the tenant", required=True)
    parser.add_argument(
        "-c", "--config", metavar="PATH", help="Path for the config file")
    parser.add_argument(
        "-u", "--sudo", help="Run the submission as superuser",  action="store_true")
    parser.add_argument("--dry-run",help="Runs in test mode without actually submitting the job",
        action="store_true", dest="dry_run")
    sys.exit(main(parser.parse_args()))
