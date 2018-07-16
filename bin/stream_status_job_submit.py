#!/usr/bin/env python
import sys
import argparse
import datetime
from snakebite.client import Client
import logging
from utils.argo_config import ArgoConfig
from utils.common import cmd_to_string, date_rollback, flink_job_submit, hdfs_check_path, get_config_paths, get_log_conf

log = logging.getLogger(__name__)


def compose_hdfs_commands(year, month, day, args, config):
    # set up the hdfs client to be used in order to check the files
    namenode = config.get("HDFS", "namenode")
    client = Client(namenode.hostname, namenode.port, use_trash=False)

    # hdfs sync  path for the tenant

    hdfs_user = config.get("HDFS", "user")
    tenant = args.tenant
    hdfs_sync = config.get("HDFS", "path_sync")
    hdfs_sync = hdfs_sync.fill(namenode=namenode.geturl(), user=hdfs_user, tenant=tenant).geturl()

    # dictionary holding all the commands with their respective arguments' name
    hdfs_commands = dict()

    # file location of metric profile (local or hdfs)
    hdfs_commands["--sync.mps"] = date_rollback(
        hdfs_sync + "/" + args.report + "/" + "metric_profile_" + "{{date}}" + ".avro", year, month, day, config,
        client)

    # file location of operations profile (local or hdfs)
    hdfs_commands["--sync.ops"] = hdfs_check_path(hdfs_sync+"/"+args.tenant+"_ops.json", client)

    # file location of aggregations profile (local or hdfs)
    hdfs_commands["--sync.apr"] = hdfs_check_path(hdfs_sync+"/"+args.tenant+"_"+args.report+"_ap.json", client)

    #  file location of endpoint group topology file (local or hdfs)
    hdfs_commands["-sync.egp"] = date_rollback(
        hdfs_sync + "/" + args.report + "/" + "group_endpoints_" + "{{date}}" + ".avro", year, month, day, config,
        client)

    return hdfs_commands


def compose_command(config, args,  hdfs_commands):

    # job submission command
    cmd_command = []

    if args.sudo is True:
        cmd_command.append("sudo")

    tenant = args.tenant

    # get needed config params
    section_tenant = "TENANTS:" + args.tenant
    section_tenant_job = "TENANTS:" + args.tenant + ":stream-status"
    job_namespace = config.get("JOB-NAMESPACE", "stream-status-namespace")
    ams_endpoint = config.get("AMS", "endpoint")


    ams_project = config.get(section_tenant, "ams_project")
    ams_sub_metric = config.get(section_tenant_job, "ams_sub_metric")
    ams_sub_sync = config.get(section_tenant_job, "ams_sub_sync")


    # flink executable
    cmd_command.append(config.get("FLINK", "path"))

    cmd_command.append("run")

    cmd_command.append("-c")

    # Job's class inside the jar
    cmd_command.append(config.get("CLASSES", "stream-status"))

    # jar to be submitted to flink
    cmd_command.append(config.get("JARS", "stream-status"))

    # ams endpoint
    cmd_command.append("--ams.endpoint")
    cmd_command.append(ams_endpoint.hostname)

    # ams port
    cmd_command.append("--ams.port")
    cmd_command.append(ams_endpoint.port)

    # tenant's token for ams
    cmd_command.append("--ams.token")
    cmd_command.append(config.get(section_tenant, "ams_token"))

    # ams project
    cmd_command.append("--ams.project")
    cmd_command.append(ams_project)

    # ams sub metric
    cmd_command.append("--ams.sub.metric")
    cmd_command.append(ams_sub_metric)

    # ams sub sync
    cmd_command.append("--ams.sub.sync")
    cmd_command.append(ams_sub_sync)

    # fill job namespace template with the required arguments
    job_namespace.fill(ams_endpoint=ams_endpoint.hostname, ams_port=ams_endpoint.port, ams_project=ams_project,
                       ams_sub_metric=ams_sub_metric, ams_sub_sync=ams_sub_sync)

    # add the hdfs commands
    for command in hdfs_commands:
        cmd_command.append(command)
        cmd_command.append(hdfs_commands[command])

    # date
    cmd_command.append("--run.date")
    cmd_command.append(args.date)

    # flink parallelism
    cmd_command.append("--p")
    cmd_command.append(config.get(section_tenant_job, "flink_parallelism"))

    # grab tenant configuration section for stream-status

    outputs = config.get(section_tenant_job, "outputs")
    if len(outputs) == 0:
        log.fatal("No output formats found.")
        sys.exit(1)
    else:
        for output in outputs:
            if output == "hbase":
                # hbase endpoint
                if config.has(section_tenant_job, "hbase_master"):
                    cmd_command.append("--hbase.master")
                    cmd_command.append(config.get(section_tenant_job, "hbase_master").hostname)
                # hbase endpoint port
                if config.has(section_tenant_job, "hbase_master"):
                    cmd_command.append("--hbase.port")
                    cmd_command.append(config.get(section_tenant_job, "hbase_master").port)

                # comma separate list of zookeeper servers
                if config.has(section_tenant_job, "hbase_zk_quorum"):
                    cmd_command.append("--hbase.zk.quorum")
                    cmd_command.append(config.get(section_tenant_job, "hbase_zk_quorum"))

                # port used by zookeeper servers
                if config.has(section_tenant_job, "hbase_zk_port"):
                    cmd_command.append("--hbase.zk.port")
                    cmd_command.append(config.get(section_tenant_job, "hbase_zk_port"))

                # table namespace, usually tenant
                if config.has(section_tenant_job, "hbase_namespace"):
                    cmd_command.append("--hbase.namespace")
                    cmd_command.append(config.get(section_tenant_job, "hbase_namespace"))

                # table name, usually metric data
                if config.has(section_tenant_job, "hbase_table"):
                    cmd_command.append("--hbase.table")
                    cmd_command.append(config.get(section_tenant_job, "hbase_table"))

            elif output == "kafka":
                # kafka list of servers
                if config.has(section_tenant_job, "kafka_servers"):
                    cmd_command.append("--kafka.servers")
                    kafka_servers = ','.join(config.get(section_tenant_job,"kafka_servers"))
                    cmd_command.append(kafka_servers)
                # kafka topic to send status events to
                if config.has(section_tenant_job, "kafka_topic"):
                    cmd_command.append("--kafka.topic")
                    cmd_command.append(config.get(section_tenant_job, "kafka_topic"))
            elif output == "fs":
                # filesystem path for output(use "hdfs://" for hdfs path)
                if config.has(section_tenant_job, "fs_output"):
                    cmd_command.append("--fs.output")
                    cmd_command.append(config.get(section_tenant_job, "fs_output"))
            elif output == "mongo":
                cmd_command.append("--mongo.uri")
                mongo_uri = config.get(section_tenant, "mongo_uri")
                cmd_command.append(mongo_uri.geturl())
                # mongo method
                cmd_command.append("--mongo.method")
                cmd_command.append(config.get(section_tenant_job, "mongo_method"))

    # num of messages to be retrieved from AMS per request
    cmd_command.append("--ams.batch")
    cmd_command.append(config.get(section_tenant_job, "ams_batch"))

    # interval in ms betweeb AMS service requests
    cmd_command.append("--ams.interval")
    cmd_command.append(config.get(section_tenant_job, "ams_interval"))

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

    if args.timeout is not None:
        cmd_command.append("--timeout")
        cmd_command.append(args.timeout)

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

    year, month, day = [int(x) for x in args.date.split("T")[0].split("-")]

    # dictionary containing the argument's name and the command assosciated with each name
    hdfs_commands = compose_hdfs_commands(year, month, day, args, config)

    cmd_command, job_namespace = compose_command(config, args, hdfs_commands)

    log.info("Getting ready to submit job")
    log.info(cmd_to_string(cmd_command)+"\n")

    # submit the script's command
    flink_job_submit(config, cmd_command, job_namespace)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Stream Status Job submit script")
    parser.add_argument(
        "-t", "--tenant", metavar="STRING", help="Name of the tenant", required=True, dest="tenant")
    parser.add_argument(
        "-d", "--date", metavar="DATE(ISO-8601)",
        default=str(datetime.datetime.utcnow().replace(microsecond=0).isoformat()) + "Z",
        help="Date in ISO-8601 format", dest="date")
    parser.add_argument(
        "-r", "--report", metavar="STRING", help="Report status", required=True, dest="report")
    parser.add_argument(
        "-c", "--config", metavar="PATH", help="Path for the config file", dest="config")
    parser.add_argument(
        "-u", "--sudo", help="Run the submition as superuser",  action="store_true", dest="sudo")
    parser.add_argument(
        "-timeout", "--timeout", metavar="INT",
        help="Controls default timeout for event regeneration (used in notifications)", dest="timeout")

    # Pass the arguments to main method
    sys.exit(main(parser.parse_args()))
