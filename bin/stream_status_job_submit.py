#!/usr/bin/env python
import sys
import os
import argparse
import datetime
from snakebite.client import Client
import ConfigParser
import logging
from utils.argo_log import ArgoLogger
from utils.common import cmd_toString, date_rollback, flink_job_submit, hdfs_check_path


def compose_hdfs_commands(year, month, day, args, config, logger):

    # set up the hdfs client to be used in order to check the files
    client = Client(config.get("HDFS", "hdfs_host"), config.getint("HDFS", "hdfs_port"), use_trash=False)

    # hdfs sync  path for the tenant
    hdfs_sync = config.get("HDFS", "hdfs_sync")
    hdfs_sync = hdfs_sync.replace("{{hdfs_host}}", config.get("HDFS", "hdfs_host"))
    hdfs_sync = hdfs_sync.replace("{{hdfs_port}}", config.get("HDFS", "hdfs_port"))
    hdfs_sync = hdfs_sync.replace("{{hdfs_user}}", config.get("HDFS", "hdfs_user"))
    hdfs_sync = hdfs_sync.replace("{{tenant}}", args.Tenant)

    # dictionary holding all the commands with their respective arguments' name
    hdfs_commands = {}

    # file location of metric profile (local or hdfs)
    hdfs_commands["--sync.mps"] = date_rollback(hdfs_sync+"/"+args.Report+"/"+"metric_profile_"+"{{date}}"+".avro", year, month, day, config, logger, client)

    # file location of operations profile (local or hdfs)
    hdfs_commands["--sync.ops"] = hdfs_check_path(hdfs_sync+"/"+args.Tenant+"_ops.json", logger, client)

    # file location of aggregations profile (local or hdfs)
    hdfs_commands["--sync.apr"] = hdfs_check_path(hdfs_sync+"/"+args.Tenant+"_"+args.Report+"_ap.json", logger, client)

    #  file location of endpoint group topology file (local or hdfs)
    hdfs_commands["-sync.egp"] = date_rollback(hdfs_sync+"/"+args.Report+"/"+"group_endpoints_"+"{{date}}"+".avro", year, month, day, config, logger, client)

    return hdfs_commands


def compose_command(config, args,  hdfs_commands, logger=None):

    # job sumbission command
    cmd_command = []

    # job namespace on the flink manager
    job_namespace = config.get("JOB-NAMESPACE", "stream-status-namespace")

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
    cmd_command.append(config.get("CLASSES", "stream-status"))

    # jar to be sumbitted to flink
    cmd_command.append(config.get("JARS", "stream-status"))

    # ams endpoint
    cmd_command.append("--ams.endpoint")
    cmd_command.append(config.get("AMS", "ams_endpoint"))
    job_namespace = job_namespace.replace("{{ams_endpoint}}", config.get("AMS", "ams_endpoint"))

    # ams port
    cmd_command.append("--ams.port")
    cmd_command.append(config.get("AMS", "ams_port"))
    job_namespace = job_namespace.replace("{{ams_port}}", config.get("AMS", "ams_port"))

    # tenant's token for ams
    cmd_command.append("--ams.token")
    cmd_command.append(config.get("TENANTS:"+args.Tenant, "ams_token"))

    # ams project
    cmd_command.append("--ams.project")
    cmd_command.append(config.get("TENANTS:"+args.Tenant, "ams_project"))
    job_namespace = job_namespace.replace("{{project}}", config.get("TENANTS:"+args.Tenant, "ams_project"))

    # ams sub metric
    cmd_command.append("--ams.sub.metric")
    cmd_command.append(config.get("TENANTS:"+args.Tenant+":stream-status", "ams.sub.metric"))
    job_namespace = job_namespace.replace("{{ams_sub_metric}}", config.get("TENANTS:"+args.Tenant+":stream-status", "ams.sub.metric"))

    # ams sub sync
    cmd_command.append("--ams.sub.sync")
    cmd_command.append(config.get("TENANTS:"+args.Tenant+":stream-status", "ams.sub.sync"))
    job_namespace = job_namespace.replace("{{ams_sub_sync}}", config.get("TENANTS:"+args.Tenant+":stream-status", "ams.sub.sync"))

    # add the hdfs commands
    for command in hdfs_commands:
        cmd_command.append(command)
        cmd_command.append(hdfs_commands[command])

    # date
    cmd_command.append("--run.date")
    cmd_command.append(args.Date)

    # flink parallelism
    cmd_command.append("--p")
    cmd_command.append(config.get("TENANTS:"+args.Tenant+":stream-status", "flink_parallelism"))

    outputs = config.get("TENANTS:"+args.Tenant+":stream-status", "outputs").split(",")
    if len(outputs) == 0:
        logger.print_and_log(logging.INFO, "No output formats found.")
    else:
        for output in outputs:
            if output == "hbase":
                # hbase endpoint
                if logger.config_str_validator(config, "TENANTS:"+args.Tenant+":stream-status", "hbase.master"):
                    cmd_command.append("--hbase.master")
                    cmd_command.append(config.get("TENANTS:"+args.Tenant+":stream-status", "hbase.master"))

                # hbase endpoint port
                if logger.config_str_validator(config, "TENANTS:"+args.Tenant+":stream-status", "hbase.master.port"):
                    cmd_command.append("--hbase.port")
                    cmd_command.append(config.get("TENANTS:"+args.Tenant+":stream-status", "hbase.master.port"))

                # comma separate list of zookeeper servers
                if logger.config_str_validator(config, "TENANTS:"+args.Tenant+":stream-status", "hbase.zk.quorum"):
                    cmd_command.append("--hbase.zk.quorum")
                    cmd_command.append(config.get("TENANTS:"+args.Tenant+":stream-status", "hbase.zk.quorum"))

                # port used by zookeeper servers
                if logger.config_str_validator(config, "TENANTS:"+args.Tenant+":stream-status", "hbase.zk.port"):
                    cmd_command.append("--hbase.zk.port")
                    cmd_command.append(config.get("TENANTS:"+args.Tenant+":stream-status", "hbase.zk.port"))

                # table namespace, usually tenant
                if logger.config_str_validator(config, "TENANTS:"+args.Tenant+":stream-status", "hbase.namespace"):
                    cmd_command.append("--hbase.namespace")
                    cmd_command.append(config.get("TENANTS:"+args.Tenant+":stream-status", "hbase.namespace"))

                # table name, usually metric data
                if logger.config_str_validator(config, "TENANTS:"+args.Tenant+":stream-status", "hbase.table"):
                    cmd_command.append("--hbase.table")
                    cmd_command.append(config.get("TENANTS:"+args.Tenant+":stream-status", "hbase.table"))

            elif output == "kafka":
                # kafka list of servers
                if logger.config_str_validator(config, "TENANTS:"+args.Tenant+":stream-status", "kafka.servers"):
                    cmd_command.append("--kafka.servers")
                    cmd_command.append(config.get("TENANTS:"+args.Tenant+":stream-status", "kafka.servers"))
                # kafka topic to send status events to
                if logger.config_str_validator(config, "TENANTS:"+args.Tenant+":stream-status", "kafka.topic"):
                    cmd_command.append("--kafka.topic")
                    cmd_command.append(config.get("TENANTS:"+args.Tenant+":stream-status", "kafka.topic"))
            elif output == "fs":
                # filesystem path for output(use "hdfs://" for hdfs path)
                if logger.config_str_validator(config, "TENANTS:"+args.Tenant+":stream-status", "fs.output"):
                    cmd_command.append("--fs.output")
                    cmd_command.append(config.get("TENANTS:"+args.Tenant+":stream-status", "fs.output"))

    if config.getboolean("TENANTS:"+args.Tenant+":stream-status", "use_mongo"):
        #  MongoDB uri for outputting the results to (e.g. mongodb://localhost:21017/example_db)
        cmd_command.append("--mongo.uri")
        mongo_tenant = "TENANTS:"+args.Tenant+":MONGO"
        mongo_uri = config.get(mongo_tenant, "mongo_uri")
        mongo_uri = mongo_uri.replace("{{mongo_host}}", config.get(mongo_tenant, "mongo_host"))
        mongo_uri = mongo_uri.replace("{{mongo_port}}", config.get(mongo_tenant, "mongo_port"))
        cmd_command.append(mongo_uri)

        # mongo method
        cmd_command.append("--mongo.method")
        cmd_command.append(config.get("TENANTS:"+args.Tenant+":stream-status", "mongo_method"))

    # num of messages to be retrieved from AMS per request
    cmd_command.append("--ams.batch")
    cmd_command.append(config.get("TENANTS:"+args.Tenant+":stream-status", "ams_batch"))

    # interval in ms betweeb AMS service requests
    cmd_command.append("--ams.interval")
    cmd_command.append(config.get("TENANTS:"+args.Tenant+":stream-status", "ams_interval"))

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

    if args.Timeout is not None:
        cmd_command.append("--timeout")
        cmd_command.append(args.Timeout)

    return cmd_command, job_namespace


def main(args=None):

    # make sure the argument are in the correct form
    args.Tenant = args.Tenant.upper()
    args.Report = args.Report.capitalize()

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

    year, month, day = [int(x) for x in args.Date.split("T")[0].split("-")]

    # dictionary containing the argument's name and the command assosciated with each name
    hdfs_commands = compose_hdfs_commands(year, month, day, args, config, logger)

    cmd_command, job_namespace = compose_command(config, args, hdfs_commands, logger)

    logger.print_and_log(logging.INFO, "Getting ready to submit job")
    logger.print_and_log(logging.INFO, cmd_toString(cmd_command)+"\n")

    # submit the script's command
    flink_job_submit(config, logger, cmd_command, job_namespace)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Stream Status Job submit script")
    parser.add_argument(
        "-t", "--Tenant", type=str, help="Name of the tenant", required=True)
    parser.add_argument(
        "-d", "--Date", type=str, default=str(datetime.datetime.utcnow().replace(microsecond=0).isoformat())+"Z", help="Date in ISO-8601 format")
    parser.add_argument(
        "-r", "--Report", type=str, help="Report status", required=True)
    parser.add_argument(
        "-c", "--ConfigPath", type=str, help="Path for the config file")
    parser.add_argument(
        "-u", "--Sudo", help="Run the submition as superuser",  action="store_true")
    parser.add_argument(
        "-timeout", "--Timeout", type=str, help="Controls default timeout for event regeneration (used in notifications)")

    # Pass the arguments to main method
    sys.exit(main(parser.parse_args()))
