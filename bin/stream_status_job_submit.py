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
    """Checks hdfs for available files back in time and prepares the correct hdfs arguments

    Args:
        year (int): year part of the date to check for hdfs files
        month (int): month part of the date to check for hdfs files
        day (int): day part of the date to check for hdfs files
        config (obj.): argo configuration object


    Returns:
        list: A list of all hdfs arguments to be used in flink job submission
    """

    # set up the hdfs client to be used in order to check the files
    namenode = config.get("HDFS", "namenode")
    client = Client(namenode.hostname, namenode.port, use_trash=False)

    # hdfs sync  path for the tenant

    hdfs_user = config.get("HDFS", "user")
    tenant = args.tenant
    hdfs_sync = config.get("HDFS", "path_sync")
    hdfs_sync = hdfs_sync.fill(namenode=namenode.geturl(
    ), hdfs_user=hdfs_user, tenant=tenant).geturl()

    # dictionary holding all the commands with their respective arguments' name
    hdfs_commands = dict()

    # if profile historic mode is used reference profiles by date
    if args.historic:
        # file location of historic operations profile (local or hdfs)
        hdfs_commands["--ops"] = hdfs_check_path(
            hdfs_sync+"/"+args.tenant+"_ops_" + args.date + ".json",  client)

        # file location of historic aggregations profile (local or hdfs)
        hdfs_commands["--apr"] = hdfs_check_path(
            hdfs_sync+"/"+args.tenant+"_"+args.report+"_ap_" + args.date + ".json", client)

        # TODO: Don't Use YET metric profiles from api in json form until status computation jobs are updated
        # accordingly - After that uncomment the following
        # #file location of historic metric profile (local or hdfs) which is in json format
        # hdfs_commands["--mps"] = hdfs_check_path(
        #     hdfs_sync+"/"+args.tenant+"_"+args.report+"_metric_" + args.date + ".json", client)

        # TODO: when compute jobs are updated to use metric profiles in json format comment the following:
        # file location of metric profile (local or hdfs)
        hdfs_commands["--mps"] = date_rollback(
            hdfs_sync + "/" + args.report + "/" + "metric_profile_" +
            "{{date}}" + ".avro", year, month, day, config,
            client)
    else:

        # file location of operations profile (local or hdfs)
        hdfs_commands["--ops"] = hdfs_check_path(
            hdfs_sync+"/"+args.tenant+"_ops.json",  client)

        # file location of aggregations profile (local or hdfs)
        hdfs_commands["--apr"] = hdfs_check_path(
            hdfs_sync+"/"+args.tenant+"_"+args.report+"_ap.json", client)

        # file location of metric profile (local or hdfs)
        hdfs_commands["--mps"] = date_rollback(
            hdfs_sync + "/" + args.report + "/" + "metric_profile_" +
            "{{date}}" + ".avro", year, month, day, config,
            client)

    # get downtime
    # file location of metric profile (local or hdfs)
    hdfs_commands["--sync.downtime"] = date_rollback(
        hdfs_sync + "/" + args.report + "/" + "downtimes_" +
        "{{date}}" + ".avro", year, month, day, config,
        client)

    #  file location of endpoint group topology file (local or hdfs)
    hdfs_commands["-sync.egp"] = date_rollback(
        hdfs_sync + "/" + args.report + "/" + "group_endpoints_" +
        "{{date}}" + ".avro", year, month, day, config,
        client)

    return hdfs_commands


def compose_command(config, args,  hdfs_commands):
    """Composes a command line execution string for submitting a flink job.

    Args:
        config (obj.): argo configuration object
        args (dict): command line arguments of this script
        hdfs_commands (list): a list of hdfs related arguments to be passed in flink job

    Returns:
        list: A list of all command line arguments for performing the flink job submission
    """

    # job submission command
    cmd_command = []

    if args.sudo is True:
        cmd_command.append("sudo")

    # get needed config params
    section_tenant = "TENANTS:" + args.tenant
    section_tenant_job = "TENANTS:" + args.tenant + ":stream-status"

    ams_endpoint = config.get("AMS", "endpoint")

    ams_project = config.get(section_tenant, "ams_project")
    if args.report.lower() == "critical":
        ams_sub_metric = config.get(section_tenant_job, "ams_sub_metric")
        ams_sub_sync = config.get(section_tenant_job, "ams_sub_sync")
    else:
        ams_sub_metric = "stream_metric_" + report.lower()
        ams_sub_sync = "stream_sync_" + report.lower()

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
    ams_port = cmd_command.append("--ams.port")
    if not ams_port:
        ams_port = 443
    cmd_command.append(str(ams_port))

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
    job_namespace = config.get("JOB-NAMESPACE", "stream-status-namespace")
    job_namespace = job_namespace.fill(ams_endpoint=ams_endpoint.hostname, ams_port=ams_port, ams_project=ams_project,
                                       ams_sub_metric=ams_sub_metric, ams_sub_sync=ams_sub_sync)

    # add the hdfs commands
    for command in hdfs_commands:
        cmd_command.append(command)
        cmd_command.append(hdfs_commands[command])

    # date
    cmd_command.append("--run.date")
    cmd_command.append(args.date)

    # report
    cmd_command.append("--report")
    cmd_command.append(args.report)

    # flink parallelism
    cmd_command.append("--p")
    flink_parallelism = config.get(section_tenant_job, "flink_parallelism")
    if not flink_parallelism:
        flink_parallelism = "1"
    cmd_command.append(flink_parallelism)

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
                    cmd_command.append(config.get(
                        section_tenant_job, "hbase_master").hostname)
                # hbase endpoint port
                if config.has(section_tenant_job, "hbase_master"):
                    cmd_command.append("--hbase.port")
                    cmd_command.append(config.get(
                        section_tenant_job, "hbase_master").port)

                # comma separate list of zookeeper servers
                if config.has(section_tenant_job, "hbase_zk_quorum"):
                    cmd_command.append("--hbase.zk.quorum")
                    cmd_command.append(config.get(
                        section_tenant_job, "hbase_zk_quorum"))

                # port used by zookeeper servers
                if config.has(section_tenant_job, "hbase_zk_port"):
                    cmd_command.append("--hbase.zk.port")
                    cmd_command.append(config.get(
                        section_tenant_job, "hbase_zk_port"))

                # table namespace, usually tenant
                if config.has(section_tenant_job, "hbase_namespace"):
                    cmd_command.append("--hbase.namespace")
                    cmd_command.append(config.get(
                        section_tenant_job, "hbase_namespace"))

                # table name, usually metric data
                if config.has(section_tenant_job, "hbase_table"):
                    cmd_command.append("--hbase.table")
                    cmd_command.append(config.get(
                        section_tenant_job, "hbase_table"))

            elif output == "kafka":
                # kafka list of servers
                if config.has(section_tenant_job, "kafka_servers"):
                    cmd_command.append("--kafka.servers")
                    kafka_servers = ','.join(config.get(
                        section_tenant_job, "kafka_servers"))
                    cmd_command.append(kafka_servers)
                # kafka topic to send status events to
                if config.has(section_tenant_job, "kafka_topic"):
                    cmd_command.append("--kafka.topic")
                    cmd_command.append(config.get(
                        section_tenant_job, "kafka_topic"))
            elif output == "fs":
                # filesystem path for output(use "hdfs://" for hdfs path)
                if config.has(section_tenant_job, "fs_output"):
                    cmd_command.append("--fs.output")
                    cmd_command.append(config.get(
                        section_tenant_job, "fs_output"))
            elif output == "mongo":
                cmd_command.append("--mongo.uri")
                mongo_endpoint = config.get("MONGO", "endpoint").geturl()
                mongo_uri = config.get(section_tenant, "mongo_uri").fill(
                    mongo_endpoint=mongo_endpoint, tenant=args.tenant)
                cmd_command.append(mongo_uri.geturl())
                # mongo method
                mongo_method = config.get("MONGO", "mongo_method")
                if not mongo_method:
                    mongo_method = "insert"
                cmd_command.append("--mongo.method")
                cmd_command.append(mongo_method)
                # report id
                report_id = config.get(section_tenant, "report_" + args.report)
                cmd_command.append("--report-id")
                cmd_command.append(report_id)

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

    # optional call to update profiles
    if args.profile_check:
        dateParam = None
        if args.historic:
            dateParam = args.date
        profile_mgr = ArgoProfileManager(config)
        profile_type_checklist = [
            "operations", "aggregations", "reports", "thresholds", "metrics"]
        for profile_type in profile_type_checklist:
            profile_mgr.profile_update_check(
                args.tenant, args.report, profile_type, dateParam)

    # dictionary containing the argument's name and the command assosciated with each name
    hdfs_commands = compose_hdfs_commands(year, month, day, args, config)

    cmd_command, job_namespace = compose_command(config, args, hdfs_commands)

    # submit the script's command
    flink_job_submit(config, cmd_command, job_namespace, args.dry_run)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Stream Status Job submit script")
    parser.add_argument(
        "-t", "--tenant", metavar="STRING", help="Name of the tenant", required=True, dest="tenant")
    parser.add_argument(
        "-d", "--date", metavar="DATE(ISO-8601)",
        default=str(datetime.datetime.utcnow().replace(
            microsecond=0).isoformat()) + "Z",
        help="Date in ISO-8601 format", dest="date")
    parser.add_argument(
        "-r", "--report", metavar="STRING", help="Report status", required=True, dest="report")
    parser.add_argument(
        "-c", "--config", metavar="PATH", help="Path for the config file", dest="config")
    parser.add_argument(
        "-u", "--sudo", help="Run the submission as superuser",  action="store_true", dest="sudo")
    parser.add_argument("--dry-run", help="Runs in test mode without actually submitting the job",
                        action="store_true", dest="dry_run")
    parser.add_argument("--historic-profiles", help="use historic profiles",
                        dest="historic", action="store_true")
    parser.add_argument("--profile-check", help="check if profiles are up to date before running job",
                        dest="profile_check", action="store_true")
    parser.add_argument(
        "-timeout", "--timeout", metavar="INT",
        help="Controls default timeout for event regeneration (used in notifications)", dest="timeout")

    # Pass the arguments to main method
    sys.exit(main(parser.parse_args()))
