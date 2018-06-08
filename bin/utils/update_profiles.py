#!/usr/bin/env python
import requests
import json
from snakebite.client import Client
from snakebite.errors import FileNotFoundException
from ConfigParser import SafeConfigParser
import logging
import logging.handlers
import os
import uuid
from urlparse import urlparse
from argparse import ArgumentParser
import sys
import subprocess

CONF_ETC = '/etc/argo-streaming/argo-streaming.cfg'
CONF_LOCAL = './argo-streaming.cfg'


class ArgoApiClient:
    """
    ArgoApiClient class

    It connects to an argo-web-api host and retrieves profile information per tenant and report
    """

    def __init__(self, host, tenant_keys):
        """
        Initialize ArgoApiClient which is used to retrieve profiles from argo-web-api
        Args:
            host: str. argo-web-api host
            tenant_keys: dict. a dictionary of {tenant: api_token} entries
        """
        self.host = host
        self.paths = dict()
        self.tenant_keys = tenant_keys
        self.paths.update({
            'reports': '/api/v2/reports',
            'operations': '/api/v2/operations_profiles',
            'aggregations': '/api/v2/aggregation_profiles'
        })

    def get_url(self, resource, item_uuid):
        """
        Constructs an argo-web-api url based on the resource and item_uuid
        Args:
            resource:   str. resource to be retrieved (reports|ops)
            item_uuid: str. retrieve a specific item from the resource

        Returns:
            str: url path

        """
        if item_uuid is None:
            return "".join(["https://", self.host, self.paths[resource]])
        else:
            return "".join(["https://", self.host, self.paths[resource], "/", item_uuid])

    def get_resource(self, tenant, url):
        """
        Returns an argo-web-api resource by tenant and url
        Args:
            tenant: str. tenant to be used (for auth)
            url: str. url of resource

        Returns:
            dict: resource contents

        """
        headers = dict()
        headers.update({
            'Accept': 'application/json',
            'x-api-key': self.tenant_keys[tenant]
        })
        r = requests.get(url, headers=headers, verify=False)

        if 200 == r.status_code:
            return json.loads(r.text)["data"]
        else:
            return None

    def get_profile(self, tenant, report, profile_type):
        """
        Gets an argo-web-api profile by tenant, report and profile type
        Args:
            tenant: str. tenant name
            report: str. report name
            profile_type: str. profile type (ops)

        Returns:
            dict: profile contents

        """
        item_uuid = self.find_profile_uuid(tenant, report, profile_type)
        profiles = self.get_resource(tenant, self.get_url(profile_type, item_uuid))
        if profiles is not None:
            return profiles[0]

    def get_reports(self, tenant):
        """
        Get all reports for a tenant

        Args:
            tenant: str. tenant name

        Returns:
            list(dict): a list of reports (dict objects)

        """
        return self.get_resource(tenant, self.get_url("reports", None))

    def find_report_uuid(self, tenant, name):
        """

        Args:
            tenant: str. tenant name to be used
            name: str. report name to be used

        Returns:
            str: report's uuid

        """
        r = self.get_reports(tenant)
        if r is None:
            return ''

        for report in r:
            if report["info"]["name"] == name:
                return report["id"]

    def get_report(self, tenant, item_uuid):
        """
        Gets a report based on a report uuid or a report list if uuid is None
        Args:
            tenant: str. Tenant to retrieve report from
            item_uuid: str. report uuid to be used - or None to get all reports

        Returns:
            obj: Returns an array of reports or one report

        """
        reports = self.get_resource(tenant, self.get_url("reports", item_uuid))
        if reports is not None:
            return reports[0]

    def find_profile_uuid(self, tenant, report, profile_type):
        """
        Finds a profile uuid by tenant, report and it's type (ops)
        Args:
            tenant: str. Tenant name to retrieve profile uuid from
            report: str. Report name that the profile is used
            profile_type: str. Type of the profile (ops)

        Returns:

        """
        report = self.get_report(tenant, self.find_report_uuid(tenant, report))
        for profile in report["profiles"]:
            if profile["type"] == profile_type:
                return profile["id"]


class HdfsReader:
    """
    HdfsReader class

    Connects to an hdfs endpoint (namenode) and checks argo profile files stored there
    Uses a specific base path for determining argo file destinations
    """

    def __init__(self, namenode, port, base_path):
        """
        Initialized HdfsReader which is used to check/read profile files from hdfs
        Args:
            namenode: str. hdfs namenode host
            port: int. hdfs namenode port
            base_path: str. base path to  destination used for argo
        """
        self.client = Client(namenode, port)
        self.base_path = base_path

    def gen_profile_path(self, tenant, report, profile_type):
        """
        Generates a valid hdfs path to a specific profile
        Args:
            tenant: str. tenant to be used
            report: str. report to be used
            profile_type: str. profile_type (operations|reports|aggregations)

        Returns:
            str: hdfs path

        """
        templates = dict()
        templates.update({
            'operations': '{0}_ops.json',
            'aggregations': '{0}_{1}_ap.json',
            'reports': '{0}_{1}_cfg.json'
        })

        sync_path = self.base_path.replace("{{tenant}}", tenant)
        filename = templates[profile_type].format(tenant, report)
        return os.path.join(sync_path, filename)

    def cat(self, tenant, report, profile_type):
        """
        Returns the contents of a profile stored in hdfs
        Args:
            tenant: str. tenant name
            report: str. report name
            profile_type: str. profile type (operations|reports|aggregations)

        Returns:

        """
        path = self.gen_profile_path(tenant, report, profile_type)
        try:
            txt = self.client.cat([path])
            j = json.loads(txt.next().next())
            return j, True
        except FileNotFoundException:
            return None, False

    def rem(self, tenant, report, profile_type):
        """
        Removes a profile file that already exists in hdfs (in order to be replaced)
        Args:
            tenant: str. tenant name
            report: str. report name
            profile_type: str. profile type (operations|reports|aggregations)

        Returns:

        """
        path = self.gen_profile_path(tenant, report, profile_type)

        try:
            self.client.delete([path]).next()
            return True
        except FileNotFoundException:
            return False


class ArgoProfileManager:
    """
    ArgoProfileManager

    Checks argo profile definitions per tenant between an argo-web-api instance and
    an hdfs destination. If argo profiles are out of date in hdfs they are updated
    from their latest profile definitions given from argo-web-api.
    """

    def __init__(self, cfg_path):
        """
        Initialized ArgoProfileManager which manages updating argo profile files in hdfs
        Args:
            cfg_path: str. path to the configuration file used
        """
        self.cfg = get_config(cfg_path)

        self.log = get_logger("argo-profile-mgr", self.cfg["log_level"])

        # process hdfs base path
        full_path = str(self.cfg["hdfs_sync"])
        full_path = full_path.replace("{{hdfs_host}}", str(self.cfg["hdfs_host"]))
        full_path = full_path.replace("{{hdfs_port}}", str(self.cfg["hdfs_port"]))
        full_path = full_path.replace("{{hdfs_user}}", str(self.cfg["hdfs_user"]))

        short_path = urlparse(full_path).path

        self.hdfs = HdfsReader(self.cfg["hdfs_host"], int(self.cfg["hdfs_port"]), short_path)
        self.api = ArgoApiClient(self.cfg["api_host"], self.cfg["tenant_keys"])

    def profile_update_check(self, tenant, report, profile_type):
        """
        Checks if latest api profiles are aligned with profile files stored in hdfs.
        If not the updated api profile are uploaded to hdfs
        Args:
            tenant: str. Tenant name to check profiles from
            report: str. Report name to check profiles from
        """

        prof_api = self.api.get_profile(tenant, report, profile_type)
        self.log.info("retrieved %s profile(api): %s", profile_type, prof_api)

        prof_hdfs, exists = self.hdfs.cat(tenant, report, profile_type)

        if exists:
            self.log.info("retrieved %s profile(hdfs): %s ", profile_type, prof_hdfs)
            prof_update = prof_api != prof_hdfs

            if prof_update:
                self.log.info("%s profile mismatch", profile_type)
            else:
                self.log.info("%s profiles match -- no need for update", profile_type)
        else:
            # doesn't exist so it should be uploaded
            prof_update = True
            self.log.info("%s profile doesn't exist in hdfs, should be uploaded", profile_type)

        # Upload if it's deemed to be uploaded
        if prof_update:
            self.upload_profile_to_hdfs(tenant, report, profile_type, prof_api, exists)

    def upload_profile_to_hdfs(self, tenant, report, profile_type, profile, exists):
        """
        Uploads an updated profile (from api) to the specified hdfs destination
        Args:
            tenant: str. The tenant name used in hdfs path
            report: str. The report name used in hdfs path/filenames
            profile_type: str. The profile type to be used in hdfs filename
            profile: dict. Content data of the profile
            exists: bool. if an old profile exists already in hdfs

        Returns:
            bool: if the operation was successful or not

        """

        # If file exists on hdfs should be removed first
        if exists:
            is_removed = self.hdfs.rem(tenant, report, profile_type)
            if not is_removed:
                self.log.error("Could not remove old %s profile from hdfs", profile_type)
                return

        # If all ok continue with uploading the new file to hdfs

        hdfs_write_bin = self.cfg["hdfs_writer"]
        hdfs_write_cmd = "put"

        temp_fn = "prof_" + str(uuid.uuid4())
        local_path = "/tmp/" + temp_fn
        with open(local_path, 'w') as outfile:
            json.dump(profile, outfile)

        hdfs_host = str(self.cfg["hdfs_host"])
        hdfs_path = self.hdfs.gen_profile_path(tenant, report, profile_type)

        status = subprocess.check_call([hdfs_write_bin, hdfs_write_cmd, local_path, hdfs_path])

        if status == 0:
            self.log.info("File uploaded successfully to hdfs host: %s path: %s", hdfs_host, hdfs_path)
            return True
        else:
            self.log.error("File uploaded unsuccessful to hdfs host: %s path: %s", hdfs_host, hdfs_path)
            return False


def get_config(path):
    """
    Creates a specific dictionary with only needed configuration options retrieved from an argo config file
    Args:
        path: str. path to the generic argo config file to be used

    Returns:
        dict: a specific configuration option dictionary with only necessary parameters

    """
    # set up the config parser
    config = SafeConfigParser()

    if path is None:

        if os.path.isfile(CONF_ETC):
            # Read default etc_conf
            config.read(CONF_ETC)
        else:
            # Read local
            config.read(CONF_LOCAL)
    else:
        config.read(path)

    cfg = dict()
    cfg.update(dict(log_level=config.get("LOGS", "log_level"), hdfs_host=config.get("HDFS", "hdfs_host"),
                    hdfs_port=config.get("HDFS", "hdfs_port"), hdfs_user=config.get("HDFS", "hdfs_user"),
                    hdfs_sync=config.get("HDFS", "hdfs_sync"), api_host=config.get("API", "host"),
                    hdfs_writer=config.get("HDFS", "writer_bin")))

    tenant_list = config.get("API", "tenants").split(",")
    tenant_keys = dict()
    # Create a list of tenants -> api_keys
    for tenant in tenant_list:
        tenant_key = config.get("API", tenant + "_key")
        tenant_keys[tenant] = tenant_key
    cfg["tenant_keys"] = tenant_keys

    return cfg


def get_logger(log_name, log_level):
    """
    Instantiates a logger
    Args:
        log_name: str. log name to be used in logger
        log_level: str. log level

    Returns:
        obj: logger

    """
    # Instantiate logger with proper name
    log = logging.getLogger(log_name)
    log.setLevel(logging.DEBUG)

    log_level = log_level.upper()

    # Instantiate default levels
    levels = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }

    # Log to console
    stream_log = logging.StreamHandler()
    stream_log.setLevel(logging.INFO)
    log.addHandler(stream_log)

    # Log to syslog
    sys_log = logging.handlers.SysLogHandler("/dev/log")
    sys_format = logging.Formatter('%(name)s[%(process)d]: %(levelname)s %(message)s')
    sys_log.setFormatter(sys_format)
    sys_log.setLevel(levels[log_level])
    log.addHandler(sys_log)

    return log


def run_profile_update(args):
    """
    Method to be executed when this is invoked as a command line script.
    Reads tenant,report and config arguments from cli and instantiates an
    ArgoProfileManager which does a profile update check
    Args:
        args: obj. command line arguments from arg parser
    """
    # Set a new profile manager to be used
    argo = ArgoProfileManager(args.config)

    # check for the following profile types -- now just operations
    profile_type_checklist = ["operations", "aggregations"]
    for profile_type in profile_type_checklist:
        argo.profile_update_check(args.tenant, args.report, profile_type)


if __name__ == '__main__':
    # Feed Argument parser with the description of the 3 arguments we need
    arg_parser = ArgumentParser(
        description="update profiles for specific tenant/report")
    arg_parser.add_argument(
        "-t", "--tenant", help="tenant owner ", dest="tenant", metavar="STRING", required=True)
    arg_parser.add_argument(
        "-r", "--report", help="report", dest="report", metavar="STRING", required=True)
    arg_parser.add_argument(
        "-c", "--config", help="config", dest="config", metavar="STRING")

    # Parse the command line arguments accordingly and introduce them to the run method
    sys.exit(run_profile_update(arg_parser.parse_args()))
