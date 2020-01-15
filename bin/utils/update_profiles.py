#!/usr/bin/env python
import requests
import json
from snakebite.client import Client
from snakebite.errors import FileNotFoundException
import logging
import os
import uuid
from urllib.parse import urlparse
from argparse import ArgumentParser
import sys
import subprocess
from .common import get_log_conf, get_config_paths
from .argo_config import ArgoConfig

log = logging.getLogger(__name__)


class ArgoApiClient:
    """
    ArgoApiClient class

    It connects to an argo-web-api host and retrieves profile information per tenant and report
    """

    def __init__(self, host, tenant_keys, verify=False, http_proxy_url=None):
        """
        Initialize ArgoApiClient which is used to retrieve profiles from argo-web-api
        Args:
            host: str. argo-web-api host
            tenant_keys: dict. a dictionary of {tenant: api_token} entries
            verify (boolean): flag if the remote web api host should be verified
            http_proxy_url (str.): optional url for local http proxy to be used
        """
        self.host = host
        self.verify = verify
        if http_proxy_url:
            self.proxies = {'http': http_proxy_url, 'https': http_proxy_url}
        else:
            self.proxies = None

        self.paths = dict()
        self.tenant_keys = tenant_keys
        self.paths.update({
            'reports': '/api/v2/reports',
            'operations': '/api/v2/operations_profiles',
            'metrics': '/api/v2/metric_profiles',
            'aggregations': '/api/v2/aggregation_profiles',
            'thresholds': '/api/v2/thresholds_profiles',
            'tenants': '/api/v2/admin/tenants'
        })

    def get_url(self, resource, item_uuid=None, date=None):
        """
        Constructs an argo-web-api url based on the resource and item_uuid
        Args:
            resource:   str. resource to be retrieved (reports|ops)
            item_uuid: str. retrieve a specific item from the resource
            date: str. returns the historic version of the resource 

        Returns:
            str: url path

        """
        dateQuery = ""
        if date:
            dateQuery = "?date=" + date
        if item_uuid is None:
            return "".join(["https://", self.host, self.paths[resource], dateQuery])
        else:
            return "".join(["https://", self.host, self.paths[resource], "/", item_uuid, dateQuery])

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
        r = requests.get(url, headers=headers,
                         verify=self.verify, proxies=self.proxies)

        if 200 == r.status_code:
            return json.loads(r.text)["data"]
        else:
            return None

    def get_tenants(self, token):
        """
        Returns an argo-web-api resource by tenant and url
        Args:
            token: str. web-api token to be used (for auth)

        Returns:
            dict: list of tenants and access keys

        """

        tenants = self.get_admin_resource(token, self.get_url("tenants"))
        tenant_keys = dict()
        for item in tenants:
            for user in item["users"]:
                if user["name"].startswith("argo_engine_") and user["api_key"]:
                    tenant_keys[item["info"]["name"]] = user["api_key"]
        return tenant_keys

    def get_admin_resource(self, token, url):
        """
        Returns an argo-web-api resource by tenant and url
        Args:
            token: str. admin token to be used
            url: str. url of resource

        Returns:
            dict: resource contents

        """
        headers = dict()
        headers.update({
            'Accept': 'application/json',
            'x-api-key': token
        })
        r = requests.get(url, headers=headers,
                         verify=self.verify, proxies=self.proxies)

        if 200 == r.status_code:
            return json.loads(r.text)["data"]
        else:
            return None

    def get_profile(self, tenant, report, profile_type, date=None):
        """
        Gets an argo-web-api profile by tenant, report and profile type
        Args:
            tenant: str. tenant name
            report: str. report name
            profile_type: str. profile type (ops)

        Returns:
            dict: profile contents

        """
        # default recomputation is always an empty json array
        if profile_type == "recomputations":
            return []

        item_uuid = self.find_profile_uuid(tenant, report, profile_type)

        if item_uuid is None:
            return None
        profiles = self.get_resource(
            tenant, self.get_url(profile_type, item_uuid, date))
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
        if profile_type is "aggregations":
            profile_type = "aggregation"

        if profile_type is "metrics":
            profile_type = "metric"

        report = self.get_report(tenant, self.find_report_uuid(tenant, report))
        if profile_type == "reports":
            return report["id"]
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

    def gen_profile_path(self, tenant, report, profile_type, date=None):
        """
        Generates a valid hdfs path to a specific profile
        Args:
            tenant: str. tenant to be used
            report: str. report to be used
            profile_type: str. profile_type (operations|reports|aggregations|thresholds|metrics)

        Returns:
            str: hdfs path

        """
        templates = dict()
        if date:
            date = "_" + date
        else:
            date = ""
        templates.update({
            'operations': '{0}_{1}_ops{2}.json',
            'aggregations': '{0}_{1}_ap{2}.json',
            'metrics': '{0}_{1}_metrics{2}.json',
            'reports': '{0}_{1}_cfg.json',
            'thresholds': '{0}_{1}_thresholds{2}.json',
            'recomputations': 'recomp.json'
        })

        sync_path = self.base_path.replace("{{tenant}}", tenant)
        filename = templates[profile_type].format(tenant, report, date)
        return os.path.join(sync_path, filename)

    def cat(self, tenant, report, profile_type, date=None):
        """
        Returns the contents of a profile stored in hdfs
        Args:
            tenant: str. tenant name
            report: str. report name
            profile_type: str. profile type (operations|reports|aggregations|thresholds|metric)

        Returns:

        """
        path = self.gen_profile_path(tenant, report, profile_type, date)
        try:
            txt = self.client.cat([path])
            j = json.loads(next(next(txt)))
            return j, True
        except FileNotFoundException:
            return None, False

    def rem(self, tenant, report, profile_type, date=None):
        """
        Removes a profile file that already exists in hdfs (in order to be replaced)
        Args:
            tenant: str. tenant name
            report: str. report name
            profile_type: str. profile type (operations|reports|aggregations|thresholds)

        Returns:

        """
        path = self.gen_profile_path(tenant, report, profile_type, date)

        try:
            next(self.client.delete([path]))
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

    def __init__(self, config):
        """
        Initialized ArgoProfileManager which manages updating argo profile files in hdfs
        Args:
            config: obj. ArgoConfig object containing the main configuration
        """

        self.cfg = config

        # process hdfs base path
        namenode = config.get("HDFS", "namenode")
        hdfs_user = config.get("HDFS", "user")
        full_path = config.get("HDFS", "path_sync")
        full_path = full_path.partial_fill(
            namenode=namenode.geturl(), hdfs_user=hdfs_user)

        short_path = urlparse(full_path).path

        tenant_list = config.get("API", "tenants")
        tenant_keys = dict()
        # Create a list of tenants -> api_keys
        for tenant in tenant_list:
            tenant_key = config.get("API", tenant + "_key")
            tenant_keys[tenant] = tenant_key

        ams_proxy = config.get("API", "proxy")
        if ams_proxy:
            ams_proxy = ams_proxy.geturl()

        self.hdfs = HdfsReader(namenode.hostname, namenode.port, short_path)
        self.api = ArgoApiClient(config.get("API", "endpoint").netloc, tenant_keys, config.get(
            "API", "verify"), ams_proxy)

    def profile_update_check(self, tenant, report, profile_type, date=None):
        """
        Checks if latest api profiles are aligned with profile files stored in hdfs.
        If not the updated api profile are uploaded to hdfs
        Args:
            tenant: str. Tenant name to check profiles from
            report: str. Report name to check profiles from
            profile_type: str. Name of the profile type used (operations|aggregations|reports)
            date: str. Optional date to retrieve historic version of the profile
        """

        prof_api = self.api.get_profile(tenant, report, profile_type, date)
        if prof_api is None:
            log.info(
                "profile type %s doesn't exist in report --skipping", profile_type)
            return

        log.info("retrieved %s profile(api): %s", profile_type, prof_api)

        prof_hdfs, exists = self.hdfs.cat(tenant, report, profile_type, date)

        if exists:
            log.info("retrieved %s profile(hdfs): %s ",
                     profile_type, prof_hdfs)
            prof_update = prof_api != prof_hdfs

            if prof_update:
                log.info("%s profile mismatch", profile_type)
            else:
                log.info("%s profiles match -- no need for update", profile_type)
        else:
            # doesn't exist so it should be uploaded
            prof_update = True
            log.info(
                "%s profile doesn't exist in hdfs, should be uploaded", profile_type)

        # Upload if it's deemed to be uploaded
        if prof_update:
            self.upload_profile_to_hdfs(
                tenant, report, profile_type, prof_api, exists, date)

    def upload_profile_to_hdfs(self, tenant, report, profile_type, profile, exists, date=None):
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
            is_removed = self.hdfs.rem(tenant, report, profile_type, date)
            if not is_removed:
                log.error(
                    "Could not remove old %s profile from hdfs", profile_type)
                return

        # If all ok continue with uploading the new file to hdfs

        hdfs_write_bin = self.cfg.get("HDFS", "writer_bin")
        hdfs_write_cmd = "put"

        temp_fn = "prof_" + str(uuid.uuid4())
        local_path = "/tmp/" + temp_fn
        with open(local_path, 'w') as outfile:
            json.dump(profile, outfile)
        hdfs_host = self.cfg.get("HDFS", "namenode").hostname
        hdfs_path = self.hdfs.gen_profile_path(
            tenant, report, profile_type, date)
        status = subprocess.check_call(
            [hdfs_write_bin, hdfs_write_cmd, local_path, hdfs_path])

        if status == 0:
            log.info(
                "File uploaded successfully to hdfs host: %s path: %s", hdfs_host, hdfs_path)
            return True
        else:
            log.error(
                "File uploaded unsuccessful to hdfs host: %s path: %s", hdfs_host, hdfs_path)
            return False

    def upload_tenant_reports_cfg(self, tenant):
        reports = self.api.get_reports(tenant)
        report_name_list = []
        for report in reports:

            # double check if indeed report belongs to tenant
            if report["tenant"] == tenant:
                report_name = report["info"]["name"]
                report_name_list.append(report_name)
                report_uuid = report["id"]
                # Set report in configuration
                self.cfg.set("TENANTS:"+tenant, "report_" +
                             report_name, report_uuid)

        # update tenant's report name list
        self.cfg.set("TENANTS:"+tenant, "reports", ",".join(report_name_list))

    def upload_tenants_cfg(self):
        """
        Uses admin access credentials to contact argo-web-api and retrieve tenant list.
        Then updates configuration with tenant list, tenant reports etc.
        """
        token = self.cfg.get("API", "access_token")
        tenant_keys = self.api.get_tenants(token)
        self.api.tenant_keys = tenant_keys
        tenant_names = ",".join(list(tenant_keys.keys()))

        self.cfg.set("API", "tenants", tenant_names)

        # For each tenant update also it's report list
        for tenant_name in list(tenant_keys.keys()):
            self.cfg.set("API", tenant_name+"_key", tenant_keys[tenant_name])
            # Update tenant's report definitions in configuration
            self.upload_tenant_reports_cfg(tenant_name)
            self.upload_tenant_defaults(tenant_name)

    def upload_tenant_defaults(self, tenant):
        # check
        section_tenant = "TENANTS:" + tenant
        section_metric = "TENANTS:" + tenant + ":ingest-metric"
        mongo_endpoint = self.cfg.get("MONGO", "endpoint").geturl()
        mongo_uri = self.cfg.get_default(section_tenant, "mongo_uri").fill(
            mongo_endpoint=mongo_endpoint, tenant=tenant).geturl()
        hdfs_user = self.cfg.get("HDFS", "user")
        namenode = self.cfg.get("HDFS", "namenode").netloc
        hdfs_check = self.cfg.get_default(section_metric, "checkpoint_path").fill(
            namenode=namenode, hdfs_user=hdfs_user, tenant=tenant)

        self.cfg.get("MONGO", "endpoint")

        self.cfg.set(section_tenant, "mongo_uri", mongo_uri)
        self.cfg.set_default(section_tenant, "mongo_method")

        self.cfg.set_default(section_metric, "ams_interval")
        self.cfg.set_default(section_metric, "ams_batch")
        self.cfg.set(section_metric, "checkpoint_path", hdfs_check.geturl())
        self.cfg.set_default(section_metric, "checkpoint_interval")
        section_sync = "TENANTS:" + tenant + ":ingest-sync"

        self.cfg.set_default(section_sync, "ams_interval")
        self.cfg.set_default(section_sync, "ams_batch")

        section_stream = "TENANTS:" + tenant + ":stream-status"
        streaming_kafka_servers = self.cfg.get("STREAMING", "kafka_servers")
        if (streaming_kafka_servers):
            streaming_kafka_servers = ",".join(streaming_kafka_servers)
            self.cfg.set(section_stream, "kafka_servers",
                         streaming_kafka_servers)
        else:
            self.cfg.set_default(section_stream, "kafka_servers")

        self.cfg.set_default(section_stream, "ams_sub_sync")
        self.cfg.set_default(section_stream, "ams_sub_metric")
        self.cfg.set_default(section_stream, "ams_interval")
        self.cfg.set_default(section_stream, "ams_batch")
        self.cfg.set(section_stream, "output", "kafka,mongo")

        self.cfg.set(section_stream, "mongo_method", "insert")

    def save_config(self, file_path):
        """
        Saves configuration to a specified ini file

        Args:
            file_path: str. path to the configuration file to be saved
        """
        self.cfg.save_as(file_path)
        log.info("Configuration file %s updated...", file_path)


def run_profile_update(args):
    """
    Method to be executed when this is invoked as a command line script.
    Reads tenant,report and config arguments from cli and instantiates an
    ArgoProfileManager which does a profile update check
    Args:
        args: obj. command line arguments from arg parser
    """
    # Get configuration paths
    conf_paths = get_config_paths(args.config)

    # Get logger config file
    get_log_conf(conf_paths['log'])

    # Get main configuration and schema
    config = ArgoConfig(conf_paths["main"], conf_paths["schema"])

    argo = ArgoProfileManager(config)

    if args.tenant is not None:
        # check for the following profile types
        profile_type_checklist = [
            "reports", "operations", "aggregations",  "thresholds", "recomputations", "metrics"]
        reports = []
        if args.report is not None:
            reports.append(args.report)
        else:
            reports = config.get("TENANTS:"+args.tenant, "reports")

        for report in reports:
            for profile_type in profile_type_checklist:
                argo.profile_update_check(
                    args.tenant, report, profile_type, args.date)
    else:
        argo.upload_tenants_cfg()
        argo.save_config(conf_paths["main"])


if __name__ == '__main__':
    # Feed Argument parser with the description of the 3 arguments we need
    arg_parser = ArgumentParser(
        description="update profiles for specific tenant/report")
    arg_parser.add_argument(
        "-t", "--tenant", help="tenant owner ", dest="tenant", metavar="STRING", required=False, default=None)
    arg_parser.add_argument(
        "-r", "--report", help="report", dest="report", metavar="STRING", required=False, default=None)
    arg_parser.add_argument(
        "-c", "--config", help="config", dest="config", metavar="STRING")
    arg_parser.add_argument(
        "-d", "--date", help="historic date", dest="date", metavar="STRING", required=False, default=None)

    # Parse the command line arguments accordingly and introduce them to the run method
    sys.exit(run_profile_update(arg_parser.parse_args()))
