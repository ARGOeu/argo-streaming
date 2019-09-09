#!/usr/bin/env python
import requests
import json
import logging
from common import get_config_paths, get_log_conf
from argo_config import ArgoConfig
from argparse import ArgumentParser
import sys


log = logging.getLogger(__name__)


class ArgoAmsClient:
    """
    ArgoAmsClient class

    It connects to an argo-messaging host and retrieves project/user/topic/subscription information
    """

    def __init__(self, host, admin_key, verify=True, http_proxy_url=None):
        """
        Initialize ArgoAAmsClient
        Args:
            host: str. argo ams host
            admin_key: str. admin token
            verify (boolean): flag if the remote web api host should be verified
            http_proxy_url (str.): optional url for local http proxy to be used
        """

        # flag to verify https connections or not
        self.verify = verify
        # proxy configuration
        if http_proxy_url:
            self.proxies = {'http':http_proxy_url,'https':http_proxy_url}
        else:
            self.proxies = None
        # ams host
        self.host = host
        # admin key to access ams service
        self.admin_key = admin_key

        # ams api resource paths
        self.paths = dict()
        self.paths.update({
            'projects': '/v1/projects',
            'users': '/v1/users',
            'topics': '/v1/projects/{}/topics',
            'subscriptions': '/v1/projects/{}/subscriptions',
        })

    def get_url(self, resource, item_id=None, group_id=None, action=None):
        """
        Constructs an ams url based on resource, item and group(project)
        Args:
            resource:   str. resource to be retrieved (projects/users/topics/susbscriptions)
            item_id: str. retrieve a specific item from the resource
            group_id: str. retrieve item from a specific group (grouped resource)
            action: str. additional actions for some urls (such as acl, modifyAcl etc)

        Returns:
            str: url path

        """
        # get resource path
        resource_path = self.paths[resource]
        # if grouped resource (like topics or subs that belong under a project)
        if '{}' in resource_path:
            # add groups name into the resource path
            resource_path = resource_path.format(group_id)
        # if a resource item is not specified get all items
        if item_id is None:
            return "".join(["https://", self.host, resource_path, "?key=", self.admin_key])
        else:
            # if a specific item is given check if an additional api action is specified on item (like :acl on topics)
            if action is not None:
                return "".join(
                    ["https://", self.host, resource_path, "/", item_id, ":", action, "?key=", self.admin_key])
            return "".join(["https://", self.host, resource_path, "/", item_id, "?key=", self.admin_key])

    def post_resource(self, url, data):
        """
        Posts (inserts) an ams resource by token
        Args:
            url: str. url of resource
            data: dict. data to post

        Returns:
            dict: resource contents

        """
        # Set up headers
        headers = dict()
        headers.update({
            'Accept': 'application/json'
        })
        # do the post requests
        r = requests.post(url, headers=headers, verify=self.verify, data=json.dumps(data), proxies=self.proxies)
        # if successful return data (or empty json)
        if 200 == r.status_code:
            if r.text == "":
                return {}
            return json.loads(r.text)
        # If not successful return None
        else:
            log.critical(r.text)
            return None

    def put_resource(self, url, data):
        """
        Puts (inserts/updates) an ams resource by token
        Args:
            url: str. url of resource
            data: dict. data to post

        Returns:
            dict: resource contents

        """
        # Set up headers
        headers = dict()
        headers.update({
            'Accept': 'application/json'
        })
        # do the put request
        r = requests.put(url, headers=headers, verify=self.verify, data=json.dumps(data), proxies=self.proxies)
        # if successful return json data (or empty json)
        if 200 == r.status_code:
            if r.text == "":
                return {}
            return json.loads(r.text)
        # if not successful return None
        else:
            log.critical(r.text)
            return None

    def get_resource(self, url):
        """
        Returns an ams resource by token
        Args:
            url: str. url of resource

        Returns:
            dict: resource contents

        """
        # Set up headers
        headers = dict()
        headers.update({
            'Accept': 'application/json'
        })
        # do the get resource
        r = requests.get(url, headers=headers, verify=self.verify, proxies=self.proxies)
        # if successful return the json data or empty json
        if 200 == r.status_code:
            if r.text == "":
                return {}
            return json.loads(r.text)
        else:
            return None

    def get_project(self, project):
        """
        Get a specific project from AMS
        Args:
            project: str. name of the project

        Returns:
            dict. json representation of the project

        """
        url = self.get_url("projects", project)
        return self.get_resource(url)

    def get_users(self):
        """
        Get the list of users from AMS

        Returns:
            dict. json representation of the list of users

        """
        url = self.get_url("users")
        return self.get_resource(url)["users"]

    def get_user(self, name):
        """
        Get a specific AMS user
        Args:
            name: str. username

        Returns:
            dict. json representation of the AMS user

        """
        url = self.get_url("users", name)
        return self.get_resource(url)

    def get_topics(self, project):
        """
        Get list of topics for an AMS project
        Args:
            project: str. name of the project

        Returns:
            dict. json representation of the list of topics

        """
        url = self.get_url("topics", None, project)
        return self.get_resource(url)

    def get_subs(self, project):
        """
        Get list of subs for an AMS project
        Args:
            project: str. name of the project

        Returns:
            dict. json represenatation of the list of subscriptions

        """
        url = self.get_url("subscriptions", None, project)
        return self.get_resource(url)

    def get_topic_num_of_messages(self, project, topic):
        """
        Get number of messages arrived in a topic
        Args:
            project: str. project name
            topic: str. topic name

        Returns:
           int. number of messages

        """
        url = self.get_url("topics", topic, project, "metrics")
        metrics =  self.get_resource(url)["metrics"]
        
        for metric in metrics:
            if metric["metric"] == "topic.number_of_messages":
                ts = metric["timeseries"]
                if len(ts) > 0:
                    return ts[0]["value"]
        return 0


    def get_topic_acl(self, project, topic):
        """
        Get ACL list for a specific project's topic
        Args:
            project: str. project name
            topic: str. topic name

        Returns:
            list. a list of authorized users

        """
        url = self.get_url("topics", topic, project, "acl")
        return self.get_resource(url)["authorized_users"]

    def get_sub_acl(self, project, sub):
        """
        Get ACL list for a specific project's subscription
        Args:
            project: str. project name
            sub: str. topic name

        Returns:
            list. a list of authorized users

        """
        url = self.get_url("subscriptions", sub, project, "acl")
        return self.get_resource(url)["authorized_users"]

    def get_tenant_project(self, tenant):
        """
        For a given tenant name get the corresponding project in AMS
        Args:
            tenant: str. tenant name

        Returns:
            dict. json representation of AMS project

        """
        project = tenant.upper()
        return self.get_project(project)

    def get_tenant_user(self, tenant, role):
        """
        For a given tenant and role get the corresponding AMS user
        Args:
            tenant: str. tenant name
            role: str. user role (project_admin|consumer|publisher)

        Returns:
            dict. json representation of AMS user

        """
        if role == "project_admin":
            username = "ams_{}_admin".format(tenant.lower())
        else:
            username = "ams_{}_{}".format(tenant.lower(), role)
        return self.get_user(username)

    def get_tenant_users(self, tenant):
        """
        For a given tenant get a list of all AMS users associated with this tenant

        Args:
            tenant: str. tenant name

        Returns:
            dict. json representation of list of AMS users

        """
        # tenant must have 4 users: project_admin, publisher, consumer, archiver(consumer)
        lookup = [("project_admin", "ams_{}_admin"), ("publisher", "ams_{}_publisher"), ("consumer", "ams_{}_consumer"), ("archiver", "ams_{}_archiver")]
        lookup = [(x, y.format(tenant.lower())) for (x, y) in lookup]
        users = dict()
        for (role, name) in lookup:
            data = self.get_user(name)
            if data is not None:
                users[role] = data

        return users

    def get_tenant_topics(self, tenant):
        """
        For a specific tenant get all relevant AMS topics
        Args:
            tenant: str. tenant name

        Returns:
            dict. A set of relevant AMS topics

        """
        project = tenant.upper()
        found = dict()
        topics = self.get_topics(project)

        if topics is not None and "topics" in topics:
            topics = topics['topics']
        for topic in topics:
            name = topic["name"]
            if name.endswith('sync_data'):
                found["sync_data"] = name
            if name.endswith('metric_data'):
                found["metric_data"] = name
        return found

    def get_tenant_subs(self, tenant, topics):
        """
        For a given tenant and specific topics get relevant subscriptions
        tied to those topics
        Args:
            tenant: str. tenant name
            topics: list. list of ams topics

        Returns:
            dict. a set of relevant subscriptions

        """
        project = tenant.upper()
        found = dict()
        subs = self.get_subs(project)
        if subs is not None and "subscriptions" in subs:
            subs = subs['subscriptions']
        for sub in subs:
            name = sub["name"]
            if name.endswith('ingest_sync'):
                if topics["sync_data"] == sub["topic"]:
                    found["ingest_sync"] = name
            if name.endswith('ingest_metric'):
                if topics["metric_data"] == sub["topic"]:
                    found["ingest_metric"] = name
            if name.endswith('status_sync'):
                if topics["sync_data"] == sub["topic"]:
                    found["status_sync"] = name
            if name.endswith('status_metric'):
                if topics["metric_data"] == sub["topic"]:
                    found["status_metric"] = name
            if name.endswith('archive_metric'):
                if topics["metric_data"] == sub["topic"]:
                    found["archive_metric"] = name
        return found

    @staticmethod
    def user_get_topics(user, project):
        """
        Given an AMS user definition and a specific project
        get project's topics that this user has access to
        Args:
            user: dict. AMS user representation
            project: str. project name

        Returns:
            list(str) list of topics

        """
        for item in user['projects']:
            if item['project'] == project:
                return item['topics']
        return []

    @staticmethod
    def user_get_subs(user, project):
        """
        Given an AMS user definition and a specific project
        get project's subscriptions that this user has access to
        Args:
            user: dict. AMS user representation
            project: str. project name

        Returns:
            list(str) list of subscriptions

        """
        for item in user['projects']:
            if item['project'] == project:
                return item['subscriptions']
        return []

    def update_tenant_configuration(self, tenant, config):
        """
        Given a tenant and an instance of argo configuration
        update argo configuration with latest tenant's AMS details
        Args:
            tenant: str. tenant name
            config: obj. ArgoConfig object
        """

        # Get tenant's section in argo configuration
        section_tenant = "TENANTS:" + tenant

        # update tenant ams project
        ams_project = self.get_tenant_project(tenant)

        if ams_project is not None:
            if ams_project["name"] != config.get(section_tenant, "ams_project"):
                config.set(section_tenant, "ams_project", ams_project["name"])

        # update tenant ams_token
        ams_consumer = self.get_tenant_user(tenant, "consumer")

        if ams_consumer["token"] != config.get(section_tenant, "ams_token"):
            config.set(section_tenant, "ams_token", ams_consumer["token"])

        # get tenant's job sections in argo configuration
        section_ingest_metric = "TENANTS:{}:ingest-metric".format(tenant)
        section_ingest_sync = "TENANTS:{}:ingest-sync".format(tenant)
        section_status_stream = "TENANTS:{}:stream-status".format(tenant)

        # update tenant ingest-metric
        if config.get(section_ingest_metric, "ams_sub") != "ingest_metric":
            config.set(section_ingest_metric, "ams_sub", "ingest_metric")

        # update tenant ingest-sync
        if config.get(section_ingest_sync, "ams_sub") != "ingest_sync":
            config.set(section_ingest_sync, "ams_sub", "ingest_sync")

        # update tenant status_stream
        if config.get(section_status_stream, "ams_sub_metric") != "status_metric":
            config.set(section_status_stream, "ams_sub_metric", "status_metric")

        if config.get(section_status_stream, "ams_sub_sync") != "status_sync":
            config.set(section_status_stream, "ams_sub_sync", "status_sync")

    def create_tenant_project(self, tenant):
        """
        For a given tenant create an AMS project
        Args:
            tenant: str. tenant name

        Returns:
            dict. json representation of the newly created AMS project

        """
        project_name = tenant.upper()
        url = self.get_url("projects", project_name)
        data = {"description": "generated by argo engine"}
        return self.post_resource(url, data)

    def create_tenant_topic(self, tenant, topic):
        """
        For a given tenant and topic name create a new AMS topic
        Args:
            tenant: str. tenant name
            topic: str. topic name

        Returns:
            dict. json representation of the newly created AMS topic

        """
        project_name = tenant.upper()
        url = self.get_url("topics", topic, project_name)
        return self.put_resource(url, "")

    def create_tenant_sub(self, tenant, topic, sub):
        """
        For a given tenant, topic and subscription name create a new AMS subscription
        tied to that topic

        Args:
            tenant: str. tenant name
            topic: str. topic name
            sub: str. subscription name

        Returns:
            dict. json representation of the newly created AMS subscription

        """
        project_name = tenant.upper()
        url = self.get_url("subscriptions", sub, project_name)
        data = {"topic": "projects/{}/topics/{}".format(project_name, topic)}
        return self.put_resource(url, data)

    def create_tenant_user(self, tenant, role):
        """
        For a given tenant and user role create a new user tied to that tenant
        Args:
            tenant: str. tenant name
            role: str. user role (project_admin|consumer|publisher)

        Returns:
            dict: json representation of the newly created AMS user

        """
        project_name = tenant.upper()
        if role == "project_admin":
            username = "ams_{}_admin".format(tenant.lower())
        elif role == "archiver":
            username = "ams_{}_archiver".format(tenant.lower())
            # archiver is actually a consumer
            role = "consumer"
        else:
            username = "ams_{}_{}".format(tenant.lower(), role)

        
        url = self.get_url("users", username)
        data = {"projects": [{"project": project_name, "roles": [role]}]}
        return self.post_resource(url, data)

    def set_topic_acl(self, tenant, topic, acl):
        """
        For a given tenant, topic and acl list, update topic's acl

        Args:
            tenant: str. tenant name
            topic: str. topic name
            acl: list(str). list of authorized usernames

        Returns:
            dict. json representation of the updated AMS acl
        """
        project_name = tenant.upper()
        url = self.get_url("topics", topic, project_name, "modifyAcl")
        data = {"authorized_users": acl}
        return self.post_resource(url, data)

    def set_sub_acl(self, tenant, sub, acl):
        """
        For a given tenant, subscription and acl list, update subscription's acl

        Args:
            tenant: str. tenant name
            sub: str. subscription name
            acl: list(str). list of authorized usernames

        Returns:
            dict. json representation of the updated AMS acl
        """
        project_name = tenant.upper()
        url = self.get_url("subscriptions", sub, project_name, "modifyAcl")
        data = {"authorized_users": acl}
        return self.post_resource(url, data)

    def check_tenant(self, tenant):
        """
        For a given tenant check AMS for missing tenant definitions (topics,subs,users,acls)
        Args:
            tenant: str. tenant name

        Returns:
            dict: a dictionary containing missing topics,subs,users and acls

        """

        project = tenant.upper()

        # Things that sould be present in AMS definitions
        topics_lookup = ["sync_data", "metric_data"]
        subs_lookup = ["ingest_sync", "ingest_metric", "status_sync", "status_metric", "archive_metric"]
        users_lookup = ["project_admin", "publisher", "consumer", "archiver"]
        topic_acl_lookup = ["sync_data", "metric_data"]
        sub_acl_lookup = ["ingest_sync", "ingest_metric", "archive_metric"]

        # Initialize a dictionary with missing definitions
        missing = dict()
        missing["topics"] = list()
        missing["subs"] = list()
        missing["users"] = list()
        missing["topic_acls"] = list()
        missing["sub_acls"] = list()

        # Get tenant's AMS topics, subs and users
        topics = self.get_tenant_topics(tenant)
        subs = self.get_tenant_subs(tenant, topics)
        users = self.get_tenant_users(tenant)

        # If AMS definitions are None create empty lists
        if topics is None:
            topics = {}
        if subs is None:
            subs = {}
        if users is None:
            users = {}

        

        # For each expected topic check if it was indeed found in AMS or if it's missing
        for item in topics_lookup:
            if item not in topics.keys():
                missing["topics"].append(item)

        # For each expected sub check if it was indeed found in AMS or if it's missing
        for item in subs_lookup:
            if item not in subs.keys():
                missing["subs"].append(item)

        # For each expected user check if it was indeed found in AMS or if it's missing
        for item in users_lookup:
            if item not in users.keys():
                missing["users"].append(item)

        user_topics = []
        user_subs = []

        # Get publisher topics
        if "publisher" in users:
            user_topics = self.user_get_topics(users["publisher"], project)

        # Check expected topic acls if are missing
        for item in topic_acl_lookup:
            if item not in user_topics:
                missing["topic_acls"].append(item)
        # Get consumers subscriptions
        if "consumer" in users:
            user_subs = self.user_get_subs(users["consumer"], project)

        # check expected sub acls if are missing
        for item in sub_acl_lookup:
            if item not in user_subs:
                missing["sub_acls"].append(item)

        return missing

    def fill_missing(self, tenant, missing):
        """
        Giving a dictionary with missing tenant definitions (returned from check_tenant method) attempt to create
        the missing defnitions in AMS

        Args:
            tenant: str. tenant name
            missing: dict. tenant's missing definitions (topics,subs,users,acls)

        """

        # For each missing topic attempt to create it in AMS
        for topic in missing["topics"]:
            # create topic
            topic_new = self.create_tenant_topic(tenant, topic)
            log.info("Tenant:{} - created missing topic: {}".format(tenant, topic_new["name"]))

        # For each missing sub attempt to create it in AMS
        for sub in missing["subs"]:
            # create sub
            if sub.startswith("archive") and sub.endswith("metric"): 
                topic = "metric_data"
            if sub.endswith("metric"):
                topic = "metric_data"
            elif sub.endswith("sync"):
                topic = "sync_data"
            else:
                continue

            sub_new = self.create_tenant_sub(tenant, topic, sub)
            
            log.info("Tenant:{} - created missing subscription: {} on topic: {}".format(tenant, sub_new["name"],
                                                                                        sub_new["topic"]))

        # For each missing user attempt to create it in AMS
        for user in missing["users"]:
            # create user
            user_new = self.create_tenant_user(tenant, user)
            log.info("Tenant:{} - created missing user: {}".format(tenant, user_new["name"]))

        # For each missing topic acl attempt to set it in AMS
        for topic_acl in missing["topic_acls"]:
            acl = self.get_topic_acl(tenant, topic_acl)
            user_pub = "ams_{}_publisher".format(tenant.lower())
            if user_pub not in acl:
                acl.append(user_pub)

            r = self.set_topic_acl(tenant, topic_acl, acl)
            if r is not None:
                log.info("Tenant:{} - set missing acl on topic: {} for user: {}".format(tenant, topic_acl, user_pub))

        # For each missing subscription attempt to set it in AMS
        for sub_acl in missing["sub_acls"]:
            acl = self.get_sub_acl(tenant, sub_acl)
            if sub_acl.startswith("archive"):
                user_con = "ams_{}_archiver".format(tenant.lower())
            else:
                user_con = "ams_{}_consumer".format(tenant.lower())
            if user_con not in acl:
                acl.append(user_con)

            r = self.set_sub_acl(tenant, sub_acl, acl)
            if r is not None:
                log.info(
                    "Tenant:{} - set missing acl on subscription: {} for user: {}".format(tenant, sub_acl, user_con))

    def check_project_exists(self, tenant):
        """
        Given a tenant name check if corresponding AMS project exists
        If doesnt exist calls create_tenant_project() method to create it
        Args:
            tenant: str. tenant name

        Returns:
           dict. json representation of the project
        """
        # check if project exists
        project = self.get_tenant_project(tenant)
        if project is None:
            # if project doesn't exist attempt to create it
            log.info("{} project not found on ams".format(tenant))
            project = self.create_tenant_project(tenant)
            log.info("{} project created for tenant: {}".format(project["name"], tenant))
            return project
        log.info("{} project found for tenant: {}".format(project["name"], tenant))
        return project


def is_tenant_complete(missing):
    """
    Given a dict of missing tenant definitions, check if the missing
    definitions are empty thus the tenant is complete
    Args:
        missing: dict. tenant's missing definitions (topics,subs,users,acls)

    Returns:
        bool: True if tenant's missing definitions are empty

    """
    check_list = ["topics", "subs", "users", "topic_acls", "sub_acls"]
    for item in check_list:
        if len(missing[item]) > 0:
            return False
    return True


def run_ams_update(args):
    """
    Runs when this script is called from CLI.
    Runs for a specific tenant or all tenants in argo config
    Reads argo configuration tenant list. For each tenant, creates missing tenant's definitions
    and updates argo configuration with tenant's ams parameters

    Args:
        args: cli arguments

    """
    # Get configuration paths
    conf_paths = get_config_paths(args.config)

    # Get logger config file
    get_log_conf(conf_paths['log'])

    # Get main configuration and schema
    config = ArgoConfig(conf_paths["main"], conf_paths["schema"])

    ams_token = config.get("AMS", "access_token")
    ams_host = config.get("AMS", "endpoint").hostname
    ams_verify = config.get("AMS", "verify")
    ams_proxy = config.get("AMS", "proxy")
    if ams_proxy:
        ams_proxy = ams_proxy.geturl()
    log.info("ams api used {}".format(ams_host))

    tenant_list = config.get("API", "tenants")
    ams = ArgoAmsClient(ams_host, ams_token, ams_verify, ams_proxy)

    if args.tenant is not None:
        # Check if tenant exists in argo configuarion
        if args.tenant not in tenant_list:
            log.fatal("Tenant:{} is not in argo configuration - exiting...".format(args.tenant))
            sys.exit(1)
        # Check only the tenant that user specified
        check_tenants = [args.tenant]
    else:
        # Check all tenants that are specified in argo configuration
        check_tenants = tenant_list

    for tenant in check_tenants:
        ams.check_project_exists(tenant)
        missing = ams.check_tenant(tenant)
        if is_tenant_complete(missing):
            log.info("Tenant {} definition on AMS is complete!".format(tenant))
        else:
            ams.fill_missing(tenant, missing)
        # Update tenant configuration
        ams.update_tenant_configuration(tenant, config)

    # Save changes to designated configuration file
    config.save_as(config.conf_path)


if __name__ == '__main__':
    # Feed Argument parser with the description of the arguments we need
    arg_parser = ArgumentParser(
        description="update ams with relevant tenant configuration")
    arg_parser.add_argument(
        "-c", "--config", help="config", dest="config", metavar="STRING")
    arg_parser.add_argument(
        "-t", "--tenant", help="tenant", dest="tenant", metavar="STRING", required=False, default=None)
    arg_parser.add_argument(
        "-v", "--verify", help="verify", dest="verify", action="store_true")

    # Parse the command line arguments accordingly and introduce them to the run method
    sys.exit(run_ams_update(arg_parser.parse_args()))
