import logging
import os
import uuid
from urllib.parse import urlparse

import requests
from jinja2 import Environment, FileSystemLoader

from argo_config import ArgoConfig
from argo_web_api import ArgoWebApi, TopoItem

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 30
TEMPLATE_FILE = "desy.cron.j2"
CRON_DIR = "/etc/cron.d"


def get_desy_topology(url: str):
    """Retrieve topology from desy marketplace"""
    logger.debug(f"retrieving topology from desy marketplace: {url}")

    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logger.warning(f"desy marketplace has no topology: {url}")
            return []
        else:
            raise

    return response.json()


def create_desy_cron(config: ArgoConfig, tenant_name: str) -> bool:
    """Create cron file for desy integration"""
    template_dir = os.path.dirname(os.path.abspath(__file__))

    env = Environment(loader=FileSystemLoader(template_dir), keep_trailing_newline=True)
    template = env.get_template(TEMPLATE_FILE)
    content = template.render(tenant=tenant_name)

    filepath = os.path.join(CRON_DIR, f"argo_desy_{tenant_name}")
    try:
        with open(filepath, "w") as f:
            f.write(content)
        os.chmod(filepath, 0o644)
        return True
    except PermissionError:
        logger.error("Permission error while creating desy integration cron file")

    return False


def remove_desy_cron(tenant_name: str) -> bool:
    """Remove cron file for desy integration"""
    logger.info(f"Attempting to remove desy cron file: argo_desy_{tenant_name}")
    filepath = os.path.join(CRON_DIR, f"argo_desy_{tenant_name}")
    try:
        os.remove(filepath)
        logger.info(f"File removed: argo_desy_{tenant_name}")
        return True
    except FileNotFoundError:
        logger.info(f"File not found: argo_desy_{tenant_name}")
        return True
    except PermissionError:
        logger.error("Permission error while removing desy integration cron file")

    return False


def get_desy_feed(config: ArgoConfig, tenant_name: str) -> str:

    # get local tenant configuration
    tenant = config.tenants.get(tenant_name)
    tenant_id = tenant["id"]
    tenant_web_api_token = tenant["web_api_token"]

    # init connection to web-api
    web_api = ArgoWebApi(config)

    data = web_api.get_topology_feed(tenant_id, tenant_name, tenant_web_api_token)

    if len(data) > 0:
        feed = data[0]
        if feed.get("type") == "desy-marketplace":
            feed_url = feed.get("feed_url")
            if feed_url:
                return feed_url

    logger.warning("Tenant has no desy-topology feed - aborting...")
    return ""


def init_desy(config: ArgoConfig, tenant_name: str) -> bool:
    url = get_desy_feed(config, tenant_name)
    if url:
        create_desy_cron(config, tenant_name)
        update_desy_topology(config, tenant_name, url)
        return True
    return False


def update_desy_topology(config: ArgoConfig, tenant_name: str, url: str):

    # get local tenant configuration
    tenant = config.tenants.get(tenant_name)
    tenant_id = tenant["id"]
    tenant_web_api_token = tenant["web_api_token"]

    # init connection to web-api
    web_api = ArgoWebApi(config)

    # get desy topology from remote endpoint
    desy = get_desy_topology(url)

    old_endpoints = web_api.get_topology(
        tenant_id, tenant_name, tenant_web_api_token, TopoItem.ENDPOINTS
    )
    old_groups = web_api.get_topology(
        tenant_id, tenant_name, tenant_web_api_token, TopoItem.GROUPS
    )

    index_old_endpoints = {}
    index_old_groups = {}
    new_endpoints = []
    new_groups = []

    for item in old_endpoints:
        info_URL = item.get("tags").get("info_URL")
        if info_URL:
            index_old_endpoints[info_URL] = item

    for item in old_groups:
        old_group_name = item.get("subgroup")
        index_old_groups[old_group_name] = item

    changed_endpoints = False
    changed_groups = False

    for item in desy:

        item_URL = item.get("webpage_url")
        item_name = item.get("name")
        item_contact = item.get("helpdesk_email")

        old_endpoint = index_old_endpoints.get(item_URL)
        old_group = index_old_groups.get(item_name)

        if old_group:

            # Remove date key because it is not relevant for comparison
            old_group.pop("date", None)
            new_group = {
                "group": tenant_name,
                "type": "PROJECT",
                "subgroup": item_name,
                "tags": {"monitored": "1", "scope": tenant_name},
                "notifications": {
                    "contacts": [item_contact],
                    "enabled": True,
                },
            }

            new_groups.append(new_group)
            if new_group != old_group:
                changed_groups = True

        else:
            new_groups.append(
                {
                    "group": tenant_name,
                    "type": "PROJECT",
                    "subgroup": item_name,
                    "tags": {"monitored": "1", "scope": tenant_name},
                    "notifications": {
                        "contacts": [item_contact],
                        "enabled": True,
                    },
                }
            )
            changed_groups = True

        if old_endpoint:

            item_uuid = old_endpoint.get("tags", {}).get("info_ID", {})
            hostname = urlparse(item_URL).hostname
            if not item_uuid:
                item_uuid = str(uuid.uuid4())

            # Remove date key because it is not relevant for comparison
            old_endpoint.pop("date", None)

            new_endpoint = {
                "group": item_name,
                "type": "SERVICEGROUPS",
                "service": "webportal",
                "hostname": f"{hostname}_{item_uuid}",
                "tags": {
                    "hostname": hostname,
                    "info_ID": item_uuid,
                    "info_URL": item_URL,
                    "monitored": "1",
                },
                "notifications": {
                    "contacts": [item_contact],
                    "enabled": True,
                },
            }

            new_endpoints.append(new_endpoint)

            if new_endpoint != old_endpoint:

                changed_endpoints = True
        else:
            # create the new endpoint
            item_uuid = str(uuid.uuid4())
            hostname = urlparse(item_URL).hostname
            new_endpoints.append(
                {
                    "group": item_name,
                    "type": "SERVICEGROUPS",
                    "service": "webportal",
                    "hostname": f"{hostname}_{item_uuid}",
                    "tags": {
                        "hostname": hostname,
                        "info_ID": item_uuid,
                        "info_URL": item_URL,
                        "monitored": "1",
                    },
                    "notifications": {
                        "contacts": [item_contact],
                        "enabled": True,
                    },
                }
            )
            changed_endpoints = True

    if not changed_endpoints and not changed_groups:
        if len(new_endpoints) == len(old_endpoints) and len(new_groups) == len(
            old_groups
        ):
            logger.info(
                f"Desy-connector: Topology for tenant {tenant_name} remains the same - no upload"
            )
            return

    logger.info(
        f"Desy-connector: Topology changes found for tenant {tenant_name} - will upload"
    )

    web_api.create_topology_groups(
        tenant_id, tenant_name, tenant_web_api_token, new_groups
    )
    web_api.create_topology_endpoints(
        tenant_id, tenant_name, tenant_web_api_token, new_endpoints
    )
