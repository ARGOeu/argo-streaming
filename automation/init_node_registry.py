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
TEMPLATE_FILE = "node-registry.cron.j2"
CRON_DIR = "/etc/cron.d"
CRON_PREFIX = "argo_node_registry"
SERVICE_TYPE = "webportal"


def get_node_registry(url: str, token: str):
    """Retrieve the list of nodes from the node registry"""
    logger.debug(f"retrieving nodes from node registry: {url}")

    headers = {"x-api-key": token, "Accept": "application/json"}

    try:
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            logger.warning(f"node registry has no nodes: {url}")
            return []
        raise

    data = response.json()

    # tolerate both a bare list and a wrapped {"results": [...]} payload
    if isinstance(data, dict):
        for key in ("results", "data", "nodes"):
            if isinstance(data.get(key), list):
                return data[key]
        return []

    return data


def get_node_capabilities(node_endpoint: str):
    """Retrieve the capability list of a single node (unauthenticated GET)"""
    logger.debug(f"retrieving capabilities from node endpoint: {node_endpoint}")

    try:
        response = requests.get(node_endpoint, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.warning(f"unable to retrieve node endpoint {node_endpoint}: {e}")
        return []

    try:
        data = response.json()
    except ValueError:
        logger.warning(f"node endpoint {node_endpoint} did not return valid json")
        return []

    return data.get("capabilities") or []


def create_node_registry_cron(config: ArgoConfig, tenant_name: str) -> bool:
    """Create cron file for node registry integration"""
    template_dir = os.path.dirname(os.path.abspath(__file__))

    env = Environment(loader=FileSystemLoader(template_dir), keep_trailing_newline=True)
    template = env.get_template(TEMPLATE_FILE)
    content = template.render(tenant=tenant_name)

    filepath = os.path.join(CRON_DIR, f"{CRON_PREFIX}_{tenant_name}")
    try:
        with open(filepath, "w") as f:
            f.write(content)
        os.chmod(filepath, 0o644)
        return True
    except PermissionError:
        logger.error(
            "Permission error while creating node registry integration cron file"
        )

    return False


def remove_node_registry_cron(tenant_name: str) -> bool:
    """Remove cron file for node registry integration"""
    filename = f"{CRON_PREFIX}_{tenant_name}"
    logger.info(f"Attempting to remove node registry cron file: {filename}")

    filepath = os.path.join(CRON_DIR, filename)
    try:
        os.remove(filepath)
        logger.info(f"File removed: {filename}")
        return True
    except FileNotFoundError:
        logger.info(f"File not found: {filename}")
        return True
    except PermissionError:
        logger.error(
            "Permission error while removing node registry integration cron file"
        )

    return False


def init_node_registry(config: ArgoConfig, tenant_name: str) -> bool:
    if config.node_registry_token and config.node_registry_token:
        create_node_registry_cron(config, tenant_name)
        update_node_registry_topology(config, tenant_name)
        return True
    return False


def build_group(tenant_name: str, node_name: str) -> dict:
    return {
        "group": tenant_name,
        "type": "PROJECT",
        "subgroup": node_name,
        "tags": {"monitored": "1", "scope": tenant_name},
    }


def build_endpoint(node_name: str, capability: dict, item_uuid: str) -> dict:
    item_URL = capability.get("endpoint")
    hostname = str(urlparse(item_URL).hostname)

    tags = {
        "hostname": hostname,
        "info_ID": item_uuid,
        "info_URL": item_URL,
        "monitored": "1",
    }

    capability_type = capability.get("capability_type")
    if capability_type:
        tags["info_capability_type"] = capability_type
        tags["labels"] = capability_type

    protocol = capability.get("protocol")
    if protocol:
        tags["info_protocol"] = protocol

    return {
        "group": node_name,
        "type": "SERVICEGROUPS",
        "service": SERVICE_TYPE,
        "hostname": f"{hostname}_{item_uuid}",
        "tags": tags,
    }


def update_node_registry_topology(config: ArgoConfig, tenant_name: str):

    url = config.node_registry_url
    token = config.node_registry_token

    # get local tenant configuration
    tenant = config.tenants.get(tenant_name)
    tenant_id = tenant["id"]
    tenant_web_api_token = tenant["web_api_token"]

    # init connection to web-api
    web_api = ArgoWebApi(config)

    # get node list from the remote node registry
    nodes = get_node_registry(url, token)

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
        info_URL = item.get("tags", {}).get("info_URL")
        if info_URL:
            # an endpoint is identified by its group + capability url
            index_old_endpoints[(item.get("group"), info_URL)] = item

    for item in old_groups:
        index_old_groups[item.get("subgroup")] = item

    changed_endpoints = False
    changed_groups = False

    for node in nodes:

        node_name = node.get("name")
        node_endpoint = node.get("node_endpoint")

        if not node_name:
            logger.warning("node registry entry without a name - skipping")
            continue

        new_group = build_group(tenant_name, node_name)
        new_groups.append(new_group)

        old_group = index_old_groups.get(node_name)
        if old_group:
            # Remove date key because it is not relevant for comparison
            old_group.pop("date", None)
            if new_group != old_group:
                changed_groups = True
        else:
            changed_groups = True

        if not node_endpoint:
            logger.warning(f"node {node_name} has no node_endpoint - skipping")
            continue

        for capability in get_node_capabilities(node_endpoint):

            item_URL = capability.get("endpoint")
            if not item_URL:
                logger.warning(
                    f"node {node_name} has a capability without an endpoint - skipping"
                )
                continue

            old_endpoint = index_old_endpoints.get((node_name, item_URL))

            if old_endpoint:
                item_uuid = old_endpoint.get("tags", {}).get("info_ID")
                if not item_uuid:
                    item_uuid = str(uuid.uuid4())

                new_endpoint = build_endpoint(node_name, capability, item_uuid)
                new_endpoints.append(new_endpoint)

                # Remove date key because it is not relevant for comparison
                old_endpoint.pop("date", None)
                if new_endpoint != old_endpoint:
                    changed_endpoints = True
            else:
                item_uuid = str(uuid.uuid4())
                new_endpoints.append(build_endpoint(node_name, capability, item_uuid))
                changed_endpoints = True

    if not changed_endpoints and not changed_groups:
        if len(new_endpoints) == len(old_endpoints) and len(new_groups) == len(
            old_groups
        ):
            logger.info(
                f"Node-registry-connector: Topology for tenant {tenant_name} "
                "remains the same - no upload"
            )
            return

    logger.info(
        f"Node-registry-connector: Topology changes found for tenant {tenant_name} "
        " - will upload"
    )

    web_api.create_topology_groups(
        tenant_id, tenant_name, tenant_web_api_token, new_groups
    )
    web_api.create_topology_endpoints(
        tenant_id, tenant_name, tenant_web_api_token, new_endpoints
    )
