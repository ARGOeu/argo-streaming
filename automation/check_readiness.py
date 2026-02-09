import datetime
import logging

import requests

from argo_config import ArgoConfig
from argo_web_api import ArgoWebApi, TopoItem

REQUEST_TIMEOUT = 30

logger = logging.getLogger(__name__)


def check_hdfs(config: ArgoConfig, tenant_id: str, tenant_name: str) -> bool:
    """Checks if data for today exist in hdfs tenant folders"""

    logger.debug(
        f"tenant: {tenant_name} ({tenant_id}) - retrieving report information from web-api..."
    )
    today = datetime.date.today().strftime("%Y-%m-%d")
    url = f"{config.hdfs_check_path}/{tenant_name}/mdata/{today}?op=LISTSTATUS"
    headers = {
        "Accept": "application/json",
    }

    try:
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        result = response.json().get("FileStatuses").get("FileStatus")
        if len(result) > 0:
            return True
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logger.warning(
                f"tenant: {tenant_name} ({tenant_id}) - tenant path not found in hdfs"
            )
            return False
        else:
            raise

    return False


def check_readiness(config: ArgoConfig, tenant_id: str, tenant_name: str) -> object:
    """Checks tenants readiness by doing web-api requests to see if topology and
    reports are defined and also by checking if data are present both in ams and hdfs"""

    web_api = ArgoWebApi(config)

    # get access token from config file
    tenant_token = config.tenants.get(tenant_name, {}).get("web_api_token")

    # check if topology exists
    topology_ready = True
    topology_msg = []
    topo_endpoints = web_api.get_topology(
        tenant_id, tenant_name, tenant_token, TopoItem.ENDPOINTS
    )
    topo_groups = web_api.get_topology(
        tenant_id, tenant_name, tenant_token, TopoItem.GROUPS
    )
    topo_service_types = web_api.get_topology(
        tenant_id, tenant_name, tenant_token, TopoItem.SERVICE_TYPES
    )

    if len(topo_endpoints) > 0:
        topology_msg.append("Topology endpoints are set.")
    else:
        topology_msg.append("Topology endpoints are missing!")
        topology_ready = False

    if len(topo_groups) > 0:
        topology_msg.append("Topology groups are set.")
    else:
        topology_msg.append("Topology groups are missing!")
        topology_ready = False

    if len(topo_service_types) > 0:
        topology_msg.append("Topology service-types are set.")
    else:
        topology_msg.append("Topology service-types are missing!")
        topology_ready = False

    # check reports
    reports_ready = True
    reports_msg = "Tenant has at least one report"

    reports = web_api.get_reports(tenant_id, tenant_name, tenant_token)

    if len(reports) < 0:
        reports_msg = "Tenant has no reports!"

    # check metric data in hdfs
    hdfs_ready = True
    hdfs_msg = "Tenant has metric data in HDFS for today"
    hdfs_check = check_hdfs(config, tenant_id, tenant_name)

    if not hdfs_check:
        hdfs_ready = False
        hdfs_msg = "Tenant doesn't have metric data in HDFS for today!"

    # update the state
    payload = {
        "data": {"ready": hdfs_ready, "message": hdfs_msg},
        "topology": {"ready": topology_ready, "message": " ".join(topology_msg)},
        "reports": {"ready": reports_ready, "message": reports_msg},
        "last_check": datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y:%m:%dT%H:%M:%SZ"
        ),
    }

    # update the payload to web-api
    result = web_api.update_ready_state(tenant_id, tenant_name, payload)
    if result:
        return True
    return False
