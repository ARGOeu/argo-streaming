import json
import logging

from argo_config import ArgoConfig
from argo_web_api import ArgoWebApi

logger = logging.getLogger(__name__)


def init_compute_engine(
    config: ArgoConfig,
    tenant_id: str,
    tenant_name: str,
) -> bool:
    """Initialise compute engine users"""

    web_api = ArgoWebApi(config)

    engine_username = f"argo_engine_{tenant_name}"
    monbox_username = f"argo_monbox_{tenant_name}"
    probe_username = f"argo_probe_{tenant_name}"
    ui_username = f"argo_ui_{tenant_name}"
    poem_username = f"argo_poem_admin_{tenant_name}"
    poem_viewer_username = f"argo_poem_viewer_{tenant_name}"
    connector_username = f"argo_connector_{tenant_name}"

    # map users roles and components
    users_roles_comps = [
        (engine_username, "admin", "engine"),
        (monbox_username, "admin", "monbox"),
        (probe_username, "viewer", "probe"),
        (ui_username, "admin_ui", "ui"),
        (poem_username, "admin", "poem-admin"),
        (poem_viewer_username, "viewer", "poem-viewer"),
        (connector_username,"admin","connector")
    ]

    engine_user_key = None

    for username, role, component in users_roles_comps:

        logger.info(f"engine tenant {tenant_name} - creating user: {username}")
        # check if user for specific component exists
        user = web_api.get_component_user(tenant_id, tenant_name, username)
        if user:
            # user exists
            logger.info(f"engine tenant {tenant_name} - user: {username} exists!")
            if username == engine_username:
                engine_user_key = user.get("api_key")

        else:
            # create the user
            user = web_api.create_user(
                tenant_id, tenant_name, username, role, component
            )
            if user and username == engine_username:
                engine_user_key = user.get("api_key")

    # if engine_user_key save it to config and create the ops profile

    if engine_user_key:

        config.set_tenant_web_api_access(tenant_id, tenant_name, engine_user_key)
        config.save()

        ops_id = None
        metric_id = None
        agg_id = None

        with open(config.default_ops_profile_file, "r") as f:

            ops_payload = json.load(f)
            ops_id = web_api.create_ops_profile(
                tenant_id, tenant_name, engine_user_key, ops_payload
            )

        with open(config.default_metric_profile_file, "r") as f:

            metric_payload = json.load(f)
            metric_id = web_api.create_metric_profile(
                tenant_id, tenant_name, engine_user_key, metric_payload
            )

        with open(config.default_agg_profile_file, "r") as f:

            agg_payload = json.load(f)
            agg_payload["namespace"] = tenant_name
            agg_payload["metric_profile"]["id"] = metric_id
            agg_id = web_api.create_aggregation_profile(
                tenant_id, tenant_name, engine_user_key, agg_payload
            )

        with open(config.default_report_file, "r") as f:

            report_payload = json.load(f)
            profiles = [
                {"id": ops_id, "type": "operations"},
                {"id": metric_id, "type": "metric"},
                {"id": agg_id, "type": "aggregation"},
            ]
            report_payload["profiles"] = profiles
            web_api.create_default_report(
                tenant_id, tenant_name, engine_user_key, report_payload
            )

        with open(config.default_services_file, "r") as f:

            services_payload = json.load(f)
            web_api.create_topology_service_types(
                tenant_id, tenant_name, engine_user_key, services_payload
            )

    return True
