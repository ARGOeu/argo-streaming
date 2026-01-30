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

    # map users and roles and create them
    user_roles = [
        (engine_username, "admin"),
        (monbox_username, "admin"),
        (probe_username, "viewer"),
        (ui_username, "admin_ui"),
        (poem_username, "admin"),
        (poem_viewer_username, "viewer"),
    ]

    engine_user_key = None

    for username, role in user_roles:

        logger.info(f"engine tenant {tenant_name} - creating user: {username}")
        # check if user exists already
        user = web_api.get_username(tenant_id, tenant_name, username)
        if user:
            # user exists
            logger.info(f"engine tenant {tenant_name} - user: {username} exists!")
            if username == engine_username:
                engine_user_key = user.get("api_key")

        else:
            # create the user
            user = web_api.create_user(tenant_id, tenant_name, username, role)
            if user and username == engine_username:
                engine_user_key = user.get("api_key")

    # if engine_user_key save it to config and create the ops profile

    if engine_user_key:

        config.set_tenant_web_api_access(tenant_id, tenant_name, engine_user_key)
        config.save()

        with open(config.default_ops_profile_file, "r") as f:

            payload = json.load(f)
            web_api.create_ops_profile(tenant_id, tenant_name, engine_user_key, payload)

    return True
