import logging

import requests

from argo_config import ArgoConfig

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 30


class ArgoWebApi:

    def __init__(self, config: ArgoConfig):
        self.config = config

    def create_user(
        self,
        tenant_id: str,
        tenant_name: str,
        username: str,
        role: str,
    ):
        """Http call to web-api to create a user"""
        logger.debug(
            f"tenant: {tenant_name} ({tenant_id}) - web-api creating user {username}..."
        )

        payload = {
            "name": username,
            "email": self.config.argo_ops_email,
            "roles": [role],
        }

        url = f"https://{self.config.web_api_endpoint}/api/v2/admin/tenants/{tenant_id}/users"
        headers = {
            "x-api-key": self.config.web_api_token,
            "Accept": "application/json",
        }
        response = requests.post(
            url, json=payload, headers=headers, timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()

        logger.info(
            f"tenant: {tenant_name} ({tenant_id}) - web-api user created: {username}"
        )

        # get newly created user details by id
        user_id = response.json().get("data").get("id")

        return self.get_user(tenant_id, tenant_name, user_id)

    def get_username(
        self, tenant_id: str, tenant_name: str, username: str
    ) -> dict | None:
        """Http call to web-api to check if a username exists"""
        logger.debug(
            f"tenant: {tenant_name} ({tenant_id}) - web-api checking username: {username}..."
        )

        url = f"https://{self.config.web_api_endpoint}/api/v2/admin/tenants/{tenant_id}/users"
        headers = {
            "x-api-key": self.config.web_api_token,
            "Accept": "application/json",
        }
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        users = response.json().get("data")
        if users:
            return next((user for user in users if user["name"] == username), None)
        return None

    def get_user(self, tenant_id: str, tenant_name: str, user_id: str):
        """Http call to web-api to get a specific user of a tenant"""
        logger.debug(
            f"tenant: {tenant_name} ({tenant_id}) - web-api getting user {user_id}..."
        )

        url = f"https://{self.config.web_api_endpoint}/api/v2/admin/tenants/{tenant_id}/users/{user_id}"
        headers = {
            "x-api-key": self.config.web_api_token,
            "Accept": "application/json",
        }
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        return response.json()


def init_compute_engine(
    config: ArgoConfig,
    tenant_id: str,
    tenant_name: str,
) -> bool:
    """Initialise compute engine users"""

    web_api = ArgoWebApi(config)

    engine_username = f"argo_engine_{tenant_name}"
    monbox_username = f"monbox_{tenant_name}"
    probe_username = f"mobnox_probe_{tenant_name}"
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

    for username, role in user_roles:

        logger.info(f"engine tenant {tenant_name} - creating user: {username}")
        # check if user exists already
        user = web_api.get_username(tenant_id, tenant_name, username)
        if user:
            # user exists
            logger.info(f"engine tenant {tenant_name} - user: {username} exists!")
            if username == engine_username:
                config.set_tenant_web_api_access(
                    tenant_id, tenant_name, user.get("api_key")
                )
                config.save()

        else:
            # create the user
            user = web_api.create_user(tenant_id, tenant_name, username, role)
            if user and username == engine_username:
                config.set_tenant_web_api_access(
                    tenant_id, tenant_name, user.get("api_key")
                )
                config.save()

    return True
