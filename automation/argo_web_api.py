import logging
from typing import Dict, Optional

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

    def update_tenant_db_info(
        self,
        tenant_id: str,
        tenant_name: str,
        store: str,
        server: str,
        port: int,
        database: str,
    ):
        """Http call to web-api to update the database conf"""
        logger.debug(
            f"tenant: {tenant_name} ({tenant_id}) - web-api updating db conf..."
        )

        payload = {
            "db_conf": [
                {
                    "store": store,
                    "server": server,
                    "port": port,
                    "database": database,
                    "username": "",
                    "password": "",
                }
            ]
        }

        url = f"https://{self.config.web_api_endpoint}/api/v2/admin/tenants/{tenant_id}/db-conf"
        headers = {
            "x-api-key": self.config.web_api_token,
            "Accept": "application/json",
        }
        response = requests.put(
            url, json=payload, headers=headers, timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()

        logger.info(
            f"tenant: {tenant_name} ({tenant_id}) - web-api updating db conf updated"
        )

    def create_ops_profile(
        self,
        tenant_id: str,
        tenant_name: str,
        tenant_access_token: str,
        payload: object,
    ):
        """Http call to web-api to create ops profile"""
        logger.debug(
            f"tenant: {tenant_name} ({tenant_id}) - web-api creating default ops profile..."
        )

        url = f"https://{self.config.web_api_endpoint}/api/v2/operations_profiles"
        headers = {
            "x-api-key": tenant_access_token,
            "Accept": "application/json",
        }
        try:
            response = requests.post(
                url, json=payload, headers=headers, timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 409:
                logger.warning(
                    f"tenant: {tenant_name} ({tenant_id}) - web-api ops profile already exists"
                )
                return
            else:
                raise

        logger.info(
            f"tenant: {tenant_name} ({tenant_id}) - web-api ops profile created"
        )

    def get_username(
        self, tenant_id: str, tenant_name: str, username: str
    ) -> Optional[Dict]:
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

        users = response.json().get("data")
        if users and len(users) > 0:
            return users[0]

        return None
