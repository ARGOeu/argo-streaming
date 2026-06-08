import logging
from enum import Enum
from typing import Dict, Optional

import requests

from argo_config import ArgoConfig

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 30


class TopoItem(Enum):
    ENDPOINTS = "endpoints"
    GROUPS = "groups"
    SERVICE_TYPES = "service-types"


class ArgoWebApi:

    def __init__(self, config: ArgoConfig):
        self.config = config

    def create_user(
        self, tenant_id: str, tenant_name: str, username: str, role: str, component: str
    ):
        """Http call to web-api to create a user"""
        logger.debug(
            f"tenant: {tenant_name} ({tenant_id}) - web-api creating user {username}..."
        )

        payload = {
            "name": username,
            "email": self.config.argo_ops_email,
            "roles": [role],
            "component": component,
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

    def get_topology_feed(
        self,
        tenant_id: str,
        tenant_name: str,
        tenant_access_token: str,
    ):
        """Retrieve topology feed for specific tenant"""
        logger.debug(
            f"tenant: {tenant_name} ({tenant_id}) - retrieving topology feed from web-api..."
        )
        url = f"https://{self.config.web_api_endpoint}/api/v2/feeds/topology"
        headers = {
            "x-api-key": tenant_access_token,
            "Accept": "application/json",
        }

        try:
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(
                    f"tenant: {tenant_name} ({tenant_id}) - has no topology feed"
                )
                return []
            else:
                raise

        return response.json().get("data")


    def get_topology(
        self,
        tenant_id: str,
        tenant_name: str,
        tenant_access_token: str,
        topology_item: TopoItem,
    ):
        """Retrieve topology items for specific tenant"""
        logger.debug(
            f"tenant: {tenant_name} ({tenant_id}) - retrieving report information from web-api..."
        )
        url = f"https://{self.config.web_api_endpoint}/api/v2/topology/{topology_item.value}"
        headers = {
            "x-api-key": tenant_access_token,
            "Accept": "application/json",
        }

        try:
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(
                    f"tenant: {tenant_name} ({tenant_id}) - has no topology"
                )
                return []
            else:
                raise

        return response.json().get("data")

    def get_reports(
        self,
        tenant_id: str,
        tenant_name: str,
        tenant_access_token: str,
    ):
        """Retrieve report names and report ids for specific tenant"""
        logger.debug(
            f"tenant: {tenant_name} ({tenant_id}) - retrieving report information from web-api..."
        )
        url = f"https://{self.config.web_api_endpoint}/api/v2/reports"
        headers = {
            "x-api-key": tenant_access_token,
            "Accept": "application/json",
        }

        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        return response.json().get("data")

    def get_report_ids(
        self, tenant_id: str, tenant_name: str, tenant_access_token: str
    ):
        """Retrieve report names and ids for specific tenant"""
        reports = self.get_reports(tenant_id, tenant_name, tenant_access_token)
        return {item["info"]["name"]: item["id"] for item in reports}

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
            logger.info(
                f"tenant: {tenant_name} ({tenant_id}) - web-api ops profile created"
            )
            return response.json()["data"]["id"]

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 409:
                logger.warning(
                    f"tenant: {tenant_name} ({tenant_id}) - web-api ops profile already exists"
                )
                return
            else:
                raise

    def create_metric_profile(
        self,
        tenant_id: str,
        tenant_name: str,
        tenant_access_token: str,
        payload: object,
    ):
        """Http call to web-api to create metric profile"""
        logger.debug(
            f"tenant: {tenant_name} ({tenant_id}) - web-api creating default metric profile..."
        )

        url = f"https://{self.config.web_api_endpoint}/api/v2/metric_profiles"
        headers = {
            "x-api-key": tenant_access_token,
            "Accept": "application/json",
        }
        try:
            response = requests.post(
                url, json=payload, headers=headers, timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            logger.info(
                f"tenant: {tenant_name} ({tenant_id}) - web-api metric profile created"
            )
            return response.json()["data"]["id"]

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 409:
                logger.warning(
                    f"tenant: {tenant_name} ({tenant_id}) - web-api metric profile already exists"
                )
                return
            else:
                raise

    def create_aggregation_profile(
        self,
        tenant_id: str,
        tenant_name: str,
        tenant_access_token: str,
        payload: object,
    ):
        """Http call to web-api to create aggregation profile"""
        logger.debug(
            f"tenant: {tenant_name} ({tenant_id}) - web-api creating default aggregation profile..."
        )

        url = f"https://{self.config.web_api_endpoint}/api/v2/aggregation_profiles"
        headers = {
            "x-api-key": tenant_access_token,
            "Accept": "application/json",
        }
        try:
            response = requests.post(
                url, json=payload, headers=headers, timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            logger.info(
                f"tenant: {tenant_name} ({tenant_id}) - web-api aggregation profile created"
            )
            return response.json()["data"]["id"]

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 409:
                logger.warning(
                    f"tenant: {tenant_name} ({tenant_id}) - web-api aggregation profile already exists"
                )
                return
            else:
                raise

    def create_default_report(
        self,
        tenant_id: str,
        tenant_name: str,
        tenant_access_token: str,
        payload: object,
    ):
        """Http call to web-api to create report"""
        logger.debug(
            f"tenant: {tenant_name} ({tenant_id}) - web-api creating default report..."
        )

        url = f"https://{self.config.web_api_endpoint}/api/v2/reports"
        headers = {
            "x-api-key": tenant_access_token,
            "Accept": "application/json",
        }
        try:
            response = requests.post(
                url, json=payload, headers=headers, timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            logger.info(
                f"tenant: {tenant_name} ({tenant_id}) - web-api default report created"
            )
            return response.json()["data"]["id"]

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 409:
                logger.warning(
                    f"tenant: {tenant_name} ({tenant_id}) - web-api default report already exists"
                )
                return
            else:
                raise


    def create_topology_groups(
            self,
            tenant_id: str,
            tenant_name: str,
            tenant_access_token: str,
            payload: object,
        ):
        """Http call to web-api to create topology groups"""
        logger.debug(
            f"tenant: {tenant_name} ({tenant_id}) - web-api creating topology groups..."
        )

        url = f"https://{self.config.web_api_endpoint}/api/v2/topology/groups"
        headers = {
            "x-api-key": tenant_access_token,
            "Accept": "application/json",
        }
        try:
            response = requests.post(
                url, json=payload, headers=headers, timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            logger.info(
                f"tenant: {tenant_name} ({tenant_id}) - web-api topology groups created"
            )

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 409:
                logger.warning(
                    f"tenant: {tenant_name} ({tenant_id}) - web-api topology groups already exist for specific date"
                )
                return
            else:
                raise


    def create_topology_service_types(
        self,
        tenant_id: str,
        tenant_name: str,
        tenant_access_token: str,
        payload: object,
    ):
        """Http call to web-api to create service types"""
        logger.debug(
            f"tenant: {tenant_name} ({tenant_id}) - web-api creating default service-types..."
        )

        url = f"https://{self.config.web_api_endpoint}/api/v2/topology/service-types"
        headers = {
            "x-api-key": tenant_access_token,
            "Accept": "application/json",
        }
        try:
            response = requests.post(
                url, json=payload, headers=headers, timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            logger.info(
                f"tenant: {tenant_name} ({tenant_id}) - web-api topology service-types created"
            )

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 409:
                logger.warning(
                    f"tenant: {tenant_name} ({tenant_id}) - web-api topology service-types already exist for specific date"
                )
                return
            else:
                raise

    def create_topology_endpoints(
        self,
        tenant_id: str,
        tenant_name: str,
        tenant_access_token: str,
        payload: object,
    ):
        """Http call to web-api to create endpoints"""
        logger.debug(
            f"tenant: {tenant_name} ({tenant_id}) - web-api creating endpoints..."
        )

        url = f"https://{self.config.web_api_endpoint}/api/v2/topology/endpoints?force=true"
        headers = {
            "x-api-key": tenant_access_token,
            "Accept": "application/json",
        }
        try:
            response = requests.post(
                url, json=payload, headers=headers, timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            logger.info(
                f"tenant: {tenant_name} ({tenant_id}) - web-api topology endpoints created"
            )

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 409:
                logger.warning(
                    f"tenant: {tenant_name} ({tenant_id}) - web-api topology endpoints already exist for specific date"
                )
                return
            else:
                raise

    def create_topology_groups(
        self,
        tenant_id: str,
        tenant_name: str,
        tenant_access_token: str,
        payload: object,
    ):
        """Http call to web-api to create groups"""
        logger.debug(
            f"tenant: {tenant_name} ({tenant_id}) - web-api creating groups..."
        )

        url = f"https://{self.config.web_api_endpoint}/api/v2/topology/groups?force=true"
        headers = {
            "x-api-key": tenant_access_token,
            "Accept": "application/json",
        }
        try:
            response = requests.post(
                url, json=payload, headers=headers, timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            logger.info(
                f"tenant: {tenant_name} ({tenant_id}) - web-api topology groups created"
            )

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 409:
                logger.warning(
                    f"tenant: {tenant_name} ({tenant_id}) - web-api topology groups already exist for specific date"
                )
                return
            else:
                raise


    def update_ready_state(
        self,
        tenant_id: str,
        tenant_name: str,
        payload: object,
    ):
        """Http call to web-api to update the readiness state for a specific tenant"""
        logger.debug(
            f"tenant: {tenant_name} ({tenant_id}) - web-api update readiness state..."
        )

        url = f"https://{self.config.web_api_endpoint}/api/v2/admin/tenants/{tenant_id}/ready"
        headers = {
            "x-api-key": self.config.web_api_token,
            "Accept": "application/json",
        }

        response = requests.put(
            url, json=payload, headers=headers, timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()

        logger.info(f"tenant: {tenant_name} ({tenant_id}) - web-api readiness updated")

    def get_component_user(
        self, tenant_id: str, tenant_name: str, component: str
    ) -> Optional[Dict]:
        """Http call to web-api to check if a component user exists"""
        logger.debug(
            f"tenant: {tenant_name} ({tenant_id}) - web-api checking user for component: {component}..."
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
            return next(
                (user for user in users if user.get("component") == component), None
            )
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
