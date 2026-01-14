#!/usr/bin/env python3

import argparse
import enum
import http.client
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from enum import Enum

import requests
import yaml
from argo_ams_library import ArgoMessagingService

from init_ams import init_ams
from init_mongo import init_mongo

REQUEST_TIMEOUT = 30
TOKEN_REFRESH_BUFFER = 60
LOOP_DELAY = 5


logger = logging.getLogger(__name__)


class JobName(Enum):
    INIT_MONGO = "INIT_MONGO"
    INIT_AMS = "INIT_AMS"


class JobStatus(Enum):
    INITIALISING = "INITIALISING"
    INITIALISED = "INITIALISED"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


class Config:
    """Loads and holds the configuration from a yaml file"""

    def __init__(self, path: str):
        with open(path) as f:
            data = yaml.safe_load(f)["automation"]

        self.ams_endpoint = data.get("ams_endpoint")
        self.ams_event_token = data.get("ams_event_token")
        self.ams_event_project = data.get("ams_event_project")
        self.ams_event_subscription = data.get("ams_event_subscription")
        self.ams_admin_token = data.get("ams_admin_token")
        self.oidc_token_url = data.get("oidc_token_url")
        self.oidc_client_id = data.get("oidc_client_id")
        self.oidc_client_secret = data.get("oidc_client_secret")
        self.mon_api_endpoint = data.get("mon_api_endpoint")
        self.mongodb_url = data.get("mongodb_url")
        self.tenant_db_prefix = data.get("tenant_db_prefix")
        self.argo_ops_email = data.get("argo_ops_email")


class MonApiClient:
    """Client to do status updates to argo mon api"""

    def __init__(self, config: Config):
        self.config = config
        self._access_token = None
        self._token_expires_at = 0
        self._token_lock = threading.Lock()

    def _refresh_token(self):
        """checks and refreshes the oidc token"""

        # Use locking mechanism due to threading
        with self._token_lock:
            if time.time() >= self._token_expires_at - TOKEN_REFRESH_BUFFER:
                logger.debug("Fetching new access token...")
                response = requests.post(
                    self.config.oidc_token_url,
                    data={
                        "grant_type": "client_credentials",
                        "client_id": self.config.oidc_client_id,
                        "client_secret": self.config.oidc_client_secret,
                        "scope": "openid entitlements",
                    },
                    timeout=REQUEST_TIMEOUT,
                )
                response.raise_for_status()
                data = response.json()
                self._access_token = data["access_token"]

                self._token_expires_at = time.time() + data.get(
                    "expires_in", TOKEN_REFRESH_BUFFER
                )
                logger.debug("Access token fetched")

    def update_status(
        self, tenant_id: str, tenant_name: str, job_name: str, status: str, message: str
    ):
        """Http call to update tenant status"""
        logger.debug(f"tenant: {tenant_name} ({tenant_id}) - job status updating...")
        self._refresh_token()

        job_status = {"name": job_name, "status": status, "message": message}
        if status == JobStatus.COMPLETED.value:
            utc_now = datetime.now(timezone.utc)
            job_status["end"] = utc_now.strftime("%Y-%m-%dT%H:%M:%SZ")

        payload = {"jobs": [job_status]}

        url = f"https://{self.config.mon_api_endpoint}/v1/automation/tenants/{tenant_id}/status"
        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json",
        }
        response = requests.patch(
            url, json=payload, headers=headers, timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()

        logger.info(
            f"tenant: {tenant_name} ({tenant_id}) updated to {status} - details: {message}"
        )
        return response.json()

    def get_status(self, tenant_id: str, tenant_name: str, job_name: str):
        """Http call to check tenant status"""
        try:
            logger.debug(
                f"tenant: {tenant_name} ({tenant_id}) - jobs status fetching..."
            )
            self._refresh_token()
            url = f"https://{self.config.mon_api_endpoint}/v1/automation/tenants/{tenant_id}/status"
            headers = {
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/json",
            }
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            # search in the tenant status if job exists
            jobs = response.json().get("status").get("jobs")
            if jobs:

                result_job = next(
                    (job for job in jobs if job["name"] == job_name), None
                )
                return result_job
            logger.debug(
                f"tenant: {tenant_name} ({tenant_id}) - job: {job_name} not found"
            )
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                logger.warn(
                    f"tenant: {tenant_name} ({tenant_id}) does not exist in mon api"
                )
            else:
                raise

        return None


class ArgoAutomator:
    """ArgoAutomator listens to AMS for events and runs background tasks(jobs) in threads"""

    def __init__(self, config: Config, max_workers: int = 10):
        logger.info("initialising automator...")
        self.config = config
        self.mon_api = MonApiClient(config)
        self.ams = ArgoMessagingService(
            endpoint=config.ams_endpoint,
            token=config.ams_event_token,
            project=config.ams_event_project,
        )
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

    def process_event(self, message):
        """Parse an AMS message as an event. Check for specific fields"""
        try:
            raw_data = message.get_data()
            data = json.loads(raw_data.decode("utf-8"))
            job_name = data.get("name")
            props = data.get("properties", {})
            tenant_id = props.get("tenant_id")
            tenant_name = props.get("tenant_name")
            if not all([job_name, tenant_id, tenant_name]):
                logger.warning(f"bad ams event received: {data}")
            logger.debug(
                f"ams job {job_name} event received for tenant: {tenant_name} ({tenant_id})"
            )

            # check if tenant and job definition (based on event type) exists in mon api
            job_status = self.mon_api.get_status(tenant_id, tenant_name, job_name)
            if not job_status:
                logger.debug(
                    f"ams job {job_name} event for tenant: {tenant_name} ({tenant_id}) discarded"
                )
                return

            if job_name == JobName.INIT_MONGO.value:
                logger.debug("YO started init mongo")
                self.executor.submit(self.job_init_mongo, tenant_id, tenant_name)
                return
            if job_name == JobName.INIT_AMS.value:
                logger.debug("YO starterd init ams")
                self.executor.submit(self.job_init_ams, tenant_id, tenant_name)
                return
        except Exception as e:
            logger.exception(f"Failed to process event: {e}")

    def job_init_mongo(self, tenant_id: str, tenant_name: str):
        """Job placeholder to init mongo"""
        try:
            logger.info(
                f"job started: initialising mongo for tenant {tenant_name} with id: {tenant_id}"
            )
            # To do stuff here
            self.mon_api.update_status(
                tenant_id,
                tenant_name,
                JobName.INIT_MONGO.value,
                JobStatus.IN_PROGRESS.value,
                "Initialising database indexes",
            )

            job_done = init_mongo(
                self.config.mongodb_url, self.config.tenant_db_prefix + tenant_name
            )

            if job_done:
                self.mon_api.update_status(
                    tenant_id,
                    tenant_name,
                    JobName.INIT_MONGO.value,
                    JobStatus.COMPLETED.value,
                    "Mongo database initialised succesfully",
                )
                logger.info(
                    f"job completed: initialising mongo for tenant {tenant_name}"
                )
            else:
                self.mon_api.update_status(
                    tenant_id,
                    tenant_name,
                    JobName.INIT_MONGO.value,
                    JobStatus.FAILED.value,
                    "Mongo database failed to initialise",
                )
                logger.error(f"job failed: initialising mongo for tenant {tenant_name}")
        except Exception as e:
            logger.exception(f"job failed for tenant {tenant_name}: {e}")

    def job_init_ams(self, tenant_id: str, tenant_name: str):
        """Job placeholder to init ams"""
        try:
            logger.info(
                f"job started: initialising ams for tenant {tenant_name} with id: {tenant_id}"
            )

            self.mon_api.update_status(
                tenant_id,
                tenant_name,
                JobName.INIT_AMS.value,
                JobStatus.IN_PROGRESS.value,
                "Initialising AMS project",
            )

            job_done = init_ams(
                self.config.ams_endpoint,
                self.config.ams_admin_token,
                tenant_name,
                self.config.argo_ops_email,
            )

            if job_done:
                self.mon_api.update_status(
                    tenant_id,
                    tenant_name,
                    JobName.INIT_AMS.value,
                    JobStatus.COMPLETED.value,
                    "AMS project initialised succesfully",
                )
                logger.info(
                    f"job completed: initialising AMS project for tenant {tenant_name}"
                )
            else:
                self.mon_api.update_status(
                    tenant_id,
                    tenant_name,
                    JobName.INIT_AMS.value,
                    JobStatus.FAILED.value,
                    "AMS project failed to initialise",
                )
                logger.error(
                    f"job failed: initialising AMS project for tenant {tenant_name}"
                )
        except Exception as e:
            logger.exception(f"job failed for tenant {tenant_name}: {e}")

    def run(self):
        """Main automator loop"""

        logger.info(f"Connecting to AMS: {self.config.ams_endpoint}")

        try:
            while True:
                try:
                    messages = self.ams.pull_sub(
                        sub=self.config.ams_event_subscription,
                        num=1,
                        return_immediately=True,
                        timeout=30,
                    )

                    for ack_id, message in messages:

                        self.process_event(message)

                        self.ams.ack_sub(
                            sub=self.config.ams_event_subscription, ids=[ack_id]
                        )

                except Exception as e:
                    logger.exception(f"Error pulling message: {e}")

                time.sleep(LOOP_DELAY)
        finally:
            self.executor.shutdown(wait=True)


def main():
    parser = argparse.ArgumentParser(description="Argo Automator")
    parser.add_argument(
        "-c",
        "--config",
        default="config.yml",
        help="Path to configuration file (default: config.yml)",
    )
    parser.add_argument(
        "-l",
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set logging level (default: INFO)",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    config = Config(args.config)

    automator = ArgoAutomator(config, 10)
    automator.run()


if __name__ == "__main__":
    main()
