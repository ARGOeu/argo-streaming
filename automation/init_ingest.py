import logging
import subprocess
import requests
import sys

from argo_config import ArgoConfig


logger = logging.getLogger(__name__)


def is_ingest_running(flink_url: str, tenant_name: str) -> bool:
    """Check if an ingest job is already running for the given tenant."""
    pattern = f"projects/{tenant_name}/subscriptions"

    jobs = requests.get(f"{flink_url}/joboverview").json().get("running", [])
    return any(pattern in job.get("name", "") for job in jobs)


def run_ingest(
    config: ArgoConfig,
    tenant_name: str,
    tenant_ams_token: str,
    dry_run: bool,
    verify: str,
):
    """Function that composes the appropriate cli command to submit an ingest job execution in flink"""

    # if ingestion job is already running skip it
    if is_ingest_running(config.flink_url, tenant_name):
        logger.warning(f"Ingestion job already running for tenant {tenant_name}")
        return 0

    cmd = [
        config.flink_path,
        "run",
        "--detached",
        "-c",
        "argo.streaming.AmsIngestMetric",
        config.ingest_jar_path,
        "--ams.endpoint",
        config.ams_endpoint,
        "--ams.port",
        "443",
        "--ams.token",
        tenant_ams_token,
        "--ams.project",
        tenant_name,
        "--ams.sub",
        "ingest_metric",
        "--hdfs.path",
        f"{config.hdfs_path}/{tenant_name}/mdata",
        "--check.path",
        f"{config.hdfs_path}/{tenant_name}/check",
        "--check.interval",
        "3000",
        "--ams.interval",
        "300",
        "--ams.batch",
        "100",
        "--ams.verify",
        verify,
        "--tenant",
        tenant_name,
    ]

    if dry_run:
        print(("\033[92m" + " ".join(str(x) for x in cmd) + "\033[0m"))
        return 0
    else:
        try:
            subprocess.run(cmd, check=True)
        except Exception as e:
            logger.error(f"Batch Job Error: {e}")
            return 1
