import logging

import yaml

logger = logging.getLogger(__name__)


class ArgoConfig:
    """Loads and holds the configuration from a yaml file"""

    def __init__(self, path: str):
        with open(path) as f:
            config_data = yaml.safe_load(f)
            if not config_data:
                logger.error("No configuration found - exiting")
                exit(1)
            automation = config_data.get("automation", {})
            tenants = config_data.get("tenants", {})
            run = config_data.get("run", {})

        self.path = path
        self.automation = automation
        self.tenants = tenants
        self.run = run
        self.ams_endpoint = automation.get("ams_endpoint")
        self.ams_event_token = automation.get("ams_event_token")
        self.ams_event_project = automation.get("ams_event_project")
        self.ams_event_subscription = automation.get("ams_event_subscription")
        self.ams_admin_token = automation.get("ams_admin_token")
        self.oidc_token_url = automation.get("oidc_token_url")
        self.oidc_client_id = automation.get("oidc_client_id")
        self.oidc_client_secret = automation.get("oidc_client_secret")
        self.mon_api_endpoint = automation.get("mon_api_endpoint")
        self.mongodb_url = automation.get("mongodb_url")
        self.tenant_db_prefix = automation.get("tenant_db_prefix")
        self.argo_ops_email = automation.get("argo_ops_email")
        self.web_api_endpoint = automation.get("web_api_endpoint")
        self.web_api_token = automation.get("web_api_token")
        self.default_ops_profile_file = automation.get("default_ops_profile_file")
        self.default_metric_profile_file = automation.get("default_metric_profile_file")
        self.default_agg_profile_file = automation.get("default_agg_profile_file")
        self.default_services_file = automation.get("default_services_file")
        self.default_report_file = automation.get("default_report_file")
        self.hdfs_path = run.get("hdfs_path")
        self.hdfs_check_path = run.get("hdfs_check_path")
        self.flink_path = run.get("flink_path")
        self.flink_url = run.get("flink_url")
        self.batch_jar_path = run.get("batch_jar_path")
        self.ingest_jar_path = run.get("ingest_jar_path")

    def save(self) -> None:
        """Save current configuration back to yaml file"""
        with open(self.path, "w") as f:
            data = {
                "automation": self.automation,
                "run": self.run,
                "tenants": self.tenants,
            }
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
            logger.info("engine config - saved to disk")

    def set_tenant_web_api_access(self, tenant_id, tenant_name, web_api_token):
        self.ensure_tenant(tenant_id, tenant_name)
        self.tenants.get(tenant_name)["web_api_token"] = web_api_token
        logger.info(f"engine config - tenant {tenant_name} web_api_token prop set")
        self.save()

    def set_tenant_reports(self, tenant_id: str, tenant_name: str, reports: dict):
        self.ensure_tenant(tenant_id, tenant_name)
        self.tenants.get(tenant_name)["reports"] = reports
        logger.info(f"engine config - tenant {tenant_name} reports prop set")
        self.save()

    def set_tenant_ams_access(self, tenant_id, tenant_name, ams_token):
        self.ensure_tenant(tenant_id, tenant_name)
        self.tenants.get(tenant_name)["ams_token"] = ams_token
        logger.info(f"engine config - tenant {tenant_name} ams_token prop set")
        self.save()

    def ensure_tenant(self, tenant_id, tenant_name):
        cur_tenant = self.tenants.get(tenant_name)
        if not cur_tenant:
            self.tenants[tenant_name] = {"id": tenant_id}
            logger.info(f"engine config - tenant {tenant_name} definition created")
            return
        cur_tenant_id = cur_tenant.get("id")
        if not cur_tenant_id or cur_tenant_id != tenant_id:
            cur_tenant["id"] = tenant_id
            logger.info(f"engine config - tenant {tenant_name} tenant_id prop set")
