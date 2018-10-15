#!/usr/bin/env python

from argparse import ArgumentParser
from  utils.common import get_config_paths, get_log_conf
import sys
import shutil
import os.path
import logging
import json
from datetime import datetime
from utils.argo_config import ArgoConfig
from utils.update_profiles import ArgoProfileManager
from utils.update_ams import ArgoAmsClient, is_tenant_complete
from utils.check_tenant import get_now_iso, get_today, check_tenant_ams, check_tenant_hdfs, upload_tenant_status, get_tenant_uuids
from utils.update_cron import gen_tenant_all, update_cron_tab
from snakebite.client import Client


log = logging.getLogger("argo.update_engine")


def main(args):
   
    if args.config is not None and not os.path.isfile(args.config):
        log.info(args.config + " file not found")
    
   # Get configuration paths
    conf_paths = get_config_paths(args.config)

    # Get logger config file
    get_log_conf(conf_paths['log'])

    # Get main configuration and schema
    config = ArgoConfig(conf_paths["main"], conf_paths["schema"])
    
    # if backup-conf selected backup the configuration file 
    if args.backup:
        date_postfix = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        backup = args.config + "." + date_postfix
        shutil.copyfile(args.config, backup)
        log.info("backed-up current configuration to: " + backup)
    

    argo_profiles = ArgoProfileManager(config)
    argo_profiles.upload_tenants_cfg()
    config.save_as(conf_paths["main"])

    # reload config and profile manager
    config = ArgoConfig(conf_paths["main"], conf_paths["schema"])
    argo_profiles = ArgoProfileManager(config)
    
    tenants = config.get("API","tenants")
    profile_type_checklist = ["operations", "aggregations", "reports", "thresholds"]
    for tenant in tenants:
        reports = config.get("TENANTS:"+tenant,"reports")
        for report in reports:
            for profile_type in profile_type_checklist:
                argo_profiles.profile_update_check(tenant, report, profile_type)

    # update ams 
    ams_token = config.get("AMS", "access_token")
    ams_host = config.get("AMS", "endpoint").hostname
    log.info("ams api used {}".format(ams_host))
    
    ams = ArgoAmsClient(ams_host, ams_token)

    for tenant in tenants:
        ams.check_project_exists(tenant)
        missing = ams.check_tenant(tenant)
        if is_tenant_complete(missing):
            log.info("Tenant {} definition on AMS is complete!".format(tenant))
        else:
            ams.fill_missing(tenant, missing)
        # Update tenant configuration
        ams.update_tenant_configuration(tenant, config)

    # Save changes to designated configuration file
    config.save_as(config.conf_path)

    # check tenant status

    # Upload tenant statuses in argo web api 
    api_endpoint = config.get("API","endpoint").netloc
    api_token = config.get("API","access_token")

    # Get tenant uuids 
    tenant_uuids = get_tenant_uuids(api_endpoint, api_token)

    target_date = get_today()
     # hdfs client init
    namenode = config.get("HDFS","namenode")
    hdfs_user = config.get("HDFS","user")
    client = Client(namenode.hostname, namenode.port)
    log.info("connecting to HDFS: {}".format(namenode.hostname))

    # ams client init
    ams_token = config.get("AMS", "access_token")
    ams_host = config.get("AMS", "endpoint").hostname
    ams = ArgoAmsClient(ams_host, ams_token)
    log.info("connecting to AMS: {}".format(ams_host))

    cron_body = ""

    for tenant in tenants:
        status_tenant = {} 
        # add tenant name
        status_tenant["tenant"] = tenant
        # add check timestamp in UTC
        status_tenant["last_check"] = get_now_iso()
        # add engine_config category 
        status_tenant["engine_config"] = True
        # get hdfs status
        status_tenant["hdfs"] = check_tenant_hdfs(tenant,target_date,namenode,hdfs_user,client,config)
        # get ams status
        status_tenant["ams"] = check_tenant_ams(tenant,target_date,ams,config)
        
        log.info("Status for tenant[{}] = {}".format(tenant,json.dumps(status_tenant)))
        # Upload tenant status to argo-web-api
        upload_tenant_status(api_endpoint,api_token,tenant,tenant_uuids[tenant],status_tenant)
        # upload cron for tenant
        ok_reports = tenant_ok_reports(status_tenant)
        cron_body = cron_body + gen_tenant_all(config,tenant,ok_reports)
    
    update_cron_tab(cron_body)

def tenant_ok_reports(status):
    rep_list = list()
    if status["hdfs"]["metric_data"] is False:
        return rep_list
    
    for report_name in status["hdfs"]["sync_data"]:
        result = 1
        report = status["hdfs"]["sync_data"][report_name]
        for key in report.keys():
            result = result * report[key]
        if result > 0:
            rep_list.append(report_name)
    return rep_list
    

if __name__ == "__main__":

    parser = ArgumentParser(description="Update engine")
    parser.add_argument(
        "-b", "--backup-conf", help="backup current configuration", action="store_true", dest="backup")
    parser.add_argument(
        "-c", "--config", metavar="PATH", help="Path for the config file", dest="config")

    # Pass the arguments to main method
    sys.exit(main(parser.parse_args()))
