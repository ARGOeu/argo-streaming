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
from utils.check_tenant import check_tenants, get_today
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
    log.info("Argo-engine update stated.")
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
    profile_type_checklist = ["operations", "aggregations", "reports", "thresholds", "recomputations"]
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
    statuses = check_tenants(tenants,get_today(),3,config)

    # Update cron accordingly
    cron_body = ""  
    for status in statuses:
        cron_body = cron_body + gen_tenant_all(config,status["tenant"],tenant_ok_reports(status))
    update_cron_tab(cron_body)
    log.info("Argo-engine update finished.")

def tenant_ok_reports(status):
    rep_list = list()
    if status["hdfs"]["metric_data"] is False:
        return rep_list
    
    for report_name in status["hdfs"]["sync_data"]:
        result = 1
        report = status["hdfs"]["sync_data"][report_name]
        for key in list(report.keys()):
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
