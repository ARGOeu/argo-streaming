#!/usr/bin/env python

from argparse import ArgumentParser
from common import get_config_paths, get_log_conf
from argo_config import ArgoConfig
import sys
import logging
import json
from snakebite.client import Client
from datetime import datetime
from update_ams import ArgoAmsClient
import requests



log = logging.getLogger(__name__)


def get_today():
    """Get todays date in YYYY-MM-DD format
    
    Returns:
        str.: today's date in YYYY-MM-DD format
    """

    return datetime.today().strftime('%Y-%m-%d')


def get_now_iso():
    """Get current utc datetime in YYYY-MM-DDTHH:MM:SSZ format
    
    Returns:
        str.: current datetime in YYY-MM-DDTHH:MM:SSZ format
    """

    return datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')


def check_tenant_hdfs(tenant, target_date, namenode, hdfs_user, client, config):
    """Given a tenant and an hdfs client check if the tenant is properly configured
    in the hdfs destination.
    
    Args:
        tenant (str.): tenant's name
        target_date (str.): YYYY-MM-DD date string
        namenode (str.): url of hdfs namenode
        hdfs_user (str.): name of hdfs user
        client (obj.): hdfs snakebite client
        config (obj.): argo config object
    
    Returns:
        dict.: json representation of the hdfs status check
    """

    
    # check hdfs metric data
    hdfs_metric = config.get("HDFS","path_metric").fill(namenode=namenode.geturl(),user=hdfs_user,tenant=tenant)
    hdfs_status = dict()
    metric_path = "".join([hdfs_metric.path,"/",target_date,"/*"])
    count = 0

    try:
        for _  in  client.count([metric_path]):
            count = count + 1 
    except Exception:
        log.error("Error on hdfs location for tenant {}".format(tenant))
    # append hdfs metric check results in hdfs category
    if count > 0:
        hdfs_status["metric_data"] = True
    else:
        hdfs_status["metric_data"] = False

    # check hdfs sync data
    hdfs_sync= config.get("HDFS","path_sync").fill(namenode=namenode.geturl(),user=hdfs_user,tenant=tenant)
    sync_list = ["metric_profile", "group_groups", "group_endpoints", "weights", "downtimes"]
    report_profiles = {"configuration_profile": "{}_{}_cfg.json", 
                "aggregation_profile": "{}_{}_ap.json",
                "operations_profile": "{}_ops.json",
                "blank_recomputation": "recomp.json"
                }
    reports = config.get("TENANTS:{}".format(tenant),"reports")

    sync_result = dict()
    for report in reports:
        sync_result[report]={}
        for item in sync_list:
            sync_path = "".join([hdfs_sync.path,"/",report,"/",item,"_",target_date,".avro"])
            try: 
                client.test(sync_path)
                sync_result[report][item] = True
            except Exception:
                sync_result[report][item] = False
        
        for item in report_profiles.keys():
            profile_path = "".join([hdfs_sync.path,"/",report_profiles[item].format(tenant,report)])
            try: 
                client.test(profile_path)
                sync_result[report][item] = True
            except Exception:
                sync_result[report][item] = False
    # append hdfs sync check results in hdfs category
    hdfs_status["sync_data"] = sync_result
    # append current tenant to the list of tenants in general status report
    return hdfs_status


def check_tenant_ams(tenant, target_date, ams, config):
    """Given a tenant and ams client check if the tenant is properly configured 
    in remote ams
    
    Args:
        tenant (str.): tenant's name
        target_date (str.): YYYY-MM-DD date string
        ams (obj.): ams client handling connection to a remote ams endpoint
        config (obj.): argo configuration object
    
    Returns:
        dict.: json string representing tenant's ams status check
    """

    # check ams
    ams_tenant = {
        "metric_data": {
            "publishing": False,
            "ingestion":False,
            "status_streaming": False,
            "messages_arrived": 0,
        },
        "sync_data": {
            "publishing": False,
            "ingestion":False,
            "status_streaming": False,
            "messages_arrived": 0,
        }
    }

    if ams.check_project_exists(tenant):
        
        tenant_topics = ams.get_tenant_topics(tenant)
        topic_types = tenant_topics.keys()
        if "metric_data" in topic_types:
            ams_tenant["metric_data"]["publishing"] = True 
        if "sync_data" in topic_types:
            ams_tenant["sync_data"]["publishing"] = True 
        
        sub_types = ams.get_tenant_subs(tenant,tenant_topics).keys()
        if "ingest_metric" in sub_types:
            ams_tenant["metric_data"]["ingestion"] = True 
        if "status_metric" in sub_types:
            ams_tenant["metric_data"]["status_streaming"] = True 
        if "ingest_sync" in sub_types:
            ams_tenant["sync_data"]["ingestion"] = True 
        if "status_sync" in sub_types:
            ams_tenant["sync_data"]["status_streaming"] = True
    
        ams_tenant["metric_data"]["messages_arrived"] = ams.get_topic_num_of_messages(tenant,"metric_data")
        ams_tenant["sync_data"]["messages_arrived"] = ams.get_topic_num_of_messages(tenant,"sync_data")
    
    return ams_tenant


def check_tenants(args):
    """Run tenant/s check routine and print status json to stdout
    
    Args:
        args (obj): command line arguments
    """

     # Get configuration paths
    conf_paths = get_config_paths(args.config)

    # Get logger config file
    get_log_conf(conf_paths['log'])
    # Get main configuration and schema
    config = ArgoConfig(conf_paths["main"], conf_paths["schema"])
    

    # if argo_engine configuration is invalid return
    if config.valid is False:
        log.error("Argo engine not properly configured check file:{}".format(conf_paths["main"]))
        sys.exit(1)


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

    # check for specific date or today
    if args.date is not None:
        target_date = args.date 
    else:
        target_date = get_today()

    # check for specific tenant or all of them
    tenants = config.get("API","tenants")
    if args.tenant is not None:
        if args.tenant in tenants:
            tenants = [args.tenant]
        else:
            log.error("tenant {} not found".format(args.tenant))
            return

    # Upload tenant statuses in argo web api 
    api_endpoint = config.get("API","endpoint").netloc
    api_token = config.get("API","access_token")

    # Get tenant uuids 
    tenant_uuids = get_tenant_uuids(api_endpoint, api_token)
    if not tenant_uuids: 
        log.error("Without tenant uuids service is unable to check and upload tenant status")
        sys.exit(1)
    

    for tenant in tenants:
        status_tenant = {}
        
        # add tenant name
        status_tenant["tenant"] = tenant
        # add check timestamp in UTC
        status_tenant["last_check"] = get_now_iso()
        # add engine_config category 
        status_tenant["engine_config"] = config.valid
        # get hdfs status
        status_tenant["hdfs"] = check_tenant_hdfs(tenant,target_date,namenode,hdfs_user,client,config)
        # get ams status
        status_tenant["ams"] = check_tenant_ams(tenant,target_date,ams,config)
        
        log.info("Status for tenant[{}] = {}".format(tenant,json.dumps(status_tenant)))
        # Upload tenant status to argo-web-api
        upload_tenant_status(api_endpoint,api_token,tenant,tenant_uuids[tenant],status_tenant)

def get_tenant_uuids(api_endpoint, api_token):
    """Get tenant uuids from remote argo-web-api endpoint
    
    Args:
        api_endpoint (str.): hostname of the remote argo-web-api endpoint
        api_token (str.): access token for the remote argo-web-api endpoint
    
    Returns:
        dict.: dictionary with mappings of tenant names to tenant uuidss
    """

    log.info("Retrieving tenant uuids from api: {}".format(api_endpoint))
    result = dict()
    url = "https://{}/api/v2/admin/tenants".format(api_endpoint)
    headers = dict()
    headers.update({
        'Accept': 'application/json',
        'x-api-key': api_token
    })
    r = requests.get(url, headers=headers, verify=False)

    if 200 == r.status_code:
       
        tenants = json.loads(r.text)["data"]
        for tenant in tenants:
            result[tenant["info"]["name"]] = tenant["id"]
        log.info("tenant uuids retrieved")
        return result 
    else:
        log.error("unable to retrieve tenant uuids")
        return result

    
def upload_tenant_status(api_endpoint, api_token, tenant, tenant_id, tenant_status):
    """Uploads tenant's status to a remote argo-web-api endpoint
    
    Args:
        api_endpoint (str.): hostname of the remote argo-web-api endpoint
        api_token (str.): access token for remote argo-web-api
        tenant (str.): tenant name
        tenant_id (str.): tenant uuid
        tenant_status (obj.): json representation of tenant's status report
    
    Returns:
        bool: true if upload is successfull
    """

    log.info("Uploading status for tenant: {}({}) at api: {}".format(tenant,tenant_id,api_endpoint))
    url = "https://{}/api/v2/admin/tenants/{}/status".format(api_endpoint,tenant_id)
    headers = dict()
    headers.update({
        'Accept': 'application/json',
        'x-api-key': api_token
    })
    r = requests.put(url, headers=headers, data=json.dumps(tenant_status), verify=False)
    if 200 == r.status_code:
        log.info("Tenant's {} status upload succesfull to {}".format(tenant, api_endpoint))
        return True
    else:
        log.error("Error uploading to the api {}, status_code: {} - response: {}".format(api_endpoint,r.status_code, r.text))
        return False
   
    


if __name__ == '__main__':
    # Feed Argument parser with the description of the 3 arguments we need
    arg_parser = ArgumentParser(
        description="check status of tenant")
    arg_parser.add_argument(
        "-t", "--tenant", help="tenant owner ", dest="tenant", metavar="STRING", required=False, default=None)
    arg_parser.add_argument(
        "-c", "--config", help="config", dest="config", metavar="STRING")
    arg_parser.add_argument(
        "-d", "--date", help="date", dest="date", metavar="STRING")
    
    # Parse the command line arguments accordingly and introduce them to the run method
    sys.exit(check_tenants(arg_parser.parse_args()))
