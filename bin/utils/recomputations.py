#!/usr/bin/env python

import os
import sys
import json
from argparse import ArgumentParser
from pymongo import MongoClient
from bson import json_util
import logging
from common import get_config_paths
from common import get_log_conf
from argo_config import ArgoConfig
import subprocess


log = logging.getLogger(__name__)

def write_output(results, tenant, report, target_date, config):
    """Write recomputation output to hdfs
    
    Args:
        results (list(obj)): List of recomputation definitions
        tenant (str.): tenant name
        report (str.): report name 
        target_date ([type]): target date
        config ([type]): argo configuration object  
    
    Returns:
        bool: False if upload had errors
    """

    if len(results) == 0:
        log.info("No recomputations found skipping")
        return True
    # create a temporary recalculation file in the ar-sync folder
    recomp_name = "".join(["recomp", "_", tenant, "_", report, "_", target_date, ".json"])
    recomp_filepath = os.path.join("/tmp/", recomp_name)

    # write output file to the correct job path
    with open(recomp_filepath, 'w') as output_file:
        json.dump(results, output_file, default=json_util.default)
    
    # upload file to hdfs
    hdfs_writer = config.get("HDFS", "writer_bin")
    hdfs_namenode = config.get("HDFS", "namenode")
    hdfs_user = config.get("HDFS", "user")
    hdfs_sync = config.get("HDFS", "path_sync").fill(namenode=hdfs_namenode.geturl(), hdfs_user=hdfs_user, tenant=tenant).path
    
    status = subprocess.check_call([hdfs_writer, "put", recomp_filepath, hdfs_sync])
    # clear temp local file
    os.remove(recomp_filepath)
    if status == 0:
        log.info("File uploaded successfully to hdfs: %s", hdfs_sync )
        return True
    else:
        log.error("File uploaded unsuccessful to hdfs: %s", hdfs_sync)
        return False


def get_mongo_collection(mongo_uri, collection):
    """Return a pymongo collection object from a collection name
    
    Args:
        mongo_uri (obj.): mongodb uri
        collection (str.): collection name
    
    Returns:
        obj.: pymongo collection object
    """

    log.info ("Connecting to mongodb: %s", mongo_uri.geturl())
    print mongo_uri.geturl()
    client = MongoClient(mongo_uri.geturl())
    log.info("Opening database: %s", mongo_uri.path[1:])
    db = client[mongo_uri.path[1:]] 
    log.info("Opening collection: %s", collection)
    col = db[collection]

    return col


def get_mongo_results(collection, target_date, report):
    """Get recomputation results from mongo collection for specific date and report
    
    Args:
        collection (obj.): pymongo collection object
        target_date (str.): date to target
        report (str.): report name
    
    Returns:
        list(dict): list of dictionaries containing recomputation definitions
    """

    # Init results list
    results = []
    # prepare the query to find requests that include the target date
    query = "'%s' >= this.start_time.split('T')[0] && '%s' <= this.end_time.split('T')[0]" % (target_date, target_date)
    # run the query
    for item in collection.find({"report":report,"$where": query}, {"_id": 0}):
        results.append(item)

    return results

def upload_recomputations(tenant, report, target_date, config):
    """Given a tenant, report and target date upload the relevant recomputations
       as an hdfs file
    
    Args:
        tenant (str.): tenant name
        report (str.): report name
        target_date (str.): target date
        config (obj.): argo configuration object
    
    Returns:
        bool: True if upload was succesfull
    """

    tenant_group = "TENANTS:" +tenant
    mongo_endpoint = config.get("MONGO","endpoint").geturl()
    mongo_location = config.get_default(tenant_group,"mongo_uri").fill(mongo_endpoint=mongo_endpoint,tenant=tenant)
    col = get_mongo_collection(mongo_location, "recomputations" )
    recomp_data = get_mongo_results(col, target_date, report)
    return write_output(recomp_data, tenant, report, target_date, config)



def main(args=None):
   # Get configuration paths
    conf_paths = get_config_paths(args.config)

    # Get logger config file
    get_log_conf(conf_paths['log'])
    # Get main configuration and schema
    config = ArgoConfig(conf_paths["main"], conf_paths["schema"])
    res = upload_recomputations(args.tenant, args.report, args.date, config)
    if not res:
        sys.exit(1)

if __name__ == "__main__":

    # Feed Argument parser with the description of the 3 arguments we need
    # (input_file,output_file,schema_file)
    arg_parser = ArgumentParser(description="Get relevant recomputation requests")
    arg_parser.add_argument(
        "-d", "--date", help="date", dest="date", metavar="DATE", required="TRUE")
    arg_parser.add_argument(
        "-t", "--tenant", help="tenant owner ", dest="tenant", metavar="STRING", required="TRUE")
    arg_parser.add_argument(
        "-r", "--report", help="report ", dest="report", metavar="STRING", required="TRUE")
    arg_parser.add_argument(
        "-c", "--config", help="config ", dest="config", metavar="STRING", required="TRUE")
    # Parse the command line arguments accordingly and introduce them to
    # main...
    sys.exit(main(arg_parser.parse_args()))