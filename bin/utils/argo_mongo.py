#!/usr/bin/env python
import sys
import os
import logging
import argparse
import ConfigParser
from pymongo import MongoClient
from argo_log import ArgoLogger

class ArgoMongoClient(object):

    def __init__(self, args, config, logger, cols):
        self.args = args
        self.config = config
        self.logger = logger
        self.cols = cols


    def mongo_clean_ar(self, uri):

        # Create a date integer for use in the database queries
        date_int = int(self.args.Date.replace("-", ""))
        # set up the mongo client
        client = MongoClient(uri)
        # specify the db we will be using. e.g argo_TENANTA
        db = client["argo_"+self.args.Tenant]
        # whenever a report is specified, check if the tenant supports such report, else the programm will exist from the str_validator method
        if self.args.Report and self.logger.config_str_validator(self.config, "TENANTS:"+self.args.Tenant+":REPORTS", self.args.Report):
            tenant_report = self.config.get("TENANTS:"+self.args.Tenant+":REPORTS", self.args.Report)
            
        # iterate over the specified collections
        for col in self.cols:
            if self.args.Report:
                num_of_rows = db[col].find({"date": date_int, "report": tenant_report}).count()
                self.logger.print_and_log(logging.WARNING, "Collection: "+col+" -> Found " +str(num_of_rows)+" entries for date: "+self.args.Date+" and report: " +self.args.Report)
            else:
                num_of_rows = db[col].find({"date": date_int}).count()
                self.logger.print_and_log(logging.WARNING,"Found %s entries for date %s")
                self.logger.print_and_log(logging.WARNING, "Collection: "+col+" -> Found " +str(num_of_rows)+" entries for date: "+self.args.Date+". No report specified!")

            if num_of_rows > 0:

                if self.args.Report:
                    # response returned from the delete operation
                    res = db[col].delete_many({"date": date_int, "report": tenant_report})
                    self.logger.print_and_log(logging.WARNING, "Collection: "+col+" -> Removed " +str(res.deleted_count)+" entries for date: "+self.args.Date+" and report: " +self.args.Report)
                else:
                    # response returned from the delete operation
                    res = db[col].delete_many({"date": date_int, "report": tenant_report})
                    self.logger.print_and_log(logging.WARNING, "Collection: "+col+" -> Removed " +str(res.deleted_count)+" entries for date: "+self.args.Date+". No report specified!")
                self.logger.print_and_log(logging.INFO, "Entries removed successfully")
            else:
                self.logger.print_and_log(logging.INFO, "Zero entries found. Nothing to remove.")

def main_clean_ar(args=None):
    # stand alone method to be used whenever we want to call the mongo_clean_ar method independently

    # make sure the argument are in the correct form
    args.Report = args.Report.capitalize()
    args.Tenant = args.Tenant.upper()

    # set up the config parser
    config = ConfigParser.ConfigParser()

    # check if config file has been given as cli argument else
    # check if config file resides in /etc/argo-streaming/ folder else
    # check if config file resides in local folder
    if args.ConfigPath is None:
        if os.path.isfile("/etc/argo-streaming/conf/conf.cfg"):
            config.read("/etc/argo-streaming/conf/conf.cfg")
        else:
            config.read("../../conf/conf.cfg")
    else:
        config.read(args.ConfigPath)

    # set up the logger
    logger = ArgoLogger(log_name="batch-ar", config=config)

    # check if configuration for the given tenant exists
    if not config.has_section("TENANTS:"+args.Tenant):
        logger.print_and_log(logging.CRITICAL, "Tenant: "+args.Tenant+" doesn't exist.", 1)

    # set up the mongo uri
    mongo_tenant = "TENANTS:"+args.Tenant+":MONGO"
    mongo_uri = config.get(mongo_tenant, "mongo_uri")
    mongo_uri = mongo_uri.replace("{{mongo_host}}", config.get(mongo_tenant, "mongo_host"))
    mongo_uri = mongo_uri.replace("{{mongo_port}}", config.get(mongo_tenant, "mongo_port"))

    argo_mongo_client = ArgoMongoClient(args, config, logger, ["service_ar","endpoint_group_ar"])
    argo_mongo_client.mongo_clean_ar(mongo_uri)

# Provide the ability to the script, to be runned as a standalone module
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="Mongo clean up script")
    parser.add_argument(
        "-t", "--Tenant", type=str, help="Name of the tenant", required=True)
    parser.add_argument(
        "-r", "--Report", type=str, help="Report status", required=True)
    parser.add_argument(
        "-d", "--Date", type=str, help="Date to run the job for", required=True)
    parser.add_argument(
        "-c", "--ConfigPath", type=str, help="Path for the config file")
    parser.add_argument(
        "-j", "--Job", type=str, help="Stand alone method we wish to run", required=True)
    args = parser.parse_args()
    if args.Job == "clean_ar":
        sys.exit(main_clean_ar(args))
