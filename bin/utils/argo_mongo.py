#!/usr/bin/env python
import sys
import logging
import argparse
import pymongo
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from .argo_config import ArgoConfig
from .common import get_config_paths
from .common import get_log_conf

log = logging.getLogger(__name__)


class ArgoMongoClient(object):

    def __init__(self, args, config, cols):
        self.args = args
        self.config = config
        self.cols = cols

    def ensure_status_indexes(self, db):
        """Checks if required indexes exist in specific argo status-related collections
        in mongodb

        Args:
            db (obj): pymongo database object

        """

        log.info("Checking required indexes in status collections...")

        def is_index_included(index_set, index):
            """gets a set of mongodb indexes and checks if specified
            mongodb index exists in this set

            Args:
                index_set (dict): pymongo mongodb index object
                index (obj): pymongo index object

            Returns:
                bool: If index exists in set return true
            """

            for name in list(index_set.keys()):
                if index_set[name]['key'] == index:
                    return True
            return False
        # Used in all status collections
        index_report_date = [("report", pymongo.ASCENDING),
                             ("date_integer", pymongo.ASCENDING)]
        # Used only in status_metrics collection
        index_date_host = [("date_integer", pymongo.ASCENDING),
                           ("report", pymongo.ASCENDING)]
        status_collections = ["status_metrics", "status_endpoints",
                              "status_services", "status_endpoint_groups"]

        index_history = [("date_integer",pymongo.DESCENDING),
                         ("id",pymongo.ASCENDING)]

        index_downtimes = [("date_integer",pymongo.DESCENDING)]

        # Check indexes in sync collections
        for col_name in ["topology_endpoints", "topology_groups", "weights"]:
            col = db[col_name]
            indexes = col.index_information()
            if not is_index_included(indexes, index_history):
                # ensure index
                col.create_index(index_history, background=True)
                log.info("Created (date_integer,id) index in %s.%s",
                         col.database.name, col.name)

        # Check for index in downtimes
        col = db["downtimes"]
        indexes = col.index_information()
        if not is_index_included(indexes, index_downtimes):
            col.create_index(index_downtimes, background=True)
            log.info("Created (date_integer) index in %s.%s",
                     col.database.name, col.name)             



        # Check first for index report,date
        for status_name in status_collections:
            col = db[status_name]
            indexes = col.index_information()
            if not is_index_included(indexes, index_report_date):
                # ensure index
                col.create_index(index_report_date, background=True)
                log.info("Created (report,date) index in %s.%s",
                         col.database.name, col.name)

        # Check for index date,host in status_metrics
        col = db["status_metrics"]
        indexes = col.index_information()
        if not is_index_included(indexes, index_date_host):
            col.create_index(index_date_host, background=True)
            log.info("Created (report,date) index in %s.%s",
                     col.database.name, col.name)

    def mongo_clean_ar(self, uri, dry_run=False):
        """Gets a mongo database reference as a uri string and performs
        a/r data removal for a specific date

        Args:
            uri (str.): uri string pointing to a specific mongodb database
            dry_run (bool, optional): Optional flag that specifies if the execution is in dry mode run. 
                                      If yes no data removal is performed. Defaults to False.
        """

        tenant_report = None

        # if report is given check if report exists in configuration
        if self.args.report:
            report_name = self.args.report
            tenant_group = "TENANTS:" + self.args.tenant
            if report_name in self.config.get(tenant_group, "reports"):
                tenant_report = self.config.get(
                    tenant_group, "report_"+report_name)
            else:
                log.critical("Report %s not found", report_name)
                sys.exit(1)

        # Create a date integer for use in the database queries
        date_int = int(self.args.date.replace("-", ""))

        # set up the mongo client
        try:
            log.info("Trying to connect to: " + uri.geturl())
            client = MongoClient(uri.geturl())
            # force a connection to test the client
            client.server_info()
        except ServerSelectionTimeoutError as pse:
            log.fatal(pse)
            sys.exit(1)

        # specify the db we will be using. e.g argo_TENANTA
        # from the uri, take the path, which reprents the db, and ignore the / in the begging
        db = client[uri.path[1:]]

        # iterate over the specified collections
        for col in self.cols:
            if tenant_report is not None:
                num_of_rows = db[col].find(
                    {"date": date_int, "report": tenant_report}).count()
                log.info("Collection: " + col + " -> Found " + str(
                    num_of_rows) + " entries for date: " + self.args.date + " and report: " + self.args.report)
            else:
                num_of_rows = db[col].find({"date": date_int}).count()
                log.info("Collection: " + col + " -> Found " + str(
                    num_of_rows) + " entries for date: " + self.args.date + ". No report specified!")
            if dry_run:
                log.info("Results won't be removed to dry-run mode")
            else:
                if num_of_rows > 0:
                    if tenant_report is not None:
                        # response returned from the delete operation
                        res = db[col].delete_many(
                            {"date": date_int, "report": tenant_report})
                        log.info("Collection: " + col + " -> Removed " + str(res.deleted_count) +
                                 " entries for date: " + self.args.date + " and report: " + self.args.report)
                    else:
                        # response returned from the delete operation
                        res = db[col].delete_many(
                            {"date": date_int, "report": tenant_report})
                        log.info("Collection: " + col + " -> Removed " + str(
                            res.deleted_count) + " entries for date: " + self.args.date + ". No report specified!")
                    log.info("Entries removed successfully")
                else:
                    log.info("Zero entries found. Nothing to remove.")

        # close the connection with mongo
        client.close()

    def mongo_clean_status(self, uri, dry_run=False):
        """Gets a mongo database reference as a uri string and performs
        status data removal for a specific date

        Args:
            uri (str.): uri string pointing to a specific mongodb database
            dry_run (bool, optional): Optional flag that specifies if the execution is in dry mode run. 
                                      If yes no data removal is performed. Defaults to False.
        """

        tenant_report = None

        # if report is given check if report exists in configuration
        if self.args.report:
            report_name = self.args.report
            tenant_group = "TENANTS:" + self.args.tenant
            if report_name in self.config.get(tenant_group, "reports"):
                tenant_report = self.config.get(
                    tenant_group, "report_"+report_name)
            else:
                log.critical("Report %s not found", report_name)
                sys.exit(1)

        # Create a date integer for use in the database queries
        date_int = int(self.args.date.replace("-", ""))

        # set up the mongo client
        try:
            log.info("Trying to connect to: " + uri.geturl())
            client = MongoClient(uri.geturl())
            # force a connection to test the client
            client.server_info()
        except pymongo.errors.ServerSelectionTimeoutError as pse:
            log.fatal(pse)
            sys.exit(1)

        # specify the db we will be using. e.g argo_TENANTA
        # from the uri, retrieve the path section, which reprents the db, and ignore the / in the begging
        db = client[uri.path[1:]]

        # ensure indexes for status collections
        self.ensure_status_indexes(db)

        # iterate over the specified collections
        for col in self.cols:
            if tenant_report is not None:
                num_of_rows = db[col].find(
                    {"date_integer": date_int, "report": tenant_report}).count()
                log.info("Collection: " + col + " -> Found " + str(
                    num_of_rows) + " entries for date: " + self.args.date + " and report: " + self.args.report)
            else:
                num_of_rows = db[col].find({"date": date_int}).count()
                log.info("Collection: " + col + " -> Found " + str(
                    num_of_rows) + " entries for date: " + self.args.date + ". No report specified!")

            if dry_run:
                log.info("Results won't be removed to dry-run mode")
            else:
                if num_of_rows > 0:
                    if tenant_report is not None:
                        # response returned from the delete operation
                        res = db[col].delete_many(
                            {"date_integer": date_int, "report": tenant_report})
                        log.info("Collection: " + col + " -> Removed " + str(res.deleted_count) +
                                 " entries for date: " + self.args.date + " and report: " + self.args.report)
                    else:
                        # response returned from the delete operation
                        res = db[col].delete_many(
                            {"date_integer": date_int, "report": tenant_report})
                        log.info("Collection: " + col + " -> Removed " + str(
                            res.deleted_count) + " entries for date: " + self.args.Date + ". No report specified!")
                    log.info("Entries removed successfully")
                else:
                    log.info("Zero entries found. Nothing to remove.")

        # close the connection with mongo
        client.close()


def main_clean(args=None):
    # stand alone method to be used whenever we want to call the mongo_clean_status method independently

    # Get configuration paths
    conf_paths = get_config_paths(args.config)

    # Get logger config file
    get_log_conf(conf_paths['log'])

    # Get main configuration and schema
    config = ArgoConfig(conf_paths["main"], conf_paths["schema"])

    # set up the mongo uri
    section_tenant = "TENANTS:" + args.tenant
    mongo_endpoint = config.get("MONGO", "endpoint")
    mongo_uri = config.get(section_tenant, "mongo_uri").fill(
        mongo_endpoint=mongo_endpoint, tenant=args.tenant)

    if args.job == "clean_ar":
        argo_mongo_client = ArgoMongoClient(
            args, config, ["endpoint_ar", "service_ar", "endpoint_group_ar"])
        argo_mongo_client.mongo_clean_ar(mongo_uri, args.dry_run)

    elif args.job == "clean_status":
        argo_mongo_client = ArgoMongoClient(args, config, ["status_metrics", "status_endpoints", "status_services",
                                                           "status_endpoint_groups"])
        argo_mongo_client.mongo_clean_status(mongo_uri, args.dry_run)


# Provide the ability to the script, to run as a standalone module
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mongo clean up script")
    parser.add_argument(
        "-t", "--tenant", metavar="STRING", help="Name of the tenant", required=True, dest="tenant")
    parser.add_argument(
        "-r", "--report", metavar="STRING", help="Tenant report to be used", required=True, dest="report")
    parser.add_argument(
        "-d", "--date", metavar="STRING", help="Date to run the job for", required=True, dest="date")
    parser.add_argument(
        "-c", "--config", metavar="STRING", help="Path for the config file", dest="config")
    parser.add_argument(
        "-j", "--job", metavar="STRING", help="Stand alone method we wish to run", required=True, dest="job")
    parser.add_argument("--dry-run", help="Runs in test mode without actually submitting the job",
                        action="store_true", dest="dry_run")

    # Parse the arguments
    sys.exit(main_clean(parser.parse_args()))
