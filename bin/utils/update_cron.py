#!/usr/bin/env python
import os
import sys
import tempfile
import logging
from argparse import ArgumentParser
from subprocess import check_output, CalledProcessError, check_call
from datetime import datetime
from common import get_log_conf, get_config_paths
from argo_config import ArgoConfig


log = logging.getLogger(__name__)

# CONSTANTS
# relative path to ar job submit script
BATCH_AR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../ar_job_submit.py'))
# relative path to status job submit script
BATCH_STATUS =  os.path.abspath(os.path.join(os.path.dirname(__file__), '../status_job_submit.py'))
# bash argument to get today's date (utc)
TODAY = """$(/bin/date --utc +\%Y-\%m-\%d)"""
# bash argument to get previous date date (utc)
YESTERDAY = """$(/bin/date --utc --date '-1 day'  +\%Y-\%m-\%d)"""
# minute to start hourly cron jobs
HOURLY_MIN = 5
# hour to start daily cron jobs
DAILY_HOUR = 5


def get_hourly(minutes=5):
    """
    Given a starting minute returns a cron string prefix for hourly repetition

    Args:
        minutes: int. specify starting minute for hourly cron job

    Returns: str. hourly cron prefix
    """
    return "{} * * * *".format(minutes)


def get_daily(hour=5, minutes=0):
    """
    Given a starting hour and minute returns a cron string prefix for daily repetition
    Args:
        hour: int. specify starting hour for daily cron job
        minutes: int. specify starting minute for daily cron job

    Returns: str. daily cron prefix
    """
    return "{} {} * * *".format(minutes, hour)


def gen_entry(period, cmd, user=None, description=None):
    """
    For a given period, cmd, user and description generate a text snippet
    with the complete cron entry

    Args:
        period: str. cron prefix specifying repetition
        cmd: str. bash command to run
        user: str. optional user that runs the command
        description: str. text comment to accompany cron entry

    Returns: str. text snippet with cron entry
    """
    txt = ""
    if description is not None:
        txt = "#{}\n".format(description)

    if user is not None:
        txt = txt + "{} {} {}\n".format(period, user, cmd)
    else:
        txt = txt + "{} {}\n".format(period, cmd)

    return txt


def gen_batch_ar(config, tenant, report, iteration="hourly", user=None, mongo_method="insert"):
    """
    For a given tenant and report generate a daily or hourly cron entry for ar batch job
    Args:
        config: obj. Argo Configuration object
        tenant: str. tenant name
        report: str. report name
        iteration: str. either hourly or daily
        user: str. optional user name
        mongo_method: str. defines how to store data in mongodb, either insert or upsert

    Returns: str. text snippet with cron entry
    """
    description = "{}:{} {} A/R".format(tenant, report, iteration)

    if iteration == "daily":
        # generate an daily job
        cron_prefix = get_daily(DAILY_HOUR, HOURLY_MIN)
        # daily jobs target today's date
        target_date = YESTERDAY
    else:
        # generate a hourly job
        cron_prefix = get_hourly(HOURLY_MIN)
        # hourly jobs target day before
        target_date = TODAY

    cmd = "{} -t {} -r {} -d {} -m {} -c {}".format(BATCH_AR, tenant, report, target_date, mongo_method,
                                                    os.path.abspath(config.conf_path))
    return gen_entry(cron_prefix, cmd, user, description)


def gen_batch_status(config, tenant, report, iteration="hourly", user=None, mongo_method="insert"):
    """
    For a given tenant and report generate a daily or hourly cron entry for status batch job
    Args:
        config: obj. Argo Configuration
        tenant: str. tenant name
        report: str. report name
        iteration: str. either hourly or daily
        user: str. optional user name
        mongo_method: str. defines how to store data in mongodb, either insert or upsert

    Returns: str. text snippet with cron entry
    """
    description = "{}:{} {} Status".format(tenant, report, iteration)

    if iteration == "daily":
        # generate a daily job
        cron_prefix = get_daily(DAILY_HOUR, HOURLY_MIN)
        # daily jobs target today's date
        target_date = YESTERDAY
    else:
        # generate an hourly job
        cron_prefix = get_hourly(HOURLY_MIN)
        # hourly jobs target day before
        target_date = TODAY
    cmd = "{} -t {} -r {} -d {} -m {} -c {}".format(BATCH_STATUS, tenant, report, target_date, mongo_method,
                                                    os.path.abspath(config.conf_path))
    return gen_entry(cron_prefix, cmd, user, description)


def gen_tenant_all(config, tenant, reports=None, user=None):
    """
    For a given tenant and all of his reports generate a/r and status jobs
    Args:
        config: obj. Argo Configuration
        tenant: str. tenant name
        user: str. optional user name

    Returns: str. text snippet with all cron entries for this tenant
    """
    cron_body = "#Jobs for {}\n\n".format(tenant)
    section_tenant = "TENANTS:"+tenant
    if reports is None:
        reports = config.get(section_tenant, "reports")
    mongo_method = config.get(section_tenant, "mongo_method")
    for report in reports:
        # Generate hourly batch Ar
        cron_body = cron_body + gen_batch_ar(config, tenant, report, "hourly", user, mongo_method) + "\n"
        # Generate daily batch Ar
        cron_body = cron_body + gen_batch_ar(config, tenant, report, "daily", user, mongo_method) + "\n"
        # Generate hourly batch Status
        cron_body = cron_body + gen_batch_status(config, tenant, report, "hourly", user, mongo_method) + "\n"
        # Generate daily batch Status
        cron_body = cron_body + gen_batch_status(config, tenant, report, "daily", user, mongo_method) + "\n"
    return cron_body + "\n"


def gen_for_all(config, user=None):
    """
    For given configuration generate cron jobs for all tenants
    Args:
        config: obj. Argo configuration
        user: str. optional user name

    Returns: str. text snippet with all cron entries for this tenant
    """
    cron_body = ""
    tenants = config.get("API", "tenants")
    for tenant in tenants:
        cron_body = cron_body + gen_tenant_all(config, tenant, user)
    return cron_body


def update_cron_tab(cron_gen_data):
    """
    Reads existing cron data from cronfile. Combines existing cron data with
    given cron_gen_data. Uses a tempfile to hold the result and calls
    crontab to update from this tempfile

    Args:
        cron_gen_data: str. generated cron data
    """

    # Try to grab existing cron data (so as to maintain them)
    try:
        cron_data = check_output(["crontab", "-l"])
    except CalledProcessError:
        cron_data = ""

    # If cron data already includes generated entries remove them using signifier
    signifier = "\n#The rest are generated by argo engine"
    cron_data = cron_data.split(signifier)[0]

    # Append newly generated cron data to the existing
    date_stamp = "#The rest are generated by argo engine at {}".format(str(datetime.utcnow()))
    cron_data = cron_data + "\n" + date_stamp + "\n\n" + cron_gen_data

    # Open a temp file to write update cron data
    tmp_cron_fd, tmp_cron_path = tempfile.mkstemp()
    try:
        with os.fdopen(tmp_cron_fd, 'w') as tmp_cron:
            # write cron data
            tmp_cron.write(cron_data)
            # make cron update from the new file
            tmp_cron.close()
            ret_code = check_call(["crontab", tmp_cron_path])
            if ret_code != 0:
                log.critical("Could not update cron file")
            else:
                log.info("Cron file updated successfully")
    finally:
        pass
        os.remove(tmp_cron_path)


def run_cron_update(args):
    """
    Method to be executed when this is invoked from cli. Reads configuration
    and updates crontab for all tenants/jobs

    Args:
        args: obj. command line arguments from argparser
    """

    # Get configuration paths
    conf_paths = get_config_paths(args.config)

    # Get logger config file
    get_log_conf(conf_paths['log'])

    # Get main configuration and schema
    config = ArgoConfig(conf_paths["main"], conf_paths["schema"])

    # Generate cron data
    cron_gen = gen_for_all(config)

    # Update cron
    update_cron_tab(cron_gen)


if __name__ == '__main__':
    # Feed Argument parser with the description of the 3 arguments we need
    arg_parser = ArgumentParser(
        description="update crontab with batch jobs for all tenants/reports")
    arg_parser.add_argument(
        "-c", "--config", help="config", dest="config", metavar="STRING")
    arg_parser.add_argument(
        "-u", "--user", help="config", dest="user", metavar="STRING", required=False, default=None)

    # Parse the command line arguments accordingly and introduce them to the run method
    sys.exit(run_cron_update(arg_parser.parse_args()))