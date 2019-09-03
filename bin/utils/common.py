import datetime
import requests
import json
import subprocess
from subprocess import check_call
from urlparse import urlparse, urlsplit, urlunsplit
import logging.config
import logging
import os.path
import sys


log = logging.getLogger(__name__)


def cmd_to_string(cmd):
    """ Take as input a list containing the job sumbission command
        and return a string representation
    Attributes:
              cmd(list): list containing the submition command
    Returns:
           (str): String representation of the submition command
    """

    return " ".join(str(x) for x in cmd)


def date_rollback(path, year, month, day, config, client):
    """Method that checks if a file(inside the hdfs) that is described by a date exists.
       If it doesn't exist, it will rollback the date up to a number days specified by the conf file
       and see if the file exists for any of the dates.
       If a file is not found it will exit with the appropriate message.
    Attributes:
              path(str): the hdfs filepath
              year(int): the year of the starting date
              month(int): the month of the starting date
              day(int) : the day of the starting date
              config(ConfigParser): script's configuration
              client(snakebite.Client): Client for hdfs
    Returns:
           (str): the hdfs file path of the latest date
    """
    snakebite_path = urlparse(path).path

    # maximun number of days it should rollback
    days = config.get("HDFS", "rollback_days")

    starting_date = datetime.date(year, month, day)

    if client.test(snakebite_path.replace("{{date}}", str(starting_date)), exists=True):
        return path.replace("{{date}}", str(starting_date))
    else:
        log.warn("File : " + snakebite_path.replace("{{date}}", str(
            starting_date)) + " was not found. Beggining rollback proccess for: " + str(days) + " days")
        day_to_check = 1
        # while the number of maximum days we can rollback is greater than the number of days we have already
        # rolledback, continue searching
        while days >= day_to_check:
            log.warn(
                "Trying: " + snakebite_path.replace("{{date}}", str(starting_date - datetime.timedelta(day_to_check))))
            if client.test(snakebite_path.replace("{{date}}", str(starting_date - datetime.timedelta(day_to_check))),
                           exists=True):
                log.warn("File found after: " + str(day_to_check) + " days.\n")
                snakebite_path = snakebite_path.replace("{{date}}",
                                                        str(starting_date - datetime.timedelta(day_to_check)))
                # decompose the original path into a list, where scheme, netlock, path, query,
                # fragment are the list indices from 0 to 4 respectively
                path_decomposed = list(urlsplit(path))
                # update the path with the snakebite path containg the date the file was found
                path_decomposed[2] = snakebite_path
                # recompose the path and return it
                return urlunsplit(path_decomposed)

            # if the file was not found, rollback one more day     
            day_to_check += 1

    log.critical("No file found after "+str(day_to_check-1)+" days.")
    sys.exit(1)


def flink_job_submit(config, cmd_command, job_namespace=None, dry_run=False):
    """Method that takes a command and executes it, after checking for flink being up and running.
    If the job_namespace is defined, then it will also check for the specific job if its already running.
    If flink is not running or the job is already submitted, it will execute.
    Attributes:
              config(ConfigParser): script's configuration
              cmd_command(list): list contaning the command to be submitted
              job_namespace(string): the job's name
              dry_run(boolean, optional): signifies a dry-run execution - no submission is performed
    """
    # check if flink is up and running
    if dry_run:
        log.info("This is a dry run. Job won't be submitted")
    else:
        log.info("Getting ready to submit job")
       
    log.info(cmd_to_string(cmd_command)+"\n") 
    try:
        flink_response = requests.get(config.get("FLINK", "job_manager").geturl()+"/joboverview/running")
        issues = False
        job_already_runs = False
        if job_namespace is not None:
            # if the job's already running then exit, else sumbit the command
            for job in json.loads(flink_response.text)["jobs"]:
                if job["name"] == job_namespace:
                    log.critical("Job: "+"'"+job_namespace+"' is already running")
                    job_already_runs = True
                    issues = True

        log.info("Everything is ok")

        try:
            if not dry_run and not job_already_runs:
                check_call(cmd_command)
        except subprocess.CalledProcessError as esp:
            log.fatal("Job was not submitted. Error exit code: "+str(esp.returncode))
            issues = True
            sys.exit(1)
    except requests.exceptions.ConnectionError:
        log.fatal("Flink is not currently running. Tried to communicate with job manager at: " +
                  config.get("FLINK", "job_manager").geturl())
        issues = True

    # print dry-run message if needed
    if dry_run:
        # print output in green and exit
        print "\033[92m" + cmd_to_string(cmd_command) + "\033[0m"
    # if isses exit
    if issues:
        exit(1)

def hdfs_check_path(uri, client):
    """Method that checks if a path in hdfs exists. If it exists it will return the path,
    else it will exit the script"""
    if client.test(urlparse(uri).path, exists=True):
        return uri
    log.critical("File: " + uri + " doesn't exist.")
    sys.exit(1)


def get_config_paths(config_file=None):

    def check_paths(path_directory, main_filename=None):
        std_names = ['argo-streaming.conf', 'config.schema.json', 'logger.conf']
        path_list = []
        if main_filename is not None:
            std_names[0] = main_filename

        for name in std_names:
            full_path = os.path.join(path_directory, name)
            if not os.path.exists(os.path.join(path_directory, name)):
                return None
            path_list.append(full_path)

        # we need three paths
        if len(path_list) == 3:
            return {'main': path_list[0], 'schema': path_list[1], 'log': path_list[2]}
        else:
            return None

    # check if configuration file was given as an argument
    if config_file:
        path_dir = os.path.dirname(config_file)
        config_paths = check_paths(path_dir, os.path.basename(config_file))
        if config_paths is not None:
            return config_paths

    # then check in local setup
    path_dir = os.path.abspath('../../conf')
    config_paths = check_paths(path_dir)
    if config_paths is not None:
        return config_paths

    # lastly check in /etc/
    path_dir = os.path.abspath('/etc/argo-streaming/')
    config_paths = check_paths(path_dir)
    if config_paths is not None:
        return config_paths

    # lastly display error -- logger wont be configured yet
    get_log_conf()
    log.fatal('No configuration found in arguments, local conf folder or /etc/argo-streaming/')
    raise RuntimeError("no configuration found")


def get_log_conf(log_config_file=None):
    """
    Method that searches and gets the default location of configuration and logging configuration
    """
    if log_config_file is not None:
        logging.config.fileConfig(log_config_file, disable_existing_loggers=False)
    else:
        logging.basicConfig(level=logging.INFO, format=logging.BASIC_FORMAT)
