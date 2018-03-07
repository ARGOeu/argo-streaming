import datetime
import logging
import requests
import json
import subprocess
from subprocess import check_call
from urlparse import urlparse, urlsplit, urlunsplit


def cmd_toString(cmd):
    """ Take as input a list containing the job sumbission command
        and return a string representation
    Attributes:
              cmd(list): list containing the submition command
    Returns:
           (str): String representation of the submition command
    """

    return " ".join(x for x in cmd)


def date_rollback(path, year, month, day, config, logger, client):
    """Method that checks if a file(inside the hdfs) that is described by a date exists.
       If it doesn't exist, it will rollback the date up to a number days specified by the conf file and see if the file exists
       for any of the dates.If a file is not found it will exit with the appropriate message.
    Attributes:
              path(str): the hdfs filepath
              year(int): the year of the starting date
              month(int): the month of the starting date
              day(int) : the day of the starting date
              config(ConfigParser): script's configuration
              logger(ArgoLogger): logger
              client(snakebite.Client): Client for hdfs
    Returns:
           (str): the hdfs file path of the latest date
    """
    snakebite_path = urlparse(path).path

    # maximun number of days it should rollback
    days = config.getint("HDFS", "rollback_days")

    # number of day to check
    day_to_check = 0

    starting_date = datetime.date(year, month, day)

    if client.test(snakebite_path.replace("{{date}}", str(starting_date)), exists=True):
        return path.replace("{{date}}", str(starting_date))
    else:
        logger.print_and_log(logging.WARNING, "- File : "+snakebite_path.replace("{{date}}", str(starting_date))+" was not found. Beggining rollback proccess for: "+str(days)+" days")
        day_to_check = 1
        # while the number of maximum days we can rollback is greater than the number of days we have already rolledback, continue searching
        while days >= day_to_check:
            logger.print_and_log(logging.WARNING, "Trying: "+snakebite_path.replace("{{date}}", str(starting_date-datetime.timedelta(day_to_check))))
            if client.test(snakebite_path.replace("{{date}}", str(starting_date-datetime.timedelta(day_to_check))), exists=True):
                logger.print_and_log(logging.WARNING, "File found after: "+str(day_to_check)+" days.\n")
                snakebite_path = snakebite_path.replace("{{date}}", str(starting_date-datetime.timedelta(day_to_check)))
                # decompose the original path into a list, where scheme, netlock, path, query, fragment are the list indices from 0 to 4 respectively
                path_decomposed = list(urlsplit(path))
                # update the path with the snakebite path containg the date the file was found
                path_decomposed[2] = snakebite_path
                # recompose the path and return it
                return urlunsplit(path_decomposed)

            # if the file was not found, rollback one more day     
            day_to_check += 1

    logger.print_and_log(logging.INFO, "No file found after "+str(day_to_check-1)+" days.", 1)


def flink_job_submit(config, logger, cmd_command, job_namespace=None):
    """Method that takes a command and executes it, after checking for flink being up and running.
    If the job_namespace is defined, then it will also check for the specific job if its already running.
    If flink is not running or the job is already submitted, it will execute.
    Attributes:
              config(ConfigParser): script's configuration
              logger(ArgoLogger): logger
              cmd_command(list): list contaning the command to be submitted
              job_namespace(string): the job's name
    """
    # check if flink is up and running
    try:
        flink_response = requests.get(config.get("FLINK", "job_manager")+"/joboverview/running")

        if job_namespace is not None:
            # if the job's already running then exit, else sumbit the command
            for job in json.loads(flink_response.text)["jobs"]:
                if job["name"] == job_namespace:
                    logger.print_and_log(logging.CRITICAL, "\nJob: "+"'"+job_namespace+"' is already running", 1)

        logger.print_and_log(logging.INFO, "Everything is ok")

        try:
            check_call(cmd_command)
        except subprocess.CalledProcessError as esp:
            logger.print_and_log(logging.CRITICAL, "Job was not submitted. Error exit code: "+str(esp.returncode), 1)
    except requests.exceptions.ConnectionError:
        logger.print_and_log(logging.CRITICAL, "Flink is not currently running. Tried to communicate with job manager at: " + config.get("FLINK", "job_manager"), 1)


def hdfs_check_path(path, logger, client):
    """Method that checks if a path in hdfs exists. If it exists it will return the path, else it will exit the script"""
    if client.test(urlparse(path).path, exists=True):
        return path
    logger.print_and_log(logging.CRITICAL, "- File: "+path+" doesn't exist.", 1)
