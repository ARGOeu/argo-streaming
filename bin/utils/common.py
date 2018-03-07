from urlparse import urlparse, urlsplit, urlunsplit
import datetime
import logging

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
    day_to_check= 0
    
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
