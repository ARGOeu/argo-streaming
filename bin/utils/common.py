def cmd_toString(cmd):
    """ Take as input a list containing the job sumbission command
        and return a string representation
    Attributes:
              cmd(list): list containing the submition command
    Returns:
           (str): String representation of the submition command
    """

    return " ".join(x for x in cmd)
