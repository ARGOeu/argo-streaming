#!/usr/bin/env python

import sys
import logging
import logging.handlers


class ArgoLogger(logging.Logger):

    def __init__(self, log_name=None, config=None):

        if log_name is not None and config is not None:
            # constructor of the super class Logger expects the name of the logger
            # we pass it through super()
            super(ArgoLogger, self).__init__(log_name)

            log_modes = config.get("LOGS", "log_modes").split(",")
            log_level = config.get("LOGS", "log_level")

            levels = {
                'DEBUG': logging.DEBUG,
                'INFO': logging.INFO,
                'WARNING': logging.WARNING,
                'ERROR': logging.ERROR,
                'CRITICAL': logging.CRITICAL
            }

            for log_mode in log_modes:
                kwargs = {}
                kwargs["log_mode"] = log_mode
                kwargs["global_log_level"] = log_level
                kwargs["config"] = config
                kwargs["levels"] = levels
                if log_mode == "console":
                    self.add_stream_handler(**kwargs)
                elif log_mode == "syslog":
                    self.add_syslog_handler(**kwargs)
                elif log_mode == "file":
                    self.add_file_handler(**kwargs)
        else:
            # set up a test logger whenever running tests
            super(ArgoLogger, self).__init__("test")
            stream_log = logging.StreamHandler()
            stream_log.setLevel(logging.INFO)
            self.addHandler(stream_log)

    def print_and_log(self, level, message, exit_code=None):
        """ Logger method that prints the logging message aswell
        Attributes:
                  self(ArgoLogger): an instance of the ArgoLogger class
                  messsage(str): message to be logged, aswell printed, depending on stream handler's level
                  level(int): the level of the log message
                  exit_code(int): if specified, the programm will exit with the specified code
    """
        self.log(level, message)

        if exit_code is not None:
            sys.exit(exit_code)

    def config_str_validator(self, config, section, option):
        """ A method to check whether or not a config string option is not only present, but has a value aswell. It also checks the existance of the section"""
        if config.has_section(section):
            if config.has_option(section, option):
                if len(config.get(section, option)) != 0:
                    return True
        self.print_and_log(logging.CRITICAL, "Section: "+section+" with option: "+option+" was not found in the conf file", 1)

    def add_stream_handler(self, **kwargs):
        """Method that takes care setting up a StreamHandler properly"""

        levels = kwargs["levels"]

        stream_handler = logging.StreamHandler()

        if self.config_str_validator(kwargs["config"], "LOGS", "console_level"):
            stream_handler.setLevel(levels[kwargs["config"].get("LOGS", "console_level").upper()])
        else:
            stream_handler.setLevel(levels[kwargs["global_log_level"].upper()])

        self.addHandler(stream_handler)

    def add_syslog_handler(self, **kwargs):
        """Method that take cares setting up a Syslog Handler properly. Making sure socket and level are configured properly"""

        levels = kwargs["levels"]

        if self.config_str_validator(kwargs["config"], "LOGS", "syslog_socket"):
            sys_log = logging.handlers.SysLogHandler(kwargs["config"].get("LOGS", "syslog_socket"))
        else:
            raise TypeError("No socket specified for syslog handler")

        if self.config_str_validator(kwargs["config"], "LOGS", "syslog_level"):
            sys_log.setLevel(levels[kwargs["config"].get("LOGS", "syslog_level").upper()])
        else:
            sys_log.setLevel(levels[kwargs["global_log_level"].upper()])

        sys_log.setFormatter(logging.Formatter('%(name)s[%(process)d]: %(levelname)s %(message)s'))

        self.addHandler(sys_log)

    def add_file_handler(self, **kwargs):
        """ Method that takes care setting up a FileHandler properly. Making sure that the path to save file and level are configured properly"""

        levels = kwargs["levels"]

        if self.config_str_validator(kwargs["config"], "LOGS", "file_path"):
            file_log = logging.FileHandler(kwargs["config"].get("LOGS", "file_path"))
        else:
            raise TypeError("No filepath specified for file handler")

        if self.config_str_validator(kwargs["config"], "LOGS", "file_level"):
            file_log.setLevel(levels[kwargs["config"].get("LOGS", "file_level").upper()])
        else:
            file_log.setLevel(levels[kwargs["global_log_level"].upper()])

        file_log.setFormatter(logging.Formatter('%(asctime)s %(name)s[%(process)d]: %(levelname)s %(message)s'))

        self.addHandler(file_log)
