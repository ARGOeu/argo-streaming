#! /usr/bin/env python

# Metric data publisher utility
#
# This utility allows to open argo-consumer metric_data files and publish their contents
# to an Argo Messaging Service topic
#
# Usage:
#     ./metric-publisher (-cli arguments)
# Supported arguments:
#     -f, --file: consumer avro file
#     -e, --endpoint: ams endpoint
#     -k, --key :ams key(token)
#     -p, --project: ams project
#     -t, --topic: ams topic

from avro.datafile import DataFileReader
from avro.io import DatumReader
from avro.io import DatumWriter
from avro.io import BinaryEncoder
import io
import base64
import json
from argparse import ArgumentParser
import requests
import sys
import logging
import logging.handlers


from time import sleep


def publish(message, endpoint, project, topic, key,log):
    url = "https://" + endpoint + "/v1/projects/"+project+"/topics/" + topic + ":publish?key=" + key
    log.debug("publishing to: " + url)
    r = requests.post(url, data=message, timeout=10)
    if r.status_code != 200:
        log.error(r.text)
    else:
        log.debug(r.text)




def main(args):
    log = logging.getLogger(__name__)
    log.setLevel(logging.INFO)

    sys_log = logging.handlers.SysLogHandler("/dev/log")
    sys_format = logging.Formatter('%(name)s[%(process)d]: %(levelname)s %(message)s')
    sys_log.setFormatter(sys_format)

    log.addHandler(sys_log)

    reader = DataFileReader(open(args.avro_file, "r"), DatumReader())

    schema = reader.datum_reader.writers_schema

    for i, row in enumerate(reader):
        log.debug("Consumer row:" + str(row))
        writer = DatumWriter(schema)
        bytes_writer = io.BytesIO()
        encoder = BinaryEncoder(bytes_writer)
        writer.write(row, encoder)
        raw_bytes = bytes_writer.getvalue()
        b64enc = base64.b64encode(raw_bytes)
        msg = {"messages": [{"data": b64enc}]}

        json_str = json.dumps(msg)
        log.debug("json msg:" + json_str)
        publish(json_str, args.ams_endpoint, args.ams_project, args.ams_topic, args.ams_key, log)

if __name__ == "__main__":

    # Feed Argument parser with the description of the 3 arguments we need
    # (input_file,output_file,schema_file)
    arg_parser = ArgumentParser(description="Read a consumer avro file and publish rows to AMS")
    arg_parser.add_argument(
        "-f", "--file", help="consumer avro file ", dest="avro_file", metavar="STRING", required="TRUE")
    arg_parser.add_argument(
        "-e", "--endpoint", help="ams endpoint", dest="ams_endpoint", metavar="STRING", required="TRUE")
    arg_parser.add_argument(
        "-k", "--key", help="ams key(token) ", dest="ams_key", metavar="STRING", required="TRUE")
    arg_parser.add_argument(
        "-p", "--project", help="ams project ", dest="ams_project", metavar="STRING", required="TRUE")
    arg_parser.add_argument(
        "-t", "--topic", help="ams topic ", dest="ams_topic", metavar="STRING", required="TRUE")

    # Parse the command line arguments accordingly and introduce them to
    # main...
    sys.exit(main(arg_parser.parse_args()))
