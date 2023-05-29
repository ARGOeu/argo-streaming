#!/usr/bin/env python
import io
import requests
import sys
import argparse
from argo_ams_library import ArgoMessagingService
import avro.schema
from avro.io import BinaryDecoder, DatumReader
from influxdb_client import InfluxDBClient
from influxdb_client .client.write_api import SYNCHRONOUS
from time import sleep
import logging

# HOW TO RUN:
"""
chmod u+x ./ingest_performance_data.py

./ingest_performance_data.py --api-endpoint my-web-api \
--api-token "*******" --ams-endpoint my-ams-endpoint --ams-token "*****" \
--ams-project MYPROJECT --ams-sub metric_data_test --influx-endpoint "http://localhost:8086" \
--influx-token "*******" --influx-org myorg --influx-bucket mybucket --interval 1 --batch 20
"""

# need this schema to decode each ams message that contains an encoded bytestream of avro metric data
# each ams message is encoded in avro but doesn't include the schema
AVRO_SCHEMA = '''
{"namespace": "argo.avro",
 "type": "record",
 "name": "metric_data",
 "fields": [
        {"name": "timestamp", "type": "string"},
        {"name": "service", "type": "string"},
        {"name": "hostname", "type": "string"},
        {"name": "metric", "type": "string"},
        {"name": "status", "type": "string"},
        {"name": "monitoring_host", "type": ["null", "string"]},
        {"name": "actual_data", "type": ["null", "string"], "default": null},
        {"name": "summary", "type": ["null", "string"]},
        {"name": "message", "type": ["null", "string"]},
        {"name": "tags", "type" : ["null", {"name" : "Tags",
                                             "type" : "map",
                                             "values" : ["null", "string"]
                                           }]
        }]
}
'''

# setup basic logging to the console
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


# A class to handle performance data consumption
class PerfDataConsumer:

    # initialize with a bunch of arguments
    # we need args for connecting to ams
    # we need args for connecting to argo web api
    # we need args for connecting to remote influx db

    def __init__(self, api_endpoint=None, api_token=None, ams_endpoint=None, ams_token=None,
                 ams_project=None, ams_sub=None, influx_endpoint=None, influx_token=None,
                 influx_org=None, influx_bucket=None):
        self.topology = {}
        self.topology_date = None
        self.api_endpoint = api_endpoint
        self.api_token = api_token
        self.schema = avro.schema.parse(AVRO_SCHEMA)
        self.avro_reader = DatumReader(self.schema)
        self.ams_endpoint = ams_endpoint
        self.ams_token = ams_token
        self.ams_project = ams_project
        self.ams_sub = ams_sub
        self.influx_endpoint = influx_endpoint
        self.influx_token = influx_token
        self.influx_org = influx_org
        self.influx_bucket = influx_bucket
        self.ams_client = None
        self.influx_client = None

        # if enough args given init connections to ams and influx
        if (ams_endpoint and ams_token and ams_project):
            self.init_ams()
        if (influx_endpoint and influx_token and influx_bucket and influx_org):
            self.init_influx()

    # init connection to ams
    def init_ams(self):
        self.ams_client = ArgoMessagingService(endpoint=self.ams_endpoint, token=self.ams_token,
                                               project=self.ams_project)

    # init connection to influx
    def init_influx(self):
        self.influx_client = InfluxDBClient(
            url="http://localhost:8086", token=self.influx_token, org=self.influx_org
        )

    # update the topology
    def update_topology(self, dt):
        if dt != self.topology_date:
            # get topology endpoints for specific date from argo-web-api
            response = requests.get(
                "https://{}/api/v2/topology/endpoints?date={}".format(self.api_endpoint, dt),
                headers={'Accept': 'application/json', 'x-api-key': self.api_token}
            )
            if response.status_code == 200:
                topo_data = response.json()["data"]
                # create the topology index
                topo_index = {}
                # access index by using the service_type/hostname as a key pattern
                for item in topo_data:
                    key = item["service"]+"/"+item["hostname"]
                    # prep the value
                    result = {}
                    # add url information if present
                    if "info_URL" in item["tags"]:
                        result["url"] = item["tags"]["info_URL"]
                    # add id information if present
                    if "info_ID" in item["tags"]:
                        result["id"] = item["tags"]["info_ID"]
                    result["group"] = item["group"]
                    # add item to index
                    topo_index[key] = result
                # update current topology reference to new index
                self.topology = topo_index
                # update current tracking date
                self.topology_date = dt
                # return true if topology was indeed updated
                return True
        return False

    # consume from ams
    def consume_msgs(self, num_of_msgs=100):
        msgs = []
        ackids = list()
        # pull messages
        for id, msg in self.ams_client.pull_sub(self.ams_sub, num_of_msgs):
            # get payload
            data = msg.get_data()
            # prepare byte reader
            bytes_reader = io.BytesIO(data)
            # prepare avro decoder
            decoder = BinaryDecoder(bytes_reader)
            decoded_message = self.avro_reader.read(decoder)
            # grab ack id for each message
            ackids.append(id)

            # if message has actual data - collect it
            if "actual_data" in decoded_message and decoded_message["actual_data"] is not None:
                msgs.append(decoded_message)

        # if ack ids collected use them to ack the messages
        if ackids:
            self.ams_client.ack_sub(self.ams_sub, ackids)

        # return both messages and ackids (ackids used in logging)
        return msgs, ackids

    # transforms messages to datapoints (json representation convenient for influx)
    def msgs_to_datapoints(self, msgs):

        # small util function to extract the numeric value and the unit of measurement
        def extract_value(val):
            numpart = ""
            uompart = ""
            # each character that is digit or dot collect it and store it to the numeric part
            for char in val:
                if char.isdigit() or char == '.':
                    numpart = numpart + char
                else:
                    # at the first non digit / dot character stop
                    break
            # the rest of the string is the uom
            uompart = val[len(numpart):]
            return float(numpart), uompart

        # collect the data point for influx
        data_points = []
        # for each msg
        for msg in msgs:
            # extract date from timestamp
            msg_date = msg["timestamp"].split("T")[0]
            if msg_date != self.topology_date:
                self.update_topology(msg_date)
            # parse actual data
            perf = msg["actual_data"].split(";")[0].split("=")
            # measurement is the first token
            measurement = perf[0]
            # value/uom are extracted from the second token
            val, uom = extract_value(perf[1])
            # grab extra info from topology based on the service type and host
            extra = self.topology.get(msg["service"]+"/"+msg["hostname"])
            # prep extra info holders
            url = None
            group = None
            sid = None
            url = None
            if extra:
                if "group" in extra:
                    group = extra["group"]
                if "id" in extra:
                    sid = extra["id"]
                if "url" in extra:
                    url = extra["url"]
            # create the datapoint and add it to the collection
            data_points.append({
                'time': msg["timestamp"],
                'measurement': measurement,
                'fields': {
                    'value': val,
                    'unit': uom,
                },
                'tags': {
                    'hostname': msg["hostname"],
                    'service': msg["service"],
                    'metric': msg["metric"],
                    'group': group,
                    'id': sid,
                    'url': url
                }
            })
        # return all the datapoints
        return data_points

    # with the datapoints created send them to influx
    def send_to_influx(self, data_points):
        if self.influx_client:
            write_api = self.influx_client.write_api(
                write_options=SYNCHRONOUS)
            write_api.write(bucket=self.influx_bucket, record=data_points)

    # close influx connection
    def close_influx(self):
        self.influx_client.close()


def main(args):

    # create a perf data consumer with all the cli args given
    pd = PerfDataConsumer(
        args.api_endpoint, args.api_token, args.ams_endpoint, args.ams_token,
        args.ams_project, args.ams_sub, args.influx_endpoint, args.influx_token,
        args.influx_org, args.influx_bucket
    )

    # parse interval and batch size
    interval = int(args.interval)
    num_msg = int(args.batch)
    # consume loop
    while True:
        # consume the messages from ams
        msgs, ackids = pd.consume_msgs(num_msg)
        # convert messages to data points (json representation convenient for influx)
        points = pd.msgs_to_datapoints(msgs)
        pd.send_to_influx(points)
        if ackids:
            logging.info("consumed {} messages, sub offset: {}".format(num_msg, ackids[-1]))
        sleep(interval)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Ingest performance data and write them to influx db")
    parser.add_argument(
        "-a", "--api-endpoint", metavar="STRING", help="argo web api endpoint",
        required=True, dest="api_endpoint")
    parser.add_argument(
        "-A", "--api-token", metavar="STRING", help="argo web api token",
        required=True, dest="api_token")
    parser.add_argument(
        "-M", "--ams-token", metavar="STRING", help="ams token", required=True, dest="ams_token")
    parser.add_argument(
        "-m", "--ams-endpoint", metavar="STRING", help="ams endpoint", required=True, dest="ams_endpoint")
    parser.add_argument(
        "-s", "--ams-sub", metavar="STRING", help="ams endpoint", required=True, dest="ams_sub")
    parser.add_argument(
        "-p", "--ams-project", metavar="STRING", help="ams project", required=True, dest="ams_project")
    parser.add_argument(
        "-i", "--influx-token", metavar="STRING", help="influx db token", required=True, dest="influx_token")
    parser.add_argument(
        "-I", "--interval", metavar="STRING", help="interval", required=True, dest="interval")
    parser.add_argument(
        "-B", "--batch", metavar="STRING", help="number of messages per batch", required=True, dest="batch")
    parser.add_argument(
        "-e", "--influx-endpoint", metavar="STRING",
        help="influx endpoint", required=True, dest="influx_endpoint")
    parser.add_argument(
        "-o", "--influx-org", metavar="STRING", help="influx organisation", required=True, dest="influx_org")
    parser.add_argument(
        "-b", "--influx-bucket", metavar="STRING", help="influx bucket", required=True, dest="influx_bucket")

    sys.exit(main(parser.parse_args()))
