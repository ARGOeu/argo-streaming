#!/usr/bin/env python

import argparse
import logging
import json
import requests
import sys
import re

# Receives input from a json config file and checks if specific jobs run for each tenant at flink cluster
# example of config.json:
# [
#     {
#         "tenant":"TENANT_A",
#         "streaming":["stream_job_1","stream_job_2"]
#     },
#     {
#         "tenant":"TENANT_B",
#         "streaming":[]
#     }
# ]

log = logging.getLogger(__name__)

def read_check_json(check_file_path):
    with open(check_file_path, 'r') as check_file:
        return json.load(check_file)

def read_flink_jobs(flink_url):
    flink_url = flink_url + "/joboverview/running"
    return requests.get(flink_url).json()

def check_flink_jobs(flink_json, checklist):
    for item in flink_json["jobs"]:
        job_txt = item["name"]
        print(job_txt)
        m = re.search(r"\/projects\/(\w+)", job_txt)
        if m:
            tenant = m.group(1)
            if tenant in checklist:
                if job_txt.startswith("Ingesting metric data"):
                    checklist[tenant]["ingest_metric"] = "OK"
                elif job_txt.startswith("Streaming status using data"):
                    m = re.search(r"\/(\w+)$", job_txt)
                    if m:
                        source = m.group(1)
                        checklist[tenant]["streaming"][source] = "OK"
    return checklist


def gen_checklist(check_json):
    tenants = {}
    for item in check_json:
        tenant_body = {}
        stream_body = {}
        for stream_name in item["streaming"]:
            stream_body[stream_name] = "CRITICAL"
        tenant_body["streaming"] = stream_body
        tenant_body["ingest_metric"] = "CRITICAL"
        tenants[item["tenant"]] = tenant_body
    return tenants

def summarize(results):
    summary = "OK"
    errors = []
    for tenant in results:
        if results[tenant]["ingest_metric"] == False:
            summary = "ERROR"
            errors.append("ERROR - " + tenant + ": ingest metric data off")
        for item in results[tenant]["streaming"]:
            if results[tenant]["streaming"][item] == False:
                summary = "ERROR"
                errors.append("ERROR - " + tenant + ": streaming off for " + item)

    errors.insert(0,summary)
    return errors


def main(args=None):

    checklist = gen_checklist(read_check_json(args.check))
    jobs = read_flink_jobs(args.flink)
    result = check_flink_jobs(jobs,checklist)
    errors = summarize(result)
    if args.output:
        content = json.dumps({"status": errors[0], "tenants": result}, indent=4)
        with open(args.output, 'w') as file:
            file.write(content)
    else:
        print(result)

    if len(errors) > 1:
        for error in error:
            print(error)
        sys.exit(1)




if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Argo flink hdfs healthcheck")
    parser.add_argument(
        "-f", "--flink-endpoint", metavar="STRING", help="flink endpoint", required=True, dest="flink")
    parser.add_argument(
        "-c", "--check", metavar="STRING", help="check filename", required=True, dest="check")
    parser.add_argument(
        "-o", "--output", metavar="STRING", help="check filename", required=False, dest="output")

    # Pass the arguments to main method
    sys.exit(main(parser.parse_args()))