#!/usr/bin/env python

import argparse
import logging
import json
import requests
import sys
import re
from datetime import datetime

# Receives input from a json config file and checks if specific jobs run for each tenant at flink cluster
# example of config.json:
# {
#     "tenants": [
#         {
#             "tenant":"TENANT_A",
#             "streaming":["stream_job_1","stream_job_2"]
#         },
#         {
#             "tenant":"TENANT_B",
#             "streaming":[]
#         }
#     ],
#     "expected_taskmanagers": 3
# }

log = logging.getLogger(__name__)

def read_json(file_path):
    with open(file_path, 'r') as json_file:
        return json.load(json_file)

def read_flink_jobs(flink_url):
    flink_url = flink_url + "/joboverview/running"
    return requests.get(flink_url).json()["jobs"]

def read_flink_completed(flink_url):
    flink_url = flink_url  + "/joboverview/completed"
    return requests.get(flink_url).json()["jobs"]

def read_flink_taskmanagers(flink_url):
    flink_url = flink_url + "/taskmanagers"
    return requests.get(flink_url).json()["taskmanagers"]

def check_completed(job_list, prev_date):
    jobs = []
    if prev_date:
        jobs = [item for item in job_list if item["end-time"] > prev_date]
    else:
        jobs = job_list

    completed = 0
    failed = 0

    for item in jobs:
        if item["state"] == "FINISHED":
            completed = completed + 1
        if item["state"] == "FAILED":
            failed = failed + 1

    status = "OK"
    if failed > 0:
        status = "CRITICAL"
    return { "status": status, "completed": completed, "failed": failed }

def check_taskmanagers(taskmanager_list, expected):
    status = "OK"
    if len(taskmanager_list) < expected:
        status = "CRITICAL"
    nodes = []
    for item in taskmanager_list:
        host = item["path"].split("@")[1].split(":")[0]
        nodes.append(host)
    return { "status": status, "nodes": nodes }


def check_flink_jobs(job_list, checklist):
    for item in job_list:
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
    for item in check_json["tenants"]:
        tenant_body = {}
        stream_body = {}
        for stream_name in item["streaming"]:
            stream_body[stream_name] = "CRITICAL"
        tenant_body["streaming"] = stream_body
        tenant_body["ingest_metric"] = "CRITICAL"
        tenants[item["tenant"]] = tenant_body
    return tenants

def summarize(result_running, result_completed, result_taskmanagers):
    summary = "OK"
    errors = []
    for tenant in result_running:
        if result_running[tenant]["ingest_metric"] == False:
            summary = "ERROR"
            errors.append("ERROR - " + tenant + ": ingest metric data off")
        for item in result_running[tenant]["streaming"]:
            if result_running[tenant]["streaming"][item] == False:
                summary = "ERROR"
                errors.append("ERROR - " + tenant + ": streaming off for " + item)

    if result_completed["status"] == "CRITICAL":
        summary = "ERROR"
        errors.append("ERROR - failed jobs:" + str(result_completed["failed"]))

    if result_taskmanagers["status"] == "CRITICAL":
        summary = "ERROR"
        errors.append("ERROR - missing taskamangers: " + str(len(result_taskmanagers["nodes"])))

    errors.insert(0,summary)
    return errors


def main(args=None):
    prev_date=None
    if args.output:
        prev_data = read_json(args.output)
        if "timestamp" in prev_data:
            prev_date = datetime.fromisoformat(prev_data["timestamp"]).timestamp()

    checkdata = read_json(args.check)
    expected_taskmanagers = checkdata["expected_taskmanagers"]
    completed_data = read_flink_completed(args.flink)
    taskmanager_data = read_flink_taskmanagers(args.flink)
    result_completed = check_completed(completed_data, prev_date)
    result_taskmanagers = check_taskmanagers(taskmanager_data, expected_taskmanagers)
    checklist = gen_checklist(read_json(args.check))
    jobs = read_flink_jobs(args.flink)
    result_running = check_flink_jobs(jobs,checklist)

    errors = summarize(result_running, result_completed, result_taskmanagers)
    if args.output:
        content = json.dumps({"status": errors[0], "tenants": result_running, "timestamp": datetime.now().isoformat(), "taskmanagers": result_taskmanagers, "jobs":result_completed}, indent=4)
        with open(args.output, 'w') as file:
            file.write(content)
    else:
        print(result_running)

    if len(errors) > 1:
        for error in errors:
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