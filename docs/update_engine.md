# Python script for updating argo-engine

The specific script triggers a compete update of argo-engine configuration,
tenant profiles, ams definitions and cron-jobs by requesting tenant information
from argo-web-api.

## Update steps
The script executes in-order the following update steps:
  - Update argo-engine configuration from argo-web-api
  - Check AMS and create missing topics, subscriptions, users. Update back argo-engine configuration
  - Update for each tenant/report the required profiles in HDFS
  - Check tenant status for each tenant and report
  - For valid reports (reports that have ready data on hdfs) generate cron jobs
  - Update crontab for the specified cron jobs

## Backup of configuration
Script gives the ability to backup the current configuration before update (Use the `-b cli argument`).
The configuration will be backed up in the form of `/path/to/argo-engine.conf.2018-06-06T00:33:22`

# Execution
To run the script issue:
`./update-egine.py -c /etc/argo-engine.conf -b`

Arguments:
`-c`: specify the path to argo-engine main configuration file
`-b`: (optional) backup the current configuration
