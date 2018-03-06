# Python utility scripts for easier flink job submission/handling

| Script | Description | Shortcut |
|--------|-------------|---------- |
| metric_ingestion_submit.py | Python wrapper over flink sumbit metric ingestion job.| [Details](#ingest-metric) |
| sync_ingestion_submit.py | Python wrapper over flink submit sync ingestion job.| [Details](#ingest-synbc) |
| ar_job_submit.py | Python wrapper over the flink batch AR job. | [Details](#batch-ar) |

<a id="ingest-metric"></a>
## Metric Ingestion Submit Script
Python wrapper over flink sumbit metric ingestion job.
Metric Ingestion job receives metric data from an AMS endpoint subscription and stores them to a proper hdfs destination.

`metric_ingestion_submit.py -t <Tenant> -c <ConfigPath> -u<Sudoless>`

`-t : Specify the tenant the job will run for`

`-c : Check if config path has been given as a cli argument, else check /etc/argo-streaming/conf/conf.cfg else check conf folder inside the repo`

`-u : If specified the flink command will run without sudo`

<a id="ingest-sync"></a>
## Sync Ingestion Submit Script
Same as Metric Ingestion but for connector data
This job connects to AMS and stores connector data (by report) in an hdfs destination

`sync_ingestion_submit.py -t <Tenant> -c <ConfigPath> -u<Sudoless>`

`-t : Specify the tenant the job will run for`

`-c : Check if config path has been given as a cli argument, else check /etc/argo-streaming/conf/conf.cfg else check conf folder inside the repo`

`-u : If specified the flink command will run without sudo`

<a id="batch-ar"></a>
## A/R Batch Job
A/R job submission is a batch job that will run and finish on the cluster

`ar_job_submit.py -t <Tenant> -c <ConfigPath> -u<Sudoless> -r<Report> -d<Date> -m<Method>`

`-t : Specify the tenant the job will run for`

`-c : Check if config path has been given as a cli argument, else check /etc/argo-streaming/conf/conf.cfg else check conf folder inside the repo`

`-u : If specified the flink command will run without sudo`

`-r : The type of report, e.g. Critical`

`-d : The date we want the job to run for. Format should be YYYY-MM-DD`

`-m : How mongoDB will handle the generated results. Either insert or upsert`

### Important

Sometimes connector data (metric profiles,endpoint,group endpoints,weights) appear delayed (in comparison with the metric data) or might be missing. We have a check mechanism that looks back (up to three days) for connector data that might be missing and uses that.
Flink job receives a parameter of insert or upsert when storing results. Give the ability to honor that parameter and when insert is used, call a clean mongo script for removing (if present) any mongo a/r report data for that very day

## Configuration file
```
[HDFS]
HDFS credentials

[LOGS]
log modes describe what kind of logging handlers we want to use. For each handler we specify its logging level and resource. If there is no specified level for each handler, we specify a global log level for all handlers.

[FLINK]
Specify where to find the flink executable and the job manager.

[CLASSES] [JARS]
The classes and jars needed for each job.

[AMS]
AMS port to connect to and endpoint where you find the service

[TENANTS:]
Token for each tenant to access the service
[TENANTS:TENANTA:JOB]
Specific parameters needed for each job to run
ams_batch : num of messages to be retrieved per request to AMS service
ams_interval : interval (in ms) between AMS service requests
check_interval : interval for checkpointing (in ms)
check_path : path to store flink checkpoints
```
