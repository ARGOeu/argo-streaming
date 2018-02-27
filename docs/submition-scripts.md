# Python utility scripts for easier flink job submission/handling

| Script | Description | Shortcut |
|--------|-------------|---------- |
| metric_ingestion_submit.py | Python wrapper over flink sumbit metric ingestion job.| [Details](#ingest-metric) |

<a id="ingest-metric"></a>
## Metric Ingestion Submit Script
Python wrapper over flink sumbit metric ingestion job.
Metric Ingestion job receives metric data from an AMS endpoint subscription and stores them to a proper hdfs destination.

`metric_ingestion_submit.py -t <Tenant> -c <ConfigPath> -u<Sudoless>`

`-t : Specify the tenant the job will run for`

`-c : Check if config path has been given as a cli argument, else check /etc/argo-streaming/conf/conf.cfg else check conf folder inside the repo`

`-u : If specified the flink command will run without sudo`

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
