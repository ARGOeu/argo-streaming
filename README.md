[![Build Status](https://travis-ci.org/ARGOeu/argo-streaming.svg?branch=devel)](https://travis-ci.org/ARGOeu/argo-streaming)
#ARGO STREAMING

## Flink Jobs

### AMS ingest metric data (and store them to HDFS and/or Hbase)

Flink job that connects as a subscriber to an ARGO Messaging Service, pulls messages from a specific project/subscription and stores them to a remote hdfs and/or hbase cluster.

Prepare job to submit in flink:

- `cd flink_jobs/ams_ingest_metric`
- `mvn clean && mvn package`


Run jar in flink:

- `flink run ams-ingest-metric-0.1.jar --ams.endpoint {...} --ams.port {...} --ams.token {...} -ams.project {...} --ams.sub {...} --avro.schema {...} --hbase.master {...} --hbase.zk.quorum {...} --hbase.zk.port {...} --hbase.namespace {...} --hbase.table {...} --hbase.master.port {...} --hdfs.path {...} --check.path {...} --check.interval  --ams.batch {...} --ams.interval {...}`

Job required cli parameters:

`--ams.endpoint`      : ARGO messaging api endoint to connect to msg.example.com

`--ams.port`          : ARGO messaging api port

`--ams.token`         : ARGO messaging api token

`--ams.project`       : ARGO messaging api project to connect to

`--ams.sub`           : ARGO messaging subscription to pull from

Job optional cli parameters:

`--hbase.master`      : hbase endpoint

`--hbase.master.port` : hbase master port

`--hbase.zk.quorum`   : comma separated list of hbase zookeeper servers

`--hbase.zk.port`     : port used by hbase zookeeper servers

`--hbase.namespace`   : table namespace used (usually tenant name)

`--hbase.table`       : table name (usually metric_data)

`--hdfs.path`         : base path for storing metric data on hdfs

`--check.path`        : path to store flink checkpoints

`--check.interval`    : interval for checkpointing (in ms)

`--ams.batch`         : num of messages to be retrieved per request to AMS service

`--ams.interval`      : interval (in ms) between AMS service requests

`--ams.proxy`         : optional http proxy url to be used for AMS requests

`--ams.verify`        : optional turn on/off ssl verify

### Restart strategy
Job has a fixed delay restart strategy. If it fails it will try to restart for a maximum of 10 attempt with a retry interval of 2 minutes
between each attempt

### Metric data hbase schema

Metric data are stored in hbase tables using different namespaces for different tenants (e.g. hbase table name = '{TENANT_name}:metric_data')

Each table has the following row key (composed by a unique combination of fields):

`rowkey`= `{hostname}` + `|` + `{service_name}` + `|` + `{metric_name}` + `|` + `{monitoring_engine_name}` + `|` + `{timestamp}`

#### Hbase columns

Each hbase table has a column family named 'data' and the following columns:

`timestamp`         : Timestamp of the metric event

`host`              : hostname of the endpoint that the metric check was run to

`service`           : name of the service related to the metric data

`metric`            : name of the metric check

`monitoring_host`   : name of the monitoring host running the check

`status`            : status of the metric check

`summary`           : summary of the metric result

`message`           : details of the metric result

`tags`              : json list of tags used to add metadata to the metric event


### AMS ingest connector (sync) data to HDFS

Flink job that connects as a subscriber to an ARGO Messaging Service, pulls messages that contain connector (sync) data (metric profiles, topology, weight etc.) from a specific project/subscription and stores them to an hdfs destination. Each message should have the following attributes:
- report: name of the report that the connector data belong to
- type: type of the connector data (metric_profile, group_endpoints, group_groups, weights, downtimes)
- partition_date: YYYY-MM-DD format of date that the current connector data relates to.

Prepare job to submit in flink:

- `cd flink_jobs/ams_ingest_sync`
- `mvn clean && mvn package`


Run jar in flink:

- `flink run  ams-ingest-sync-0.1.jar- --ams.endpoint {...} --ams.port {...} --ams.token {...} -ams.project {...} --ams.sub {...} --hdfs.path {...} --ams.batch {...} --ams.interval {...}

Job required cli parameters:

`--ams.endpoint`      : ARGO messaging api endoint to connect to msg.example.com

`--ams.port`          : ARGO messaging api port

`--ams.token`         : ARGO messaging api token

`--ams.project`       : ARGO messaging api project to connect to

`--ams.sub`           : ARGO messaging subscription to pull from

`--hdfs.path`         : Base hdfs path to store connector data to (e.g. hdfs://localhost:9000/user/foo/path/to/tenant)

`--ams.batch`         : num of messages to be retrieved per request to AMS service

`--ams.interval`      : interval (in ms) between AMS service requests

`--ams.proxy`         : optional http proxy url to be used for AMS requests

`--ams.verify`        : optional turn on/off ssl verify

### Restart strategy
Job has a fixed delay restart strategy. If it fails it will try to restart for a maximum of 10 attempt with a retry interval of 2 minutes
between each attempt

### Stream Status

Flink job that connects as a subscriber to an ARGO Messaging Service, pulls messages from a specific project/subscription.
For each metric data message the job calculates status changes in the whole topology and produces status Events.
The status events are then forwarded to a specific kafka topic (and/or) hbase table (and/or) filesystem

Prepare job to submit in flink:

- `cd flink_jobs/stream_status
- `mvn clean && mvn package`


Run jar in flink:

- `flink run streaming-status-0.1.jar --ams.endpoint {...} --ams.port {...} --ams.token {...} -ams.project {...} --ams.sub.metric {...} --ams.sub.sync {...}  --sync.mps {...} --sync.egp {...} --sync.aps {...} --sync.ops {...} --hbase.master {...} --hbase.zk.quorum {...} --hbase.zk.port {...} --hbase.namespace {...} --hbase.table {...} --hbase.master.port {...} --kafka.servers {...} --kafka.topic {...} --fs.output {...} --ams.batch {...} --ams.interval {...}`

Job required cli input parameters:

`--ams.endpoint`      : ARGO messaging api endoint to connect to msg.example.com

`--ams.port`          : ARGO messaging api port

`--ams.token`         : ARGO messaging api token

`--ams.project`       : ARGO messaging api project to connect to

`--ams.sub.metric`    : ARGO messaging subscription to pull metric data from

`--ams.sub.sync`      : ARGO messaging subscription to pull sync data from

`--sync.mps`          : Metric profile file used

`--sync.egp`          : Endpoint group topology file used

`--sync.aps`          : Aggregation profile used

`--sync.ops`          : Operations profile used

Job optional cli parameters for hbase output:

`--hbase.master`      : hbase endpoint

`--hbase.master.port` : hbase master port

`--hbase.zk.quorum`   : comma separated list of hbase zookeeper servers

`--hbase.zk.port`     : port used by hbase zookeeper servers

`--hbase.namespace`   : table namespace used (usually tenant name)

`--hbase.table`       : table name (usually metric_data)

Job optional cli parameters for kafka output:

`--kafka.servers`     : Kafka server list to connect to

`--kafka.topic`       : Kafka topic to send status events to

Job optional cli parameters for filesystem output (local/hdfs):

`--fs.output`         : filesystem path for output (prefix with "hfds://" for hdfs usage)

Job optional cli parameters for mongo output:

`--mongo.uri`         : Mongo uri to store status events to

`--mongo.method`      : Mongo store method used (insert/upsert)

Job Optional cli parameters for ams ingestion related

`--ams.batch`         : num of messages to be retrieved per request to AMS service

`--ams.interval`      : interval (in ms) between AMS service requests
Other optional cli parameters
`--init.status`       : "OK", "MISSING" - initialize statuses for new items to a default value. Optimistically defaults to "OK"

`--daily`             : true/false - controls daily regeneration of events (not used in notifications)

`--timeout`           : long(ms) - controls default timeout for event regeneration (used in notifications)

`--ams.proxy`         : optional http proxy url to be used for AMS requests

`--ams.verify`        : optional turn on/off ssl verify

### Restart strategy
Job has a fixed delay restart strategy. If it fails it will try to restart for a maximum of 10 attempt with a retry interval of 2 minutes
between each attempt


### Status events schema

Status events are generated as JSON messages that are defined by the following common schema:
```
{"type":" ","endpoint_group":" ", "service": " ", "hostname": " ", "metric": " ", "status": " ", "timestamp": " "}
```

`type`: is the type of status event (metric,service,endpoint,endpoint_group)
`endpoint_group`: is the name of the  affected endpoint_group
`service`: is the name of the affected service
`hostname`: is the hostname of the affected endpoint
`metric`: is the name of the affected metric
`status`: is the value of the new status
`monitoring_host`: hostname of the monitoring node that produced the event
`ts_monitored`: the timestamp when the event was generated in the monitoring engine
`ts_processed`: the timestamp when the event was processed in streaming engine

### Number of events produced for each metric data received
A metric data message can produce zero, one or more status metric events. The system analyzes the new status introduced by the metric and then aggregates on top levels to see if any other status changes are produced.
If a status of an item actually changes an appropriate status event is produced based on the item type (endpoint_group,service,endpoint,metric).

## Threshold rule files
Each report can be accompanied by a threshold rules file which includes rules on low level metric data which may accompany a monitoring message with the field 'actual_data'.
The rule file is in JSON format and has the following schema:
```
{
  "rules": [
    {
      "group" : "site-101",
      "host" : "host.foo",
      "metric": "org.namespace.metric",
      "thresholds": "firstlabel=10s;30;50:60;0;100 secondlabel=5;0:10;20:30;50;30"
    }
   ]
}
```
Each rule has multiple thresholds separated by whitespace. Each threshold has the following format:
`firstlabel=10s;30;50:60;0;100` which corresponds to `{{label}}={{value}}{{uom}};{{warning-range}};{{critical-range}};{{min}};{{max}}`. Each range is in the form of`{{floor}}:{{ceiling}}` but some shortcuts can be taken in declarations.


## Batch Status

Flink batch job that calculates status results for a specific date

Prepare job to submit in flink:

- `cd flink_jobs/batch_status
- `mvn clean && mvn package`


Run jar in flink:

- `flink run batch-status-0.1.jar {list of cli parameters}`

Job required cli parameters:

`--pdata`             : file location of previous day's metric data (local or hdfs)

`--mdata`             : file location of target day's metric data (local or hdfs)

`--egp`               : file location of endpoint group topology file (local or hdfs)

`--ggp`               : file location of group of groups topology file (local or hdfs)

`--mps`               : file location of metric profile (local or hdfs)

`--aps`               : file location of aggregations profile (local or hdfs)

`--ops`               : file location of operations profile (local or hdfs)

`--rec`               : file location of recomputations file (local or hdfs)

`--conf`              : file location of report configuration json file (local or hdfs)

`--run.date`          : target date in DD-MM-YYYY format

`--mongo.uri`         : MongoDB uri for outputting the results to (e.g. mongodb://localhost:21017/example_db)

`--mongo.method`      : MongoDB method to be used when storing the results ~ either: `insert` or `upsert`

`--thr`               : (optional) file location of threshold rules


## Batch AR

Flink batch job that calculates a/r results for a specific date

Prepare job to submit in flink:

- `cd flink_jobs/batch_ar
- `mvn clean && mvn package`


Run jar in flink:

- `flink run ArgoArBatch-1.0.jar {list of cli parameters}

Job required cli parameters:

`--pdata`             : file location of previous day's metric data (local or hdfs)

`--mdata`             : file location of target day's metric data (local or hdfs)

`--egp`               : file location of endpoint group topology file (local or hdfs)

`--ggp`               : file location of group of groups topology file (local or hdfs)

`--mps`               : file location of metric profile (local or hdfs)

`--aps`               : file location of aggregations profile (local or hdfs)

`--ops`               : file location of operations profile (local or hdfs)

`--rec`               : file location of recomputations file (local or hdfs)

`--weights`           : file location of weights file (local or hdfs)

`--downtimes`         : file location of downtimes file (local or hdfs)

`--conf`              : file location of report configuration json file (local or hdfs)

`--run.date`          : target date in DD-MM-YYYY format

`--mongo.uri`         : MongoDB uri for outputting the results to (e.g. mongodb://localhost:21017/example_db)

`--mongo.method`      : MongoDB method to be used when storing the results ~ either: `insert` or `upsert`

`--thr`               : (optional) file location of threshold rules


## Flink job names
Running flink jobs can be listed either in flink dashboard by visiting `http://{{flink.webui.host}}:{{flink.webui.port}}`
or by quering jobmanager api at `http://{{flink.webui.host}:{{flink.webui.port}}/joboverview/running

Each job submitted has a discerning job name based on a specific template. Job names are used also by submission wrapper scripts (`/bin` folder) to check if a identical job runs (to avoid duplicate submission)

Job Name schemes:
Job Type| Job Name scheme
--------|----------------
Ingest Metric | Ingesting metric data from `{{ams-endpoint}}`/v1/projects/`{{project}}`/subscriptions/`{{subscription}}`
Ingest Sync | Ingesting sync data from `{{ams-endpoint}}`/v1/projects/`{{project}}`/subscriptions/`{{subscription}}`
Batch AR | Ar Batch job for tenant:`{{tenant}}` on day:`{{day}}` using report:`{{report}}`
Batch Status | Status Batch job for tenant:`{{tenant}}` on day:`{{day}}` using report:`{{report}}`
Streaming Status | Streaming status using data from `{{ams-endpoint}}`/v1/projects/`{{project}}`/subscriptions/`[`{{metric_subscription}}`,`{{sync_subscription}}`]

## Status Trends
Flink batch Job that calculate status trends for critical,warning,unknown status
Job requires parameters:

`--yesterdayData`             : file location of previous day's data
`--todayData`                 : file location of today day's data
`--N`              	       : (optional) number of displayed top results
`--criticaluri`               : uri to the mongo db collection, to store critical status results
`--warninguri`                : uri to the mongo db collection, to store warning status results
`--unknownuri`                : uri to the mongo db collection, to store unknown status results

`--baseuri`                   : uri to the web-api
`--metricProfileUUID`         : uuid of the metric_profile report, to be retrieved from the api request
`--key`                       : users's token, used for authentication


Flink batch Job that calculate flip flop trends for service endpoints metrics
Job requires parameters:

`--yesterdayData`              : file location of previous day's data
`--todayData`                  : file location of today day's data
`--N`              		: (optional) number of displayed top results
`--flipflopuri`                : uri to the mongo db , to store flip flop results
`--baseuri`                   : uri to the web-api
`--metricProfileUUID`         : uuid of the metric_profile report, to be retrieved from the api request
`--key`                       : users's token, used for authentication

Flink batch Job that calculate flip flop trends for service endpoints 
Job requires parameters:

`--yesterdayData`              : file location of previous day's data
`--todayData`                  : file location of today day's data
`--opProfilePath`              : file location of operations' profile truth tables
`--N`              		: (optional) number of displayed top results
`--servendpflipflopsuri`       : uri to the mongo db , to store flip flop results
`--op`                         : the name of the operation that will apply on the timeline
`--baseuri`                   : uri to the web-api
`--metricProfileUUID`         : uuid of the metric_profile report, to be retrieved from the api request
`--key`                       : users's token, used for authentication
