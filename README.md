[![Build Status](https://travis-ci.org/ARGOeu/argo-streaming.svg?branch=devel)](https://travis-ci.org/ARGOeu/argo-streaming)
#ARGO STREAMING

## Flink Jobs

### AMS ingest metric data (and store them to HDFS and/or Hbase)

Flink job that connects as a subscriber to an ARGO Messaging Service, pulls messages from a specific project/subscription and stores them to a remote hdfs and/or hbase cluster.

Prepare job to submit in flink:

- `cd flink_jobs/ams_ingest_metric`
- `mvn clean && mvn package`


Run jar in flink:

- `flink run ams-ingest-metric-0.1.jar --ams.endpoint {...} --ams.port {...} --ams.token {...} -ams.project {...} --ams.sub {...} --avro.schema {...} --hbase.master {...} --hbase.zk.quorum {...} --hbase.zk.port {...} --hbase.namespace {...} --hbase.table {...} --hbase.master.port {...} --hdfs.path {...} --check.path {...} --check.interval`

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

- `cd flink_jobs/stream_sync`
- `mvn clean && mvn package`


Run jar in flink:

- `flink run  AmsSyncHDFS-0.0.1- --ams.endpoint {...} --ams.port {...} --ams.token {...} -ams.project {...} --ams.sub {...} --base.path {...}

Job required cli parameters:

`--ams.endpoint`      : ARGO messaging api endoint to connect to msg.example.com

`--ams.port`          : ARGO messaging api port

`--ams.token`         : ARGO messaging api token

`--ams.project`       : ARGO messaging api project to connect to

`--ams.sub`           : ARGO messaging subscription to pull from

`--base.path`         : Base hdfs path to store connector data to (e.g. hdfs://localhost:9000/user/foo/path/to/tenant)

`--hbase-master`      : hbase endpoint

`--hbase-master-port` : hbase master port

`--hbase-zk-quorum`   : comma separated list of hbase zookeeper servers

`--hbase-zk-port`     : port used by hbase zookeeper servers

`--hbase-namespace`   : table namespace used (usually tenant name)

`--hbase-table`       : table name (usually metric_data)

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


### Stream Status

Flink job that connects as a subscriber to an ARGO Messaging Service, pulls messages from a specific project/subscription.
For each metric data message the job calculates status changes in the whole topology and produces status Events.
The status events are then forwarded to a specific kafka topic (and/or) hbase table (and/or) filesystem

Prepare job to submit in flink:

- `cd flink_jobs/stream_status
- `mvn clean && mvn package`


Run jar in flink:

- `flink run streaming-status-0.1.jar --ams.endpoint {...} --ams.port {...} --ams.token {...} -ams.project {...} --ams.sub.metric {...} --ams.sub.sync {...}  --sync.mps {...} --sync.egp {...} --sync.aps {...} --sync.ops {...} --hbase.master {...} --hbase.zk.quorum {...} --hbase.zk.port {...} --hbase.namespace {...} --hbase.table {...} --hbase.master.port {...} --kafka.servers {...} --kafka.topic {...} --fs.output {...}`

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

`--report`            : report uuid

`--run.date`          : target date in DD-MM-YYYY format

`--egroup.type`       : endpoint group type used in report (for e.g. SITES)

`--mongo.uri`         : MongoDB uri for outputting the results to (e.g. mongodb://localhost:21017/example_db)

`--mongo.method`      : MongoDB method to be used when storing the results ~ either: `insert` or `upsert`


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
