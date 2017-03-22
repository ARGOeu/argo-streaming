[![Build Status](https://travis-ci.org/ARGOeu/argo-streaming.svg?branch=devel)](https://travis-ci.org/ARGOeu/argo-streaming)
#ARGO STREAMING

## Flink Jobs

### AMS stream to Hbase

Flink job that connects as a subscriber to an ARGO Messaging Service, pulls messages from a specific project/subscription and stores them to a remote hbase cluster.

Prepare job to submit in flink:

- `cd flink_jobs/ams_stream_hbase`
- `mvn clean && mvn package`


Run jar in flink:

- `flink run ams-stream-hbase-0.1.jar --ams-endpoint {...} --ams-port {...} --ams-token {...} -ams-project {...} --ams-sub {...} --avro-schema {...} --hbase-master {...} --hbase-zk-quorum {...} --hbase-zk-port {...} --hbase-namespace {...} --hbase-table {...} --hbase-master-port {...}`

Job required cli parameters:

`--ams-endpoint`      : ARGO messaging api endoint to connect to msg.example.com

`--ams-port`          : ARGO messaging api port

`--ams-token`         : ARGO messaging api token

`--ams-project`       : ARGO messaging api project to connect to

`--ams-sub`           : ARGO messaging subscription to pull from

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
The status events are then forwarded to a specific kafka topic

Prepare job to submit in flink:

- `cd flink_jobs/stream_status
- `mvn clean && mvn package`


Run jar in flink:

- `flink run streaming-status-0.1.jar --ams-endpoint {...} --ams-port {...} --ams-token {...} -ams-project {...} --ams-sub {...} --avro-schema {...} --hbase-master {...} --hbase-zk-quorum {...} --hbase-zk-port {...} --hbase-namespace {...} --hbase-table {...} --hbase-master-port {...} --sync-mps {...} --sync-egp {...} --sync-aps {...} --sync-ops {...}`

Job required cli parameters:

`--ams.endpoint`      : ARGO messaging api endoint to connect to msg.example.com

`--ams.port`          : ARGO messaging api port

`--ams.token`         : ARGO messaging api token

`--ams.project`       : ARGO messaging api project to connect to

`--ams.sub`           : ARGO messaging subscription to pull from

`--avro.schema`       : Schema used for the decoding of metric data payload

`--hbase-master`      : hbase endpoint

`--hbase-master-port` : hbase master port

`--hbase-zk-quorum`   : comma separated list of hbase zookeeper servers

`--hbase-zk-port`     : port used by hbase zookeeper servers

`--hbase-namespace`   : table namespace used (usually tenant name)

`--hbase-table`       : table name (usually metric_data)

`--sync.mps`          : Metric profile file used

`--sync.egp`          : Endpoint group topology file used

`--sync.aps`          : Aggregation profile used

`--sync.ops`           :Operations profile used

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
