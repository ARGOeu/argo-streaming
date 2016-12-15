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
