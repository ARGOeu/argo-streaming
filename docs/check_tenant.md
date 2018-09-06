# Python script for checking tenant configuration (in engine, hdfs and ams)

The specific script reads tenant configuration (as defined in argo-engine global config file)
and checks if the tenant has been properly configured in hdfs destination (computation profiles deployed, folders present)
and in ams (topics and subscriptions established.) Also checks the incoming data status for e.g.

- The number of messages in hdfs
- And if latest metric and sync data are present in hdfs directories

To check for a specific tenant issue:
`./bin/utils/check_tenant.py -t FOO -d 2018-09-10 -c /path/to/argo-streaming.conf`

To check for a specific tenant for today's date issue:
`./bin/utils/check_tenant.py -t FOO -c /path/to/argo-streaming.conf`

To check for all tenants for today's date issue:
`./bin/utils/check_tenant.py -c /path/to/argo-streaming.conf`

# Report format
Script produces a status report in json format with the following schema:
```json
{
  "engine_config": true,
  "tenants": [
    {
      "tenant": "Foo_tenant",
      "last_check": "2018-09-10T13:58:14Z",
      "engine_config": true,
      "hdfs": {
        "metric_data": true,
        "sync_data": {
          "Critical": {
            "downtimes": true,
            "group_endpoints": true,
            "blank_recomputation": true,
            "configuration_profile": true,
            "group_groups": true,
            "weights": true,
            "operations_profile": true,
            "metric_profile": true,
            "aggregation_profile": true
          }
        }
      },
      "ams": {
        "metric_data": {
          "ingestion": true,
          "status_streaming": true,
          "publishing": true,
          "messages_arrived": 1774700
        },
        "sync_data": {
          "ingestion": true,
          "status_streaming": true,
          "publishing": true,
          "messages_arrived": 715
        }
      }
    }
  ]
}
```

- `engine_config`: reports if the tenant configuration in argo engine is valid
- `last_check`: reports the timestamp of the check performed 
- `tenant`: refers tto the tenant name
- `hdfs`: contains status check results for the tenant in the HDFS datastore 
- `ams`: contains status checks for the tenant in the Argo Messaging Service