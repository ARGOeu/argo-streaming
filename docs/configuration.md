#Argo Streaming Engine Main configuration

Argo Streaming Engine's main configuration is maintained in a single ini file
located by default in `/etc/argo-streaming/argo-streaming.conf`

Argo Streaming configuration is extensible and includes iteratable sections
for each tenant

## Configuration sections
Main configuration includes the following sections - (some of them are iteratable)

###[HDFS]
Connectivity to hdfs cluster for storing metric and connector data

###[API]
Connectivity to argo-web-api
Supported tenants are specified here

###[FLINK]
Connectivity to the flink cluster

###[JARS]
Specify jar paths used for each computation/job type

###[CLASSES]
Specify classes in each jar used for each computation/job type

###[AMS]
Connectivity to Argo Messaging Cluster for ingesting data and storing events

###[TENANTS:FooTenant]
Specific section with parameters for FooTenant
Tenant's AMS specifics (project, credentials) and MongoDB destinations.
Also Tenant reports are specified here

###[TENANTS:FooTenant:ingest-metric]
Specific parameters for FooTenant's ingest-metric jobs.
AMS subscription parameters and ingestion rates.

###[TENANTS:FooTenant:ingest-sync]
Specific parameters for FooTenant's ingest-metric jobs.
AMS subscription parameters and ingestion rates.

###[TENANTS:FooTenant:stream-status]
Specific parameters for FooTenant's stream-status jobs.
AMS subscription parameters and ingestion rates.
Status event output destinations (kafka, hbase, fs)

## Configuration consistency validation and schema
To maintain consistency in main configuration file especially in the expandable sections (tenants)
a configuration schema accompanies the file in the same folder (`/etc/argo-streaming/config.schema.json`)

Schema is a json structure that describes the expected configuration sections and fields. For each field
an expected type is specified, along with a verbose description and an optional flag. For sections or fields
that are bound to iterate (such as tenant sections) a special iteration symbol `~` is used in names. Also
the iterable item has always an `~` parameter which specifies the list of items that are used for iterating

For example the `API.tenants` holds a list of available tenants `ta`,`tb`,`tc`. The specific
section for each tenant is declared with the schema name `TENANTS:~` and it's iteration field
`"~" : API.tenants` dictates that the the following sections will be generated based on `TENANTS:~` name
and the list `API.tenants` items: `ta`, `tb`, `tc` such as:
- `TENANT:ta`
- `TENANT:tb`
- `TENANT:tc`

A snippet of the schema configuration defining the above is provided below:
```
{
  "API": {
    "tenants": {
      "type": "list",
      "desc": "a list of tenants provided by the argo-web-api and supported in argo-streaming engine"
    }
  },
  "TENANT:~": {
      "~" : "API.tenants"
      "ams_project": {
        type: "string",
        "desc": "argo-messaging server project for this tenant"
      },
      "mongo_uri": {
        type: "uri",
        "desc": "mongodb destination for storing tenant's results"
      }
  }
}
```

## Logging configuration
Logging in Argo Streaming Service is handled by a third configuration file in json format deployed
in the same folder as main configuration `/etc/argo-streaming/logger.conf`

Logging.conf uses python's logging configuration file format as described
here: https://docs.python.org/2/library/logging.config.html#logging-config-fileformat
