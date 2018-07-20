# Python script for updating ams (argo-web-api --> argo streaming engine --> ams)

Having an updated configuration from argo-web-api, argo engine can access Argo
Messaging service to check if expected projects/topics/subscriptions/users have been
setup for each tenant. If items are missing update_ams script can create them (by
  setting up new tenant project, topics, subscriptions and users)

To manually trigger an argo ams check for all tenants, issue:
`./bin/utils/update_ams.py -c /path/to/argo-streaming.conf`

To manually trigger an argo ams check for a specific tenant issue:
`./bin/utils/update_ams.py -c /path/to/argo-streaming.conf -t tenant_a`

Script will read the designated `argo-streaming.conf` and extract information
for all supported Tenants. For each tenant will examine if the corresponding
project exists in AMS endpoint and if the needed topics for publishing and
ingestion have been set-up.

# Expected AMS items for each tenant

Given a tenant with name 'foo' the engine expects the following to be set-up
in AMS:
- tenant-project: `FOO` (project always has same name as tenant but in all caps)
- tenant-topics:
 - `metric_data` (feed for metric data)
 - `sync_data` (feed for connector data)
- tenant-subscriptions:
 - `ingest_metric` (connected to `metric_data` used for ingestion)
 - `ingest_sync` (connected to `sync_data` used for ingestion)
 - `status_metric` (connected to `metric_data` used for streaming computations)
 - `status_sync` (connected to `metric_data` used for streaming computations)
 - users:
  - `ams_foo_admin` with role: `project_admin` (project admin)
  - `ams_foo_publisher` with role `publisher` (used for publishing clients)
  - `ams_foo_consumer`with role `consumer` (used for ingestion jobs)

Also the topics must include `ams_foo_publisher` in their acls and in the same way
subscriptions must include `ams_foo_consumer` in their acls

Update_ams for a given tenant, checks AMS endpoint for the above expected items.
If an item is found missing it's automatically created.
