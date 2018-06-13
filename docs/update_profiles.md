# Python script for updating profiles (argo-web-api --> hdfs)

Argo-streaming engine maintains profile files stored in flink shared storage per tenant and report.
These profiles are essential for computing a/r and status results during flink-jobs and are not provided
automatically by connectors. Profiles include:
- operations_profile: `TENANT_ops.json` which includes truth tables about the fundamental aggregation operations applied
  on monitor status timelines (such as 'AND', 'OR' .etc between statuses of 'OK', 'WARNING', 'CRITICAL', 'MISSING' etc.)
- aggregation_profile: `TENANT_REPORT_aps.json` which includes information on what operations ('AND','OR') and how are
  applied on different service levels
- report configuration profile: `TENANT_REPORT_cfg.json` which includes information on the report it self, what profiles
it uses and how filters data

Each report uses an operations profile. The operation profile is defined also in argo-web-api instance at the following url
`GET https://argo-web-api.host.example/api/v2/operations_profiles/{{profile_uuid}}`

Each report uses an aggregation profile. The aggregation profile is defined also in argo-web-api instance at the following url
`GET https://argo-web-api.host.example/api/v2/aggregation_profiles/{{profile_uuid}}`

Each report contains a configuration profile. The report is defined also in argo-web-api instance at the following url
`GET https://argo-web-api.host.example/api/v2/reports/{{report_uuid}}`


Providing a specific `tenant` and a specific `report`, script `update_profiles` checks corresponding profiles on hdfs  against
latest profiles provided by argo-web-api. If they don't match it uploads the latest argo-web-api profile definition in hdfs

# Submission scripts automatic invoke
Script logic is programmatically called in a/r and status job submission scripts

# Invoke manually from command line
Script logic can be invoked from command line by issuing
`$ ./update_profiles -t TENANT -r REPORT`

help on command line arguments given by issuing
`$ ./update_profiles -h`

```
usage: update_profiles.py [-h] -t STRING -r STRING [-c STRING]

update profiles for specific tenant/report

optional arguments:
  -h, --help            show this help message and exit
  -t STRING, --tenant STRING
                        tenant owner
  -r STRING, --report STRING
                        report
  -c STRING, --config STRING
                        config
```

# Config file parameters used
Update_profiles script will search for the main argo-streaming.conf file but uses only the following
configuration parameters:

```
[HDFS]
# hdfs namenode hostname
hdfs_host : hostname
# hdfs namenode port
hdfs_port : portnumber
# hdfs username used
hdfs_user : user
# path template to default argo hdfs base sync folder
hdfs_sync : hdfs/path/to/argo/{{tenant}}/sync/folder
# path to local binary that allows hdfs upload (hdfs client)
writer_bin: local/path/to/binary

[API]
# endpoint of argo-web-api used
host : argo-web-api.host.example.foo

# Tenant list supported on argo-web-api (might retrieved automatically later)
tenants : TENANT_A, TENANT_B

# for each of the above tenants specify it's api_token by using the pattern {{TENANT}}_key : {{value}}
TENANT_A_key = secret1
TENANT_B_key = secret2

[LOGS]
# get log level
log_level: info
```

# Dependencies
Update_profiles script is deployed alongside the other scripts included in the `./bin/` folder of argo-streaming engine
and relies on the same dependencies. Specifically it uses `requests` lib for contacting argo-web-api and python `snakebite` lib
for checking hdfs files. Because `snakebite` lib lacks upload mechanism the script relies on a binary client wrapper to upload
files (maybe a shell script over java-based hdfs-client or other implementations). The wrapper script must be invoked with the following
argument pattern `./hdfs_writer.sh put {local_path} {hdfs_path}`
