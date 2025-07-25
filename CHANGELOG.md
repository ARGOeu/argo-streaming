# Changelog

All notable changes in argo-streaming project are documented here

## [v3.0.0](https://github.com/ARGOeu/argo-streaming/releases/tag/v3.0.0) - (2025-07-24)

### Added:
- ARGO-4326 Combined tenant implementation added to batch_multi, flink_jobs_v3 ([#396](https://github.com/ARGOeu/argo-streaming/pull/396))
- ARGO-4312 Performance data ingestion proof of concept in python ([#397](https://github.com/ARGOeu/argo-streaming/pull/397))
- ARGO-4312 Ingest performance data to timeseries database ([#398](https://github.com/ARGOeu/argo-streaming/pull/398))
- add tentant name to point for influx db ([#409](https://github.com/ARGOeu/argo-streaming/pull/409))
- Add healthcheck script ([#422](https://github.com/ARGOeu/argo-streaming/pull/422))
- ARGO-4904 ARGO-4893 Support new type of recomputations that changes statuses directly (Lot1) / Issue during excluding metric egi.cloud.openstack-vm from fedcloud SITES ([#439](https://github.com/ARGOeu/argo-streaming/pull/439))
- ARGO-5069 Add CHANGELOG.md ([#445](https://github.com/ARGOeu/argo-streaming/pull/445))


### Fixed:
- ARGO-4337 Handle issue for not existing service in Aggregation Profiles ([#399](https://github.com/ARGOeu/argo-streaming/pull/399))
- ARGO-4372 Investigate potential issue with downtimes end and prolongue within same day ([#401](https://github.com/ARGOeu/argo-streaming/pull/401))
- ARGO-4396 Include latest fixes on flink_jobs_v2 to flink_jobs_v3 ([#406](https://github.com/ARGOeu/argo-streaming/pull/406))
- fix ingestion job to both write in hdfs and in influx db ([#408](https://github.com/ARGOeu/argo-streaming/pull/408))
- ARGO-4466 Issue with topology sync during removing APEL service from metric profiles ([#411](https://github.com/ARGOeu/argo-streaming/pull/411))
- ARGO-4558: ARGO Streaming: Fix broken History Url in Notifications ([#413](https://github.com/ARGOeu/argo-streaming/pull/413))
- ARGO-4615 Handle duplicate entries in dataset ([#415](https://github.com/ARGOeu/argo-streaming/pull/415))
- ARGO-4879 Streaming job fails due to null pointer exception when argo web-api is unreachable ([#426](https://github.com/ARGOeu/argo-streaming/pull/426))
- ARGO-4879 Streaming job fails due to null pointer exception when argo web-api is unreachable ([#427](https://github.com/ARGOeu/argo-streaming/pull/427))
- ARGO-4884 Missing statuses at the beginning of day for cert validity checks ([#428](https://github.com/ARGOeu/argo-streaming/pull/428))
- ARGO-4893 Issue during excluding metric egi.cloud.openstack-vm from fedcloud SITES ([#429](https://github.com/ARGOeu/argo-streaming/pull/429))
- ARGO-4879 Streaming job fails due to null pointer exception when argo web-api is unreachable ([#430](https://github.com/ARGOeu/argo-streaming/pull/430))
- ARGO-4879 Streaming job fails due to null pointer exception when argo web-api is unreachable ([#438](https://github.com/ARGOeu/argo-streaming/pull/438))
- ARGO-5009 Overwriten status during status recomputation request for same metric ([#440](https://github.com/ARGOeu/argo-streaming/pull/440))
- ARGO-5059 Ingestion job seems to request topology from api on each data published to influx ([#444](https://github.com/ARGOeu/argo-streaming/pull/444))

### Changed:
- Bump hadoop-client from 2.6.0 to 2.7.3.2.6.5.0-292 in /flink_jobs_v3/stream-status ([#393](https://github.com/ARGOeu/argo-streaming/pull/393))
- ARGO-4286 Log4j dependency - vulnerability issue ([#394](https://github.com/ARGOeu/argo-streaming/pull/394))
- Bump requests from 2.22.0 to 2.31.0 in /bin ([#395](https://github.com/ARGOeu/argo-streaming/pull/395))
- Bump org.apache.avro:avro from 1.10.2 to 1.11.3 in /flink_jobs/ProfilesManager ([#402](https://github.com/ARGOeu/argo-streaming/pull/402))
- Bump org.apache.avro:avro from 1.10.2 to 1.11.3 in /flink_jobs_v2/ProfilesManager ([#403](https://github.com/ARGOeu/argo-streaming/pull/403))
- Bump org.apache.avro:avro from 1.10.2 to 1.11.3 in /flink_jobs_v3/profiles-manager ([#404](https://github.com/ARGOeu/argo-streaming/pull/404))
- bump dependency avro ([#405](https://github.com/ARGOeu/argo-streaming/pull/405))
- AROG-4451 Handle topology sync during ingestion of performance data ([#410](https://github.com/ARGOeu/argo-streaming/pull/410))
- Bump pymongo from 3.10.0 to 4.6.3 in /bin ([#412](https://github.com/ARGOeu/argo-streaming/pull/412))
- Bump requests from 2.22.0 to 2.32.0 in /bin ([#414](https://github.com/ARGOeu/argo-streaming/pull/414))
- ARGO-4659 Make flinkv2 jobs able to write to Mongo 6.0 ([#416](https://github.com/ARGOeu/argo-streaming/pull/416))
- ARGO-4670 Confirm that flink_jobs_v3 containes the same features and fixes as flink_jobs_v2 ([#417](https://github.com/ARGOeu/argo-streaming/pull/417))
- ARGO-4671 Investigate moving to HDFS version compatible with flink 1.17 ([#418](https://github.com/ARGOeu/argo-streaming/pull/418))
- clear poms for flink1.17 ([#419](https://github.com/ARGOeu/argo-streaming/pull/419))
- update poms to be cleared ([#420](https://github.com/ARGOeu/argo-streaming/pull/420))
- update poms and sync the ingestion jobs ([#421](https://github.com/ARGOeu/argo-streaming/pull/421))
- update script for ams configuration ([#423](https://github.com/ARGOeu/argo-streaming/pull/423))
- Bump commons-io:commons-io from 2.10.0 to 2.14.0 in /flink_jobs/OperationsManager ([#424](https://github.com/ARGOeu/argo-streaming/pull/424))
- Bump org.apache.avro:avro from 1.10.2 to 1.11.4 in /flink_jobs/ProfilesManager ([#425](https://github.com/ARGOeu/argo-streaming/pull/425))
- ARGO-4922 Update flink healthcheck script with checks for taskmanager… ([#431](https://github.com/ARGOeu/argo-streaming/pull/431))
- Before mongo6 ([#432](https://github.com/ARGOeu/argo-streaming/pull/432))
- Try mongo-driver-sync v4.7.2 ([#433](https://github.com/ARGOeu/argo-streaming/pull/433))
- use mongo 3.x driver in multijob ([#434](https://github.com/ARGOeu/argo-streaming/pull/434))
- ARGO-4949 Set argo-web-api verify to false for multijob and streaming… ([#435](https://github.com/ARGOeu/argo-streaming/pull/435))
- Add mongo 3.x driver to multijob and streaming job ([#436](https://github.com/ARGOeu/argo-streaming/pull/436))
- Revert "ARGO-4949 Set argo-web-api verify to false for multijob and streaming…" ([#437](https://github.com/ARGOeu/argo-streaming/pull/437))

## [v2.1.2](https://github.com/ARGOeu/argo-streaming/releases/tag/v2.1.2) - (2023-05-10)

### Added:
- ARGO-4257 Create flink_jobs_v3 moduled project and add modules migrated to flink 1.17

### Changed:
- ARGO-4244 introduce level params in stream status
- ARGO-4280 Introduce basispath argument in multijob submit script
- ARGO-4289 Update multijob to support source-data and source-topology arguments
- ARGO-4278 Refactor multi job , to support EOSC combined mode
- ARGO-4260 Update jenkins build/test paths

### Fixed:
- ARGO-4282 Fix source-topo and source-data arguments to be optional
- ARGO-4188 minor fix for missing metric profiles


## [2.1.1](https://github.com/ARGOeu/argo-streaming/releases/tag/v.2.1.1) - (2023-02-02)

### Added:

- ARGO-3378 Support a tenant that has been declared as having combined data in job submission

### Changed:

- Remove deprecated hbase code and MetricParse class
- ARGO-4133 AMS connector offset advancement based on date


### Fixed:

- minor fix initializing parameter latest.offset
- Fix NULL exception during using ArgoMessagingSource in stream status
- ARGO-4132 Fix class cast issue due to Avro SpecificDatumReader caching
- ARGO-4123 Issue with generating events when there is a downtime

## [v2.1.0](https://github.com/ARGOeu/argo-streaming/releases/tag/v2.1.0-1) - (2022-12-05)

### Added:

- ARGO-3984 Make streaming job initialise by default with no run.date argument and advance the subscription to the - lastest message
- Write generated alerts to Ams instead of Kafka in stream_status job
- ARGO-3904 Handle repeated events such as reminders different from normal ones

### Changed:

- ARGO-3977 Improve logs generated by the flink jobs
- ARGO-3987 Make AMS related code common in flink jobs
- ARGO-3973 update dependencies versions in flink_jobs_v2 
- ARGO-3953 Ingestion jobs should use the header ams credential method
- Streaming job doPull, doAck requests to AMS should use the header ams credential method
- ARGO-3961 Change DateTime timezone to utc during handling timestamps in Timelines and multi_job
- ARGO-3731 upgrade mongodb connector to v3.12
- Produce streaming events with friendly urls instead of encoded hostnames

### Fixed:

- ARGO-3943 Status extending current time appears in the timeline of EGI ALL report
- ARGO-3922 Fix streaming job exception when handling ui urls with spaces
- ARGO-3921 Import fix in argo-streaming multijob

## [v2.0.0](https://github.com/ARGOeu/argo-streaming/releases/tag/V.2.0.0-1) - (2022-06-29)

### Added:

- ARGO-3857 Notifications: give the ability to configure reminder intervals
- ARGO-3875 Multijob should check what to compute from argo-web-api /reports call
- ARGO-3852 Calculate info in status metrics and add the field to the mongo db status metric collection
- ARGO-3587 Create a multi job submit script. Minor fixes in old scripts
- ARGO-3632 Add urlhelp field to event message of status streaming job
- ARGO-3631 Αdd urlhistory field to event message of status streaming job
- ARGO-3641 POC to create a simple flink connector to publish messages to AMS
- ARGO-3584 Add api timeout configuration in status streaming job
- ARGO-3512 Fill timeline with EXCLUDED status for excluded metric , for a specific period
- ARGO-3513 Handle combinations of EXCLUDED status with the other defined statuses
- ARGO-3511 Add EXCLUDED status in OperationsManager
- ARGO-3504 Fιll timeline with a specific status for a period of time
- ARGO-3484 Exclude groups defined at recomputations from ar calculations
- ARGO-3454 Add StreamStatus job to flink_job_v2
- ARGO-3446 Implement threshold rule flag to timelines & make tags to appear as array
- ARGO-3424 Add tags for each metric to thε mongo db output, on the calculation of status metric trends
- ARGO-3438 Implement metric tag request to retrieve response from argo-web-api
- ARGO-3431 Implement MetricTagProfile manager to read metric tags from argo-web-api
- ARGO-3399 Add parameter calcTrends=ON/OFF & calcFlipFlops=ON/OFF to define status trends and flip flops execution
- ARGO-3315 Integrate status trends for group level
- ARGO-3314 Integrate flip flops of group level
- ARGO-3313 Integrate status trends for service level
- ARGO-3312 Integrate flip flops of service level
- ARGO-3300 Integrate status trends for endpoint level
- ARGO-3299 Integrate flip flops of endpoint level
- ARGO-3294 Integrate status trends for metric level
- ARGO-3294 Integrate status trends for metric level
- ARGO-3293 Integrate flip flops of metric level
- ARGO-3254 Calculate and write to mongo db a/r results for group timelines
- ARGO-3253 Calculate and write to mongo db status timelines for groups
- ARGO-3252 Calculate and write to mongo db a/r results for service timelines
- ARGO-3251 Calculate and write to mongo db status timelines for services
- ARGO-3250 Calculate and write to mongo db a/r results for endpoint timelines
- ARGO-3249 Calculate and write to mongo db status timelines for endpoints
- ARGO-3302 Calculate Status Timelines for metric level
- ARGO-3321 Add url information to low level status metric results
- ARGO-3240 Add timeout configuration when connecting to argo-web-api
- ARGO-3257 Use run.date argument as proper date query parameter when requesting resources from argo-web-api
- ARGO-3239 Implement status trends calculations based on the duration of each status appearance to the checks
- ARGO-3211 Compute Status Trends for upper levels
- ARGO-3209 Create Downtimes Profile Parser independent Component
- ARGO-3231 Create Threshold Profile Parser independent Component
- ARGO-3206 Create Report Profile Parser independent Component

### Changed:

- ARGO-3912 Make multijob submit compute parameter optional
- ARGO-3868 Bump gson dependencies in flink projects to v2.8.9
- ARGO-3752 Implement a mechanism using ObjectId to clean the previous records
- ARGO-3592 Enrich test dataset to test downtimes defined on service endpoints
- ARGO-3577 Enrich test dataset to test threshold rule applied on metric data
- ARGO-3576 Enrich test dataset to apply test excluded metrics in recomputations
- ARGO-3559 Prepare expected result data to test generated A/R results of all levels of hierarchy
- ARGO-3560 Prepare expected result data to test generated flip flops and trends results of all levels of hierarchy
- ARGO-3558 Prepare expected result data to test generated statuses of all levels of hierarchy
- ARGO-3578 Prepare expected dataset to test MapStatusMetricTags calculations
- ARGO-3673 Enable proxy flag in ams sink when proxy cli parameter is present. Consolidate api.proxy and ams.proxy cli - parameters to one proxy parameter
- ARGO-3557 Prepare expected result data to test generated timelines of all levels of hierarchy
- ARGO-3556 Prepare expected result data to test CalcPrevStatus function
- ARGO-3604 Clear Mongo DB of status timelines and AR results
- ARGO-3579 Rename ArgoBatchStatus.jar to ArgoMultiJob
- ARGO-3564 Pass changes of #ARGO-3563 and #ARGO-3457 to flink_jobs_v2
- ARGO-3555 Prepare expected result data to test MapServices function
- ARGO-3554 Prepare expected result data to test PickEndpoints function
- ARGO-3553 Prepare expected result data to test FillMissing function
- ARGO-3580 Bump xercesImpl to 2.12.2
- ARGO-3510 Update Recomputation Manager to handle exclude_metrics field and store the info
- ARGO-3463 Use ReportManager code in StreamStatus job
- ARGO-3461 Use MetricProfileManager code in StreamStatus job
- ARGO-3465 Use DowntimeManager code in StreamStatus job
- ARGO-3462 Use EndpointGroupManager code in StreamStatus job
- ARGO-3460 Use AggregationProfileManager code in StreamStatus job
- ARGO-3455 Use ApiResourceManager in StreamStatus job
- ARGO-3445 Refactor packages of ArgoStatusBatch job
- ARGO-3444 Move tags addition to existing map functions
- ARGO-3395 Change default timeout when contacting argo-web-api to 30sec
- ARGO-3336 Update old-models batch_ar and batch_status to have all commit changes up to 28 Feb 2020 (current prod jar)
- ARGO-3297 Revert batch_jobs in old flink_job folder in state before common code changes
- ARGO-3292 RecomputationManager as part of ProfilesManager module project

### Fixed:

- ARGO-3899 Fix when displaying downtimes in status timelines
- ARGO-3867 Issue with argo info tags on status results when comma appears in values
- ARGO-3848 Fix empty group name in status results
- ARGO-3837 Fix missing INFO field values from status results in mongo
- ARGO-3720 Propagate fix from old streaming job to v1 and v2 Remove old MetricData class. Fix order of arguments in - MongoStatutOutput
- ARGO-3677 Fix in resource files handling in tests
- ARGO-3642 Tag trends need to display the original count of events occured
- ARGO-3600 Fix bug in Timelines to include  DOWNTIME periods when calculated
- ARGO-3563 Fix cli args
- ARGO-3457 Fix reading info topology tags prefix from info. to info_
- ARGO-3480 Fix has_threshold_rule to output boolean value in mongo instead of string
- ARGO-3414 Fix thresholds init issue in batch jobs that use argo-web-api

## [v1.5.0](https://github.com/ARGOeu/argo-streaming/releases/tag/v1.5.0) - (2021-07-22)

### Added:

- ARGO-2988 Add url information on generated events
- ARGO-2985 Calculate flip flops for group of endpoints
- ARGO-2984 Calculate flip flop for services
- ARGO-2983 Parse report configuration from the api
- ARGO-2578 Align streaming job interface to use ApiResourceManager and ArgoApiSource connector
- ARGO-2981 Investigate and parse aggregation profiles
- ARGO-2976 read operation profile truth tables from web api
- ARGO-29976 ARGO-2954 ARGO-2955 operationProfiles and read web api requests for metric profile data and topology - endpoint data
- ARGO-2953 create the top N flip flop flopping service endpoints
- ARGO-2953 create the top N flip flop flopping service endpoints
- ARGO-2950 write results to mongodb
- ARGO-2952 calculate status flip flops for service endpoints metrics
- ARGO-2950 write results to mongodb
- ARGO-2952 calculate status flip flops for service endpoints metrics
- ARGO-2952 calculate status flip flops for service endpoints metrics
- ARGO-2949 calc topN critical warning unknown status for service endpoint metrics
- ARGO-2662 Implement custom flink source for argo-web-api

### Changed:

- ARGO-2664 status submit script: Use optional inputs and proper defaults in argo a/r batch computation. 
- ARGO-2663 ar submit script: Use optional inputs and proper defaults in argo a/r batch computation
- ARGO-2997 Remove support for old versions of metric data. Fix mongodb temp update timestatus field
- ARGO-3100 Retain old models of compute jobs until migration to new ones is complete
- ARGO-3071 Compute all flip flop trends in one job and add a flag to clear mongo db
- ARGO-3054 Check and use the MongoFormat used in existing a/r status jobs
- ARGO-3058 add date argument and write to mongo
- ARGO-2982 Use report name and date as input for trends computation
- ARGO-2891 Modify update engine process to run per tenant


### Fixed:

- ARGO-3197 Optimize status trend metric results in single collection
- ARGO-3186 Convert daily dates to integer format to help trends range queries in argo-web-api

## [v1.4.0](https://github.com/ARGOeu/argo-streaming/releases/tag/v1.4.0) - (2020-12-17)

### Added:

- ARGO-2578 Update status computation to fetch sync files from argo-web-api
- ARGO-2702 Handle remote recomputation data from argo-web-api
- ARGO-2577 Translate api retrieved resources to avro 
- ARGO-2689 Implement Argo-web-api Resource Manager 
- ARGO-2245 Handle Endpoint extra information such as url in endpoint a/r results
- ARGO-270 Update status job submit script to optionally use historic profiles
- ARGO-2069 Use historic version of argo-web-api profiles in a/r job submission script

### Changed:
- ARGO-2732 Align input interface of batch-ar job to use argo-web-api
- ARGO-2143 Finalize py3 version in argo-compute-engine scripts
- ARGO-2248 Change compute engine to handle extra topology information in status results

### Fixed:
- ARGO-2006 Streaming status job: fix issue with decommissioned topology endpoints
- ARGO-2672 Ensure indexes are enabled for sync data
- ARGO-2627 Fix sync ingestion conditional in status streming job


## [v1.3.0](https://github.com/ARGOeu/argo-streaming/releases/tag/V1.3-1) - (2019-11-07)

### Added:

- ARGO-1963 Autoconfigure archiver subs and users
- ARGO-1932 Add dry-run mode to submission scripts
- ARGO-1980 Clean-up streaming status script submission
- ARGO-1931 Use proxy options in scripts for ams and web-api
- ARGO-1784 Streaming job: Remove decommissioned endpoints
- ARGO-1823 Alerts add synopsis list of metrics included in endpoint
- ARGO-1708 Extend event schema to include group item statuses ARGO-1709 Extend status streaming job to gather status info for all group items ARGO-1770 Remove failover for MetricData old schema

Fixed:
- ARGO-1974 Fix top level aggregations in streaming event generation
- ARGO-1786 Fix: mongo clean old endpoint_ar data
- ARGO-1785 Fix excluded monitoring data for previous day


## [v1.2.0](https://github.com/ARGOeu/argo-streaming/releases/tag/v1.2) - (2019-03-22)

### Added:
- ARGO-1480 Argo engine automation: ensure mongodb indexes
- ARGO-1567 Remove restart strategy from batch jobs
- ARGO-1581 In status streaming job use optimistically OK as init status
- ARGO-1636 Extend status event schema
- ARGO-1675 Forward metric,endpoint and service values on all event levels
- ARGO-1679 Add batch endpoint a/r computation

### Fixed:
- ARGO-1626 Status batch service aggregation OR/AND fix
- ARGO-164 Fix cron autoconfiguration daily/hourly mis-match
- ARGO-1652 Fix metric data schema migration in status streaming job
- ARGO-1740 Recomputation hdfs path fix



## [v1.1.0](https://github.com/ARGOeu/argo-streaming/releases/tag/v1.1) - (2018-11-09)

### Added:
- ARGO-1464 Update requests dep to 2.20
- ARGO-1063 AMS Client logging on issues
- ARGO-1434 Make check tenant status look back in time for sync data Fix absolute paths in update cron script Upload - default empty recomputation profile if missing from HDFS
- ARGO-1403 Create argo-engine update wrapper
- ARGO-1404 Ignore metric data from services that are not included in aggregation profile
- ARGO-1402 Enable streaming-status job per report
- ARGO-1291 Recomputation handling in streaming engine
- ARGO-1298 Upload tenant configuration status to argo-web-api
- ARGO-1290 Create tenant status check script
- ARGO-1065 Establish a fixed restart strategy for streaming jobs
- ARGO-1292 Update AMS project from argo-web-api tenant info
- ARGO-1289 Update crontab for all tenants and their reports
- ARGO-1288 Update tenant reports from argo-web-api
- ARGO-1287 Update tenant list from argo-web-api
- ARGO-1308 Refactor submit scripts to use new configuration
- ARGO-1286 Parse and separate manual and automatic sections of argo-streaming conf
- ARGO-1277 Check and update thresholds profiles from argo-web-api
- ARGO-1276 Update batch submit scripts to handle threshold params
- ARGO-1274 Refactor ConfigManager to parse topology_schema and filter_tag fields
- ARGO-1273 Refactor aggregation profile parser
- ARGO-1256 Implement Threshold component in batch jobs
- ARGO-1261 Implement Threshold Manager
- ARGO-1149 Refactor batch jobs to accept new metric data schema
- ARGO-1241 Refactor Ingest Metric job to accept extra data

### Fixed:
- ARGO-1441 Fix hdfs_user param in config scripts
- ARGO-1430 Fix sync bugs in automation scripts
- ARGO-1392 Argo engine cli script fixes
- ARGO-1319 Fix missing status generation issue in batch status job



## [v1.0.0](https://github.com/ARGOeu/argo-streaming/releases/tag/v1.0) - (2018-06-13)

### Added:
- ARGO-904 Finalize job runner class. Refactor cli argument usage
- ARGO-923 Annotate results using report uuid instead of report name
- ARGO-934 Add kafka destination to status streaming
- ARGO-949 Fix handling of aggregation groups
- ARGO-942 Optimize cli argument handling in streaming/batch jobs
- ARGO-952 Refactor streaming job to receive sync data from ams subscription
- ARGO-960 Ingest Sync Data to HDFS
- ARGO-979 Refactor Metric Ingestion fix datetime buckets at HDFS
- ARGO-982 Implement and use direct MongoOutputFormat for ar batch results
- ARGO-983 Implement and use direct Mongo Output Format for storing sta
- ARGO-969 Add ability to configure AMS source ingestion rate in flink
- ARGO-988 BucketSink: Inacticity threshold increase to 30 minutes
- ARGO-1000 Remove hardcoded default parallelism from streaming status 
- ARGO-1038 Create Metric Ingestion Submit Script
- ARGO-1038 Create Metric Ingestion Submit Script
- ARGO-1040 Create A/R job submission script
- ARGO-1072 Batch status read report cfg
- ARGO-1073 Add more verbose names to flink jobs
- ARGO-1074 Add reference to config template relative to each test file
- ARGO-1041 Create Status job submit script
- ARGO-1042 Create Status Streaming submit script
- ARGO-1107 Refactor AMS source / connector to support proxy option
- ARGO-1083 Streaming status job timeout and multiple-group fixes
- ARGO-1156 Refactor flink submissions scripts with updated execution 
- ARGO-1164 Add downtime feed to streaming status
- ARGO-1221 Report name capitalization fix
- ARGO-1230 Fetch latest ops profile from argo-web-api
- ARGO-1231 Fetch latest aggregations profile from argo-web-api
- ARGO-1233 Fetch latest report cfg from argo-web-api
- ARGO-1239 Refactor Operations Profile Manager to read new schema


### Fixed:
- ARGO-924 Fix multiple service groups issue
- ARGO-970 Fix Avro ClassCast Issue
- ARGO-992 Fix hdfs instance handling in ingest sync job
- ARGO-1044 Fix Downtime handling in compute ar batch job
- ARGO-1067 Fix job submit when 0 job run in cluster
- ARGO-1160 Fix StatusManager Aggregation Initialization Bug
- ARGO-1163 Fix close on Specific Avro Writer
- ARGO-1243 Fix recomputation list initialization in batch_ar




## [v0.1.0](https://github.com/ARGOeu/argo-streaming/releases/tag/v0.1) - (2017-06-31)

### Added:
- ARGO-614 AMS subscriber stream to Hbase
- ARGO-625 Python client to publish consumer metric data to AMS
- ARGO-648 Establish rate when pulling messages
- ARGO-668 Add streaming job for status latest results
- ARGO-720 Store status results in hbase
- ARGO-721 Add monitored and processed timestamps
- ARGO-727 Add monitoring host to status event schema
- ARGO-769 Ability to store raw data in hdfs
- ARGO-798 Generate status events at the start of a new day
- ARGO-768 Add Foundation class for Status Batch Job
- ARGO-808 Implement endpoint status calculation step in status batch job
- ARGO-809 Implement Calculation of Service Status timelines
- ARGO-810 Implement Calculation of Endpoint Group Status Timelines
- ARGO-825 Add Mon-engine exclusion mechanism to status batch-job
- ARGO-828 Refactor AMS Subscriber to support event replayability using offset management
- ARGO-893 Implement Weights Manager. Implement Downtime Manager
- ARGO-895 Create Metric Timelines
- ARGO-896 Create endpoint timelines
- ARGO-897 Create Service Timelines
- ARGO-898 Create Endpoint Group Timelines
- ARGO-899 Calculate Service AR results in flink job
- ARGO-900 Calculate Endpoint Group AR results in flink job
- ARGO-901 Create Service A/R output format to datastore
- ARGO-902 Create Endpoint Group A/R output format to datastore

### Fixed:
- ARGO-229 Dependency Fix
- ARGO-800 Remove unused kafka cli arguments
