import unittest
import argparse
import os
from utils.argo_config import ArgoConfig
from stream_status_job_submit import compose_command
from utils.common import cmd_to_string

CONF_TEMPLATE = os.path.join(os.path.dirname(__file__), '../conf/conf.template')
CONF_SCHEMA = os.path.join(os.path.dirname(__file__), '../conf/config.schema.json')

expected_result = """sudo flink_path run -c test_class test.jar --ams.endpoint test_endpoint --ams.port 8080 \
--ams.token test_token --ams.project test_project --ams.sub.metric metric_status --ams.sub.sync sync_status \
--sync.apr hdfs://hdfs_test_host:hdfs_test_port/user/hdfs_test_user/argo/tenants/TENANTA/sync/TENANTA_Critical_ap.json \
--sync.egp hdfs://hdfs_test_host:hdfs_test_port/user/hdfs_test_user/argo/tenants/\
TENANTA/sync/Critical/group_endpoints_2018-03-01.avro \
--sync.mps hdfs://hdfs_test_host:hdfs_test_port/user/hdfs_test_user/argo/tenants/\
TENANTA/sync/Critical/metric_profile_2018-03-01.avro \
--sync.ops hdfs://hdfs_test_host:hdfs_test_port/user/hdfs_test_user/argo/tenants/TENANTA/\
sync/TENANTA_ops.json --run.date 2018-03-05T00:00:00Z --p 1 \
--hbase.master hbase.devel --hbase.port 8080 --hbase.zk.quorum ['test_zk_servers'] \
--hbase.zk.port 8080 --hbase.namespace test_hbase_namespace --hbase.table metric_data \
--kafka.servers kafka_server:9090,kafka_server2:9092 --kafka.topic test_kafka_topic --fs.output None --mongo.uri mongodb://mongo_test_host:21017/argo_TENANTA --mongo.method upsert --ams.batch 10 --ams.interval 300 --ams.proxy test_proxy --ams.verify true --timeout 500"""



class TestClass(unittest.TestCase):

    def test_compose_command(self):

        config = ArgoConfig(CONF_TEMPLATE, CONF_SCHEMA)

        parser = argparse.ArgumentParser()
        parser.add_argument('--tenant')
        parser.add_argument('--date')
        parser.add_argument('--report')
        parser.add_argument('--sudo', action='store_true')
        parser.add_argument('--timeout')
        args = parser.parse_args(
            ['--tenant', 'TENANTA', '--date', '2018-03-05T00:00:00Z', '--report', 'Critical', '--timeout', '500',
             '--sudo'])

        hdfs_sync = "hdfs://hdfs_test_host:hdfs_test_port/user/hdfs_test_user/argo/tenants/TENANTA/sync"

        test_hdfs_commands = dict()
        test_hdfs_commands["--sync.mps"] = hdfs_sync+"/Critical/"+"metric_profile_2018-03-01.avro"
        test_hdfs_commands["--sync.ops"] = hdfs_sync+"/TENANTA_ops.json"
        test_hdfs_commands["--sync.apr"] = hdfs_sync+"/TENANTA_Critical_ap.json"
        test_hdfs_commands["--sync.egp"] = hdfs_sync+"/Critical/group_endpoints_2018-03-01.avro"

        self.assertEquals(expected_result, cmd_to_string(compose_command(config, args, test_hdfs_commands)[0]))
