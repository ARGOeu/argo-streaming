import unittest
import argparse
import ConfigParser
import os
from stream_status_job_submit import compose_command
from utils.common import cmd_toString

CONF_TEMPLATE = os.path.join(os.path.dirname(__file__), '../conf/conf.template')



class TestClass(unittest.TestCase):

    def test_compose_command(self):

        # set up the config parser
        config = ConfigParser.ConfigParser()
        config.read(CONF_TEMPLATE)

        parser = argparse.ArgumentParser()
        parser.add_argument('--Tenant')
        parser.add_argument('--Date')
        parser.add_argument('--Report')
        parser.add_argument('--Sudo', action='store_true')
        parser.add_argument('--Method')
        args = parser.parse_args(['--Tenant', 'TENANTA', '--Date', '2018-03-05T00:00:00Z', '--Report', 'Critical', '--Method', 'upsert'])
        hdfs_sync = "hdfs://hdfs_test_host:hdfs_test_port/user/hdfs_test_user/argo/tenants/TENANTA/sync"

        test_hdfs_commands = {}
        test_hdfs_commands["--sync.mps"] = hdfs_sync+"/Critical/"+"metric_profile_2018-03-01.avro"
        test_hdfs_commands["--sync.ops"] = hdfs_sync+"/TENANTA_ops.json"
        test_hdfs_commands["--sync.apr"] = hdfs_sync+"/TENANTA_Critical_ap.json"
        test_hdfs_commands["--sync.egp"] = hdfs_sync+"/Critical/group_endpoints_2018-03-01.avro"

        test_cmd = "flink_path run -c test_class test.jar --ams.endpoint test_endpoint --ams.port test_port --ams.token test_token --ams.project test_project --ams.sub.metric metric_status --ams.sub.sync sync_status --sync.apr hdfs://hdfs_test_host:hdfs_test_port/user/hdfs_test_user/argo/tenants/TENANTA/sync/TENANTA_Critical_ap.json --sync.egp hdfs://hdfs_test_host:hdfs_test_port/user/hdfs_test_user/argo/tenants/TENANTA/sync/Critical/group_endpoints_2018-03-01.avro --sync.mps hdfs://hdfs_test_host:hdfs_test_port/user/hdfs_test_user/argo/tenants/TENANTA/sync/Critical/metric_profile_2018-03-01.avro --sync.ops hdfs://hdfs_test_host:hdfs_test_port/user/hdfs_test_user/argo/tenants/TENANTA/sync/TENANTA_ops.json --run.date 2018-03-05T00:00:00Z --p 1 --hbase.master hbase.devel --hbase.port test_hbase_port --hbase.zk.quorum test_zk_servers --hbase.zk.port test_zk_port --hbase.namespace test_hbase_namespace --hbase.table metric_data --kafka.servers test_kafka_servers --kafka.topic test_kafka_topic --fs.output test_fs_output"

        self.assertEquals(test_cmd, cmd_toString(compose_command(config, args, test_hdfs_commands)[0]))

