import unittest
import argparse
import os
from ar_job_submit import compose_command
from utils.common import cmd_to_string
from utils.argo_config import ArgoConfig

CONF_TEMPLATE = os.path.join(os.path.dirname(__file__), '../conf/conf.template')
CONF_SCHEMA = os.path.join(os.path.dirname(__file__), '../conf/config.schema.json')

# This is the command that the submission script is expected to compose based on given args and config
expected_result = """flink_path run -c test_class test.jar --run.date 2018-02-11 \
--mongo.uri mongodb://localhost:21017/argo_TENANTA --mongo.method upsert \
--mdata hdfs://hdfs_test_host:hdfs_test_port/user/hdfs_test_user/argo/tenants/TENANTA/mdata/2018-02-11 \
--rec hdfs://hdfs_test_host:hdfs_test_port/user/hdfs_test_user/argo/tenants/TENANTA/sync/recomp.json \
--downtimes hdfs://hdfs_test_host:hdfs_test_port/user/hdfs_test_user/argo/tenants/\
TENANTA/sync/Critical/downtimes_2018-02-11.avro \
--mps hdfs://hdfs_test_host:hdfs_test_port/user/hdfs_test_user/argo/tenants/\
TENANTA/sync/Critical/metric_profile_2018-02-11.avro \
--apr hdfs://hdfs_test_host:hdfs_test_port/user/hdfs_test_user/argo/tenants/\
TENANTA/sync/TENANTA_Critical_ap.json \
--ggp hdfs://hdfs_test_host:hdfs_test_port/user/hdfs_test_user/argo/tenants/\
TENANTA/sync/Critical/group_groups_2018-02-11.avro \
--conf hdfs://hdfs_test_host:hdfs_test_port/user/hdfs_test_user/argo/tenants/\
TENANTA/sync/TENANTA_Critical_cfg.json \
--egp hdfs://hdfs_test_host:hdfs_test_port/user/hdfs_test_user/argo/tenants/\
TENANTA/sync/Critical/group_endpoints_2018-02-11.avro \
--pdata hdfs://hdfs_test_host:hdfs_test_port/user/hdfs_test_user/argo/tenants/\
TENANTA/mdata/2018-02-10 --weights hdfs://hdfs_test_host:hdfs_test_port/user/hdfs_test_user/argo/tenants/\
TENANTA/sync/Critical/weights_2018-02-11.avro \
--ops hdfs://hdfs_test_host:hdfs_test_port/user/hdfs_test_user/argo/tenants/TENANTA/sync/TENANTA_ops.json \
--ams.proxy test_proxy --ams.verify true"""


class TestClass(unittest.TestCase):

    def test_compose_command(self):

        # set up the config parser
        config = ArgoConfig(CONF_TEMPLATE, CONF_SCHEMA)

        parser = argparse.ArgumentParser()
        parser.add_argument('--tenant')
        parser.add_argument('--date')
        parser.add_argument('--report')
        parser.add_argument('--sudo', action='store_true')
        parser.add_argument('--method')
        args = parser.parse_args(
            ['--tenant', 'TENANTA', '--date', '2018-02-11', '--report', 'Critical', '--method', 'upsert'])

        hdfs_metric = "hdfs://hdfs_test_host:hdfs_test_port/user/hdfs_test_user/argo/tenants/TENANTA/mdata"
        hdfs_sync = "hdfs://hdfs_test_host:hdfs_test_port/user/hdfs_test_user/argo/tenants/TENANTA/sync"

        test_hdfs_commands = dict()

        test_hdfs_commands["--pdata"] = hdfs_metric+"/2018-02-10"
        test_hdfs_commands["--mdata"] = hdfs_metric+"/2018-02-11"
        test_hdfs_commands["--conf"] = hdfs_sync+"/TENANTA_Critical_cfg.json"
        test_hdfs_commands["--mps"] = hdfs_sync+"/Critical/"+"metric_profile_2018-02-11.avro"
        test_hdfs_commands["--ops"] = hdfs_sync+"/TENANTA_ops.json"
        test_hdfs_commands["--apr"] = hdfs_sync+"/TENANTA_Critical_ap.json"
        test_hdfs_commands["--egp"] = hdfs_sync+"/Critical/group_endpoints_2018-02-11.avro"
        test_hdfs_commands["--ggp"] = hdfs_sync+"/Critical/group_groups_2018-02-11.avro"
        test_hdfs_commands["--weights"] = hdfs_sync+"/Critical/weights_2018-02-11.avro"
        test_hdfs_commands["--downtimes"] = hdfs_sync+"/Critical/downtimes_2018-02-11.avro"
        test_hdfs_commands["--rec"] = hdfs_sync+"/recomp.json"

        self.assertEquals(expected_result, cmd_to_string(compose_command(config, args, test_hdfs_commands)))
