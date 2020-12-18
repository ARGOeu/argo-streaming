import unittest
import argparse
import os
from ar_job_submit import compose_command
from utils.common import cmd_to_string
from utils.argo_config import ArgoConfig

CONF_TEMPLATE = os.path.join(
    os.path.dirname(__file__), '../conf/conf.template')
CONF_SCHEMA = os.path.join(os.path.dirname(
    __file__), '../conf/config.schema.json')


# This is the command that the submission script is expected to compose based on given args and config
expected_result = """flink_path run -c test_class test.jar --run.date 2018-02-11 --mongo.uri mongodb://localhost:21017/argo_TENANTA \
--mongo.method upsert --pdata hdfs://hdfs_test_host:hdfs_test_port/user/hdfs_test_user/argo/tenants/TENANTA/mdata/2018-02-10 \
--mdata hdfs://hdfs_test_host:hdfs_test_port/user/hdfs_test_user/argo/tenants/TENANTA/mdata/2018-02-11 \
--api.endpoint api.foo --api.token key0 --report.id report_uuid"""

expected_result2 = """flink_path run -c test_class test.jar --run.date 2021-01-01 --mongo.uri mongodb://localhost:21017/argo_TENANTA \
--mongo.method insert --pdata hdfs://hdfs_test_host:hdfs_test_port/user/hdfs_test_user/argo/tenants/TENANTA/mdata/2020-12-31 \
--mdata hdfs://hdfs_test_host:hdfs_test_port/user/hdfs_test_user/argo/tenants/TENANTA/mdata/2021-01-01 \
--api.endpoint api.foo --api.token key0 --report.id report_uuid"""

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
            ['--tenant', 'TENANTA', '--date', '2018-02-11', '--report', 'report_name', '--method', 'upsert'])

        hdfs_metric = "hdfs://hdfs_test_host:hdfs_test_port/user/hdfs_test_user/argo/tenants/TENANTA/mdata"
        
        test_hdfs_commands = dict()

        test_hdfs_commands["--pdata"] = hdfs_metric+"/2018-02-10"
        test_hdfs_commands["--mdata"] = hdfs_metric+"/2018-02-11"

        self.assertEqual(expected_result, cmd_to_string(
            compose_command(config, args, test_hdfs_commands)))

    def test_compose_second_command(self):

        # set up the config parser
        config = ArgoConfig(CONF_TEMPLATE, CONF_SCHEMA)

        parser = argparse.ArgumentParser()
        parser.add_argument('--tenant')
        parser.add_argument('--date', required=False, default="2021-01-01")
        parser.add_argument('--report')
        parser.add_argument('--sudo', action='store_true')
        parser.add_argument('--method', required=False, default="insert")
       
        args = parser.parse_args(
            ['--tenant', 'TENANTA', '--report', 'report_name'])

        hdfs_metric = "hdfs://hdfs_test_host:hdfs_test_port/user/hdfs_test_user/argo/tenants/TENANTA/mdata"
        
        test_hdfs_commands = dict()

        test_hdfs_commands["--pdata"] = hdfs_metric+"/2020-12-31"
        test_hdfs_commands["--mdata"] = hdfs_metric+"/2021-01-01"

        self.assertEqual(expected_result2, cmd_to_string(
            compose_command(config, args, test_hdfs_commands, True)))
