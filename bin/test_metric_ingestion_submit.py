import unittest
from metric_ingestion_submit import compose_command
from utils.common import cmd_to_string
from utils.argo_config import ArgoConfig
import argparse
import os

CONF_TEMPLATE = os.path.join(os.path.dirname(__file__), '../conf/conf.template')
CONF_SCHEMA = os.path.join(os.path.dirname(__file__), '../conf/config.schema.json')

# This is the command that the submission script is expected to compose based on given args and config
expected_result = """sudo flink_path run -c test_class test.jar --ams.endpoint test_endpoint --ams.port 8080 \
--ams.token test_token --ams.project test_project --ams.sub job_name \
--hdfs.path hdfs://hdfs_test_host:hdfs_test_host/user/hdfs_test_user/argo/tenants/TENANTA/mdata \
--check.path test_path --check.interval 30000 --ams.batch 100 --ams.interval 300 --ams.proxy test_proxy \
--ams.verify true"""


class TestClass(unittest.TestCase):

    def test_compose_command(self):
        # set up the config parser
        config = ArgoConfig(CONF_TEMPLATE, CONF_SCHEMA)

        parser = argparse.ArgumentParser()
        parser.add_argument('--tenant')
        parser.add_argument('--sudo', action='store_true')
        args = parser.parse_args(['--tenant', 'TENANTA', '--sudo'])

        print cmd_to_string(compose_command(config, args)[0])

        self.assertEquals(expected_result, cmd_to_string(compose_command(config, args)[0]))
