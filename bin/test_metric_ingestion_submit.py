import unittest
from metric_ingestion_submit import compose_command
from utils.common import cmd_toString
import ConfigParser
import argparse
import os

CONF_TEMPLATE = os.path.join(os.path.dirname(__file__), '../conf/conf.template')


class TestClass(unittest.TestCase):

    def test_compose_command(self):

        # set up the config parser
        config = ConfigParser.ConfigParser()
        config.read(CONF_TEMPLATE)

        test_cmd = "sudo flink_path run -c test_class test.jar --ams.endpoint test_endpoint --ams.port test_port --ams.token test_token --ams.project test_project --ams.sub job_name --hdfs.path hdfs://hdfs_test_host:hdfs_test_port/user/hdfs_test_user/argo/tenants/TENANTA/mdata --check.path test_path --check.interval 30000 --ams.batch 100 --ams.interval 300 --ams.proxy test_proxy --ams.verify true"

        parser = argparse.ArgumentParser()
        parser.add_argument('--Tenant')
        args = parser.parse_args(['--Tenant', 'TenantA'])

        self.assertEquals(test_cmd, cmd_toString(compose_command(config, args, True)[0]))
