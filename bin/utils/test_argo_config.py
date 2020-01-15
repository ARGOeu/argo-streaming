import unittest
import os
from .argo_config import ArgoConfig
from urllib.parse import urlparse


CONF_FILE = os.path.join(os.path.dirname(__file__), '../../conf/argo-streaming.conf')
SCHEMA_FILE = os.path.join(os.path.dirname(__file__), '../../conf/config.schema.json')


class TestClass(unittest.TestCase):

    def test_config(self):
        argo_conf = ArgoConfig()
        argo_conf.load_conf(CONF_FILE)
        argo_conf.load_schema(SCHEMA_FILE)
        argo_conf.check_conf()

        # get fixed HDFS -> user param (string)
        self.assertEqual("foo", argo_conf.get("HDFS", "user"))
        # get var TENANTS:TENANT_A -> reports param (list of strings)
        self.assertEqual(["report1", "report2"], argo_conf.get("TENANTS:TENANT_A", "reports"))
        # get var TENANTS:TENANT_B -> mongo_uri (url)
        self.assertEqual(urlparse("mongodb://localhost:21017/argo_FOO"),
                         argo_conf.get("TENANTS:TENANT_B", "mongo_uri").fill(mongo_uri=argo_conf.get("MONGO","endpoint").geturl(),tenant="FOO"))

        # get var HDFS -> path_metric (template) and fill it with specific arguments
        exp_result = urlparse("hdfs://localhost:2000/user/foobar/argo/tenants/wolf/mdata")
        self.assertEqual(exp_result, argo_conf.get("HDFS", "path_metric").fill(namenode="localhost:2000", hdfs_user="foobar",
                                                                               tenant="wolf"))
        # fill template with different user argument
        self.assertNotEqual(exp_result,
                            argo_conf.get("HDFS", "path_metric").fill(namenode="localhost:2000", hdfs_user="foobar2",
                                                                      tenant="wolf"))
