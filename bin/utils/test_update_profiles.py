import unittest
import os
from update_profiles import HdfsReader
from update_profiles import ArgoApiClient


class TestClass(unittest.TestCase):

    def test_hdfs_reader(self):
        hdfs_host = "foo"
        hdfs_port = "9000"
        hdfs_sync = "/user/foo/argo/tenants/{{tenant}}/sync"
        hdfs = HdfsReader(hdfs_host, hdfs_port, hdfs_sync)

        test_cases = [
            {"tenant": "TA", "report": "Critical", "profile_type": "operations",
             "expected": "/user/foo/argo/tenants/TA/sync/TA_ops.json"},
            {"tenant": "TA", "report": "Super-Critical", "profile_type": "operations",
             "expected": "/user/foo/argo/tenants/TA/sync/TA_ops.json"},
            {"tenant": "TA", "report": "Critical", "profile_type": "reports",
             "expected": "/user/foo/argo/tenants/TA/sync/TA_Critical_cfg.json"},
            {"tenant": "TA", "report": "Critical", "profile_type": "aggregations",
             "expected": "/user/foo/argo/tenants/TA/sync/TA_Critical_ap.json"},
            {"tenant": "TA", "report": "Crit", "profile_type": "reports",
             "expected": "/user/foo/argo/tenants/TA/sync/TA_Crit_cfg.json"},
            {"tenant": "TA", "report": "Super-Critical", "profile_type": "aggregations",
             "expected": "/user/foo/argo/tenants/TA/sync/TA_Super-Critical_ap.json"},
            {"tenant": "TB", "report": "Critical", "profile_type": "aggregations",
             "expected": "/user/foo/argo/tenants/TB/sync/TB_Critical_ap.json"},
            {"tenant": "TB", "report": "Critical", "profile_type": "reports",
             "expected": "/user/foo/argo/tenants/TB/sync/TB_Critical_cfg.json"}
        ]

        for test_case in test_cases:
            actual = hdfs.gen_profile_path(test_case["tenant"], test_case["report"], test_case["profile_type"])
            expected = test_case["expected"]
            self.assertEquals(expected, actual)

    def test_api(self):

        cfg = {
            "api_host": "foo.host",
            "tenant_keys": {"TA": "key1", "TB": "key2"}
        }

        argo_api = ArgoApiClient(cfg["api_host"], cfg["tenant_keys"])

        test_cases = [
            {"resource": "reports", "item_uuid": None,
             "expected": "https://foo.host/api/v2/reports"},
            {"resource": "reports", "item_uuid": "12",
             "expected": "https://foo.host/api/v2/reports/12"},
            {"resource": "operations", "item_uuid": None,
             "expected": "https://foo.host/api/v2/operations_profiles"},
            {"resource": "operations", "item_uuid": "12",
             "expected": "https://foo.host/api/v2/operations_profiles/12"},
            {"resource": "aggregations", "item_uuid": None,
             "expected": "https://foo.host/api/v2/aggregation_profiles"},
            {"resource": "aggregations", "item_uuid": "12",
             "expected": "https://foo.host/api/v2/aggregation_profiles/12"}
            ]

        for test_case in test_cases:
            actual = argo_api.get_url(test_case["resource"], test_case["item_uuid"])
            expected = test_case["expected"]
            self.assertEquals(expected, actual)
