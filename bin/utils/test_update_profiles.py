import unittest
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
             "expected": "/user/foo/argo/tenants/TA/sync/TA_Critical_ops.json"},
            {"tenant": "TA", "report": "Super-Critical", "profile_type": "operations",
             "expected": "/user/foo/argo/tenants/TA/sync/TA_Super-Critical_ops.json"},
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
             "expected": "/user/foo/argo/tenants/TB/sync/TB_Critical_cfg.json"},
            {"tenant": "TB", "report": "Critical", "profile_type": "metrics",
             "expected": "/user/foo/argo/tenants/TB/sync/TB_Critical_metrics.json"}
        ]

        for test_case in test_cases:
            actual = hdfs.gen_profile_path(
                test_case["tenant"], test_case["report"], test_case["profile_type"])
            expected = test_case["expected"]
            self.assertEquals(expected, actual)

        # Test with dates
        test_cases_dates = [
            {"tenant": "TA", "report": "Critical", "profile_type": "operations", "date": "2019-12-11",
             "expected": "/user/foo/argo/tenants/TA/sync/TA_Critical_ops_2019-12-11.json"},
            {"tenant": "TA", "report": "Super-Critical", "profile_type": "operations", "date": "2019-10-04",
             "expected": "/user/foo/argo/tenants/TA/sync/TA_Super-Critical_ops_2019-10-04.json"},
            {"tenant": "TA", "report": "Critical", "profile_type": "reports", "date": "2019-05-11",
             "expected": "/user/foo/argo/tenants/TA/sync/TA_Critical_cfg.json"},
            {"tenant": "TA", "report": "Critical", "profile_type": "aggregations", "date": "2019-06-06",
             "expected": "/user/foo/argo/tenants/TA/sync/TA_Critical_ap_2019-06-06.json"},
            {"tenant": "TA", "report": "Crit", "profile_type": "reports", "date": "2019-07-04",
             "expected": "/user/foo/argo/tenants/TA/sync/TA_Crit_cfg.json"},
            {"tenant": "TA", "report": "Super-Critical", "profile_type": "aggregations", "date": "2019-03-04",
             "expected": "/user/foo/argo/tenants/TA/sync/TA_Super-Critical_ap_2019-03-04.json"},
            {"tenant": "TB", "report": "Critical", "profile_type": "aggregations", "date": "2019-01-04",
             "expected": "/user/foo/argo/tenants/TB/sync/TB_Critical_ap_2019-01-04.json"},
            {"tenant": "TB", "report": "Critical", "profile_type": "reports", "date": "2019-01-05",
             "expected": "/user/foo/argo/tenants/TB/sync/TB_Critical_cfg.json"},
            {"tenant": "TB", "report": "Critical", "profile_type": "metrics", "date": "2019-02-24",
             "expected": "/user/foo/argo/tenants/TB/sync/TB_Critical_metrics_2019-02-24.json"}
        ]

        for test_case_date in test_cases_dates:
            actual = hdfs.gen_profile_path(
                test_case_date["tenant"], test_case_date["report"], test_case_date["profile_type"], test_case_date["date"])
            expected = test_case_date["expected"]
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
             "expected": "https://foo.host/api/v2/aggregation_profiles/12"},
            {"resource": "tenants", "item_uuid": None,
             "expected": "https://foo.host/api/v2/admin/tenants"},
            {"resource": "tenants", "item_uuid": "12",
             "expected": "https://foo.host/api/v2/admin/tenants/12"},
            {"resource": "metrics", "item_uuid": None,
             "expected": "https://foo.host/api/v2/metric_profiles"},
            {"resource": "metrics", "item_uuid": "12",
             "expected": "https://foo.host/api/v2/metric_profiles/12"}
        ]

        for test_case in test_cases:
            actual = argo_api.get_url(
                test_case["resource"], test_case["item_uuid"])
            expected = test_case["expected"]
            self.assertEquals(expected, actual)
