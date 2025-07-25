import unittest
import responses
from .update_ams import ArgoAmsClient


class TestClass(unittest.TestCase):

    def test_urls(self):
        ams = ArgoAmsClient("foo.host", "secret_key")

        test_cases = [
            {"resource": "projects", "item_uuid": None, "group_uuid": None, "action": None,
             "expected": "https://foo.host/v1/projects"},
            {"resource": "projects", "item_uuid": "TEST_PROJECT", "group_uuid": None, "action": None,
             "expected": "https://foo.host/v1/projects/TEST_PROJECT"},
            {"resource": "projects", "item_uuid": "TEST_PROJECT2", "group_uuid": None, "action": None,
             "expected": "https://foo.host/v1/projects/TEST_PROJECT2"},
            {"resource": "users", "item_uuid": None, "group_uuid": None, "action": None,
             "expected": "https://foo.host/v1/users"},
            {"resource": "users", "item_uuid": "userA", "group_uuid": None, "action": None,
             "expected": "https://foo.host/v1/users/userA"},
            {"resource": "users", "item_uuid": "userB", "group_uuid": None, "action": None,
             "expected": "https://foo.host/v1/users/userB"},
            {"resource": "topics", "item_uuid": None, "group_uuid": "PROJECT_A", "action": None,
             "expected": "https://foo.host/v1/projects/PROJECT_A/topics"},
            {"resource": "topics", "item_uuid": "topic101", "group_uuid": "PROJECT_A", "action": None,
             "expected": "https://foo.host/v1/projects/PROJECT_A/topics/topic101"},
            {"resource": "topics", "item_uuid": "topic102", "group_uuid": "PROJECT_A", "action": "acl",
             "expected": "https://foo.host/v1/projects/PROJECT_A/topics/topic102:acl"},
            {"resource": "subscriptions", "item_uuid": None, "group_uuid": "PROJECT_A", "action": None,
             "expected": "https://foo.host/v1/projects/PROJECT_A/subscriptions"},
            {"resource": "subscriptions", "item_uuid": "sub101", "group_uuid": "PROJECT_A", "action": None,
             "expected": "https://foo.host/v1/projects/PROJECT_A/subscriptions/sub101"},
            {"resource": "subscriptions", "item_uuid": "sub102", "group_uuid": "PROJECT_A", "action": "acl",
             "expected": "https://foo.host/v1/projects/PROJECT_A/subscriptions/sub102:acl"},

        ]

        for test_case in test_cases:
            actual = ams.get_url(test_case["resource"], test_case["item_uuid"], test_case["group_uuid"],
                                 test_case["action"])
            expected = test_case["expected"]
            self.assertEqual(expected, actual)

    @responses.activate
    def test_basic_request(self):
        # prepare fake responses for ams

        responses.add(responses.GET, 'https://ams.foo/v1/projects/PROJECTA',
                      json={
                          "name": "PROJECTA",
                          "created_on": "2018-03-27T15:56:28Z",
                          "modified_on": "2018-03-27T15:56:28Z",
                          "created_by": "foo_user_admin"
                      }, status=200)
        responses.add(responses.GET, 'https://ams.foo/v1/users',
                      json={"users": [{
                          "uuid": "id01",
                          "projects": [
                              {
                                  "project": "PROJECTA",
                                  "roles": [
                                      "publisher"
                                  ],
                                  "topics": [
                                      "sync_data",
                                      "metric_data"
                                  ],
                                  "subscriptions": []
                              }
                          ],
                          "name": "ams_projectA_publisher",

                      }, {
                          "uuid": "id02",
                          "projects": [
                              {
                                  "project": "PROJECTA",
                                  "roles": [
                                      "consumer"
                                  ],
                                  "topics": [

                                  ],
                                  "subscriptions": ["ingest_sync",
                                                    "ingest_metric",
                                                    "status_sync",
                                                    "status_metric"]
                              }
                          ],
                          "name": "ams_projecta_consumer",
                      }]
                      }, status=200)
        responses.add(responses.GET, 'https://ams.foo/v1/users/ams_projecta_consumer',
                      json={
                          "uuid": "id02",
                          "projects": [
                              {
                                  "project": "PROJECTA",
                                  "roles": [
                                      "publisher"
                                  ],
                                  "topics": [

                                  ],
                                  "subscriptions": ["ingest_metric",
                                                    "status_metric"]
                              }
                          ],
                          "name": "ams_projecta_consumer",

                      }, status=200)
        responses.add(responses.GET, 'https://ams.foo/v1/users/ams_projecta_archiver',
                      json={
                          "uuid": "id02",
                          "projects": [
                              {
                                  "project": "PROJECTA",
                                  "roles": [
                                      "consumer"
                                  ],
                                  "topics": [

                                  ],
                                  "subscriptions": ["archive_metric"]
                              }
                          ],
                          "name": "ams_projecta_archiver",

                      }, status=200)
        responses.add(responses.GET, 'https://ams.foo/v1/users/ams_projecta_publisher',
                      json={
                          "uuid": "id02",
                          "projects": [
                              {
                                  "project": "PROJECTA",
                                  "roles": [
                                      "consumer"
                                  ],
                                  "topics": ["sync_data",
                                             "metric_data"

                                             ],
                                  "subscriptions": []
                              }
                          ],
                          "name": "ams_projecta_consumer",

                      }, status=200)

        responses.add(responses.GET, 'https://ams.foo/v1/projects/PROJECTA/topics',
                      json={"topics": [{
                          "name": "projects/PROJECTA/topics/metric_data"
                      }]
                      }, status=200)

        responses.add(responses.GET, 'https://ams.foo/v1/projects/PROJECTA/subscriptions?',
                      json={"subscriptions": [{
                          "name": "projects/PROJECTA/subscriptions/ingest_metric",
                          "topic": "projects/PROJECTA/topic/metric_data"
                      },
                          {
                              "name": "projects/PROJECTA/subscriptions/status_metric",
                              "topic": "projects/PROJECTA/topic/metric_data"
                          }
                      ]
                      }, status=200)
        responses.add(responses.GET, 'https://ams.foo/v1/users/ams_projecta_admin',
                      json={"error": {"message": "user not found"}
                            }, status=404)

        ams = ArgoAmsClient("ams.foo", "faketoken")

        self.assertEqual("PROJECTA", ams.get_project("PROJECTA")["name"])
        users = ams.get_users()
        self.assertEqual("id01", users[0]["uuid"])
        self.assertEqual("id02", users[1]["uuid"])
        user = ams.get_user("ams_projecta_consumer")
        self.assertEqual("ams_projecta_consumer", user["name"])

        self.assertEqual(["sync_data", "metric_data"], ams.user_get_topics(users[0], "PROJECTA"))
        self.assertEqual([], ams.user_get_subs(users[0], "PROJECTA"))
        self.assertEqual([], ams.user_get_topics(users[1], "PROJECTA"))
        self.assertEqual(["ingest_sync", "ingest_metric", "status_sync", "status_metric"],
                          ams.user_get_subs(users[1], "PROJECTA"))

        self.assertEqual("PROJECTA", ams.check_project_exists("projectA")["name"])
        expected_missing = {'topics': ['sync_data'], 'topic_acls': [],
                            'subs': ['ingest_sync', 'ingest_metric', 'status_sync', 'status_metric', 'archive_metric'],
                            'sub_acls': ['ingest_sync', 'archive_metric'], 'users': ['project_admin']}

        self.assertEqual(expected_missing, ams.check_tenant("projectA"))
