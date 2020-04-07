import json
import unittest

from databuilder.models.dashboard_elasticsearch_document import DashboardESDocument


class TestDashboardElasticsearchDocument(unittest.TestCase):

    def test_to_json(self):
        # type: () -> None
        """
        Test string generated from to_json method
        """
        test_obj = DashboardESDocument(dashboard_group='test_dashboard_group',
                                       dashboard_name='test_dashboard_name',
                                       description='test_description',
                                       product='mode',
                                       cluster='gold',
                                       dashboard_group_description='work space group',
                                       query_names=['query1'],
                                       group_url='mode_group_url',
                                       url='mode_report_url',
                                       uri='mode_report_uri',
                                       last_successful_run_timestamp=10,
                                       total_usage=10,
                                       tags=['test'])

        expected_document_dict = {"dashboard_group": "test_dashboard_group",
                                  "dashboard_name": "test_dashboard_name",
                                  "description": "test_description",
                                  "product": "mode",
                                  "cluster": "gold",
                                  "group_url": "mode_group_url",
                                  "url": "mode_report_url",
                                  "uri": "mode_report_uri",
                                  "query_names": ['query1'],
                                  "last_successful_run_timestamp": 10,
                                  "dashboard_group_description": "work space group",
                                  "total_usage": 10,
                                  "tags": ["test"]
                                  }

        result = test_obj.to_json()
        results = result.split("\n")

        # verify two new line characters in result
        self.assertEqual(len(results), 2, "Result from to_json() function doesn't have a newline!")
        self.assertDictEqual(json.loads(results[0]), expected_document_dict)
