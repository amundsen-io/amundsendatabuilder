import logging
import random
import unittest

from mock import patch

from databuilder.rest_api.base_rest_api_query import EmptyRestApiQuerySeed
from databuilder.extractor.dashboard.redash.redash_dashboard_utils import \
    get_text_widgets, get_visualization_widgets, sort_widgets, \
    generate_dashboard_description, get_auth_headers, RedashPaginatedRestApiQuery

logging.basicConfig(level=logging.INFO)


class TestRedashDashboardUtils(unittest.TestCase):
    def test_sort_widgets(self):
        widgets = [
            {
                'text': 'a',
                'options': {}
            },
            {
                'text': 'b',
                'options': {'position': {'row': 1, 'col': 1}}
            },
            {
                'text': 'c',
                'options': {'position': {'row': 1, 'col': 2}}
            },
            {
                'text': 'd',
                'options': {'position': {'row': 2, 'col': 1}}
            }
        ]
        random.shuffle(widgets)
        sorted_widgets = sort_widgets(widgets)
        self.assertListEqual([widget['text'] for widget in sorted_widgets], ['a', 'b', 'c', 'd'])
    
    def test_widget_filters(self):
        widgets = [
            { 'text': 'asdf', 'options': {'ex': 1}},
            { 'text': 'asdf', 'options': {'ex': 2}},
            { 'visualization': {}, 'options': {'ex': 1}},
            { 'visualization': {}, 'options': {'ex': 2}},
            { 'visualization': {}, 'options': {'ex': 3}}
        ]
        self.assertEqual(len(get_text_widgets(widgets)), 2)
        self.assertEqual(len(get_visualization_widgets(widgets)), 3)
    
    def test_text_widget_props(self):
        widget_data = {
            'text': 'asdf'
        }
        widget = get_text_widgets([widget_data])[0]
        self.assertEqual(widget.text, 'asdf')
    
    def test_visualization_widget_props(self):
        widget_data = {
            'visualization': {
                'query': {
                    'id': 123,
                    'data_source_id': 1,
                    'query': 'SELECT 2+2 FROM DUAL',
                    'name': 'Test'
                }
            }
        }
        widget = get_visualization_widgets([widget_data])[0]
        
        self.assertEqual(widget.query_id, 123)
        self.assertEqual(widget.data_source_id, 1)
        self.assertEqual(widget.raw_query, 'SELECT 2+2 FROM DUAL')
        self.assertEqual(widget.query_name, 'Test')

    def test_descriptions_from_text(self):
        text_widgets = get_text_widgets([
            { 'text': 'T1' },
            { 'text': 'T2' }
        ])
        viz_widgets = get_visualization_widgets([
            {
                'visualization': {
                    'query': {
                        'id': 1,
                        'data_source_id': 1,
                        'name': 'Q1',
                        'query': 'n/a'
                    }
                }
            },
            {
                'visualization': {
                    'query': {
                        'id': 2,
                        'data_source_id': 1,
                        'name': 'Q2',
                        'query': 'n/a'
                    }
                }
            }
        ])

        # both text and viz widgets
        desc1 = generate_dashboard_description(text_widgets, viz_widgets)
        self.assertTrue('T1' in desc1)
        self.assertTrue('T2' in desc1)
        self.assertTrue('Q1' not in desc1)

        # only text widgets
        desc2 = generate_dashboard_description(text_widgets, [])
        self.assertEqual(desc1, desc2)

        # only viz widgets
        desc3 = generate_dashboard_description([], viz_widgets)
        self.assertTrue('Q1' in desc3)
        self.assertTrue('Q2' in desc3)

        # no widgets
        desc4 = generate_dashboard_description([], [])
        self.assertTrue('empty' in desc4)

    def test_auth_headers(self):
        headers = get_auth_headers('testkey')
        self.assertTrue('testkey' in headers['Authorization'])

    def test_paginated_rest_api_query(self):
        paged_content = [{
                'page': 1,
                'page_size': 5,
                'count': 12,
                'results': [{'test': True}] * 5
            },
            {
                'page': 2,
                'page_size': 5,
                'count': 12,
                'results': [{'test': True}] * 5
            },
            {
                'page': 3,
                'page_size': 5,
                'count': 12,
                'results': [{'test': True}] * 2
            },
            {
                'page': 4,
                'page_size': 5,
                'count': 12,
                'results': []
            }
        ]

        with patch('databuilder.rest_api.rest_api_query.requests.get') as mock_get:
            # .json() is called twice (ugh), so we have to double each page
            mock_get.return_value.json.side_effect = [page for page in paged_content for page in [page]*2]

            q = RedashPaginatedRestApiQuery(seed_record=EmptyRestApiQuerySeed(),
                                            query_to_join=EmptyRestApiQuerySeed(),
                                            url='example.com',
                                            json_path='results[*].[test]',
                                            params={},
                                            field_names=['test'],
                                            skip_no_result=True)
            n_records = 0
            for record in q.execute():
                self.assertEqual(record['test'], True)
                n_records += 1
            
            self.assertEqual(n_records, 12)