import logging
import unittest

import pytest
from mock import patch, Mock
from pyhocon import ConfigFactory

from databuilder import Scoped
from databuilder.extractor.bigquery_watermark_extractor import BigQueryWatermarkExtractor
from databuilder.models.table_metadata import TableMetadata

logging.basicConfig(level=logging.INFO)


NO_DATASETS = {'kind': 'bigquery#datasetList', 'etag': '1B2M2Y8AsgTpgAmY7PhCfg=='}
ONE_DATASET = {'kind': 'bigquery#datasetList', 'etag': 'yScH5WIHeNUBF9b/VKybXA==',
    'datasets': [{'kind': 'bigquery#dataset', 'id': 'your-project-here:empty', 'datasetReference':
        {'datasetId': 'empty', 'projectId': 'your-project-here'}, 'location': 'US'}]}  # noqa
NO_TABLES = {'kind': 'bigquery#tableList', 'etag': '1B2M2Y8AsgTpgAmY7PhCfg==', 'totalItems': 0}
ONE_TABLE = {'kind': 'bigquery#tableList', 'etag': 'Iaqrz2TCDIANAOD/Xerkjw==',
    'tables': [{'kind': 'bigquery#table', 'id': 'your-project-here:fdgdfgh.nested_recs', 'tableReference':
        {'projectId': 'your-project-here', 'datasetId': 'fdgdfgh', 'tableId': 'nested_recs'},
        'type': 'TABLE', 'creationTime': '1557578974009'}],
    'totalItems': 1}  # noqa
ONE_VIEW = {'kind': 'bigquery#tableList', 'etag': 'Iaqrz2TCDIANAOD/Xerkjw==',
    'tables': [{'kind': 'bigquery#table', 'id': 'your-project-here:fdgdfgh.abab', 'tableReference':
        {'projectId': 'your-project-here', 'datasetId': 'fdgdfgh', 'tableId': 'abab'},
        'type': 'VIEW', 'view': {'useLegacySql': False}, 'creationTime': '1557577874991'}],
        'totalItems': 1}  # noqa
TIME_PARTITIONED = {'kind': 'bigquery#tableList', 'etag': 'Iaqrz2TCDIANAOD/Xerkjw==',
    'tables': [{'kind': 'bigquery#table', 'id': 'your-project-here:fdgdfgh.other', 'tableReference':
            {'projectId': 'your-project-here', 'datasetId': 'fdgdfgh', 'tableId': 'other'},
            'type': 'TABLE', 'timePartitioning': {'type': 'DAY', 'requirePartitionFilter': False},
            'creationTime': '1557577779306'}], 'totalItems': 1}  # noqa
TIME_PARTITIONED_WITH_FIELD = {'kind': 'bigquery#tableList', 'etag': 'Iaqrz2TCDIANAOD/Xerkjw==',
    'tables': [{'kind': 'bigquery#table', 'id': 'your-project-here:fdgdfgh.other', 'tableReference':
            {'projectId': 'your-project-here', 'datasetId': 'fdgdfgh', 'tableId': 'other'},
            'type': 'TABLE', 'timePartitioning': {'type': 'DAY', 'field': 'processed_date', 'requirePartitionFilter': False},
            'creationTime': '1557577779306'}], 'totalItems': 1}  # noqa
TABLE_DATE_RANGE = {'kind': 'bigquery#tableList', 'etag': 'Iaqrz2TCDIANAOD/Xerkjw==',
    'tables': [{'kind': 'bigquery#table', 'id': 'your-project-here:fdgdfgh.other_20190101', 'tableReference':
            {'projectId': 'your-project-here', 'datasetId': 'fdgdfgh', 'tableId': 'date_range_20190101'},
            'type': 'TABLE', 'creationTime': '1557577779306'},
            {'kind': 'bigquery#table', 'id': 'your-project-here:fdgdfgh.other_20190102', 'tableReference':
            {'projectId': 'your-project-here', 'datasetId': 'fdgdfgh', 'tableId': 'date_range_20190102'},
            'type': 'TABLE', 'creationTime': '1557577779306'}], 'totalItems': 2}  # noqa
PARTITION_DATA = {'kind': 'bigquery#queryResponse',
 'schema': {'fields': [{'name': 'partition_id','type': 'STRING','mode': 'NULLABLE'},
                       {'name': 'creation_time', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}]},
 'jobReference': {'projectId': 'your-project-here','jobId': 'job_bfTRGj3Lv0tRjcrotXbZSgMCpNhY','location': 'EU'},
 'totalRows': '3',
 'rows': [{'f': [{'v': '20180802'}, {'v': '1.547512241348E9'}]},
          {'f': [{'v': '20180803'}, {'v': '1.547512241348E9'}]},
          {'f': [{'v': '20180804'}, {'v': '1.547512241348E9'}]}],
 'totalBytesProcessed': '0','jobComplete': True,'cacheHit': False} # noqa


try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError


class MockBigQueryClient():
    def __init__(self, dataset_list_data, table_list_data, partition_data):
        self.list_execute = Mock()
        self.list_execute.execute.return_value = table_list_data
        self.tables_method = Mock()
        self.tables_method.list.return_value = self.list_execute
        self.ds_execute = Mock()
        self.ds_execute.execute.return_value = dataset_list_data
        self.ds_list = Mock()
        self.ds_list.list.return_value = self.ds_execute
        self.query_execute = Mock()
        self.query_execute.execute.return_value = partition_data
        self.jobs_query = Mock()
        self.jobs_query.query.return_value = self.query_execute

    def datasets(self):
        return self.ds_list

    def tables(self):
        return self.tables_method

    def jobs(self):
        return self.jobs_query


class TestBigQueryWatermarkExtractor(unittest.TestCase):
    def setUp(self):
        # type: () -> None
        config_dict = {
            'extractor.bigquery_watermarks.{}'.format(BigQueryWatermarkExtractor.PROJECT_ID_KEY):
                'your-project-here'}
        self.conf = ConfigFactory.from_dict(config_dict)

    @patch('databuilder.extractor.bigquery_watermark_extractor.build')
    def test_can_handle_no_datasets(self, mock_build):
        mock_build.return_value = MockBigQueryClient(NO_DATASETS, None, None)
        extractor = BigQueryWatermarkExtractor()
        extractor.init(Scoped.get_scoped_conf(conf=self.conf,
                                              scope=extractor.get_scope()))
        result = extractor.extract()
        self.assertIsNone(result)

    @patch('databuilder.extractor.bigquery_watermark_extractor.build')
    def test_empty_dataset(self, mock_build):
        mock_build.return_value = MockBigQueryClient(ONE_DATASET, NO_TABLES, None)
        extractor = BigQueryWatermarkExtractor()
        extractor.init(Scoped.get_scoped_conf(conf=self.conf,
                                              scope=extractor.get_scope()))
        result = extractor.extract()
        self.assertIsNone(result)

    @pytest.mark.skip()
    @patch('databuilder.extractor.bigquery_watermark_extractor.build')
    def test_accepts_dataset_filter_by_label(self, mock_build):
        config_dict = {
            'extractor.bigquery_table_metadata.{}'.format(BigQueryWatermarkExtractor.PROJECT_ID_KEY):
                'your-project-here',
            'extractor.bigquery_table_metadata.{}'.format(BigQueryWatermarkExtractor.FILTER_KEY):
                'label.key:value'
        }
        conf = ConfigFactory.from_dict(config_dict)

        mock_build.return_value = MockBigQueryClient(ONE_DATASET, ONE_TABLE, TABLE_DATA)
        extractor = BigQueryWatermarkExtractor()
        extractor.init(Scoped.get_scoped_conf(conf=conf,
                                              scope=extractor.get_scope()))
        result = extractor.extract()
        self.assertIsInstance(result, TableMetadata)

    @patch('databuilder.extractor.bigquery_watermark_extractor.build')
    def test_table_without_partitions(self, mock_build):
        mock_build.return_value = MockBigQueryClient(ONE_DATASET, ONE_TABLE, None)
        extractor = BigQueryWatermarkExtractor()
        extractor.init(Scoped.get_scoped_conf(conf=self.conf,
                                              scope=extractor.get_scope()))
        result = extractor.extract()
        self.assertIsNone(result)

    @patch('databuilder.extractor.bigquery_watermark_extractor.build')
    def test_table_with_default_partitions(self, mock_build):
        mock_build.return_value = MockBigQueryClient(ONE_DATASET, TIME_PARTITIONED, PARTITION_DATA)
        extractor = BigQueryWatermarkExtractor()
        extractor.init(Scoped.get_scoped_conf(conf=self.conf,
                                              scope=extractor.get_scope()))
        result = extractor.extract()
        self.assertEquals(result.part_type, 'low_watermark')
        self.assertEquals(result.database, 'bigquery')
        self.assertEquals(result.schema, 'fdgdfgh')
        self.assertEquals(result.table, 'other')
        self.assertEquals(result.cluster, 'your-project-here')
        self.assertEquals(result.create_time, '2019-01-15 11:30:41')
        self.assertEquals(result.parts, [('_partitiontime','20180802')])

        result = extractor.extract()
        self.assertEquals(result.part_type, 'high_watermark')
        self.assertEquals(result.database, 'bigquery')
        self.assertEquals(result.schema, 'fdgdfgh')
        self.assertEquals(result.table, 'other')
        self.assertEquals(result.cluster, 'your-project-here')
        self.assertEquals(result.create_time, '2019-01-15 11:30:41')
        self.assertEquals(result.parts, [('_partitiontime','20180804')])

    @patch('databuilder.extractor.bigquery_watermark_extractor.build')
    def test_table_with_field_partitions(self, mock_build):
        mock_build.return_value = MockBigQueryClient(ONE_DATASET, TIME_PARTITIONED_WITH_FIELD, PARTITION_DATA)
        extractor = BigQueryWatermarkExtractor()
        extractor.init(Scoped.get_scoped_conf(conf=self.conf,
                                              scope=extractor.get_scope()))
        result = extractor.extract()
        self.assertEquals(result.part_type, 'low_watermark')
        self.assertEquals(result.database, 'bigquery')
        self.assertEquals(result.schema, 'fdgdfgh')
        self.assertEquals(result.table, 'other')
        self.assertEquals(result.cluster, 'your-project-here')
        self.assertEquals(result.create_time, '2019-01-15 11:30:41')
        self.assertEquals(result.parts, [('processed_date','20180802')])

        result = extractor.extract()
        self.assertEquals(result.part_type, 'high_watermark')
        self.assertEquals(result.database, 'bigquery')
        self.assertEquals(result.schema, 'fdgdfgh')
        self.assertEquals(result.table, 'other')
        self.assertEquals(result.cluster, 'your-project-here')
        self.assertEquals(result.create_time, '2019-01-15 11:30:41')
        self.assertEquals(result.parts, [('processed_date','20180804')])


    @pytest.mark.skip()
    @patch('databuilder.extractor.bigquery_watermark_extractor.build')
    def test_keypath_and_pagesize_can_be_set(self, mock_build):
        config_dict = {
            'extractor.bigquery_table_metadata.{}'.format(BigQueryWatermarkExtractor.PROJECT_ID_KEY):
                'your-project-here',
            'extractor.bigquery_table_metadata.{}'.format(BigQueryWatermarkExtractor.PAGE_SIZE_KEY):
                200,
            'extractor.bigquery_table_metadata.{}'.format(BigQueryWatermarkExtractor.KEY_PATH_KEY):
                '/tmp/doesnotexist',
        }
        conf = ConfigFactory.from_dict(config_dict)

        mock_build.return_value = MockBigQueryClient(ONE_DATASET, ONE_TABLE, TABLE_DATA)
        extractor = BigQueryWatermarkExtractor()

        with self.assertRaises(FileNotFoundError):
            extractor.init(Scoped.get_scoped_conf(conf=conf,
                                                  scope=extractor.get_scope()))

    @pytest.mark.skip()
    @patch('databuilder.extractor.bigquery_watermark_extractor.build')
    def test_table_part_of_table_date_range(self, mock_build):
        mock_build.return_value = MockBigQueryClient(ONE_DATASET, TABLE_DATE_RANGE, TABLE_DATA)
        extractor = BigQueryWatermarkExtractor()
        extractor.init(Scoped.get_scoped_conf(conf=self.conf,
                                              scope=extractor.get_scope()))

        count = 0
        result = extractor.extract()
        table_name = result.name
        while result:
            count += 1
            result = extractor.extract()

        self.assertEquals(count, 1)
        self.assertEquals(table_name, 'date_range_')
