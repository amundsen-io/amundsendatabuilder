# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import unittest

from pyhocon import ConfigFactory

from databuilder import Scoped
from databuilder.extractor.csv_extractor import (
    CsvExtractor, CsvTableColumnExtractor, CsvColumnLineageExtractor, CsvTableLineageExtractor
)
from databuilder.models.badge import Badge


class TestCsvExtractor(unittest.TestCase):

    def test_extraction_with_model_class(self) -> None:
        """
        Test Extraction using model class
        """
        config_dict = {
            f'extractor.csv.{CsvExtractor.FILE_LOCATION}': 'example/sample_data/sample_table.csv',
            f'extractor.csv.model_class': 'databuilder.models.table_metadata.TableMetadata',
        }
        self.conf = ConfigFactory.from_dict(config_dict)
        extractor = CsvExtractor()
        extractor.init(Scoped.get_scoped_conf(conf=self.conf,
                                              scope=extractor.get_scope()))

        result = extractor.extract()
        self.assertEqual(result.name, 'test_table1')
        self.assertEqual(result.description.text, '1st test table')
        self.assertEqual(result.database, 'hive')
        self.assertEqual(result.cluster, 'gold')
        self.assertEqual(result.schema, 'test_schema')
        self.assertEqual(result.tags, ['tag1', 'tag2'])
        self.assertEqual(result.is_view, 'false')

        result2 = extractor.extract()
        self.assertEqual(result2.name, 'test_table2')
        self.assertEqual(result2.is_view, 'false')

        result3 = extractor.extract()
        self.assertEqual(result3.name, 'test_view1')
        self.assertEqual(result3.is_view, 'true')

    def test_extraction_of_tablecolumn_badges(self) -> None:
        """
        Test Extraction using the combined CsvTableModel model class
        """
        config_dict = {
            f'extractor.csvtablecolumn.{CsvTableColumnExtractor.TABLE_FILE_LOCATION}':
            'example/sample_data/sample_table.csv',
            f'extractor.csvtablecolumn.{CsvTableColumnExtractor.COLUMN_FILE_LOCATION}':
            'example/sample_data/sample_col.csv',
        }
        self.conf = ConfigFactory.from_dict(config_dict)

        extractor = CsvTableColumnExtractor()
        extractor.init(Scoped.get_scoped_conf(conf=self.conf,
                                              scope=extractor.get_scope()))

        result = extractor.extract()
        self.assertEqual(result.name, 'test_table1')
        self.assertEqual(result.columns[0].badges, [Badge('pk', 'column')])
        self.assertEqual(result.columns[1].badges, [Badge('pii', 'column')])

    def test_extraction_table_lineage(self) -> None:
        """
        Test table lineage extraction using model class
        """
        config_dict = {
            f'extractor.csvtablelineage.{CsvTableLineageExtractor.TABLE_LINEAGE_FILE_LOCATION}':
                'example/sample_data/sample_table_lineage.csv'
        }
        self.conf = ConfigFactory.from_dict(config_dict)
        extractor = CsvTableLineageExtractor()
        extractor.init(Scoped.get_scoped_conf(conf=self.conf,
                                              scope=extractor.get_scope()))

        result = extractor.extract()
        self.assertEqual(result.table_key, 'hive://gold.test_schema/test_table1')
        self.assertEqual(result.downstream_deps, ['dynamo://gold.test_schema/test_table2'])

    def test_extraction_column_lineage(self) -> None:
        """
        Test column lineage extraction using model class
        """
        config_dict = {
            f'extractor.csvcolumnlineage.{CsvColumnLineageExtractor.COLUMN_LINEAGE_FILE_LOCATION}':
                'example/sample_data/sample_column_lineage.csv'
        }
        self.conf = ConfigFactory.from_dict(config_dict)
        extractor = CsvColumnLineageExtractor()
        extractor.init(Scoped.get_scoped_conf(conf=self.conf,
                                              scope=extractor.get_scope()))

        result = extractor.extract()
        self.assertEqual(result.column_key, 'hive://gold.test_schema/test_table1/col1')
        self.assertEqual(result.downstream_deps, ['dynamo://gold.test_schema/test_table2/col1'])
