import logging
import unittest

from mock import patch, MagicMock
from pyhocon import ConfigFactory
from typing import Any, Dict  # noqa: F401

from databuilder.extractor.redshift_spectrum_metadata_extractor import RedshiftSpectrumMetadataExtractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.models.table_metadata import TableMetadata, ColumnMetadata


class TestRedshiftSpectrumMetadataExtractor(unittest.TestCase):
    def setUp(self):
        # type: () -> None
        logging.basicConfig(level=logging.INFO)

        config_dict = {
            'extractor.sqlalchemy.{}'.format(SQLAlchemyExtractor.CONN_STRING):
            'TEST_CONNECTION',
            'extractor.redshift_spectrum_metadata.{}'.format(RedshiftSpectrumMetadataExtractor.CLUSTER_KEY):
            'MY_CLUSTER',
            'extractor.redshift_spectrum_metadata.{}'.format(RedshiftSpectrumMetadataExtractor.DATABASE_KEY):
            'spectrum'
        }
        self.conf = ConfigFactory.from_dict(config_dict)

    def test_extraction_with_empty_query_result(self):
        # type: () -> None
        """
        Test Extraction with empty result from query
        """
        with patch.object(SQLAlchemyExtractor, '_get_connection'):
            extractor = RedshiftSpectrumMetadataExtractor()
            extractor.init(self.conf)

            results = extractor.extract()
            self.assertEqual(results, None)

    def test_extraction_with_single_result(self):
        # type: () -> None
        with patch.object(SQLAlchemyExtractor, '_get_connection') as mock_connection:
            connection = MagicMock()
            mock_connection.return_value = connection
            sql_execute = MagicMock()
            connection.execute = sql_execute
            table = {'schema_name': 'test_schema',
                     'name': 'test_table',
                     'description': '',
                     'cluster':
                     self.conf['extractor.redshift_spectrum_metadata.{}'.format(RedshiftSpectrumMetadataExtractor.CLUSTER_KEY)]
                     }

            sql_execute.return_value = [
                self._union(
                    {'col_name': 'col_id1',
                     'col_type': 'bigint',
                     'col_description': '',
                     'col_sort_order': 0}, table),
                self._union(
                    {'col_name': 'col_id2',
                     'col_type': 'bigint',
                     'col_description': '',
                     'col_sort_order': 1}, table),
                self._union(
                    {'col_name': 'is_active',
                     'col_type': 'boolean',
                     'col_description': '',
                     'col_sort_order': 2}, table),
                self._union(
                    {'col_name': 'source',
                     'col_type': 'varchar(256)',
                     'col_description': '',
                     'col_sort_order': 3}, table),
                self._union(
                    {'col_name': 'etl_created_at',
                     'col_type': 'timestamp',
                     'col_description': '',
                     'col_sort_order': 4}, table),
                self._union(
                    {'col_name': 'ds',
                     'col_type': 'varchar(128)',
                     'col_description': '',
                     'col_sort_order': 5}, table)
            ]

            extractor = RedshiftSpectrumMetadataExtractor()
            extractor.init(self.conf)
            actual = extractor.extract()
            expected = TableMetadata('postgres', 'MY_CLUSTER', 'test_schema', 'test_table', '',
                                     [ColumnMetadata('col_id1', '', 'bigint', 0),
                                      ColumnMetadata('col_id2', '', 'bigint', 1),
                                      ColumnMetadata('is_active', '', 'boolean', 2),
                                      ColumnMetadata('source', '', 'varchar(256)', 3),
                                      ColumnMetadata('etl_created_at', '', 'timestamp', 4),
                                      ColumnMetadata('ds', '', 'varchar(128)', 5)])

            self.assertEqual(expected.__repr__(), actual.__repr__())
            self.assertIsNone(extractor.extract())

    def test_extraction_with_multiple_result(self):
        # type: () -> None
        with patch.object(SQLAlchemyExtractor, '_get_connection') as mock_connection:
            connection = MagicMock()
            mock_connection.return_value = connection
            sql_execute = MagicMock()
            connection.execute = sql_execute
            table = {'schema_name': 'test_schema1',
                     'name': 'test_table1',
                     'description': '',
                     'cluster':
                     self.conf['extractor.redshift_spectrum_metadata.{}'.format(RedshiftSpectrumMetadataExtractor.CLUSTER_KEY)]
                     }

            table1 = {'schema_name': 'test_schema1',
                      'name': 'test_table2',
                      'description': '',
                      'cluster':
                      self.conf['extractor.redshift_spectrum_metadata.{}'.format(RedshiftSpectrumMetadataExtractor.CLUSTER_KEY)]
                      }

            table2 = {'schema_name': 'test_schema2',
                      'name': 'test_table3',
                      'description': '',
                      'cluster':
                      self.conf['extractor.redshift_spectrum_metadata.{}'.format(RedshiftSpectrumMetadataExtractor.CLUSTER_KEY)]
                      }

            sql_execute.return_value = [
                self._union(
                    {'col_name': 'col_id1',
                     'col_type': 'bigint',
                     'col_description': '',
                     'col_sort_order': 0}, table),
                self._union(
                    {'col_name': 'col_id2',
                     'col_type': 'bigint',
                     'col_description': '',
                     'col_sort_order': 1}, table),
                self._union(
                    {'col_name': 'is_active',
                     'col_type': 'boolean',
                     'col_description': '',
                     'col_sort_order': 2}, table),
                self._union(
                    {'col_name': 'source',
                     'col_type': 'varchar',
                     'col_description': '',
                     'col_sort_order': 3}, table),
                self._union(
                    {'col_name': 'etl_created_at',
                     'col_type': 'timestamp',
                     'col_description': '',
                     'col_sort_order': 4}, table),
                self._union(
                    {'col_name': 'ds',
                     'col_type': 'varchar',
                     'col_description': '',
                     'col_sort_order': 5}, table),
                self._union(
                    {'col_name': 'col_name',
                     'col_type': 'varchar',
                     'col_description': '',
                     'col_sort_order': 0}, table1),
                self._union(
                    {'col_name': 'col_name2',
                     'col_type': 'varchar',
                     'col_description': '',
                     'col_sort_order': 1}, table1),
                self._union(
                    {'col_name': 'col_id3',
                     'col_type': 'varchar',
                     'col_description': '',
                     'col_sort_order': 0}, table2),
                self._union(
                    {'col_name': 'col_name3',
                     'col_type': 'varchar',
                     'col_description': '',
                     'col_sort_order': 1}, table2)
            ]

            extractor = RedshiftSpectrumMetadataExtractor()
            extractor.init(self.conf)

            expected = TableMetadata('postgres',
                                     self.conf['extractor.redshift_spectrum_metadata.{}'.format(
                                         RedshiftSpectrumMetadataExtractor.CLUSTER_KEY)],
                                     'test_schema1', 'test_table1', '',
                                     [ColumnMetadata('col_id1', '', 'bigint', 0),
                                      ColumnMetadata('col_id2', '', 'bigint', 1),
                                      ColumnMetadata('is_active', '', 'boolean', 2),
                                      ColumnMetadata('source', '', 'varchar', 3),
                                      ColumnMetadata('etl_created_at', '', 'timestamp', 4),
                                      ColumnMetadata('ds', '', 'varchar', 5)])
            self.assertEqual(expected.__repr__(), extractor.extract().__repr__())

            expected = TableMetadata('postgres',
                                     self.conf['extractor.redshift_spectrum_metadata.{}'.format(
                                         RedshiftSpectrumMetadataExtractor.CLUSTER_KEY)],
                                     'test_schema1', 'test_table2', '',
                                     [ColumnMetadata('col_name', '', 'varchar', 0),
                                      ColumnMetadata('col_name2', '', 'varchar', 1)])
            self.assertEqual(expected.__repr__(), extractor.extract().__repr__())

            expected = TableMetadata('postgres',
                                     self.conf['extractor.redshift_spectrum_metadata.{}'.format(
                                         RedshiftSpectrumMetadataExtractor.CLUSTER_KEY)],
                                     'test_schema2', 'test_table3', '',
                                     [ColumnMetadata('col_id3', '', 'varchar', 0),
                                      ColumnMetadata('col_name3', '', 'varchar', 1)])
            self.assertEqual(expected.__repr__(), extractor.extract().__repr__())

            self.assertIsNone(extractor.extract())
            self.assertIsNone(extractor.extract())

    def _union(self, target, extra):
        # type: (Dict[Any, Any], Dict[Any, Any]) -> Dict[Any, Any]
        target.update(extra)
        return target


class TestPostgresMetadataExtractorWithWhereClause(unittest.TestCase):
    def setUp(self):
        # type: () -> None
        logging.basicConfig(level=logging.INFO)
        self.where_clause_suffix = """
        where table_schema in ('public_external') and table_name = 'amundsen'
        """

        config_dict = {
            RedshiftSpectrumMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY: self.where_clause_suffix,
            'extractor.sqlalchemy.{}'.format(SQLAlchemyExtractor.CONN_STRING):
                'TEST_CONNECTION'
        }
        self.conf = ConfigFactory.from_dict(config_dict)

    def test_sql_statement(self):
        # type: () -> None
        """
        Test Extraction with empty result from query
        """
        with patch.object(SQLAlchemyExtractor, '_get_connection'):
            extractor = RedshiftSpectrumMetadataExtractor()
            extractor.init(self.conf)
            self.assertTrue(self.where_clause_suffix in extractor.sql_stmt)


if __name__ == '__main__':
    unittest.main()
