import logging
import six
from collections import namedtuple

from pyhocon import ConfigFactory, ConfigTree  # noqa: F401
from typing import Iterator, Union, Dict, Any  # noqa: F401

from databuilder import Scoped
from databuilder.extractor.base_extractor import Extractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.models.table_metadata import TableMetadata, ColumnMetadata
from itertools import groupby


TableKey = namedtuple('TableKey', ['schema_name', 'table_name'])

LOGGER = logging.getLogger(__name__)


class RedshiftSpectrumMetadataExtractor(Extractor):
    """
    Extracts Postgres table and column metadata from underlying meta store database using SQLAlchemyExtractor
    """
    # SELECT statement from postgres information_schema to extract table and column metadata
    SQL_STATEMENT = """
    SELECT 
       '{cluster_source}'     as cluster,
       schemaname           as schema_name,
       tablename            as name,
       ''                   as description,
       columnname           as col_name,
       external_type        as col_type,
       ''                   as col_description,
       columnnum            as col_sort_order
    FROM pg_catalog.svv_external_columns
    {where_clause_suffix}
    ORDER BY cluster, schema_name, name, col_sort_order;
    """

    # CONFIG KEYS
    WHERE_CLAUSE_SUFFIX_KEY = 'where_clause_suffix'
    CLUSTER_KEY = 'cluster_key'
    DATABASE_KEY = 'database_key'

    # Default values
    DEFAULT_CLUSTER_NAME = 'spectrum'

    DEFAULT_CONFIG = ConfigFactory.from_dict(
        {
            WHERE_CLAUSE_SUFFIX_KEY: ' ',
            CLUSTER_KEY: DEFAULT_CLUSTER_NAME
        }
    )

    def init(self, conf):
        # type: (ConfigTree) -> None
        conf = conf.with_fallback(RedshiftSpectrumMetadataExtractor.DEFAULT_CONFIG)
        self._cluster = '{}'.format(conf.get_string(RedshiftSpectrumMetadataExtractor.CLUSTER_KEY))

        database = conf.get_string(RedshiftSpectrumMetadataExtractor.DATABASE_KEY, default='spectrum')
        if six.PY2 and isinstance(database, six.text_type):
            database = database.encode('utf-8', 'ignore')

        self._database = database

        self.sql_stmt = RedshiftSpectrumMetadataExtractor.SQL_STATEMENT.format(
            where_clause_suffix=conf.get_string(RedshiftSpectrumMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY),
            cluster_source=self._cluster
        )

        self._alchemy_extractor = SQLAlchemyExtractor()
        sql_alch_conf = Scoped.get_scoped_conf(conf, self._alchemy_extractor.get_scope())\
            .with_fallback(ConfigFactory.from_dict({SQLAlchemyExtractor.EXTRACT_SQL: self.sql_stmt}))

        self.sql_stmt = sql_alch_conf.get_string(SQLAlchemyExtractor.EXTRACT_SQL)

        LOGGER.info('SQL for postgres metadata: {}'.format(self.sql_stmt))

        self._alchemy_extractor.init(sql_alch_conf)
        self._extract_iter = None  # type: Union[None, Iterator]

    def extract(self):
        # type: () -> Union[TableMetadata, None]
        if not self._extract_iter:
            self._extract_iter = self._get_extract_iter()
        try:
            return next(self._extract_iter)
        except StopIteration:
            return None

    def get_scope(self):
        # type: () -> str
        return 'extractor.redshift_spectrum_metadata'

    def _get_extract_iter(self):
        # type: () -> Iterator[TableMetadata]
        """
        Using itertools.groupby and raw level iterator, it groups to table and yields TableMetadata
        :return:
        """
        for key, group in groupby(self._get_raw_extract_iter(), self._get_table_key):
            columns = []

            for row in group:
                last_row = row
                columns.append(ColumnMetadata(row['col_name'], row['col_description'],
                                              row['col_type'], row['col_sort_order']))

            yield TableMetadata(self._database, last_row['cluster'],
                                last_row['schema_name'],
                                last_row['name'],
                                last_row['description'],
                                columns)

    def _get_raw_extract_iter(self):
        # type: () -> Iterator[Dict[str, Any]]
        """
        Provides iterator of result row from SQLAlchemy extractor
        :return:
        """
        row = self._alchemy_extractor.extract()
        while row:
            yield row
            row = self._alchemy_extractor.extract()

    def _get_table_key(self, row):
        # type: (Dict[str, Any]) -> Union[TableKey, None]
        """
        Table key consists of schema and table name
        :param row:
        :return:
        """
        if row:
            return TableKey(schema_name=row['schema_name'], table_name=row['name'])

        return None
