from cassandra.cluster import Cluster

from pyhocon import ConfigFactory, ConfigTree  # noqa: F401
from typing import Iterator, Union, Dict, Any  # noqa: F401

from databuilder.extractor.base_extractor import Extractor
from databuilder.models.table_metadata import TableMetadata, ColumnMetadata


class CassandraExtractor(Extractor):
    """
    Extracts tables and columns metadata from Apacha Cassandra
    """

    CLUSTER_KEY = 'cluster'
    IPS_KEY = 'ips'
    KWARGS_KEY = 'kwargs'
    FILTER_FUNCTION_KEY = 'filter'

    DEFAULT_CONFIG = ConfigFactory.from_dict({
        CLUSTER_KEY:'gold',IPS_KEY:[],KWARGS_KEY:{},FILTER_FUNCTION_KEY:None
    })

    def init(self, conf):
        conf = conf.with_fallback(CassandraExtractor.DEFAULT_CONFIG)
        self._cluster = '{}'.format(conf.get_string(CassandraExtractor.CLUSTER_KEY))
        self._filter = conf.get(CassandraExtractor.FILTER_FUNCTION_KEY)
        ips = conf.get_list(CassandraExtractor.IPS_KEY)
        kwargs = conf.get(CassandraExtractor.KWARGS_KEY)
        self._client = Cluster(ips, **kwargs)
        self._client.connect()
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
        return 'extractor.cassandra'

    def _get_extract_iter(self):
        # type: () -> Iterator[TableMetadata]
        """
        It gets all tables and yields TableMetadata
        :return:
        """
        keyspaces = self._get_keyspaces()
        for keyspace in keyspaces:
            # system keyspaces
            if keyspace.startswith('system'):
                continue
            for table in self._get_tables(keyspace):
                if self._filter and not self._filter(keyspace, table):
                    continue
                
                columns = []

                i = 0
                columns_dict = self._get_columns(keyspace, table)
                for column_name, column in columns_dict.items():
                    columns.append(ColumnMetadata(
                        column_name,
                        None,
                        column.cql_type,
                        i
                    ))
                    i += 1

                yield TableMetadata(
                    'cassandra',
                    self._cluster,
                    keyspace,
                    table,
                    None,
                    columns
                )

    def _get_keyspaces(self):
        return self._client.metadata.keyspaces

    def _get_tables(self, keyspace):
        return self._client.metadata.keyspaces[keyspace].tables

    def _get_columns(self, keyspace, table):
        return self._client.metadata.keyspaces[keyspace].tables[table].columns
