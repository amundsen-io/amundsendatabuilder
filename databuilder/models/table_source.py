from typing import Any, Dict, List, Union  # noqa: F401

from databuilder.models.neo4j_csv_serde import Neo4jCsvSerializable

from databuilder.models.table_metadata import TableMetadata
from databuilder.models.graph_node import GraphNode
from databuilder.models.graph_relationship import GraphRelationship

class TableSource(Neo4jCsvSerializable):
    # type: (...) -> None
    """
    Hive table source model.
    """
    LABEL = 'Source'
    KEY_FORMAT = '{db}://{cluster}.{schema}/{tbl}/_source'
    SOURCE_TABLE_RELATION_TYPE = 'SOURCE_OF'
    TABLE_SOURCE_RELATION_TYPE = 'SOURCE'

    def __init__(self,
                 db_name,  # type: str
                 schema,  # type: str
                 table_name,  # type: str
                 cluster,  # type: str
                 source,  # type: str
                 source_type='github',  # type: str
                 ):
        # type: (...) -> None
        self.db = db_name.lower()
        self.schema = schema.lower()
        self.table = table_name.lower()

        self.cluster = cluster.lower() if cluster else 'gold'
        # source is the source file location
        self.source = source
        self.source_type = source_type
        self._node_iter = iter(self.create_nodes())
        self._relation_iter = iter(self.create_relation())

    def create_next_node(self):
        # type: (...) -> Union[GraphNode, None]
        # return the string representation of the data
        try:
            return next(self._node_iter)
        except StopIteration:
            return None

    def create_next_relation(self):
        # type: (...) -> Union[GraphRelationship, None]
        try:
            return next(self._relation_iter)
        except StopIteration:
            return None

    def get_source_model_key(self):
        # type: (...) -> str
        return TableSource.KEY_FORMAT.format(db=self.db,
                                             cluster=self.cluster,
                                             schema=self.schema,
                                             tbl=self.table)

    def get_metadata_model_key(self):
        # type: (...) -> str
        return '{db}://{cluster}.{schema}/{table}'.format(db=self.db,
                                                          cluster=self.cluster,
                                                          schema=self.schema,
                                                          table=self.table)

    def create_nodes(self):
        # type: () -> List[GraphNode]
        """
        Create a list of Neo4j node records
        :return:
        """
        node = GraphNode(
            id=self.get_source_model_key(),
            label=TableSource.LABEL,
            node_attributes={
                'source': self.source,
                'source_type': self.source_type
            }
        )
        results = [node]
        return results

    def create_relation(self):
        # type: () -> List[GraphRelationship]
        """
        Create a list of relation map between owner record with original hive table
        :return:
        """
        relationship = GraphRelationship(
            start_label=TableSource.LABEL,
            start_key=self.get_source_model_key(),
            end_label=TableMetadata.TABLE_NODE_LABEL,
            end_key=self.get_metadata_model_key(),
            type=TableSource.SOURCE_TABLE_RELATION_TYPE,
            reverse_type=TableSource.TABLE_SOURCE_RELATION_TYPE
        )
        results = [relationship]
        return results

    def __repr__(self):
        # type: () -> str
        return 'TableSource({!r}, {!r}, {!r}, {!r}, {!r})'.format(self.db,
                                                                  self.cluster,
                                                                  self.schema,
                                                                  self.table,
                                                                  self.source)
