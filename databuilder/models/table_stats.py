from typing import Any, Dict, List, Union  # noqa: F401

from databuilder.models.graph_serializable import GraphSerializable
from databuilder.models.table_metadata import ColumnMetadata
from databuilder.models.graph_node import GraphNode
from databuilder.models.graph_relationship import GraphRelationship


class TableColumnStats(GraphSerializable):
    # type: (...) -> None
    """
    Hive table stats model.
    Each instance represents one row of hive watermark result.
    """
    LABEL = 'Stat'
    KEY_FORMAT = '{db}://{cluster}.{schema}' \
                 '/{table}/{col}/{stat_name}/'
    STAT_Column_RELATION_TYPE = 'STAT_OF'
    Column_STAT_RELATION_TYPE = 'STAT'

    def __init__(self,
                 table_name,  # type: str
                 col_name,  # type: str
                 stat_name,  # type: str
                 stat_val,  # type: str
                 start_epoch,  # type: str
                 end_epoch,  # type: str
                 db='hive',  # type: str
                 cluster='gold',  # type: str
                 schema=None  # type: str
                 ):
        # type: (...) -> None
        if schema is None:
            self.schema, self.table = table_name.split('.')
        else:
            self.table = table_name.lower()
            self.schema = schema.lower()
        self.db = db
        self.col_name = col_name.lower()
        self.start_epoch = start_epoch
        self.end_epoch = end_epoch
        self.cluster = cluster
        self.stat_name = stat_name
        self.stat_val = stat_val
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

    def get_table_stat_model_key(self):
        # type: (...) -> str
        return TableColumnStats.KEY_FORMAT.format(db=self.db,
                                                  cluster=self.cluster,
                                                  schema=self.schema,
                                                  table=self.table,
                                                  col=self.col_name,
                                                  stat_name=self.stat_name)

    def get_col_key(self):
        # type: (...) -> str
        # no cluster, schema info from the input
        return ColumnMetadata.COLUMN_KEY_FORMAT.format(db=self.db,
                                                       cluster=self.cluster,
                                                       schema=self.schema,
                                                       tbl=self.table,
                                                       col=self.col_name)

    def create_nodes(self):
        # type: () -> List[GraphNode]
        """
        Create a list of Neo4j node records
        :return:
        """
        node = GraphNode(
            key=self.get_table_stat_model_key(),
            label=TableColumnStats.LABEL,
            attributes={
                'stat_val:UNQUOTED': self.stat_val,
                'stat_name': self.stat_name,
                'start_epoch': self.start_epoch,
                'end_epoch': self.end_epoch,
            }
        )
        results = [node]
        return results

    def create_relation(self):
        # type: () -> List[GraphRelationship]
        """
        Create a list of relation map between table stat record with original hive table
        :return:
        """
        relationship = GraphRelationship(
            start_key=self.get_table_stat_model_key(),
            start_label=TableColumnStats.LABEL,
            end_key=self.get_col_key(),
            end_label=ColumnMetadata.COLUMN_NODE_LABEL,
            type=TableColumnStats.STAT_Column_RELATION_TYPE,
            reverse_type=TableColumnStats.Column_STAT_RELATION_TYPE,
            attributes={}
        )
        results = [relationship]
        return results
