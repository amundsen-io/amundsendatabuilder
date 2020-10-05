from typing import Union, Dict, Any, Iterator  # noqa: F401

from databuilder.models.graph_serializable import GraphSerializable
from databuilder.models.usage.usage_constants import (
    READ_RELATION_TYPE, READ_REVERSE_RELATION_TYPE, READ_RELATION_COUNT_PROPERTY
)
from databuilder.models.table_metadata import TableMetadata
from databuilder.models.user import User
from databuilder.models.graph_node import GraphNode
from databuilder.models.graph_relationship import GraphRelationship


class ColumnUsageModel(GraphSerializable):

    """
    A model represents user <--> column graph model
    Currently it only support to serialize to table level
    """
    TABLE_NODE_LABEL = TableMetadata.TABLE_NODE_LABEL
    TABLE_NODE_KEY_FORMAT = TableMetadata.TABLE_KEY_FORMAT

    USER_TABLE_RELATION_TYPE = READ_RELATION_TYPE
    TABLE_USER_RELATION_TYPE = READ_REVERSE_RELATION_TYPE

    # Property key for relationship read, readby relationship
    READ_RELATION_COUNT = READ_RELATION_COUNT_PROPERTY

    def __init__(self,
                 database,     # type: str
                 cluster,      # type: str
                 schema,  # type: str
                 table_name,   # type: str
                 column_name,  # type: str
                 user_email,   # type: str
                 read_count,   # type: int
                 ):
        # type: (...) -> None
        self.database = database
        self.cluster = cluster
        self.schema = schema
        self.table_name = table_name
        self.column_name = column_name
        self.user_email = user_email
        self.read_count = read_count

        self._node_iter = iter(self.create_nodes())
        self._relation_iter = iter(self.create_relation())

    def create_next_node(self):
        # type: () -> Union[GraphNode, None]

        try:
            return next(self._node_iter)
        except StopIteration:
            return None

    def create_nodes(self):
        # type: () -> List[GraphNode]
        """
        Create a list of Neo4j node records
        :return:
        """

        return User(email=self.user_email).create_nodes()

    def create_next_relation(self):
        # type: () -> Union[GraphRelationship, None]

        try:
            return next(self._relation_iter)
        except StopIteration:
            return None

    def create_relation(self):
        # type: () -> Iterator[GraphRelationship]
        relationship = GraphRelationship(
            start_key=self._get_table_key(),
            start_label=TableMetadata.TABLE_NODE_LABEL,
            end_key=self._get_user_key(self.user_email),
            end_label=User.USER_NODE_LABEL,
            type=ColumnUsageModel.TABLE_USER_RELATION_TYPE,
            reverse_type=ColumnUsageModel.USER_TABLE_RELATION_TYPE,
            relationship_attributes={
                ColumnUsageModel.READ_RELATION_COUNT: self.read_count
            }
        )
        return [relationship]

    def _get_table_key(self):
        # type: (ColumnReader) -> str
        return TableMetadata.TABLE_KEY_FORMAT.format(db=self.database,
                                                     cluster=self.cluster,
                                                     schema=self.schema,
                                                     tbl=self.table_name)

    def _get_user_key(self, email):
        # type: (str) -> str
        return User.get_user_model_key(email=email)

    def __repr__(self):
        # type: () -> str
        return 'TableColumnUsage({!r}, {!r}, {!r}, {!r}, {!r}, {!r}, {!r})'.format(self.database,
                                                                                   self.cluster,
                                                                                   self.schema,
                                                                                   self.table_name,
                                                                                   self.column_name,
                                                                                   self.user_email,
                                                                                   self.read_count)
