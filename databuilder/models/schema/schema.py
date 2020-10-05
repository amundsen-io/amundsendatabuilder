from typing import Dict, Any, Union, Iterator  # noqa: F401

from databuilder.models.graph_serializable import (GraphSerializable)
from databuilder.models.schema.schema_constant import SCHEMA_NODE_LABEL, SCHEMA_NAME_ATTR
from databuilder.models.table_metadata import DescriptionMetadata
from databuilder.models.graph_node import GraphNode
from databuilder.models.graph_relationship import GraphRelationship


class SchemaModel(GraphSerializable):

    def __init__(self,
                 schema_key,
                 schema,
                 description=None,
                 description_source=None,
                 **kwargs):
        self._schema_key = schema_key
        self._schema = schema
        self._description = DescriptionMetadata.create_description_metadata(text=description,
                                                                            source=description_source) \
            if description else None
        self._node_iterator = self._create_node_iterator()
        self._relation_iterator = self._create_relation_iterator()

    def create_next_node(self):
        # type: () -> Union[GraphNode, None]
        try:
            return next(self._node_iterator)
        except StopIteration:
            return None

    def _create_node_iterator(self):
        # type: () -> Iterator[GraphNode]
        node = GraphNode(
            id=self._schema_key,
            label=SCHEMA_NODE_LABEL,
            node_attributes={
                SCHEMA_NAME_ATTR: self._schema,
            }
        )
        yield node

        if self._description:
            yield self._description.get_node(self._get_description_node_key())

    def create_next_relation(self):
        # type: () -> Union[Dict[str, Any], None]
        try:
            return next(self._relation_iterator)
        except StopIteration:
            return None

    def _get_description_node_key(self):
        return '{}/{}'.format(self._schema_key, self._description.get_description_id())

    def _create_relation_iterator(self):
        # type: () -> Iterator[GraphRelationship]
        if self._description:
            yield self._description.get_relation(start_node=SCHEMA_NODE_LABEL,
                                                 start_key=self._schema_key,
                                                 end_key=self._get_description_node_key())
