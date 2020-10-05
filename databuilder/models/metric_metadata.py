from collections import namedtuple

from typing import Iterable, Any, Union, Iterator, Dict, Set  # noqa: F401

# TODO: We could separate TagMetadata from table_metadata to own module
from databuilder.models.table_metadata import TagMetadata
from databuilder.models.graph_serializable import (
    GraphSerializable, NODE_LABEL, NODE_KEY, RELATION_START_KEY, RELATION_END_KEY, RELATION_START_LABEL,
    RELATION_END_LABEL, RELATION_TYPE, RELATION_REVERSE_TYPE)

from databuilder.models.graph_node import GraphNode
from databuilder.models.graph_relationship import GraphRelationship


NodeTuple = namedtuple('KeyName', ['key', 'name', 'label'])
RelTuple = namedtuple('RelKeys', ['start_label', 'end_label', 'start_key', 'end_key', 'type', 'reverse_type'])


class MetricMetadata(GraphSerializable):
    """
    Table metadata that contains columns. It implements Neo4jCsvSerializable so that it can be serialized to produce
    Table, Column and relation of those along with relationship with table and schema. Additionally, it will create
    Database, Cluster, and Schema with relastionships between those.
    These are being created here as it does not make much sense to have different extraction to produce this. As
    database, cluster, schema would be very repititive with low cardinality, it will perform de-dupe so that publisher
    won't need to publish same nodes, relationships.

    This class can be used for both table and view metadata. If it is a View, is_view=True should be passed in.
    """

    DESCRIPTION_NODE_LABEL = 'Description'

    METRIC_NODE_LABEL = 'Metric'
    METRIC_KEY_FORMAT = 'metric://{name}'
    METRIC_NAME = 'name'

    METRIC_DESCRIPTION = 'description'
    METRIC_DESCRIPTION_FORMAT = 'metric://{name}/_description'
    METRIC_DESCRIPTION_RELATION_TYPE = 'DESCRIPTION'
    DESCRIPTION_METRIC_RELATION_TYPE = 'DESCRIPTION_OF'

    DASHBOARD_NODE_LABEL = 'Dashboard'
    DASHBOARD_KEY_FORMAT = '{dashboard_group}://{dashboard_name}'
    DASHBOARD_NAME = 'name'
    DASHBOARD_METRIC_RELATION_TYPE = 'METRIC'
    METRIC_DASHBOARD_RELATION_TYPE = 'METRIC_OF'

    METRIC_TYPE_NODE_LABEL = 'Metrictype'
    METRIC_TYPE_KEY_FORMAT = 'type://{type}'
    METRIC_METRIC_TYPE_RELATION_TYPE = 'METRIC_TYPE'
    METRIC_TYPE_METRIC_RELATION_TYPE = 'METRIC_TYPE_OF'

    # TODO: Idea, maybe move expression from attribute
    # to node or delete commented code below?
    # METRIC_EXPRESSION_NODE_LABEL = 'Metricexpression'
    # METRIC_EXPRESSION_KEY_FORMAT = 'expression://{name}'
    # METRIC_METRIC_EXPRESSION_RELATION_TYPE = 'DEFINITION'
    # METRIC_EXPRESSION_METRIC_RELATION_TYPE = 'DEFINITION_OF'
    METRIC_EXPRESSION_VALUE = 'expression'

    METRIC_TAG_RELATION_TYPE = 'TAG'
    TAG_METRIC_RELATION_TYPE = 'TAG_OF'

    serialized_nodes = set()  # type: Set[GraphNode]
    serialized_rels = set()  # type: Set[GraphRelationship]

    def __init__(self,
                 dashboard_group,  # type: str
                 dashboard_name,  # type: str
                 name,  # type: Union[str, None]
                 expression,   # type: str
                 description,   # type: str
                 type,   # type: str
                 tags,   # type: List
                 ):
        # type: (...) -> None

        self.dashboard_group = dashboard_group
        self.dashboard_name = dashboard_name
        self.name = name
        self.expression = expression
        self.description = description
        self.type = type
        self.tags = tags
        self._node_iterator = self._create_next_node()
        self._relation_iterator = self._create_next_relation()

    def __repr__(self):
        # type: () -> str
        return 'MetricMetadata({!r}, {!r}, {!r}, {!r}, {!r}, {!r}, {!r}'.format(
            self.dashboard_group,
            self.dashboard_name,
            self.name,
            self.expression,
            self.description,
            self.type,
            self.tags
        )

    def _get_metric_key(self):
        # type: () -> str
        return MetricMetadata.METRIC_KEY_FORMAT.format(name=self.name)

    def _get_metric_type_key(self):
        # type: () -> str
        return MetricMetadata.METRIC_TYPE_KEY_FORMAT.format(type=self.type)

    def _get_dashboard_key(self):
        # type: () -> str
        return MetricMetadata.DASHBOARD_KEY_FORMAT.format(dashboard_group=self.dashboard_group,
                                                          dashboard_name=self.dashboard_name)

    def _get_metric_description_key(self):
        # type: () -> str
        return MetricMetadata.METRIC_DESCRIPTION_FORMAT.format(name=self.name)

    def _get_metric_expression_key(self):
        # type: () -> str
        return MetricMetadata.METRIC_EXPRESSION_KEY_FORMAT.format(name=self.name)

    def create_next_node(self):
        # type: () -> Union[GraphNode, None]
        try:
            return next(self._node_iterator)
        except StopIteration:
            return None

    def _create_next_node(self):
        # type: () -> Iterator[GraphNode]

        # Metric node
        metric_node = GraphNode(
            id=self._get_metric_key(),
            label=MetricMetadata.METRIC_NODE_LABEL,
            node_attributes={
                MetricMetadata.METRIC_NAME: self.name,
                MetricMetadata.METRIC_EXPRESSION_VALUE: self.expression
            }
        )
        yield metric_node

        # Description node
        if self.description:
            description_node = GraphNode(
                id=self._get_metric_description_key(),
                label=MetricMetadata.DESCRIPTION_NODE_LABEL,
                node_attributes={
                    MetricMetadata.METRIC_DESCRIPTION: self.description
                }
            )
            yield description_node

        # Metric tag node
        if self.tags:
            for tag in self.tags:
                tag_node = GraphNode(
                    id=TagMetadata.get_tag_key(tag),
                    label=TagMetadata.TAG_NODE_LABEL,
                    node_attributes={
                        TagMetadata.TAG_TYPE: 'metric'
                    }
                )
                yield tag_node

        # Metric type node
        if self.type:
            type_node = GraphNode(
                id=self._get_metric_type_key(),
                label=MetricMetadata.METRIC_TYPE_NODE_LABEL,
                node_attributes={
                    'name': self.type
                }
            )
            yield type_node

        others = []

        for node_tuple in others:
            if node_tuple not in MetricMetadata.serialized_nodes:
                MetricMetadata.serialized_nodes.add(node_tuple)
                yield node_tuple

    def create_next_relation(self):
        # type: () -> Union[GraphRelationship, None]
        try:
            return next(self._relation_iterator)
        except StopIteration:
            return None

    def _create_next_relation(self):
        # type: () -> Iterator[GraphRelationship]

        # Dashboard > Metric relation
        dashboard_metric_relation = GraphRelationship(
            start_label=MetricMetadata.METRIC_NODE_LABEL,
            start_key=self._get_metric_key(),
            end_label=MetricMetadata.DASHBOARD_NODE_LABEL,
            end_key=self._get_dashboard_key(),
            type=MetricMetadata.METRIC_DASHBOARD_RELATION_TYPE,
            reverse_type=MetricMetadata.DASHBOARD_METRIC_RELATION_TYPE
        )
        yield dashboard_metric_relation

        # Metric > Metric description relation
        if self.description:
            metric_description_relation = GraphRelationship(
                start_label=MetricMetadata.METRIC_NODE_LABEL,
                start_key=self._get_metric_key(),
                end_label=MetricMetadata.DESCRIPTION_NODE_LABEL,
                end_key=self._get_metric_description_key(),
                type=MetricMetadata.METRIC_DESCRIPTION_RELATION_TYPE,
                reverse_type=MetricMetadata.DESCRIPTION_METRIC_RELATION_TYPE
            )
            yield metric_description_relation

        # Metric > Metric tag relation
        if self.tags:
            for tag in self.tags:
                tag_relation = GraphRelationship(
                    start_label=MetricMetadata.METRIC_NODE_LABEL,
                    start_key=self._get_metric_key(),
                    end_label=TagMetadata.TAG_NODE_LABEL,
                    end_key=TagMetadata.get_tag_key(tag),
                    type=MetricMetadata.METRIC_TAG_RELATION_TYPE,
                    reverse_type=MetricMetadata.TAG_METRIC_RELATION_TYPE
                )
                yield tag_relation

        # Metric > Metric type relation
        if self.type:
            type_relation = GraphRelationship(
                start_label=MetricMetadata.METRIC_NODE_LABEL,
                start_key=self._get_metric_key(),
                end_label=MetricMetadata.METRIC_TYPE_NODE_LABEL,
                end_key=self._get_metric_type_key(),
                type=MetricMetadata.METRIC_METRIC_TYPE_RELATION_TYPE,
                reverse_type=MetricMetadata.METRIC_TYPE_METRIC_RELATION_TYPE
            )
            yield type_relation

        others = []

        for rel_tuple in others:
            if rel_tuple not in MetricMetadata.serialized_rels:
                MetricMetadata.serialized_rels.add(rel_tuple)
                yield rel_tuple
