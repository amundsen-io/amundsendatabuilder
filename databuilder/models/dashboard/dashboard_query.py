import logging

from typing import Optional, Dict, Any, Union, Iterator  # noqa: F401

from databuilder.models.dashboard.dashboard_metadata import DashboardMetadata
from databuilder.models.graph_serializable import (
    GraphSerializable)

from databuilder.models.graph_node import GraphNode
from databuilder.models.graph_relationship import GraphRelationship

LOGGER = logging.getLogger(__name__)


class DashboardQuery(GraphSerializable):
    """
    A model that encapsulate Dashboard's query name
    """
    DASHBOARD_QUERY_LABEL = 'Query'
    DASHBOARD_QUERY_KEY_FORMAT = '{product}_dashboard://{cluster}.{dashboard_group_id}/' \
                                 '{dashboard_id}/query/{query_id}'
    DASHBOARD_QUERY_RELATION_TYPE = 'HAS_QUERY'
    QUERY_DASHBOARD_RELATION_TYPE = 'QUERY_OF'

    def __init__(self,
                 dashboard_group_id,  # type: Optional[str]
                 dashboard_id,  # type: Optional[str]
                 query_name,  # type: str
                 query_id=None,  # type: Optional[str]
                 url='',  # type: Optional[str]
                 query_text=None,  # type: Optional[str]
                 product='',  # type: Optional[str]
                 cluster='gold',  # type: str
                 **kwargs
                 ):
        self._dashboard_group_id = dashboard_group_id
        self._dashboard_id = dashboard_id
        self._query_name = query_name
        self._query_id = query_id if query_id else query_name
        self._url = url
        self._query_text = query_text
        self._product = product
        self._cluster = cluster
        self._node_iterator = self._create_node_iterator()
        self._relation_iterator = self._create_relation_iterator()

    def create_next_node(self):
        # type: () -> Union[Dict[str, Any], None]
        try:
            return next(self._node_iterator)
        except StopIteration:
            return None

    def _create_node_iterator(self):  # noqa: C901
        # type: () -> Iterator[GraphNode]
        node_attributes = {
            'id': self._query_id,
            'name': self._query_name,
        }

        if self._url:
            node_attributes['url'] = self._url

        if self._query_text:
            node_attributes['query_text'] = self._query_tex

        node = GraphNode(
            key=self._get_query_node_key(),
            label=DashboardQuery.DASHBOARD_QUERY_LABEL,
            attributes=node_attributes
        )

        yield node

    def create_next_relation(self):
        # type: () -> Union[GraphRelationship, None]
        try:
            return next(self._relation_iterator)
        except StopIteration:
            return None

    def _create_relation_iterator(self):
        # type: () -> Iterator[GraphRelationship]
        relationship = GraphRelationship(
            start_label=DashboardMetadata.DASHBOARD_NODE_LABEL,
            end_label=DashboardQuery.DASHBOARD_QUERY_LABEL,
            start_key=DashboardMetadata.DASHBOARD_KEY_FORMAT.format(
                product=self._product,
                cluster=self._cluster,
                dashboard_group=self._dashboard_group_id,
                dashboard_name=self._dashboard_id
            ),
            end_key=self._get_query_node_key(),
            type=DashboardQuery.DASHBOARD_QUERY_RELATION_TYPE,
            reverse_type=DashboardQuery.QUERY_DASHBOARD_RELATION_TYPE,
            attributes={}
        )
        yield relationship

    def _get_query_node_key(self):
        return DashboardQuery.DASHBOARD_QUERY_KEY_FORMAT.format(
            product=self._product,
            cluster=self._cluster,
            dashboard_group_id=self._dashboard_group_id,
            dashboard_id=self._dashboard_id,
            query_id=self._query_id
        )

    def __repr__(self):
        return 'DashboardQuery({!r}, {!r}, {!r}, {!r}, {!r}, {!r}, {!r}, {!r})'.format(
            self._dashboard_group_id,
            self._dashboard_id,
            self._query_name,
            self._query_id,
            self._url,
            self._query_text,
            self._product,
            self._cluster
        )
