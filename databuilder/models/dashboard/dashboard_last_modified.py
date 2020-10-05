import logging

from typing import Optional, Dict, Any, Union, Iterator  # noqa: F401

from databuilder.models.dashboard.dashboard_metadata import DashboardMetadata
from databuilder.models.graph_serializable import (
    GraphSerializable)
from databuilder.models.timestamp import timestamp_constants

from databuilder.models.graph_node import GraphNode
from databuilder.models.graph_relationship import GraphRelationship

LOGGER = logging.getLogger(__name__)


class DashboardLastModifiedTimestamp(GraphSerializable):
    """
    A model that encapsulate Dashboard's last modified timestamp in epoch
    """

    DASHBOARD_LAST_MODIFIED_KEY_FORMAT = '{product}_dashboard://{cluster}.{dashboard_group_id}/' \
                                         '{dashboard_id}/_last_modified_timestamp'

    def __init__(self,
                 dashboard_group_id,  # type: Optional[str]
                 dashboard_id,  # type: Optional[str]
                 last_modified_timestamp,  # type: int
                 product='',  # type: Optional[str]
                 cluster='gold',  # type: str
                 **kwargs
                 ):
        self._dashboard_group_id = dashboard_group_id
        self._dashboard_id = dashboard_id
        self._last_modified_timestamp = last_modified_timestamp
        self._product = product
        self._cluster = cluster
        self._node_iterator = self._create_node_iterator()
        self._relation_iterator = self._create_relation_iterator()

    def create_next_node(self):
        # type: () -> Union[GraphNode, None]
        try:
            return next(self._node_iterator)
        except StopIteration:
            return None

    def _create_node_iterator(self):  # noqa: C901
        # type: () -> Iterator[GraphNode]
        node = GraphNode(
            id=self._get_last_modified_node_key(),
            label=timestamp_constants.NODE_LABEL,
            node_attributes={
                timestamp_constants.TIMESTAMP_PROPERTY: self._last_modified_timestamp,
                timestamp_constants.TIMESTAMP_NAME_PROPERTY: timestamp_constants.TimestampName.last_updated_timestamp.name,
            }
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
            start_key=DashboardMetadata.DASHBOARD_KEY_FORMAT.format(
                product=self._product,
                cluster=self._cluster,
                dashboard_group=self._dashboard_group_id,
                dashboard_name=self._dashboard_id
            ),
            start_label=DashboardMetadata.DASHBOARD_NODE_LABEL,
            end_key=self._get_last_modified_node_key(),
            end_label=timestamp_constants.NODE_LABEL,
            type=timestamp_constants.LASTUPDATED_RELATION_TYPE,
            reverse_type=timestamp_constants.LASTUPDATED_REVERSE_RELATION_TYPE
        )
        yield relationship

    def _get_last_modified_node_key(self):
        return DashboardLastModifiedTimestamp.DASHBOARD_LAST_MODIFIED_KEY_FORMAT.format(
            product=self._product,
            cluster=self._cluster,
            dashboard_group_id=self._dashboard_group_id,
            dashboard_id=self._dashboard_id,
        )

    def __repr__(self):
        return 'DashboardLastModifiedTimestamp({!r}, {!r}, {!r}, {!r}, {!r})'.format(
            self._dashboard_group_id,
            self._dashboard_id,
            self._last_modified_timestamp,
            self._product,
            self._cluster
        )
