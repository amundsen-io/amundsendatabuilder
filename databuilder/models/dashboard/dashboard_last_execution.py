import logging

from typing import Optional, Dict, Any, Union, Iterator  # noqa: F401

from databuilder.models.dashboard_metadata import DashboardMetadata
from databuilder.models.neo4j_csv_serde import (
    Neo4jCsvSerializable, NODE_LABEL, NODE_KEY, RELATION_START_KEY, RELATION_END_KEY, RELATION_START_LABEL,
    RELATION_END_LABEL, RELATION_TYPE, RELATION_REVERSE_TYPE)

LOGGER = logging.getLogger(__name__)


class DashboardLastExecution(Neo4jCsvSerializable):
    """
    A model that encapsulate Dashboard's execution timestamp in epoch and execution state
    """
    DASHBOARD_EXECUTION_LABEL = 'Execution'
    DASHBOARD_KEY_FORMAT = '{product}_dashboard://{cluster}.{dashboard_group_id}/{dashboard_id}/_last_execution'
    DASHBOARD_LAST_EXECUTION_RELATION_TYPE = 'LAST_EXECUTED'
    LAST_EXECUTION_DASHBOARD_RELATION_TYPE = 'LAST_EXECUTION_OF'

    def __init__(self,
                 dashboard_group_id,  # type: Optional[str]
                 dashboard_id,  # type: Optional[str]
                 execution_timestamp,  # type: int
                 execution_state,  # type: str
                 product='',  # type: Optional[str]
                 cluster='gold',  # type: str
                 **kwargs
                 ):
        self._dashboard_group_id = dashboard_group_id
        self._dashboard_id = dashboard_id
        self._execution_timestamp = execution_timestamp
        self._execution_state = execution_state
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
        # type: () -> Iterator[[Dict[str, Any]]]
        yield {
            NODE_LABEL: DashboardLastExecution.DASHBOARD_EXECUTION_LABEL,
            NODE_KEY: self._get_last_execution_node_key(),
            'time_stamp': self._execution_timestamp,
            'state': self._execution_state
        }

    def create_next_relation(self):
        # type: () -> Union[Dict[str, Any], None]
        try:
            return next(self._relation_iterator)
        except StopIteration:
            return None

    def _create_relation_iterator(self):
        # type: () -> Iterator[[Dict[str, Any]]]
        yield {
            RELATION_START_LABEL: DashboardMetadata.DASHBOARD_NODE_LABEL,
            RELATION_END_LABEL: DashboardLastExecution.DASHBOARD_EXECUTION_LABEL,
            # DASHBOARD_KEY_FORMAT = '{product}_dashboard://{cluster}.{dashboard_group}/{dashboard_name}'
            RELATION_START_KEY: DashboardMetadata.DASHBOARD_KEY_FORMAT.format(
                product=self._product,
                cluster=self._cluster,
                dashboard_group=self._dashboard_group_id,
                dashboard_name=self._dashboard_id
            ),
            RELATION_END_KEY: self._get_last_execution_node_key(),
            RELATION_TYPE: DashboardLastExecution.DASHBOARD_LAST_EXECUTION_RELATION_TYPE,
            RELATION_REVERSE_TYPE: DashboardLastExecution.LAST_EXECUTION_DASHBOARD_RELATION_TYPE
        }

    def _get_last_execution_node_key(self):
        return DashboardLastExecution.DASHBOARD_KEY_FORMAT.format(
            product=self._product,
            cluster=self._cluster,
            dashboard_group_id=self._dashboard_group_id,
            dashboard_id=self._dashboard_id
        )

    def __repr__(self):
        return 'DashboardLastExecution({!r}, {!r}, {!r}, {!r}, {!r}, {!r})'.format(
            self._dashboard_group_id,
            self._dashboard_id,
            self._execution_timestamp,
            self._execution_state,
            self._product,
            self._cluster
        )
