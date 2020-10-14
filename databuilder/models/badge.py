# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

from typing import Any, Dict, List, Optional, Union

from databuilder.models.neo4j_csv_serde import Neo4jCsvSerializable, NODE_KEY, \
    NODE_LABEL, RELATION_START_KEY, RELATION_START_LABEL, RELATION_END_KEY, \
    RELATION_END_LABEL, RELATION_TYPE, RELATION_REVERSE_TYPE
from databuilder.models.owner_constants import OWNER_RELATION_TYPE, OWNER_OF_OBJECT_RELATION_TYPE
from databuilder.models.user import User


class BadgeMetadata(Neo4jCsvSerializable):
    """
    Badge model.
    """
    # Relation between entity and badge
    BADGE_RELATION_TYPE = 'HAS_BADGE'
    INVERSE_BADGE_RELATION_TYPE = 'BADGE_FOR'

    def __init__(self,
                 db_name: str,
                 schema: str,
                 table_name: str,
                 name: str,
                 category: str,
                 cluster: str = 'gold',  # is this what we want as default for badges..?
                 ):
        self._name = name
        self._category = category

        self.db = db_name.lower()
        self.schema = schema.lower()
        self.table = table_name.lower()
        self.cluster = cluster.lower()

        self._node_iter = iter(self.create_nodes())
        self._relation_iter = iter(self.create_relation())

    def create_next_node(self) -> Optional[Dict[str, Any]]:
        # return the string representation of the data
        try:
            return next(self._node_iter)
        except StopIteration:
            return None

    def create_next_relation(self) -> Optional[Dict[str, Any]]:
        try:
            return next(self._relation_iter)
        except StopIteration:
            return None

    def get_owner_model_key(self, owner: str) -> str:
        return User.USER_NODE_KEY_FORMAT.format(email=owner)

    def get_metadata_model_key(self) -> str:
        return '{db}://{cluster}.{schema}/{table}'.format(db=self.db,
                                                          cluster=self.cluster,
                                                          schema=self.schema,
                                                          table=self.table)

    def create_nodes(self) -> List[Dict[str, Any]]:
        """
        Create a list of Neo4j node records
        :return:
        """
        results = []
        for owner in self.owners:
            if owner:
                results.append({
                    NODE_KEY: self.get_owner_model_key(owner),
                    NODE_LABEL: User.USER_NODE_LABEL,
                    User.USER_NODE_EMAIL: owner
                })
        return results

    def create_relation(self) -> List[Dict[str, Any]]:
        """
        Create a list of relation map between owner record with original hive table
        :return:
        """
        results = []
        for owner in self.owners:
            results.append({
                RELATION_START_KEY: self.get_owner_model_key(owner),
                RELATION_START_LABEL: User.USER_NODE_LABEL,
                RELATION_END_KEY: self.get_metadata_model_key(),
                RELATION_END_LABEL: 'Table',
                RELATION_TYPE: TableOwner.OWNER_TABLE_RELATION_TYPE,
                RELATION_REVERSE_TYPE: TableOwner.TABLE_OWNER_RELATION_TYPE
            })

        return results

    def __repr__(self) -> str:
        return 'TableOwner({!r}, {!r}, {!r}, {!r}, {!r})'.format(self.db,
                                                                 self.cluster,
                                                                 self.schema,
                                                                 self.table,
                                                                 self.owners)
