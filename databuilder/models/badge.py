# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

from typing import Any, Dict, List, Optional, Union

from databuilder.models.neo4j_csv_serde import Neo4jCsvSerializable, NODE_KEY, \
    NODE_LABEL, RELATION_START_KEY, RELATION_START_LABEL, RELATION_END_KEY, \
    RELATION_END_LABEL, RELATION_TYPE, RELATION_REVERSE_TYPE


class Badge:
    def __init__(self, name, category):
        self.name = name
        self.category = category


class BadgeMetadata(Neo4jCsvSerializable):
    """
    Badge model.
    """
    BADGE_NODE_LABEL = 'Badge'
    BADGE_KEY_FORMAT = '{badge}'
    BADGE_CATEGORY = 'category'

    # Relation between entity and badge
    BADGE_RELATION_TYPE = 'HAS_BADGE'
    INVERSE_BADGE_RELATION_TYPE = 'BADGE_FOR'

    def __init__(self,
                 db_name: str,
                 schema: str,
                 table_name: str,
                 badges: Union[List, Badge],
                 cluster: str = 'gold',  # is this what we want as default for badges..?
                 ):
        # self._name = name
        # self._category = category
        self.badges = badges

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

    @staticmethod
    def get_badge_key(name: str) -> str:
        if not name:
            return ''
        return BadgeMetadata.BADGE_KEY_FORMAT.format(badge=name)

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
        for badge in self.badges:
            if badge:
                results.append({
                    NODE_KEY: self.get_badge_key(badge.name),
                    NODE_LABEL: self.BADGE_NODE_LABEL,
                    self.BADGE_CATEGORY: badge.category
                })
        return results

    def create_relation(self,
                        start_node: str,
                        start_key: str,
                        ) -> Dict[str, str]:
        results = []
        for badge in self.badges:
            results.append({
                RELATION_START_LABEL: start_node,
                RELATION_END_LABEL: self.BADGE_NODE_LABEL,
                RELATION_START_KEY: start_key,
                RELATION_END_KEY: self.get_badge_key(badge.name),
                RELATION_TYPE: self.BADGE_RELATION_TYPE,
                RELATION_REVERSE_TYPE: self.INVERSE_BADGE_RELATION_TYPE,
            })
        return results

    def __repr__(self) -> str:
        return 'BadgeMetadata({!r}, {!r})'.format(self.name,
                                                  self.category)
