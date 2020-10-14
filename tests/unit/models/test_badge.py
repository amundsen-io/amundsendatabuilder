# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import unittest
from databuilder.models.badge import Badge, BadgeMetadata

from databuilder.models.neo4j_csv_serde import NODE_KEY, NODE_LABEL, \
    RELATION_START_KEY, RELATION_START_LABEL, RELATION_END_KEY, \
    RELATION_END_LABEL, RELATION_TYPE, RELATION_REVERSE_TYPE

db = 'hive'
SCHEMA = 'BASE'
TABLE = 'TEST'
CLUSTER = 'DEFAULT'
badge1 = Badge('badge1', 'column')
badge2 = Badge('badge2', 'column')


class TestBadge(unittest.TestCase):
    def setUp(self) -> None:
        super(TestBadge, self).setUp()
        self.badge_metada = BadgeMetadata(db_name='hive',
                                          schema=SCHEMA,
                                          table_name=TABLE,
                                          cluster=CLUSTER,
                                          badges=[badge1, badge2])

    def test_get_badge_key(self) -> None:
        badge_key = self.badge_metada.get_badge_key(badge1.name)
        self.assertEquals(badge_key, badge1.name)

    def test_get_metadata_model_key(self) -> None:
        metadata = self.badge_metada.get_metadata_model_key()
        self.assertEquals(metadata, 'hive://default.base/test')

    def test_create_nodes(self) -> None:
        nodes = self.badge_metada.create_nodes()
        self.assertEquals(len(nodes), 2)

        node1 = {
            NODE_KEY: BadgeMetadata.BADGE_KEY_FORMAT.format(badge1.name),
            NODE_LABEL: BadgeMetadata.BADGE_NODE_LABEL,
            BadgeMetadata.BADGE_CATEGORY: badge1.category
        }
        node2 = {
            NODE_KEY: BadgeMetadata.BADGE_KEY_FORMAT.format(badge2.name),
            NODE_LABEL: BadgeMetadata.BADGE_NODE_LABEL,
            BadgeMetadata.BADGE_CATEGORY: badge2.category
        }

        self.assertTrue(node1 in nodes)
        self.assertTrue(node2 in nodes)

    def test_create_relation(self) -> None:
        relations = self.badge_metada.create_relation()
        self.assertEquals(len(relations), 2)

        start_node = 'Column'
        start_key = self.badge_metada.get_metadata_model_key() + '/ds'

        relation1 = {
            RELATION_START_LABEL: start_node,
            RELATION_END_LABEL: self.BADGE_NODE_LABEL,
            RELATION_START_KEY: start_key,
            RELATION_END_KEY: self.get_badge_key(badge1.name),
            RELATION_TYPE: self.BADGE_RELATION_TYPE,
            RELATION_REVERSE_TYPE: self.INVERSE_BADGE_RELATION_TYPE,
        }
        relation2 = {
            RELATION_START_LABEL: start_node,
            RELATION_END_LABEL: self.BADGE_NODE_LABEL,
            RELATION_START_KEY: start_key,
            RELATION_END_KEY: self.get_badge_key(badge2.name),
            RELATION_TYPE: self.BADGE_RELATION_TYPE,
            RELATION_REVERSE_TYPE: self.INVERSE_BADGE_RELATION_TYPE,
        }

        self.assertTrue(relation1 in relations)
        self.assertTrue(relation2 in relations)
