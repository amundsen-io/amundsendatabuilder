from typing import Any, Dict, List, Union  # noqa: F401

from databuilder.models.graph_serializable import GraphSerializable
from databuilder.models.graph_node import GraphNode
from databuilder.models.graph_relationship import GraphRelationship


class Watermark(GraphSerializable):
    # type: (...) -> None
    """
    Table watermark result model.
    Each instance represents one row of table watermark result.
    """
    LABEL = 'Watermark'
    KEY_FORMAT = '{database}://{cluster}.{schema}' \
                 '/{table}/{part_type}/'
    WATERMARK_TABLE_RELATION_TYPE = 'BELONG_TO_TABLE'
    TABLE_WATERMARK_RELATION_TYPE = 'WATERMARK'

    def __init__(self,
                 create_time,  # type: str
                 database,  # type: str
                 schema,  # type: str
                 table_name,  # type: str
                 part_name,  # type: str
                 part_type='high_watermark',  # type: str
                 cluster='gold',  # type: str
                 ):
        # type: (...) -> None
        self.create_time = create_time
        self.database = database.lower()
        self.schema = schema.lower()
        self.table = table_name.lower()
        self.parts = []  # type: list

        if '=' not in part_name:
            raise Exception('Only partition table has high watermark')

        # currently we don't consider nested partitions
        idx = part_name.find('=')
        name, value = part_name.lower()[:idx], part_name.lower()[idx + 1:]
        self.parts = [(name, value)]
        self.part_type = part_type.lower()
        self.cluster = cluster.lower()
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

    def get_watermark_model_key(self):
        # type: (...) -> str
        return Watermark.KEY_FORMAT.format(database=self.database,
                                           cluster=self.cluster,
                                           schema=self.schema,
                                           table=self.table,
                                           part_type=self.part_type)

    def get_metadata_model_key(self):
        # type: (...) -> str
        return '{database}://{cluster}.{schema}/{table}'.format(database=self.database,
                                                                cluster=self.cluster,
                                                                schema=self.schema,
                                                                table=self.table)

    def create_nodes(self):
        # type: () -> List[GraphNode]
        """
        Create a list of Neo4j node records
        :return:
        """
        results = []
        for part in self.parts:
            part_node = GraphNode(
                key=self.get_watermark_model_key(),
                label=Watermark.LABEL,
                attributes={
                    'partition_key': part[0],
                    'partition_value': part[1],
                    'create_time': self.create_time
                }
            )
            results.append(part_node)
        return results

    def create_relation(self):
        # type: () -> List[GraphRelationship]
        """
        Create a list of relation map between watermark record with original table
        :return:
        """
        relation = GraphRelationship(
            start_key=self.get_watermark_model_key(),
            start_label=Watermark.LABEL,
            end_key=self.get_metadata_model_key(),
            end_label='Table',
            type=Watermark.WATERMARK_TABLE_RELATION_TYPE,
            reverse_type=Watermark.TABLE_WATERMARK_RELATION_TYPE,
            attributes={}
        )
        results = [relation]
        return results
