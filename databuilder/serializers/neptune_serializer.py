import six

from typing import Dict, Any, List

from databuilder.models.graph_relationship import GraphRelationship
from databuilder.models.graph_node import GraphNode

NEPTUNE_HEADER_ID = "~id"
NEPTUNE_HEADER_LABEL = "~label"

NEPTUNE_RELATIONSHIP_HEADER_FROM = "~from"
NEPTUNE_RELATIONSHIP_HEADER_TO = "~to"


def convert_relationship(relationship):
    # type: (GraphRelationship) -> List[Dict[str, Any]]
    relation_id = "{from_vertex_id}_{to_vertex_id}_{label}".format(
        from_vertex_id=relationship.start_key,
        to_vertex_id=relationship.end_key,
        label=relationship.type
    )
    relation_id_reverse = "{from_vertex_id}_{to_vertex_id}_{label}".format(
        from_vertex_id=relationship.end_key,
        to_vertex_id=relationship.start_key,
        label=relationship.reverse_type
    )

    forward_relationship_doc = {
        NEPTUNE_HEADER_ID: relation_id,
        NEPTUNE_RELATIONSHIP_HEADER_FROM: relationship.start_key,
        NEPTUNE_RELATIONSHIP_HEADER_TO: relationship.end_key,
        NEPTUNE_HEADER_LABEL: relationship.type
    }

    reverse_relationship_doc = {
        NEPTUNE_HEADER_ID: relation_id_reverse,
        NEPTUNE_RELATIONSHIP_HEADER_FROM: relationship.end_key,
        NEPTUNE_RELATIONSHIP_HEADER_TO: relationship.start_key,
        NEPTUNE_HEADER_LABEL: relationship.reverse_type
    }
    return [
        forward_relationship_doc,
        reverse_relationship_doc
    ]


def convert_node(node):
    # type: (GraphNode) -> Dict[str, Any]
    node_dict = {
        NEPTUNE_HEADER_ID: node.id,
        NEPTUNE_HEADER_LABEL: node.label
    }

    for attr_key, attr_value in node.node_attributes.items():
        neptune_value_type = _get_neptune_type_for_value(attr_value)
        doc_key = "{key_name:neptune_value_type".format(
            key_name=attr_key,
            neptune_value_type=neptune_value_type
        )
        if doc_key not in node_dict:
            node_dict[doc_key] = attr_value

    return node_dict


def _get_neptune_type_for_value(value):
    # type: (Any) -> Optional[str]
    if isinstance(value, six.string_types):
        return "String"
    elif isinstance(value, six.integer_types):
        return "Long"
    elif isinstance(value, bool):
        return "Bool"
    elif isinstance(value, float):
        return "Double"

    return None
