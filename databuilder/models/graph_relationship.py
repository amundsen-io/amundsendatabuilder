from collections import namedtuple

GraphRelationship = namedtuple(
    'GraphRelationship',
    [
        'start_label',
        'end_label',
        'start_key',
        'end_key',
        'type',
        'reverse_type',
        'relationship_attributes'
    ]
)