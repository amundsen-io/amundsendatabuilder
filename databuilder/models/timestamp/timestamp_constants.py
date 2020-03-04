from enum import Enum

NODE_LABEL = 'Timestamp'

TIMESTAMP_PROPERTY = 'timestamp'
TIMESTAMP_NAME_PROPERTY = 'name'
DEPRECATED_TIMESTAMP_PROPERTY = 'last_updated_timestamp'

LASTUPDATED_RELATION_TYPE = 'LAST_UPDATED_AT'
LASTUPDATED_REVERSE_RELATION_TYPE = 'LAST_UPDATED_TIME_OF'


class TimestampName(Enum):
    last_updated_timestamp = 1
