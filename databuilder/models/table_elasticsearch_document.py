from typing import List, Optional  # noqa: F401

from databuilder.models.elasticsearch_document import ElasticsearchDocument


class TableESDocument(ElasticsearchDocument):
    """
    Schema for the Search index document
    """
    def __init__(self,
                 database,  # type: str
                 cluster,  # type: str
                 schema,  # type: str
                 name,  # type: str
                 key,  # type: str
                 description,  # type: str
                 last_updated_timestamp,  # type: Optional[int]
                 column_names,  # type: List[str]
                 column_descriptions,  # type: List[str]
                 total_usage,  # type: int
                 unique_usage,  # type: int
                 tags,  # type: List[str],
                 tag_data=None,  # type: Optional[List[List[str, str]]]
                 display_name=None,  # type: Optional[str]
                 ):
        # type: (...) -> None
        self.database = database
        self.cluster = cluster
        self.schema = schema
        self.name = name
        self.display_name = display_name if display_name else '{schema}.{table}'.format(schema=schema, table=name)
        self.key = key
        self.description = description
        # todo: use last_updated_timestamp to match the record in metadata
        self.last_updated_timestamp = int(last_updated_timestamp) if last_updated_timestamp else None
        self.column_names = column_names
        self.column_descriptions = column_descriptions
        self.total_usage = total_usage
        self.unique_usage = unique_usage
        # todo: will include tag_type once we have better understanding from UI flow.
        self.tags = tags
        if tag_data:
            self.badges = [badge[0] for badge in tag_data if badge[1] == 'badge']
        if tag_data:
            self.tags = [tag[0] for tag in tag_data if tag[1] == 'default']
