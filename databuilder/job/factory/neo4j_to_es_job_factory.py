import textwrap
import uuid

from elasticsearch import Elasticsearch
from pyhocon import ConfigFactory

from databuilder.extractor.neo4j_extractor import Neo4jExtractor
from databuilder.extractor.neo4j_search_data_extractor import Neo4jSearchDataExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import NoopTransformer


class Neo4jToEsJobFactory():
    """
    This is a convenience class, used to populate elasticsearch from neo4j.
    """

    # TODO - update unique tags

    def __init__(self,
                 es_host,
                 neo4j_endpoint,
                 neo4j_user,
                 neo4j_password,
                 use_unique_id=True):
        # type: (str, str, str, str, bool) -> None
        self.es_host = es_host
        self.neo4j_endpoint = neo4j_endpoint
        self.neo4j_user = neo4j_user
        self.neo4j_password = neo4j_password
        self.use_unique_id = use_unique_id

    def create_es_publisher_job(self,
                                elasticsearch_index_alias='table_search_index',
                                elasticsearch_doc_type_key='table',
                                model_name='databuilder.models.table_elasticsearch_document.TableESDocument',
                                cypher_query=None,
                                elasticsearch_mapping=None):
        # type: (str, str, str, str, str) -> DefaultJob

        """
        :param elasticsearch_index_alias:  alias for Elasticsearch used in
                                           amundsensearchlibrary/search_service/config.py as an index
        :param elasticsearch_doc_type_key: name the ElasticSearch index is prepended with. Defaults to `table` resulting in
                                           `table_search_index`
        :param model_name:                 the Databuilder model class used in transporting between Extractor and Loader
        :param cypher_query:               Query handed to the `Neo4jSearchDataExtractor` class, if None is given (default)
                                           it uses the `Table` query baked into the Extractor
        :param elasticsearch_mapping:      Elasticsearch field mapping "DDL" handed to the `ElasticsearchPublisher` class,
                                           if None is given (default) it uses the `Table` query baked into the Publisher
        """
        # loader saves data to this location and publisher reads it from here
        extracted_search_data_path = '/tmp/amundsen/search_data.json'

        task = DefaultTask(loader=FSElasticsearchJSONLoader(),
                           extractor=Neo4jSearchDataExtractor(),
                           transformer=NoopTransformer())

        # elastic search client instance
        elasticsearch_client = Elasticsearch([
            {'host': self.es_host},
        ])
        # unique name of new index in Elasticsearch
        elasticsearch_new_index_key = 'tables' + (str(uuid.uuid4()) if self.use_unique_id else "")

        job_config = ConfigFactory.from_dict({
            'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.GRAPH_URL_CONFIG_KEY): self.neo4j_endpoint,
            'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.MODEL_CLASS_CONFIG_KEY): model_name,
            'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.NEO4J_AUTH_USER): self.neo4j_user,
            'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.NEO4J_AUTH_PW): self.neo4j_password,
            'loader.filesystem.elasticsearch.{}'.format(FSElasticsearchJSONLoader.FILE_PATH_CONFIG_KEY):
                extracted_search_data_path,
            'loader.filesystem.elasticsearch.{}'.format(FSElasticsearchJSONLoader.FILE_MODE_CONFIG_KEY): 'w',
            'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.FILE_PATH_CONFIG_KEY):
                extracted_search_data_path,
            'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.FILE_MODE_CONFIG_KEY): 'r',
            'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_CLIENT_CONFIG_KEY):
                elasticsearch_client,
            'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_NEW_INDEX_CONFIG_KEY):
                elasticsearch_new_index_key,
            'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_DOC_TYPE_CONFIG_KEY):
                elasticsearch_doc_type_key,
            'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_ALIAS_CONFIG_KEY):
                elasticsearch_index_alias,
        })

        # only optionally add these keys, so need to dynamically `put` them
        if cypher_query:
            job_config.put('extractor.search_data.{}'.format(Neo4jSearchDataExtractor.CYPHER_QUERY_CONFIG_KEY),
                           cypher_query)
        if elasticsearch_mapping:
            job_config.put('publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_MAPPING_CONFIG_KEY),
                           elasticsearch_mapping)

        job = DefaultJob(conf=job_config,
                         task=task,
                         publisher=ElasticsearchPublisher())
        return job

    def create_user_es_publisher_job(self):
        # type: (None) -> DefaultJob

        user_cypher_query = textwrap.dedent(
            """
            MATCH (user:User)
            OPTIONAL MATCH (user)-[read:READ]->(a)
            OPTIONAL MATCH (user)-[own:OWNER_OF]->(b)
            OPTIONAL MATCH (user)-[follow:FOLLOWED_BY]->(c)
            OPTIONAL MATCH (user)-[manage_by:MANAGE_BY]->(manager)
            with user, a, b, c, read, own, follow, manager
            where user.full_name is not null
            return user.email as email, user.first_name as first_name, user.last_name as last_name,
            user.full_name as name, user.github_username as github_username, user.team_name as team_name,
            user.employee_type as employee_type, manager.email as manager_email, user.slack_id as slack_id,
            user.is_active as is_active,
            REDUCE(sum_r = 0, r in COLLECT(DISTINCT read)| sum_r + r.read_count) AS total_read,
            count(distinct b) as total_own,
            count(distinct c) AS total_follow
            order by user.email
            """
        )

        user_elasticsearch_mapping = """
                {
                  "mappings":{
                    "user":{
                      "properties": {
                        "email": {
                          "type":"text",
                          "analyzer": "simple",
                          "fields": {
                            "raw": {
                              "type": "keyword"
                            }
                          }
                        },
                        "first_name": {
                          "type":"text",
                          "analyzer": "simple",
                          "fields": {
                            "raw": {
                              "type": "keyword"
                            }
                          }
                        },
                        "last_name": {
                          "type":"text",
                          "analyzer": "simple",
                          "fields": {
                            "raw": {
                              "type": "keyword"
                            }
                          }
                        },
                        "name": {
                          "type":"text",
                          "analyzer": "simple",
                          "fields": {
                            "raw": {
                              "type": "keyword"
                            }
                          }
                        },
                        "total_read":{
                          "type": "long"
                        },
                        "total_own": {
                          "type": "long"
                        },
                        "total_follow": {
                          "type": "long"
                        }
                      }
                    }
                  }
                }
            """

        return self.create_es_publisher_job(
            elasticsearch_index_alias='user_search_index',
            elasticsearch_doc_type_key='user',
            model_name='databuilder.models.user_elasticsearch_document.UserESDocument',
            cypher_query=user_cypher_query,
            elasticsearch_mapping=user_elasticsearch_mapping)
