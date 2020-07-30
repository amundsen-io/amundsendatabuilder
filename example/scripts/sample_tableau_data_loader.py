"""
This is a example script demonstrating how to load data into Neo4j and
Elasticsearch without using an Airflow DAG.

It contains several jobs:
- `run_csv_job`: runs a job that extracts table data from a CSV, loads (writes)
  this into a different local directory as a csv, then publishes this data to
  neo4j.
- `run_table_column_job`: does the same thing as `run_csv_job`, but with a csv
  containing column data.
- `create_last_updated_job`: creates a job that gets the current time, dumps it
  into a predefined model schema, and publishes this to neo4j.
- `create_es_publisher_sample_job`: creates a job that extracts data from neo4j
  and pubishes it into elasticsearch.

For other available extractors, please take a look at
https://github.com/lyft/amundsendatabuilder#list-of-extractors
"""

import logging
import os
import sqlite3
import sys
import uuid

from elasticsearch import Elasticsearch
from pyhocon import ConfigFactory
from sqlalchemy.ext.declarative import declarative_base

from databuilder.extractor.csv_extractor import CsvTableColumnExtractor, CsvExtractor
from databuilder.extractor.neo4j_es_last_updated_extractor import Neo4jEsLastUpdatedExtractor
from databuilder.extractor.neo4j_search_data_extractor import Neo4jSearchDataExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher.elasticsearch_constants import DASHBOARD_ELASTICSEARCH_INDEX_MAPPING, \
    USER_ELASTICSEARCH_INDEX_MAPPING
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import ChainedTransformer
from databuilder.transformer.base_transformer import NoopTransformer
from databuilder.transformer.dict_to_model import DictToModel, MODEL_CLASS
from databuilder.transformer.generic_transformer import GenericTransformer, CALLBACK_FUNCTION, FIELD_NAME

from databuilder.extractor.dashboard.tableau.tableau_dashboard_extractor import TableauDashboardExtractor
from databuilder.extractor.dashboard.tableau.tableau_dashboard_last_modified_extractor import \
    TableauDashboardLastModifiedExtractor
from databuilder.extractor.dashboard.tableau.tableau_dashboard_query_extractor import TableauDashboardQueryExtractor
from databuilder.extractor.dashboard.tableau.tableau_dashboard_table_extractor import TableauDashboardTableExtractor
from databuilder.extractor.dashboard.tableau.tableau_external_table_extractor import \
    TableauDashboardExternalTableExtractor

es_host = os.getenv('CREDENTIALS_ELASTICSEARCH_PROXY_HOST', 'localhost')
neo_host = os.getenv('CREDENTIALS_NEO4J_PROXY_HOST', 'localhost')

es_port = os.getenv('CREDENTIALS_ELASTICSEARCH_PROXY_PORT', 9200)
neo_port = os.getenv('CREDENTIALS_NEO4J_PROXY_PORT', 7687)
if len(sys.argv) > 1:
    es_host = sys.argv[1]
if len(sys.argv) > 2:
    neo_host = sys.argv[2]

es = Elasticsearch([
    {'host': es_host, 'port': es_port},
])

DB_FILE = '/tmp/test.db'
SQLITE_CONN_STRING = 'sqlite:////tmp/test.db'
Base = declarative_base()

NEO4J_ENDPOINT = 'bolt://{}:{}'.format(neo_host, neo_port)

neo4j_endpoint = NEO4J_ENDPOINT

neo4j_user = 'neo4j'
neo4j_password = 'test'

LOGGER = logging.getLogger(__name__)

tableau_host = ""
tableau_api_version = ""
tableau_site_name = ""
tableau_personal_access_token_name = ""
tableau_personal_access_token_secret = ""
tableau_excluded_projects = []
tableau_dashboard_cluster = ""
tableau_dashboard_database = ""
tableau_external_table_cluster = ""
tableau_external_table_schema = ""

common_tableau_config = {
    'publisher.neo4j.neo4j_endpoint': neo4j_endpoint,
    'publisher.neo4j.neo4j_user': neo4j_user,
    'publisher.neo4j.neo4j_password': neo4j_password,
    'publisher.neo4j.neo4j_encrypted': False,
    'publisher.neo4j.job_publish_tag': 'unique_tag',  # should use unique tag here like {ds}
}


def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Exception:
        LOGGER.exception('exception')
    return None


def run_csv_job(file_loc, job_name, model):
    tmp_folder = '/var/tmp/amundsen/{job_name}'.format(job_name=job_name)
    node_files_folder = '{tmp_folder}/nodes'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships'.format(tmp_folder=tmp_folder)

    csv_extractor = CsvExtractor()
    csv_loader = FsNeo4jCSVLoader()

    task = DefaultTask(extractor=csv_extractor,
                       loader=csv_loader,
                       transformer=NoopTransformer())

    job_config = ConfigFactory.from_dict({
        'extractor.csv.file_location': file_loc,
        'extractor.csv.model_class': model,
        'loader.filesystem_csv_neo4j.node_dir_path': node_files_folder,
        'loader.filesystem_csv_neo4j.relationship_dir_path': relationship_files_folder,
        'loader.filesystem_csv_neo4j.delete_created_directories': True,
        'publisher.neo4j.node_files_directory': node_files_folder,
        'publisher.neo4j.relation_files_directory': relationship_files_folder,
        'publisher.neo4j.neo4j_endpoint': neo4j_endpoint,
        'publisher.neo4j.neo4j_user': neo4j_user,
        'publisher.neo4j.neo4j_password': neo4j_password,
        'publisher.neo4j.neo4j_encrypted': False,
        'publisher.neo4j.job_publish_tag': 'unique_tag',  # should use unique tag here like {ds}
    })

    DefaultJob(conf=job_config,
               task=task,
               publisher=Neo4jCsvPublisher()).launch()


def run_table_column_job(table_path, column_path):
    tmp_folder = '/var/tmp/amundsen/table_column'
    node_files_folder = '{tmp_folder}/nodes'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships'.format(tmp_folder=tmp_folder)
    extractor = CsvTableColumnExtractor()
    csv_loader = FsNeo4jCSVLoader()
    task = DefaultTask(extractor,
                       loader=csv_loader,
                       transformer=NoopTransformer())
    job_config = ConfigFactory.from_dict({
        'extractor.csvtablecolumn.table_file_location': table_path,
        'extractor.csvtablecolumn.column_file_location': column_path,
        'loader.filesystem_csv_neo4j.node_dir_path': node_files_folder,
        'loader.filesystem_csv_neo4j.relationship_dir_path': relationship_files_folder,
        'loader.filesystem_csv_neo4j.delete_created_directories': True,
        'publisher.neo4j.node_files_directory': node_files_folder,
        'publisher.neo4j.relation_files_directory': relationship_files_folder,
        'publisher.neo4j.neo4j_endpoint': neo4j_endpoint,
        'publisher.neo4j.neo4j_user': neo4j_user,
        'publisher.neo4j.neo4j_password': neo4j_password,
        'publisher.neo4j.neo4j_encrypted': False,
        'publisher.neo4j.job_publish_tag': 'unique_tag',  # should use unique tag here like {ds}
    })
    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=Neo4jCsvPublisher())
    job.launch()


def create_last_updated_job():
    # loader saves data to these folders and publisher reads it from here
    tmp_folder = '/var/tmp/amundsen/last_updated_data'
    node_files_folder = '{tmp_folder}/nodes'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships'.format(tmp_folder=tmp_folder)

    task = DefaultTask(extractor=Neo4jEsLastUpdatedExtractor(), loader=FsNeo4jCSVLoader())

    job_config = ConfigFactory.from_dict({
        'extractor.neo4j_es_last_updated.model_class':
            'databuilder.models.neo4j_es_last_updated.Neo4jESLastUpdated',

        'loader.filesystem_csv_neo4j.node_dir_path': node_files_folder,
        'loader.filesystem_csv_neo4j.relationship_dir_path': relationship_files_folder,
        'publisher.neo4j.node_files_directory': node_files_folder,
        'publisher.neo4j.relation_files_directory': relationship_files_folder,
        'publisher.neo4j.neo4j_endpoint': neo4j_endpoint,
        'publisher.neo4j.neo4j_user': neo4j_user,
        'publisher.neo4j.neo4j_password': neo4j_password,
        'publisher.neo4j.neo4j_encrypted': False,
        'publisher.neo4j.job_publish_tag': 'unique_lastupdated_tag',  # should use unique tag here like {ds}
    })

    return DefaultJob(conf=job_config,
                      task=task,
                      publisher=Neo4jCsvPublisher())


def _str_to_list(str_val):
    return str_val.split(',')


def create_dashboard_tables_job():
    # loader saves data to these folders and publisher reads it from here
    tmp_folder = '/var/tmp/amundsen/dashboard_table'
    node_files_folder = '{tmp_folder}/nodes'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships'.format(tmp_folder=tmp_folder)

    csv_extractor = CsvExtractor()
    csv_loader = FsNeo4jCSVLoader()

    generic_transformer = GenericTransformer()
    dict_to_model_transformer = DictToModel()
    transformer = ChainedTransformer(transformers=[generic_transformer, dict_to_model_transformer],
                                     is_init_transformers=True)

    task = DefaultTask(extractor=csv_extractor,
                       loader=csv_loader,
                       transformer=transformer)
    publisher = Neo4jCsvPublisher()

    job_config = ConfigFactory.from_dict({
        '{}.file_location'.format(csv_extractor.get_scope()): 'example/sample_data/sample_dashboard_table.csv',
        '{}.{}.{}'.format(transformer.get_scope(), generic_transformer.get_scope(), FIELD_NAME): 'table_ids',
        '{}.{}.{}'.format(transformer.get_scope(), generic_transformer.get_scope(), CALLBACK_FUNCTION): _str_to_list,
        '{}.{}.{}'.format(transformer.get_scope(), dict_to_model_transformer.get_scope(), MODEL_CLASS):
            'databuilder.models.dashboard.dashboard_table.DashboardTable',
        '{}.node_dir_path'.format(csv_loader.get_scope()): node_files_folder,
        '{}.relationship_dir_path'.format(csv_loader.get_scope()): relationship_files_folder,
        '{}.delete_created_directories'.format(csv_loader.get_scope()): True,
        '{}.node_files_directory'.format(publisher.get_scope()): node_files_folder,
        '{}.relation_files_directory'.format(publisher.get_scope()): relationship_files_folder,
        '{}.neo4j_endpoint'.format(publisher.get_scope()): neo4j_endpoint,
        '{}.neo4j_user'.format(publisher.get_scope()): neo4j_user,
        '{}.neo4j_password'.format(publisher.get_scope()): neo4j_password,
        '{}.neo4j_encrypted'.format(publisher.get_scope()): False,
        '{}.job_publish_tag'.format(publisher.get_scope()): 'unique_tag',  # should use unique tag here like {ds}
    })

    return DefaultJob(conf=job_config,
                      task=task,
                      publisher=publisher)


def create_es_publisher_sample_job(elasticsearch_index_alias='table_search_index',
                                   elasticsearch_doc_type_key='table',
                                   model_name='databuilder.models.table_elasticsearch_document.TableESDocument',
                                   entity_type='table',
                                   elasticsearch_mapping=None):
    """
    :param elasticsearch_index_alias:  alias for Elasticsearch used in
                                       amundsensearchlibrary/search_service/config.py as an index
    :param elasticsearch_doc_type_key: name the ElasticSearch index is prepended with. Defaults to `table` resulting in
                                       `table_{uuid}`
    :param model_name:                 the Databuilder model class used in transporting between Extractor and Loader
    :param entity_type:                Entity type handed to the `Neo4jSearchDataExtractor` class, used to determine
                                       Cypher query to extract data from Neo4j. Defaults to `table`.
    :param elasticsearch_mapping:      Elasticsearch field mapping "DDL" handed to the `ElasticsearchPublisher` class,
                                       if None is given (default) it uses the `Table` query baked into the Publisher
    """
    # loader saves data to this location and publisher reads it from here
    extracted_search_data_path = '/var/tmp/amundsen/search_data.json'

    task = DefaultTask(loader=FSElasticsearchJSONLoader(),
                       extractor=Neo4jSearchDataExtractor(),
                       transformer=NoopTransformer())

    # elastic search client instance
    elasticsearch_client = es
    # unique name of new index in Elasticsearch
    elasticsearch_new_index_key = '{}_'.format(elasticsearch_doc_type_key) + str(uuid.uuid4())

    job_config = ConfigFactory.from_dict({
        'extractor.search_data.entity_type': entity_type,
        'extractor.search_data.extractor.neo4j.graph_url': neo4j_endpoint,
        'extractor.search_data.extractor.neo4j.model_class': model_name,
        'extractor.search_data.extractor.neo4j.neo4j_auth_user': neo4j_user,
        'extractor.search_data.extractor.neo4j.neo4j_auth_pw': neo4j_password,
        'extractor.search_data.extractor.neo4j.neo4j_encrypted': False,
        'loader.filesystem.elasticsearch.file_path': extracted_search_data_path,
        'loader.filesystem.elasticsearch.mode': 'w',
        'publisher.elasticsearch.file_path': extracted_search_data_path,
        'publisher.elasticsearch.mode': 'r',
        'publisher.elasticsearch.client': elasticsearch_client,
        'publisher.elasticsearch.new_index': elasticsearch_new_index_key,
        'publisher.elasticsearch.doc_type': elasticsearch_doc_type_key,
        'publisher.elasticsearch.alias': elasticsearch_index_alias,
    })

    # only optionally add these keys, so need to dynamically `put` them
    if elasticsearch_mapping:
        job_config.put('publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_MAPPING_CONFIG_KEY),
                       elasticsearch_mapping)

    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=ElasticsearchPublisher())
    return job


def run_tableau_metadata_job():
    task = DefaultTask(extractor=TableauDashboardExtractor(), loader=FsNeo4jCSVLoader())

    tmp_folder = '/var/tmp/amundsen/tableau_dashboard_metadata'

    node_files_folder = '{tmp_folder}/nodes'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships'.format(tmp_folder=tmp_folder)

    dict_config = common_tableau_config
    dict_config.update({
        'extractor.tableau_dashboard_metadata.tableau_host': tableau_host,
        'extractor.tableau_dashboard_metadata.api_version': tableau_api_version,
        'extractor.tableau_dashboard_metadata.site_name': tableau_site_name,
        'extractor.tableau_dashboard_metadata.tableau_personal_access_token_name':
            tableau_personal_access_token_name,
        'extractor.tableau_dashboard_metadata.tableau_personal_access_token_secret':
            tableau_personal_access_token_secret,
        'extractor.tableau_dashboard_metadata.excluded_projects': tableau_excluded_projects,
        'extractor.tableau_dashboard_metadata.cluster': tableau_dashboard_cluster,
        'extractor.tableau_dashboard_metadata.database': tableau_dashboard_database,
        'extractor.tableau_dashboard_metadata.transformer.timestamp_str_to_epoch.timestamp_format':
            "%Y-%m-%dT%H:%M:%SZ",
        'loader.filesystem_csv_neo4j.node_dir_path': node_files_folder,
        'loader.filesystem_csv_neo4j.relationship_dir_path': relationship_files_folder,
        'loader.filesystem_csv_neo4j.delete_created_directories': True,
        'task.progress_report_frequency': 100,
        'publisher.neo4j.node_files_directory': node_files_folder,
        'publisher.neo4j.relation_files_directory': relationship_files_folder,
    })
    job_config = ConfigFactory.from_dict(dict_config)

    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=Neo4jCsvPublisher())

    job.launch()


def run_tableau_last_modified_job():
    task = DefaultTask(extractor=TableauDashboardLastModifiedExtractor(), loader=FsNeo4jCSVLoader())

    tmp_folder = '/var/tmp/amundsen/tableau_dashboard_user'

    node_files_folder = '{tmp_folder}/nodes'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships'.format(tmp_folder=tmp_folder)

    dict_config = common_tableau_config
    dict_config.update({
        'extractor.tableau_dashboard_last_modified.tableau_host': tableau_host,
        'extractor.tableau_dashboard_last_modified.api_version': tableau_api_version,
        'extractor.tableau_dashboard_last_modified.site_name': tableau_site_name,
        'extractor.tableau_dashboard_last_modified.tableau_personal_access_token_name':
            tableau_personal_access_token_name,
        'extractor.tableau_dashboard_last_modified.tableau_personal_access_token_secret':
            tableau_personal_access_token_secret,
        'extractor.tableau_dashboard_last_modified.excluded_projects': tableau_excluded_projects,
        'extractor.tableau_dashboard_last_modified.cluster': tableau_dashboard_cluster,
        'extractor.tableau_dashboard_last_modified.database': tableau_dashboard_database,
        'extractor.tableau_dashboard_last_modified.transformer.timestamp_str_to_epoch.timestamp_format':
            "%Y-%m-%dT%H:%M:%SZ",
        'loader.filesystem_csv_neo4j.node_dir_path': node_files_folder,
        'loader.filesystem_csv_neo4j.relationship_dir_path': relationship_files_folder,
        'loader.filesystem_csv_neo4j.delete_created_directories': True,
        'task.progress_report_frequency': 100,
        'publisher.neo4j.node_files_directory': node_files_folder,
        'publisher.neo4j.relation_files_directory': relationship_files_folder,
    })
    job_config = ConfigFactory.from_dict(dict_config)

    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=Neo4jCsvPublisher())

    job.launch()


def run_tableau_query_job():
    task = DefaultTask(extractor=TableauDashboardQueryExtractor(), loader=FsNeo4jCSVLoader())

    tmp_folder = '/var/tmp/amundsen/tableau_dashboard_query'

    node_files_folder = '{tmp_folder}/nodes'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships'.format(tmp_folder=tmp_folder)

    dict_config = common_tableau_config
    dict_config.update({
        'extractor.tableau_dashboard_query.tableau_host': tableau_host,
        'extractor.tableau_dashboard_query.api_version': tableau_api_version,
        'extractor.tableau_dashboard_query.site_name': tableau_site_name,
        'extractor.tableau_dashboard_query.tableau_personal_access_token_name':
            tableau_personal_access_token_name,
        'extractor.tableau_dashboard_query.tableau_personal_access_token_secret':
            tableau_personal_access_token_secret,
        'extractor.tableau_dashboard_query.excluded_projects': tableau_excluded_projects,
        'extractor.tableau_dashboard_query.cluster': tableau_dashboard_cluster,
        'extractor.tableau_dashboard_query.database': tableau_dashboard_database,
        'extractor.tableau_dashboard_query.transformer.timestamp_str_to_epoch.timestamp_format':
            "%Y-%m-%dT%H:%M:%SZ",
        'loader.filesystem_csv_neo4j.node_dir_path': node_files_folder,
        'loader.filesystem_csv_neo4j.relationship_dir_path': relationship_files_folder,
        'loader.filesystem_csv_neo4j.delete_created_directories': True,
        'task.progress_report_frequency': 100,
        'publisher.neo4j.node_files_directory': node_files_folder,
        'publisher.neo4j.relation_files_directory': relationship_files_folder,
    })
    job_config = ConfigFactory.from_dict(dict_config)

    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=Neo4jCsvPublisher())

    job.launch()


def run_tableau_table_job():
    task = DefaultTask(extractor=TableauDashboardTableExtractor(), loader=FsNeo4jCSVLoader())

    tmp_folder = '/var/tmp/amundsen/tableau_dashboard_table'

    node_files_folder = '{tmp_folder}/nodes'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships'.format(tmp_folder=tmp_folder)

    dict_config = common_tableau_config
    dict_config.update({
        'extractor.tableau_dashboard_table.tableau_host': tableau_host,
        'extractor.tableau_dashboard_table.api_version': tableau_api_version,
        'extractor.tableau_dashboard_table.site_name': tableau_site_name,
        'extractor.tableau_dashboard_table.tableau_personal_access_token_name':
            tableau_personal_access_token_name,
        'extractor.tableau_dashboard_table.tableau_personal_access_token_secret':
            tableau_personal_access_token_secret,
        'extractor.tableau_dashboard_table.excluded_projects': tableau_excluded_projects,
        'extractor.tableau_dashboard_table.cluster': tableau_dashboard_cluster,
        'extractor.tableau_dashboard_table.database': tableau_dashboard_database,
        'extractor.tableau_dashboard_table.external_cluster_name': tableau_external_table_cluster,
        'extractor.tableau_dashboard_table.external_schema_name': tableau_external_table_schema,
        'extractor.tableau_dashboard_table.transformer.timestamp_str_to_epoch.timestamp_format': "%Y-%m-%dT%H:%M:%SZ",
        'loader.filesystem_csv_neo4j.node_dir_path': node_files_folder,
        'loader.filesystem_csv_neo4j.relationship_dir_path': relationship_files_folder,
        'loader.filesystem_csv_neo4j.delete_created_directories': True,
        'task.progress_report_frequency': 100,
        'publisher.neo4j.node_files_directory': node_files_folder,
        'publisher.neo4j.relation_files_directory': relationship_files_folder,
    })
    job_config = ConfigFactory.from_dict(dict_config)

    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=Neo4jCsvPublisher())

    job.launch()


def run_tableau_external_table_job():
    task = DefaultTask(extractor=TableauDashboardExternalTableExtractor(), loader=FsNeo4jCSVLoader())

    tmp_folder = '/var/tmp/amundsen/tableau_dashboard_external_table'

    node_files_folder = '{tmp_folder}/nodes'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships'.format(tmp_folder=tmp_folder)

    dict_config = common_tableau_config
    dict_config.update({
        'extractor.tableau_external_table.tableau_host': tableau_host,
        'extractor.tableau_external_table.api_version': tableau_api_version,
        'extractor.tableau_external_table.site_name': tableau_site_name,
        'extractor.tableau_external_table.tableau_personal_access_token_name':
            tableau_personal_access_token_name,
        'extractor.tableau_external_table.tableau_personal_access_token_secret':
            tableau_personal_access_token_secret,
        'extractor.tableau_external_table.excluded_projects': tableau_excluded_projects,
        'extractor.tableau_external_table.cluster': tableau_dashboard_cluster,
        'extractor.tableau_external_table.database': tableau_dashboard_database,
        'extractor.tableau_external_table.external_cluster_name': tableau_external_table_cluster,
        'extractor.tableau_external_table.external_schema_name': tableau_external_table_schema,
        'loader.filesystem_csv_neo4j.node_dir_path': node_files_folder,
        'loader.filesystem_csv_neo4j.relationship_dir_path': relationship_files_folder,
        'loader.filesystem_csv_neo4j.delete_created_directories': True,
        'task.progress_report_frequency': 100,
        'publisher.neo4j.node_files_directory': node_files_folder,
        'publisher.neo4j.relation_files_directory': relationship_files_folder,
    })
    job_config = ConfigFactory.from_dict(dict_config)

    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=Neo4jCsvPublisher())

    job.launch()


if __name__ == "__main__":
    # Uncomment next line to get INFO level logging
    # logging.basicConfig(level=logging.INFO)

    if create_connection(DB_FILE):
        run_tableau_metadata_job()
        run_tableau_external_table_job()
        run_tableau_table_job()
        run_tableau_query_job()
        run_tableau_last_modified_job()

        run_table_column_job('example/sample_data/sample_table.csv', 'example/sample_data/sample_col.csv')
        run_csv_job('example/sample_data/sample_user.csv', 'test_user_metadata',
                    'databuilder.models.user.User')

        job_es_table = create_es_publisher_sample_job(
            elasticsearch_index_alias='table_search_index',
            elasticsearch_doc_type_key='table',
            entity_type='table',
            model_name='databuilder.models.table_elasticsearch_document.TableESDocument')
        job_es_table.launch()

        job_es_user = create_es_publisher_sample_job(
            elasticsearch_index_alias='user_search_index',
            elasticsearch_doc_type_key='user',
            model_name='databuilder.models.user_elasticsearch_document.UserESDocument',
            entity_type='user',
            elasticsearch_mapping=USER_ELASTICSEARCH_INDEX_MAPPING)
        job_es_user.launch()

        job_es_dashboard = create_es_publisher_sample_job(
            elasticsearch_index_alias='dashboard_search_index',
            elasticsearch_doc_type_key='dashboard',
            model_name='databuilder.models.dashboard_elasticsearch_document.DashboardESDocument',
            entity_type='dashboard',
            elasticsearch_mapping=DASHBOARD_ELASTICSEARCH_INDEX_MAPPING)
        job_es_dashboard.launch()
