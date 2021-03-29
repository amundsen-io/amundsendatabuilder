# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

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
https://github.com/amundsen-io/amundsendatabuilder#list-of-extractors
"""

import logging
import os
import sys

from elasticsearch import Elasticsearch
from pyhocon import ConfigFactory
from sqlalchemy.ext.declarative import declarative_base

from databuilder import Scoped
from databuilder.extractor.kafka_source_extractor import KafkaSourceExtractor
# from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
# from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher

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

Base = declarative_base()

NEO4J_ENDPOINT = f'bolt://{neo_host}:{neo_port}'

neo4j_endpoint = NEO4J_ENDPOINT

neo4j_user = 'neo4j'
neo4j_password = 'test'


logger = logging.getLogger(__name__)


if __name__ == "__main__":
    # Uncomment next line to get INFO level logging
    # logging.basicConfig(level=logging.INFO)
    consumer_config = {
        '"bootstrap.servers"': 'localhost:9090',
        '"group.id"': 'consumer-group',
        '"auto.offset.reset"': 'earliest',
        '"enable.auto.commit"': False,
    }
    config_dict = {
        f'extractor.kafka_source.consumer_config': consumer_config,
        f'extractor.kafka_source.{KafkaSourceExtractor.RAW_VALUE_TRANSFORMER}': 'databuilder.transformer.base_transformer.NoopTransformer',
        f'extractor.kafka_source.{KafkaSourceExtractor.TOPIC_NAME_LIST}': ['test-event-datahub'],
        f'extractor.kafka_source.{KafkaSourceExtractor.CONSUMER_TOTAL_TIMEOUT_SEC}': 1,
    }
    configuration = ConfigFactory.from_dict(config_dict)
    kafka_extractor = KafkaSourceExtractor()
    kafka_extractor.init(
        Scoped.get_scoped_conf(
            conf=configuration,
            scope=kafka_extractor.get_scope(),
        ),
    )
    records = kafka_extractor.consume()
    print(f'---------------  records {records} --------------------')
