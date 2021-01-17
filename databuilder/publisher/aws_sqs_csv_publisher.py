# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import csv
import ctypes
import json
import logging
import time
from io import open
from os import listdir
from os.path import isfile, join
from typing import List, Set

import boto3
import pandas
from botocore.config import Config
from pyhocon import ConfigFactory, ConfigTree

from databuilder.publisher.base_publisher import Publisher

# Setting field_size_limit to solve the error below
# _csv.Error: field larger than field limit (131072)
# https://stackoverflow.com/a/54517228/5972935
csv.field_size_limit(int(ctypes.c_ulong(-1).value // 2))

# Config keys
# A directory that contains CSV files for nodes
NODE_FILES_DIR = 'node_files_directory'
# A directory that contains CSV files for relationships
RELATION_FILES_DIR = 'relation_files_directory'

# AWS SQS configs
# AWS SQS region
AWS_SQS_REGION = 'aws_sqs_region'
# AWS SQS url to send a message
AWS_SQS_URL = 'aws_sqs_url'
# AWS SQS message group id
AWS_SQS_MESSAGE_GROUP_ID = 'aws_sqs_message_group_id'
# credential configuration of AWS SQS
AWS_SQS_ACCESS_KEY_ID = 'aws_sqs_access_key_id'
AWS_SQS_SECRET_ACCESS_KEY = 'aws_sqs_secret_access_key'

# This will be used to provide unique tag to the node and relationship
JOB_PUBLISH_TAG = 'job_publish_tag'

# CSV HEADER
# A header with this suffix will be pass to Neo4j statement without quote
UNQUOTED_SUFFIX = ':UNQUOTED'
# A header for Node label
NODE_LABEL_KEY = 'LABEL'
# A header for Node key
NODE_KEY_KEY = 'KEY'
# Required columns for Node
NODE_REQUIRED_KEYS = {NODE_LABEL_KEY, NODE_KEY_KEY}

DEFAULT_CONFIG = ConfigFactory.from_dict({AWS_SQS_MESSAGE_GROUP_ID: 'metadata'})

LOGGER = logging.getLogger(__name__)


class AWSSQSCsvPublisher(Publisher):
    """
    A Publisher takes two folders for input and publishes it as message to AWS SQS.
    One folder will contain CSV file(s) for Node where the other folder will contain CSV file(s) for Relationship.
    If the target AWS SQS Queue does not use content based deduplication, Message ID should be defined.
    """

    def __init__(self) -> None:
        super(AWSSQSCsvPublisher, self).__init__()

    def init(self, conf: ConfigTree) -> None:
        conf = conf.with_fallback(DEFAULT_CONFIG)

        self._node_files = self._list_files(conf, NODE_FILES_DIR)
        self._node_files_iter = iter(self._node_files)

        self._relation_files = self._list_files(conf, RELATION_FILES_DIR)
        self._relation_files_iter = iter(self._relation_files)

        # Initialize AWS SQS client
        self.client = self._get_client(conf=conf)
        self.aws_sqs_url = conf.get_string(AWS_SQS_URL)
        self.message_group_id = conf.get_string(AWS_SQS_MESSAGE_GROUP_ID)

        LOGGER.info('Publishing Node csv files {}, and Relation CSV files {}'
                    .format(self._node_files, self._relation_files))

    def _list_files(self, conf: ConfigTree, path_key: str) -> List[str]:
        """
        List files from directory
        :param conf:
        :param path_key:
        :return: List of file paths
        """
        if path_key not in conf:
            return []

        path = conf.get_string(path_key)
        return [join(path, f) for f in listdir(path) if isfile(join(path, f))]

    def publish_impl(self) -> None:  # noqa: C901
        """
        Publishes Nodes first and then Relations
        :return:
        """

        start = time.time()

        LOGGER.info('Publishing Node files: {}'.format(self._node_files))
        nodes = []
        relations = []

        try:
            while True:
                try:
                    node_file = next(self._node_files_iter)
                    nodes.extend(self._publish_record(node_file))
                except StopIteration:
                    break

            LOGGER.info('Publishing Relationship files: {}'.format(self._relation_files))
            while True:
                try:
                    relation_file = next(self._relation_files_iter)
                    relations.extend(self._publish_record(relation_file))
                except StopIteration:
                    break

            message_body = {
                'nodes': nodes,
                'relations': relations
            }

            LOGGER.info('Publish nodes and relationships to Queue {}'.format(self.aws_sqs_url))

            self.client.send_message(
                QueueUrl=self.aws_sqs_url,
                MessageBody=json.dumps(message_body),
                MessageGroupId=self.message_group_id
            )

            LOGGER.info('Successfully published. Elapsed: {} seconds'.format(time.time() - start))
        except Exception as e:
            LOGGER.exception('Failed to publish.')
            raise e

    def get_scope(self) -> str:
        return 'publisher.awssqs'

    def _publish_record(self, csv_file: str) -> list:
        """
        Iterate over the csv records of a file, each csv record transform to dict and will be added to list.
        All nodes and relations (in csv, each one is record) should have a unique key
        :param csv_file:
        :return:
        """
        ret = []

        with open(csv_file, 'r', encoding='utf8') as record_csv:
            for record in pandas.read_csv(record_csv, na_filter=False).to_dict(orient="records"):
                ret.append(record)

        return ret

    def _get_client(self, conf: ConfigTree) -> boto3.client:
        """
        Create a client object to access AWS SQS
        :return:
        """
        return boto3.client('sqs',
                            aws_access_key_id=conf.get_string(AWS_SQS_ACCESS_KEY_ID),
                            aws_secret_access_key=conf.get_string(AWS_SQS_SECRET_ACCESS_KEY),
                            config=Config(region_name=conf.get_string(AWS_SQS_REGION))
                            )