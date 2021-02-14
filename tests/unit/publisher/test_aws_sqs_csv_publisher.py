# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import logging
import os
import unittest
import uuid
import boto3

from mock import MagicMock, patch
from pyhocon import ConfigFactory

from databuilder.publisher import aws_sqs_csv_publisher
from databuilder.publisher.aws_sqs_csv_publisher import AWSSQSCsvPublisher

here = os.path.dirname(__file__)


class TestAWSSQSPublish(unittest.TestCase):

    def setUp(self) -> None:
        logging.basicConfig(level=logging.INFO)
        self._resource_path = os.path.join(here, f'../resources/csv_publisher')

    def test_publisher(self) -> None:
        with patch.object(boto3, 'client') as mock_client, \
                patch.object(AWSSQSCsvPublisher, '_publish_record') as mock_publish_record:

            mock_send_message = MagicMock()
            mock_client.return_value.send_message = mock_send_message

            publisher = AWSSQSCsvPublisher()

            conf = ConfigFactory.from_dict(
                {aws_sqs_csv_publisher.NODE_FILES_DIR: f'{self._resource_path}/nodes',
                 aws_sqs_csv_publisher.RELATION_FILES_DIR: f'{self._resource_path}/relations',
                 aws_sqs_csv_publisher.AWS_SQS_REGION: 'aws_region',
                 aws_sqs_csv_publisher.AWS_SQS_URL: 'aws_sqs_url',
                 aws_sqs_csv_publisher.AWS_SQS_ACCESS_KEY_ID: 'aws_account_access_key_id',
                 aws_sqs_csv_publisher.AWS_SQS_SECRET_ACCESS_KEY: 'aws_account_secret_access_key',
                 aws_sqs_csv_publisher.JOB_PUBLISH_TAG: str(uuid.uuid4())}
            )
            publisher.init(conf)
            publisher.publish()

            # 2 node files and 1 relation file
            self.assertEqual(mock_publish_record.call_count, 3)

            self.assertEqual(mock_send_message.call_count, 1)
