# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import csv
import importlib
# from collections import defaultdict

from pyhocon import ConfigTree
from typing import Any
import boto3

from databuilder.extractor.base_extractor import Extractor
from databuilder.models.table_metadata import TableMetadata, ColumnMetadata

class MinioExtractor(Extractor):

    """
    An Extractor that extracts records via CSV.
    """

    def init(self, conf: ConfigTree) -> None:
        """
        :param conf:
        """
        self.conf = conf
        self._extract_iter = None

        self.client = boto3.client('s3',
                          endpoint_url='http://dev-master:9000/',
                          aws_access_key_id='myaccesskey',
                          aws_secret_access_key='mysecretkey',
                          region_name='us-east-1')

    def get_s3_keys(self, bucket):
        """Get a list of keys in an S3 bucket."""
        keys = []
        resp = self.client.list_objects_v2(Bucket=bucket)
        for obj in resp['Contents']:
            if obj['Key'].split('/')[-1] == 'data.csv':
                keys.append(obj['Key'])
        return iter(keys)

    def extract(self) -> Any:

        if not self._extract_iter:
            self._extract_iter = self.get_s3_keys('dev-raw-data')
        try:
            name = next(self._extract_iter)

            r = self.client.select_object_content(
                Bucket='dev-raw-data',
                Key=name,
                ExpressionType='SQL',
                Expression="select * from s3object",
                InputSerialization={
                    'CSV': {
                        "FileHeaderInfo": "None",
                    },
                },
                OutputSerialization={'CSV': {}},
            )


            for event in r['Payload']:
                if 'Records' in event:
                    columns = event['Records']['Payload'].decode("utf-8").partition('\n')[0].split(",")

            colMetadatalist = []
            for i in range(len(columns)):
                col = ColumnMetadata(name= columns[i],
                                     description= None,
                                     col_type= 'str',
                                     sort_order= i)
                colMetadatalist.append(col)

            table = TableMetadata(database='minio',
                                  cluster='dev-raw-data',
                                  schema='minio',
                                  name=name.split('/',1)[0],
                                  description='',
                                  columns=colMetadatalist,
                                  # TODO: this possibly should parse stringified booleans;
                                  # right now it only will be false for empty strings
                                  is_view=True,
                                  tags='real-estate'
                                  )
            return table
        except StopIteration:
            return None

    def get_scope(self) -> str:
        return 'extractor.minio.csv'