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

    s3 = boto3.client('s3',
                      endpoint_url='http://dev-master:9000/',
                      aws_access_key_id='myaccesskey',
                      aws_secret_access_key='mysecretkey',
                      region_name='us-east-1')

    r = s3.select_object_content(
        Bucket='dev-raw-data',
        Key='sacramento-real-estate-transactions/data.csv',
        ExpressionType='SQL',
        Expression="select * from s3object",
        InputSerialization={
            'CSV': {
                "FileHeaderInfo": "None",
            },
        },
        OutputSerialization={'CSV': {}},
    )

    """
    An Extractor that extracts records via CSV.
    """

    def init(self, conf: ConfigTree) -> None:
        """
        :param conf:
        """
        self.conf = conf
        self.previously_called = False
        # self.file_location = conf.get_string(MinioExtractor.FILE_LOCATION)

        # model_class = conf.get('model_class', None)
        # if model_class:
        #     module_name, class_name = model_class.rsplit(".", 1)
        #     mod = importlib.import_module(module_name)
        #     self.model_class = getattr(mod, class_name)
        # self._load_csv()

    def _load_csv(self) -> None:
        """
        Create an iterator to execute sql.
        """
        if not hasattr(self, 'results'):
            with open(self.file_location, 'r') as fin:
                self.results = [dict(i) for i in csv.DictReader(fin)]

        if hasattr(self, 'model_class'):
            results = [self.model_class(**result)
                       for result in self.results]
        else:
            results = self.results
        self.iter = iter(results)

    def extract(self) -> Any:

        if self.previously_called is True:
            return None

        self.r = MinioExtractor.r
        for event in self.r['Payload']:
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
                              name='sacramento-real-estate-transactions',
                              description='sacramento-real-estate-transactions',
                              columns=colMetadatalist,
                              # TODO: this possibly should parse stringified booleans;
                              # right now it only will be false for empty strings
                              is_view=True,
                              tags='real-estate'
                              )
        self.previously_called = True
        return table

    def get_scope(self) -> str:
        return 'extractor.minio.csv'