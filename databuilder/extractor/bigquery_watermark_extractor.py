from collections import namedtuple

import logging
from pyhocon import ConfigFactory
from elasticsearch import Elasticsearch
import sqlite3
import uuid
import textwrap
import datetime


import google.oauth2.service_account
import google_auth_httplib2
from googleapiclient.discovery import build
import httplib2
from pyhocon import ConfigTree  # noqa: F401
from typing import List, Any  # noqa: F401


from databuilder.extractor.base_extractor import Extractor
from databuilder.models.table_metadata import TableMetadata, ColumnMetadata
from databuilder.models.watermark import Watermark

DatasetRef = namedtuple('DatasetRef', ['datasetId', 'projectId'])
TableKey = namedtuple('TableKey', ['schema_name', 'table_name'])
PartitionInfo = namedtuple('PartitionInfo', ['partition_id', 'epoch_created'])

LOGGER = logging.getLogger(__name__)

class BigQueryWatermarkExtractor(Extractor):


    PROJECT_ID_KEY = 'project_id'
    KEY_PATH_KEY = 'key_path'
    # sometimes we don't have a key path, but only have an variable
    CRED_KEY = 'project_cred'
    PAGE_SIZE_KEY = 'page_size'
    FILTER_KEY = 'filter'
    _DEFAULT_SCOPES = ['https://www.googleapis.com/auth/bigquery.readonly', ]
    DEFAULT_PAGE_SIZE = 300
    NUM_RETRIES = 3
    DATE_LENGTH = 8

    def init(self, conf):
        # type: (ConfigTree) -> None
        # should use key_path, or cred_key if the former doesn't exist
        self.key_path = conf.get_string(BigQueryWatermarkExtractor.KEY_PATH_KEY, None)
        self.cred_key = conf.get_string(BigQueryWatermarkExtractor.CRED_KEY, None)
        self.project_id = conf.get_string(BigQueryWatermarkExtractor.PROJECT_ID_KEY)
        self.pagesize = conf.get_int(
            BigQueryWatermarkExtractor.PAGE_SIZE_KEY,
            BigQueryWatermarkExtractor.DEFAULT_PAGE_SIZE)
        self.filter = conf.get_string(BigQueryWatermarkExtractor.FILTER_KEY, '')

        if self.key_path:
            credentials = (
                google.oauth2.service_account.Credentials.from_service_account_file(
                    self.key_path, scopes=BigQueryWatermarkExtractor._DEFAULT_SCOPES))
        else:
            if self.cred_key:
                service_account_info = json.loads(self.cred_key)
                credentials = (
                    google.oauth2.service_account.Credentials.from_service_account_info(
                        service_account_info, scopes=BigQueryWatermarkExtractor._DEFAULT_SCOPES))
            else:
                credentials, _ = google.auth.default(scopes=BigQueryWatermarkExtractor._DEFAULT_SCOPES)

        http = httplib2.Http()
        authed_http = google_auth_httplib2.AuthorizedHttp(credentials, http=http)
        self.bigquery_service = build('bigquery', 'v2', http=authed_http, cache_discovery=False)
        self.datasets = self._retrieve_datasets()
        self.iter = iter(self._iterate_over_tables())

    def extract(self):
        # type: () -> Any
        try:
            return next(self.iter)
        except StopIteration:
            return None


    def get_scope(self):
        # type: () -> str
        return 'extractor.bigquery_watermarks'

    def _iterate_over_tables(self):
        # type: () -> Any
        for dataset in self.datasets:
            for entry in self._retrieve_tables(dataset):
                yield(entry)

    def _retrieve_datasets(self):
        # type: () -> List[DatasetRef]
        datasets = []
        for page in self._page_dataset_list_results():
            if 'datasets' not in page:
                continue

            for dataset in page['datasets']:
                dataset_ref = dataset['datasetReference']
                ref = DatasetRef(**dataset_ref)
                datasets.append(ref)

        return datasets

    def _page_dataset_list_results(self):
        # type: () -> Any
        response = self.bigquery_service.datasets().list(
            projectId=self.project_id,
            all=False,  # Do not return hidden datasets
            filter=self.filter,
            maxResults=self.pagesize).execute(
                num_retries=BigQueryWatermarkExtractor.NUM_RETRIES)

        while response:
            yield response

            if 'nextPageToken' in response:
                response = self.bigquery_service.datasets().list(
                    projectId=self.project_id,
                    all=True,
                    filter=self.filter,
                    pageToken=response['nextPageToken']).execute(
                        num_retries=BigQueryWatermarkExtractor.NUM_RETRIES)
            else:
                response = None


    def _page_table_list_results(self, dataset):
        # type: (DatasetRef) -> Any
        response = self.bigquery_service.tables().list(
            projectId=dataset.projectId,
            datasetId=dataset.datasetId,
            maxResults=self.pagesize).execute(
                num_retries=BigQueryWatermarkExtractor.NUM_RETRIES)

        while response:
            yield response

            if 'nextPageToken' in response:
                response = self.bigquery_service.tables().list(
                    projectId=dataset.projectId,
                    datasetId=dataset.datasetId,
                    maxResults=self.pagesize,
                    pageToken=response['nextPageToken']).execute(
                        num_retries=BigQueryWatermarkExtractor.NUM_RETRIES)
            else:
                response = None

    def _is_sharded_table(self, table_id):
        suffix = table_id[-8:]
        return suffix.isdigit()

    def _retrieve_tables(self, dataset):
        # type: () -> Any
        sharded_table_watermarks = {}

        for page in self._page_table_list_results(dataset):
            if 'tables' not in page:
                continue

            for table in page['tables']:
                tableRef = table['tableReference']
                table_id = tableRef['tableId']

                if self._is_sharded_table(table_id):
                    suffix = table_id[-8:]
                    prefix = table_id[:-8]

                    if prefix in sharded_table_watermarks:
                        sharded_table_watermarks[prefix]['low'] = min(sharded_table_watermarks[prefix]['low'], suffix)
                        sharded_table_watermarks[prefix]['high'] = max(sharded_table_watermarks[prefix]['high'], suffix)
                    else:
                        sharded_table_watermarks[prefix] = { 'high': suffix, 'low': suffix, 'table': table }
                else:
                    partitions = self._get_partitions(table, tableRef)
                    if not partitions: continue
                    low, high = self._get_partition_watermarks(table, tableRef, partitions)
                    yield low
                    yield high

            for prefix, td in sharded_table_watermarks.items():
                table = td['table']
                tableRef = table['tableReference']

                yield Watermark(
                    datetime.datetime.fromtimestamp(float(table['creationTime']) / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                    'bigquery',
                    tableRef['datasetId'],
                    prefix,
                    '__table__={partition_id}'.format(partition_id = td['low']),
                    part_type="low_watermark",
                    cluster=tableRef['projectId']
                )

                yield Watermark(
                    datetime.datetime.fromtimestamp(float(table['creationTime']) / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                    'bigquery',
                    tableRef['datasetId'],
                    prefix,
                    '__table__={partition_id}'.format(partition_id = td['high']),
                    part_type="high_watermark",
                    cluster=tableRef['projectId']
                )

    def _get_partitions(self, table, tableRef):
        if 'timePartitioning' not in table:
            return

        query = """
        SELECT partition_id,
        TIMESTAMP(creation_time/1000) AS creation_time
        FROM [{project}:{dataset}.{table}$__PARTITIONS_SUMMARY__]
        WHERE partition_id <> '__UNPARTITIONED__'
        AND partition_id <> '__NULL__'
        """
        body = {
            'query': query.format(project=tableRef['projectId'],dataset=tableRef['datasetId'],table=tableRef['tableId']),
            'useLegacySql': True
        }
        result = self.bigquery_service.jobs().query(projectId='rea-gcp-dataservices-dev', body=body).execute()

        if 'rows' not in result:
            return

        return [PartitionInfo(row['f'][0]['v'],row['f'][1]['v']) for row in result['rows']]

    def _get_partition_watermarks(self, table, tableRef, partitions):
        if 'field' in table['timePartitioning']:
            field = table['timePartitioning']['field']
        else:
            field = '_PARTITIONTIME'

        low = min(partitions, key = lambda t: t.partition_id)
        low_wm =  Watermark(
            datetime.datetime.fromtimestamp(float(low.epoch_created)).strftime('%Y-%m-%d %H:%M:%S'),
            'bigquery',
            tableRef['datasetId'],
            tableRef['tableId'],
            '{field}={partition_id}'.format(field=field, partition_id = low.partition_id),
            part_type="low_watermark",
            cluster=tableRef['projectId']
        )

        high = max(partitions, key = lambda t: t.partition_id)
        high_wm =  Watermark(
            datetime.datetime.fromtimestamp(float(high.epoch_created)).strftime('%Y-%m-%d %H:%M:%S'),
            'bigquery',
            tableRef['datasetId'],
            tableRef['tableId'],
            '{field}={partition_id}'.format(field=field, partition_id = high.partition_id),
            part_type="high_watermark",
            cluster=tableRef['projectId']
        )

        return low_wm, high_wm
