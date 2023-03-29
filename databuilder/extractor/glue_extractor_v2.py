# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

from typing import (
    Any, Dict, Iterator, List, Union,
)

import boto3
import json
from pyhocon import ConfigFactory, ConfigTree

from databuilder.extractor.base_extractor import Extractor
from databuilder.models.table_metadata import ColumnMetadata, TableMetadata
from databuilder.models.table_owner import TableOwner
from deltalake import DeltaTable, PyDeltaTableError


class GlueExtractorV2(Extractor):
    """
    Extracts tables and columns metadata from AWS Glue metastore
    """

    CLUSTER_KEY = 'cluster'
    FILTER_KEY = 'filters'
    MAX_RESULTS_KEY = 'max_results'
    RESOURCE_SHARE_TYPE = 'resource_share_type'
    REGION_NAME_KEY = "region"
    PARTITION_BADGE_LABEL_KEY = "partition_badge_label"

    DEFAULT_CONFIG = ConfigFactory.from_dict({
        CLUSTER_KEY: 'gold',
        FILTER_KEY: None,
        MAX_RESULTS_KEY: 500,
        RESOURCE_SHARE_TYPE: "ALL",
        REGION_NAME_KEY: None,
        PARTITION_BADGE_LABEL_KEY: None,
    })

    def init(self, conf: ConfigTree) -> None:
        conf = conf.with_fallback(GlueExtractorV2.DEFAULT_CONFIG)
        self._cluster = conf.get_string(GlueExtractorV2.CLUSTER_KEY)
        self._filters = conf.get(GlueExtractorV2.FILTER_KEY)
        self._max_results = conf.get(GlueExtractorV2.MAX_RESULTS_KEY)
        self._resource_share_type = conf.get(GlueExtractorV2.RESOURCE_SHARE_TYPE)
        self._region_name = conf.get(GlueExtractorV2.REGION_NAME_KEY)
        self._partition_badge_label = conf.get(GlueExtractorV2.PARTITION_BADGE_LABEL_KEY)
        if self._region_name is not None:
            self._glue = boto3.client('glue', region_name=self._region_name)
        else:
            self._glue = boto3.client('glue')
        self._extract_iter: Union[None, Iterator] = None

    def extract(self) -> Union[TableMetadata, TableOwner, None]:
        if not self._extract_iter:
            self._extract_iter = self._get_extract_iter()
        try:
            return next(self._extract_iter)
        except StopIteration:
            return None

    def get_scope(self) -> str:
        return 'extractor.glue'

    def _get_schema_columns(self, dt) -> list:
        schema_json = json.loads(dt.schema().to_json())
        schema_fields = schema_json['fields']
        schema_columns = []
        for field in schema_fields:
            name = field['name']
            column_type = field['type']
            while 'type' in column_type:
                column_type = column_type['type']
            schema_columns.append({"Name": name, "Type": column_type})
        return schema_columns

    def _get_extract_iter(self) -> Iterator[Union[TableMetadata,TableOwner]]:
        """
        It gets all tables and yields TableMetadata
        :return:
        """
        for row in self._get_raw_extract_iter():
            columns, i = [], 0

            if 'StorageDescriptor' not in row:
                continue

            # use deltatable to get schema info
            location = row['StorageDescriptor']['Location']
            delta_table = {}
            storage_options={"AWS_REGION": "us-west-2"}

            try:
                delta_table = DeltaTable(location, storage_options=storage_options, without_files=True)
            except PyDeltaTableError:
                try:
                    storage_options={"AWS_REGION": "us-east-1"}
                    delta_table = DeltaTable(location, storage_options=storage_options, without_files=True)
                except PyDeltaTableError:
                    try:
                        storage_options={"AWS_REGION": "eu-west-1"}
                        delta_table = DeltaTable(location, storage_options=storage_options, without_files=True)
                    except PyDeltaTableError:
                        continue

            dt_columns = self._get_schema_columns(delta_table)

            for column in dt_columns:
                columns.append(ColumnMetadata(
                    name=column["Name"],
                    description="",
                    col_type=column["Type"],
                    sort_order=i,
                ))
                i += 1

            for column in row.get('PartitionKeys', []):
                columns.append(ColumnMetadata(
                    name=column["Name"],
                    description=column.get("Comment"),
                    col_type=column["Type"],
                    sort_order=i,
                    badges=[self._partition_badge_label] if self._partition_badge_label else None,
                ))
                i += 1
            if 'Owner' in row:
                yield TableOwner(
                    'glue',
                    row['DatabaseName'],
                    row['Name'],
                    [row['Owner']],
                    self._cluster
                )
                tags = row['Owner']
            else:
                tags = None

            yield TableMetadata(
                'glue',
                self._cluster,
                row['DatabaseName'],
                row['Name'],
                row.get('Description') or row.get('Parameters', {}).get('comment'),
                columns,
                row.get('TableType') == 'VIRTUAL_VIEW',
                tags
            )

    def _get_raw_extract_iter(self) -> Iterator[Dict[str, Any]]:
        """
        Provides iterator of results row from glue client
        :return:
        """
        tables = self._search_tables()
        return iter(tables)

    def _search_tables(self) -> List[Dict[str, Any]]:
        tables = []
        kwargs = {}
        if self._filters is not None:
            kwargs['Filters'] = self._filters
            kwargs['MaxResults'] = self._max_results
        if self._resource_share_type:
            kwargs['ResourceShareType'] = self._resource_share_type
        data = self._glue.search_tables(**kwargs)
        tables += data['TableList']
        while 'NextToken' in data:
            token = data['NextToken']
            kwargs['NextToken'] = token
            data = self._glue.search_tables(**kwargs)
            tables += data['TableList']
        return tables
