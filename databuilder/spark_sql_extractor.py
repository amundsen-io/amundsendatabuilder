# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import importlib
import logging

from databuilder.extractor.base_extractor import Extractor
from pyhocon import ConfigFactory, ConfigTree  # noqa: F401
from pyspark.sql import SparkSession
from typing import Iterator, Union, List, Dict, Optional, Any  # noqa: F401


LOGGER = logging.getLogger(__name__)


class SparkSQLExtractor(Extractor):
    """
    Run Spark SQL commands and extract output
    This requires a spark session to run that has a hive metastore populated with all of the delta tables
    that you are interested in.
    """
    # CONFIG KEYS
    MODEL_CLASS = "model_class"
    SQL_QUERY = 'extract_sql'
    DEFAULT_CONFIG = ConfigFactory.from_dict({SQL_QUERY: None, MODEL_CLASS: None})
    spark = None
    query = None

    def init(self, conf: ConfigTree) -> None:
        self.conf = conf.with_fallback(SparkSQLExtractor.DEFAULT_CONFIG)
        self._extract_iter = None  # type: Union[None, Iterator]
        query = self.conf.get_string(SparkSQLExtractor.SQL_QUERY)
        if query:
            self.query = query
        model_class = self.conf.get(SparkSQLExtractor.MODEL_CLASS)
        if model_class:
            if type(model_class) == str:

                module_name, class_name = model_class.rsplit(".", 1)
                mod = importlib.import_module(module_name)
                self.model_class = getattr(mod, class_name)
            else:
                self.model_class = model_class
        # Start loading data if a query is provided and spark is already configured
        if self.spark and self.query:
            self._extract_iter = self._get_extract_iter()

    def set_spark(self, spark: SparkSession) -> None:
        self.spark = spark

    def set_query(self, query: str) -> None:
        self.query = query

    def extract(self) -> Any:
        if not self._extract_iter:
            self._extract_iter = self._get_extract_iter()
        try:
            return next(self._extract_iter)
        except StopIteration:

            return None

    def get_scope(self) -> str:
        return 'extractor.spark_sql'

    def _get_extract_iter(self) -> Iterator[Any]:
        """
        Extracts the result of the query
        The spark session must be set for the extractor before extraction.
        """
        def generator():
            for row in rows:
                row_dict = row.asDict()
                if hasattr(self, 'model_class'):
                    yield self.model_class(**row_dict)
                else:
                    yield row_dict
        if self.spark is None:
            LOGGER.error("Spark session is not assigned to Spark SQL Extractor, cannot extract")
            return
        if self.query is None:
            LOGGER.error("Query is not assigned to Spark SQL Extractor, nothing to extract")
            return
        try:
            rows = self.spark.sql(self.query).collect()
        except Exception as e:
            LOGGER.error(e)
            return

        return generator()
