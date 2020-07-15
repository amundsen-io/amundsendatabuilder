import logging
import requests
import html
import re

from pyhocon import ConfigTree, ConfigFactory  # noqa: F401
from typing import Any  # noqa: F401

from databuilder import Scoped

from databuilder.extractor.base_extractor import Extractor
from databuilder.extractor.dashboard.tableau.tableau_dashboard_utils import TableauDashboardUtils, TableauDashboardAuth, TableauGraphQLApiExtractor
from databuilder.extractor.restapi.rest_api_extractor import STATIC_RECORD_DICT

from databuilder.extractor.dashboard.tableau.tableau_dashboard_constants import *

from databuilder.rest_api.rest_api_query import RestApiQuery  # noqa: F401
from databuilder.rest_api.base_rest_api_query import BaseRestApiQuery  # noqa: F401
from databuilder.rest_api.base_rest_api_query import EmptyRestApiQuerySeed
from databuilder.rest_api.base_rest_api_query import RestApiQuerySeed
from databuilder.rest_api.tableau.tableau_paginated_rest_api_query import TableauPaginatedRestApiQuery

from databuilder.transformer.base_transformer import ChainedTransformer
from databuilder.transformer.dict_to_model import DictToModel, MODEL_CLASS
from databuilder.transformer.template_variable_substitution_transformer import \
    TemplateVariableSubstitutionTransformer, TEMPLATE, FIELD_NAME as VAR_FIELD_NAME
from databuilder.transformer.timestamp_string_to_epoch import TimestampStringToEpoch, FIELD_NAME

LOGGER = logging.getLogger(__name__)


class TableauDashboardLastModifiedExtractor(Extractor):
    """
    """

    def init(self, conf):
        # type: (ConfigTree) -> None

        print(conf)
        self._conf = conf
        self._auth = TableauDashboardAuth(self._conf)
        self.query = """query {
            workbooks {
                id
                name
                projectName
                updatedAt
            }
        }"""

        self._extractor = self._build_extractor()

        transformers = []
        timestamp_str_to_epoch_transformer = TimestampStringToEpoch()
        timestamp_str_to_epoch_transformer.init(
            conf=Scoped.get_scoped_conf(self._conf, timestamp_str_to_epoch_transformer.get_scope()).with_fallback(
                ConfigFactory.from_dict({FIELD_NAME: 'last_modified_timestamp', })))
        transformers.append(timestamp_str_to_epoch_transformer)

        dict_to_model_transformer = DictToModel()
        dict_to_model_transformer.init(
            conf=Scoped.get_scoped_conf(self._conf, dict_to_model_transformer.get_scope()).with_fallback(
                ConfigFactory.from_dict(
                    {MODEL_CLASS: 'databuilder.models.dashboard.dashboard_last_modified.DashboardLastModifiedTimestamp'})))
        transformers.append(dict_to_model_transformer)

        self._transformer = ChainedTransformer(transformers=transformers)


    def extract(self):
        # type: () -> Any

        record = self._extractor.extract()
        if not record:
            return None

        return self._transformer.transform(record=record)

    def get_scope(self):
        # type: () -> str

        return 'extractor.tableau'

    def _build_extractor(self):
        """
        """
        extractor = TableauGraphQLApiLastModifiedExtractor()
        tableau_extractor_conf = \
            Scoped.get_scoped_conf(self._conf, extractor.get_scope())\
                  .with_fallback(self._conf)\
                  .with_fallback(ConfigFactory.from_dict({
                                                          STATIC_RECORD_DICT: {'product': 'tableau'}
                                                          }
                                                         )
                                 )
        extractor.init(conf=tableau_extractor_conf, auth_token=self._auth.token, query=self.query)
        return extractor


class TableauGraphQLApiLastModifiedExtractor(TableauGraphQLApiExtractor):
    """docstring for TableauDashboardMetadataExtractor"""
    def execute(self):
        response = self.execute_query()

        for workbook in [workbook for workbook in response['workbooks'] if workbook['projectName'] not in ["ZZZ - Archived", "WIP", "Tableau Samples"]]:
            data = {}
            # import ipdb
            # ipdb.set_trace()
            data['dashboard_group_id'] = workbook['projectName']
            data['dashboard_id'] = html.escape(str(workbook['name']))
            data['last_modified_timestamp'] = workbook['updatedAt']
            data['product'] = 'tableau'
            data['cluster'] = 'gold'

            yield data