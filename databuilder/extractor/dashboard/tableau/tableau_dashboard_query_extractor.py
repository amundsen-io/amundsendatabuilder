import logging

from pyhocon import ConfigTree, ConfigFactory  # noqa: F401
from typing import Any  # noqa: F401

from databuilder import Scoped

from databuilder.extractor.base_extractor import Extractor
from databuilder.extractor.restapi.rest_api_extractor import STATIC_RECORD_DICT

import databuilder.extractor.dashboard.tableau.tableau_dashboard_constants as const
from databuilder.extractor.dashboard.tableau.tableau_dashboard_utils import TableauDashboardAuth,\
    TableauGraphQLApiExtractor, TableauDashboardUtils

from databuilder.rest_api.rest_api_query import RestApiQuery  # noqa: F401
from databuilder.rest_api.base_rest_api_query import BaseRestApiQuery  # noqa: F401

from databuilder.transformer.base_transformer import ChainedTransformer
from databuilder.transformer.dict_to_model import DictToModel, MODEL_CLASS

LOGGER = logging.getLogger(__name__)


class TableauDashboardQueryExtractor(Extractor):
    """
    Extracts metadata about the queries associated with Tableau workbooks.
    In terms of Tableau's Metadata API, these queries are called "custom SQL tables".
    However, not every workbook uses custom SQL queries, and most are built with a mixture of using the
    datasource fields directly and various "calculated" columns.
    This extractor iterates through one query at a time, yielding a new relationship for every downstream
    workbook that uses the query.
    """

    API_VERSION = const.API_VERSION
    TABLEAU_HOST = const.TABLEAU_HOST
    SITE_NAME = const.SITE_NAME
    TABLEAU_ACCESS_TOKEN_NAME = const.TABLEAU_ACCESS_TOKEN_NAME
    TABLEAU_ACCESS_TOKEN_SECRET = const.TABLEAU_ACCESS_TOKEN_SECRET
    EXCLUDED_PROJECTS = const.EXCLUDED_PROJECTS
    EXTERNAL_CLUSTER_NAME = const.EXTERNAL_CLUSTER_NAME
    EXTERNAL_SCHEMA_NAME = const.EXTERNAL_SCHEMA_NAME
    EXTERNAL_TABLE_TYPES = const.EXTERNAL_TABLE_TYPES
    CLUSTER = const.CLUSTER
    DATABASE = const.DATABASE

    def init(self, conf):
        # type: (ConfigTree) -> None

        self._conf = conf
        self._auth = TableauDashboardAuth(self._conf)
        self.query = """query {
          customSQLTables {
            id
            name
            query
            downstreamWorkbooks {
              name
              projectName
            }
          }
        }"""

        self._extractor = self._build_extractor()

        transformers = []
        dict_to_model_transformer = DictToModel()
        dict_to_model_transformer.init(
            conf=Scoped.get_scoped_conf(self._conf, dict_to_model_transformer.get_scope()).with_fallback(
                ConfigFactory.from_dict(
                    {MODEL_CLASS: 'databuilder.models.dashboard.dashboard_query.DashboardQuery'})))
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

        return 'extractor.tableau_dashboard_query'

    def _build_extractor(self):
        """
        Builds a TableauGraphQLApiQueryExtractor. All data required can be retrieved with a single GraphQL call.
        :return: A TableauGraphQLApiQueryExtractor that provides dashboard query metadata.
        """
        extractor = TableauGraphQLApiQueryExtractor()
        tableau_extractor_conf = \
            Scoped.get_scoped_conf(self._conf, extractor.get_scope())\
                  .with_fallback(self._conf)\
                  .with_fallback(ConfigFactory.from_dict({STATIC_RECORD_DICT: {'product': 'tableau'}
                                                          }
                                                         )
                                 )
        extractor.init(conf=tableau_extractor_conf, auth_token=self._auth.token, query=self.query)
        return extractor


class TableauGraphQLApiQueryExtractor(TableauGraphQLApiExtractor):
    """
    Implements the extraction-time logic for parsing the GraphQL result and transforming into a dict
    that fills the DashboardQuery model. Allows workbooks to be exlcuded based on their project.
    """
    def execute(self):
        response = self.execute_query()

        for query in response['customSQLTables']:
            for workbook in query['downstreamWorkbooks']:
                if workbook['projectName'] not in self._conf.get_list(TableauGraphQLApiExtractor.EXCLUDED_PROJECTS):
                    data = {
                        'dashboard_group_id': workbook['projectName'],
                        'dashboard_id': TableauDashboardUtils.sanitize_workbook_name(workbook['name']),
                        'query_name': query['name'],
                        'query_id': query['id'],
                        'query_text': query['query'],
                        'cluster': self._conf.get_string(TableauGraphQLApiExtractor.CLUSTER)
                    }
                    yield data
