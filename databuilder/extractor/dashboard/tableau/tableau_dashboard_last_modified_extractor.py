import logging

from pyhocon import ConfigFactory  # noqa: F401

from databuilder import Scoped

from databuilder.extractor.base_extractor import Extractor
from databuilder.extractor.restapi.rest_api_extractor import STATIC_RECORD_DICT

import databuilder.extractor.dashboard.tableau.tableau_dashboard_constants as const
from databuilder.extractor.dashboard.tableau.tableau_dashboard_utils import TableauDashboardAuth,\
    TableauGraphQLApiExtractor, TableauDashboardUtils

from databuilder.transformer.base_transformer import ChainedTransformer
from databuilder.transformer.dict_to_model import DictToModel, MODEL_CLASS
from databuilder.transformer.timestamp_string_to_epoch import TimestampStringToEpoch, FIELD_NAME

LOGGER = logging.getLogger(__name__)


class TableauDashboardLastModifiedExtractor(Extractor):
    """
    Extracts metadata about the time of last update for Tableau dashboards.
    For the purposes of this extractor, Tableau "workbooks" are mapped to Amundsen dashboards, and the
    top-level project in which these workbooks preside is the dashboard group. The metadata it gathers is:
        Dashboard last modified timestamp (Workbook last modified timestamp)
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
                    {MODEL_CLASS:
                     'databuilder.models.dashboard.dashboard_last_modified.DashboardLastModifiedTimestamp'})))
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

        return 'extractor.tableau_dashboard_last_modified'

    def _build_extractor(self):
        """
        Builds a TableauGraphQLApiExtractor. All data required can be retrieved with a single GraphQL call.
        :return: A TableauGraphQLApiLastModifiedExtractor that provides dashboard update metadata.
        """
        # type: () -> TableauGraphQLApiLastModifiedExtractor
        extractor = TableauGraphQLApiLastModifiedExtractor()
        tableau_extractor_conf = \
            Scoped.get_scoped_conf(self._conf, extractor.get_scope())\
                  .with_fallback(self._conf)\
                  .with_fallback(ConfigFactory.from_dict({STATIC_RECORD_DICT: {'product': 'tableau'}
                                                          }
                                                         )
                                 )
        extractor.init(conf=tableau_extractor_conf, auth_token=self._auth.token, query=self.query)
        return extractor


class TableauGraphQLApiLastModifiedExtractor(TableauGraphQLApiExtractor):
    """
    Implements the extraction-time logic for parsing the GraphQL result and transforming into a dict
    that fills the DashboardLastModifiedTimestamp model. Allows workbooks to be exlcuded based on their project.
    """
    def execute(self):
        response = self.execute_query()

        workbooks_data = [workbook for workbook in response['workbooks']
                          if workbook['projectName'] not in self._conf.get_list(TableauGraphQLApiExtractor.EXCLUDED_PROJECTS)]

        for workbook in workbooks_data:
            data = {
                'dashboard_group_id': workbook['projectName'],
                'dashboard_id': TableauDashboardUtils.sanitize_workbook_name(workbook['name']),
                'last_modified_timestamp': workbook['updatedAt'],
                'cluster': self._conf.get_string('cluster')
            }
            yield data
