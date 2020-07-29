import logging
import html
import re

from pyhocon import ConfigTree, ConfigFactory  # noqa: F401
from typing import Any  # noqa: F401

from databuilder import Scoped

from databuilder.extractor.base_extractor import Extractor
from databuilder.extractor.dashboard.tableau.tableau_dashboard_constants import EXTERNAL_CLUSTER_NAME,\
    EXTERNAL_DATABASE_NAME
from databuilder.extractor.dashboard.tableau.tableau_dashboard_utils import TableauDashboardAuth,\
    TableauGraphQLApiExtractor, TableauDashboardUtils

from databuilder.rest_api.rest_api_query import RestApiQuery  # noqa: F401
from databuilder.rest_api.base_rest_api_query import BaseRestApiQuery  # noqa: F401

from databuilder.transformer.base_transformer import ChainedTransformer
from databuilder.transformer.dict_to_model import DictToModel, MODEL_CLASS

LOGGER = logging.getLogger(__name__)


class TableauDashboardExternalTableExtractor(Extractor):
    """
    """

    def init(self, conf):
        # type: (ConfigTree) -> None

        self._conf = conf
        self._auth = TableauDashboardAuth(self._conf)
        self.query = """query {
          databases (filter: {connectionTypeWithin: ["excel-direct", "textscan", "salesforce", "google-sheets"]}) {
            name
            connectionType
            description
            tables {
                name
            }
          }
        }"""
        self._extractor = self._build_extractor()

        transformers = []
        dict_to_model_transformer = DictToModel()
        dict_to_model_transformer.init(
            conf=Scoped.get_scoped_conf(self._conf, dict_to_model_transformer.get_scope()).with_fallback(
                ConfigFactory.from_dict(
                    {MODEL_CLASS: 'databuilder.models.table_metadata.TableMetadata'})))
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

        return 'extractor.tableau_external_table'

    def _build_extractor(self):
        """
        """
        extractor = TableauGraphQLExternalTableExtractor()
        tableau_extractor_conf = \
            Scoped.get_scoped_conf(self._conf, extractor.get_scope())\
                  .with_fallback(self._conf)\
                  .with_fallback(ConfigFactory.from_dict({}))
        extractor.init(conf=tableau_extractor_conf, auth_token=self._auth.token, query=self.query)
        return extractor


class TableauGraphQLExternalTableExtractor(TableauGraphQLApiExtractor):
    """docstring for TableauDashboardMetadataExtractor"""
    def execute(self):
        response = self.execute_query()

        for table in response['databases']:
            if table['connectionType'] in ['google-sheets', 'salesforce', 'excel-direct']:
                for downstreamTable in table['tables']:
                    data = {}
                    data['cluster'] = self._conf.get_string(EXTERNAL_CLUSTER_NAME)
                    data['database'] = TableauDashboardUtils.sanitize_database_name(html.escape(table['connectionType']))
                    data['schema'] = TableauDashboardUtils.sanitize_schema_name(html.escape(table['name']))
                    data['name'] = TableauDashboardUtils.sanitize_table_name(html.escape(downstreamTable['name']))
                    data['description'] = html.escape(table['description'])

                    yield data
            else:
                data = {}
                data['cluster'] = self._conf.get_string(EXTERNAL_CLUSTER_NAME)
                data['database'] = TableauDashboardUtils.sanitize_database_name(table['connectionType'])
                data['schema'] = self._conf.get_string(EXTERNAL_DATABASE_NAME)
                data['name'] = TableauDashboardUtils.sanitize_table_name(table['name'])
                data['description'] = html.escape(table['description'])
                yield data
