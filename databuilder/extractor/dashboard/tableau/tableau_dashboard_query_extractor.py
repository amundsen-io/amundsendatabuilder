import logging
import html

from pyhocon import ConfigTree, ConfigFactory  # noqa: F401
from typing import Any  # noqa: F401

from databuilder import Scoped

from databuilder.extractor.base_extractor import Extractor
from databuilder.extractor.dashboard.tableau.tableau_dashboard_utils import TableauDashboardAuth,\
    TableauGraphQLApiExtractor
from databuilder.extractor.restapi.rest_api_extractor import STATIC_RECORD_DICT
from databuilder.extractor.dashboard.tableau.tableau_dashboard_constants import EXCLUDED_PROJECTS

from databuilder.rest_api.rest_api_query import RestApiQuery  # noqa: F401
from databuilder.rest_api.base_rest_api_query import BaseRestApiQuery  # noqa: F401

from databuilder.transformer.base_transformer import ChainedTransformer
from databuilder.transformer.dict_to_model import DictToModel, MODEL_CLASS

LOGGER = logging.getLogger(__name__)


class TableauDashboardQueryExtractor(Extractor):
    """
    """

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

        return 'extractor.tableau'

    def _build_extractor(self):
        """
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
    """docstring for TableauDashboardMetadataExtractor"""
    def execute(self):
        response = self.execute_query()

        for query in response['customSQLTables']:
            for workbook in query['downstreamWorkbooks']:
                if workbook['projectName'] not in self._conf.get_list(EXCLUDED_PROJECTS):
                    data = {}

                    data['dashboard_group_id'] = html.escape(str(workbook['projectName']))
                    data['dashboard_id'] = html.escape(str(workbook['name']))
                    data['query_name'] = query['name']
                    data['query_id'] = query['id']
                    data['query_text'] = query['query']
                    data['product'] = 'tableau'
                    data['cluster'] = 'gold'

                    yield data
