import logging

from pyhocon import ConfigTree, ConfigFactory  # noqa: F401
from typing import Any  # noqa: F401

from databuilder import Scoped

from databuilder.extractor.base_extractor import Extractor
from databuilder.extractor.restapi.rest_api_extractor import STATIC_RECORD_DICT
from databuilder.extractor.dashboard.tableau.tableau_dashboard_constants import EXCLUDED_PROJECTS, TABLEAU_HOST
from databuilder.extractor.dashboard.tableau.tableau_dashboard_utils import TableauDashboardAuth,\
    TableauGraphQLApiExtractor, TableauDashboardUtils

from databuilder.transformer.base_transformer import ChainedTransformer
from databuilder.transformer.dict_to_model import DictToModel, MODEL_CLASS
from databuilder.transformer.timestamp_string_to_epoch import TimestampStringToEpoch, FIELD_NAME

LOGGER = logging.getLogger(__name__)


class TableauDashboardExtractor(Extractor):
    """
    Extracts core metadata about Tableau "dashboards".
    For the purposes of this extractor, Tableau "workbooks" are mapped to Amundsen dashboards, and the
    top-level project in which these workbooks preside is the dashboard group. The metadata it gathers is:
        Dashboard name (Workbook name)
        Dashboard description (Workbook description)
        Dashboard creation timestamp (Workbook creationstamp)
        Dashboard group name (Workbook top-level folder name)
    As with all the Tableau extractors, uses the Metadata API: https://help.tableau.com/current/api/metadata_api/en-us/index.html
    """

    def init(self, conf):
        # type: (ConfigTree) -> None

        self._conf = conf
        self._auth = TableauDashboardAuth(self._conf)
        self.query = """query {
            workbooks {
                id
                name
                createdAt
                description
                projectName
                projectVizportalUrlId
                vizportalUrlId
            }
        }"""

        self._extractor = self._build_extractor()

        transformers = []
        timestamp_str_to_epoch_transformer = TimestampStringToEpoch()
        timestamp_str_to_epoch_transformer.init(
            conf=Scoped.get_scoped_conf(self._conf, timestamp_str_to_epoch_transformer.get_scope()).with_fallback(
                ConfigFactory.from_dict({FIELD_NAME: 'created_timestamp', })))
        transformers.append(timestamp_str_to_epoch_transformer)

        dict_to_model_transformer = DictToModel()
        dict_to_model_transformer.init(
            conf=Scoped.get_scoped_conf(self._conf, dict_to_model_transformer.get_scope()).with_fallback(
                ConfigFactory.from_dict(
                    {MODEL_CLASS: 'databuilder.models.dashboard.dashboard_metadata.DashboardMetadata'})))
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

        return 'extractor.tableau_dashboard_metadata'

    def _build_extractor(self):
        """
        Builds a TableauGraphQLApiMetadataExtractor. All data required can be retrieved with a single GraphQL call.
        :return: A TableauGraphQLApiMetadataExtractor that provides core dashboard metadata.
        """
        # type: () -> TableauGraphQLApiMetadataExtractor
        extractor = TableauGraphQLApiMetadataExtractor()
        tableau_extractor_conf = \
            Scoped.get_scoped_conf(self._conf, extractor.get_scope())\
                  .with_fallback(self._conf)\
                  .with_fallback(ConfigFactory.from_dict({STATIC_RECORD_DICT: {'product': 'tableau'}
                                                          }
                                                         )
                                 )
        extractor.init(conf=tableau_extractor_conf, auth_token=self._auth.token, query=self.query)
        return extractor


class TableauGraphQLApiMetadataExtractor(TableauGraphQLApiExtractor):
    """
    Implements the extraction-time logic for parsing the GraphQL result and transforming into a dict
    that fills the DashboardMetadata model. Allows workbooks to be exlcuded based on their project.
    """
    def execute(self):
        response = self.execute_query()

        workbooks_data = [workbook for workbook in response['workbooks']
                          if workbook['projectName'] not in self._conf.get_list(EXCLUDED_PROJECTS)]

        for workbook in workbooks_data:
            data = {
                'dashboard_group': workbook['projectName'],
                'dashboard_name': TableauDashboardUtils.sanitize_workbook_name(workbook['name']),
                'description': workbook.get('description', ''),
                'created_timestamp': workbook['createdAt'],
                'dashboard_group_url': 'https://{}/#/projects/{}'.format(
                    self._conf.get(TABLEAU_HOST),
                    workbook['projectVizportalUrlId']
                ),
                'dashboard_url': 'https://{}/#/workbooks/{}/views'.format(
                    self._conf.get(TABLEAU_HOST),
                    workbook['vizportalUrlId']
                ),
                'cluster': self._conf.get_string('cluster')
            }
            yield data
