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


class TableauDashboardTableExtractor(Extractor):
    """
    Extracts metadata about the tables associated with Tableau workbooks.
    It can handle both "regular" database tables as well as "external" tables
    (see TableauExternalTableExtractor for more info on external tables).
    Assumes that the nodes for both the dashboard and the table have already been created.
    """

    API_VERSION = const.API_VERSION
    TABLEAU_HOST = const.TABLEAU_HOST
    SITE_NAME = const.SITE_NAME
    TABLEAU_ACCESS_TOKEN_NAME = const.TABLEAU_ACCESS_TOKEN_NAME
    TABLEAU_ACCESS_TOKEN_SECRET = const.TABLEAU_ACCESS_TOKEN_SECRET
    EXCLUDED_PROJECTS = const.EXCLUDED_PROJECTS
    EXTERNAL_CLUSTER_NAME = const.EXTERNAL_CLUSTER_NAME
    EXTERNAL_SCHEMA_NAME = const.EXTERNAL_SCHEMA_NAME
    CLUSTER = const.CLUSTER
    DATABASE = const.DATABASE

    def init(self, conf):
        # type: (ConfigTree) -> None

        self._conf = conf
        self._auth = TableauDashboardAuth(self._conf)
        self.query = """query {
          workbooks {
            name
            projectName
            upstreamTables {
              name
              schema
              database {
                name
                connectionType
              }
            }
          }
        }"""
        self._extractor = self._build_extractor()

        transformers = []
        dict_to_model_transformer = DictToModel()
        dict_to_model_transformer.init(
            conf=Scoped.get_scoped_conf(self._conf, dict_to_model_transformer.get_scope()).with_fallback(
                ConfigFactory.from_dict(
                    {MODEL_CLASS: 'databuilder.models.dashboard.dashboard_table.DashboardTable'})))
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

        return 'extractor.tableau_dashboard_table'

    def _build_extractor(self):
        """
        Builds a TableauGraphQLDashboardTableExtractor. All data required can be retrieved with a single GraphQL call.
        :return: A TableauGraphQLDashboardTableExtractor that creates dashboard <> table relationships.
        """
        extractor = TableauGraphQLDashboardTableExtractor()
        tableau_extractor_conf = \
            Scoped.get_scoped_conf(self._conf, extractor.get_scope())\
                  .with_fallback(self._conf)\
                  .with_fallback(ConfigFactory.from_dict({STATIC_RECORD_DICT: {'product': 'tableau'}
                                                          }
                                                         )
                                 )
        extractor.init(conf=tableau_extractor_conf, auth_token=self._auth.token, query=self.query)
        return extractor


class TableauGraphQLDashboardTableExtractor(TableauGraphQLApiExtractor):
    """
    Implements the extraction-time logic for parsing the GraphQL result and transforming into a dict
    that fills the DashboardTable model. Allows workbooks to be exlcuded based on their project.
    """
    def execute(self):
        response = self.execute_query()

        workbooks_data = [workbook for workbook in response['workbooks']
                          if workbook['projectName'] not in self._conf.get_list(TableauGraphQLApiExtractor.EXCLUDED_PROJECTS)]

        for workbook in workbooks_data:
            data = {
                'dashboard_group_id': workbook['projectName'],
                'dashboard_id': TableauDashboardUtils.sanitize_workbook_name(workbook['name']),
                'cluster': self._conf.get_string("cluster"),
                'table_ids': []
            }

            for table in workbook['upstreamTables']:
                table_id_format = "{database}://{cluster}.{schema}/{table}"

                # external tables have no schema, so they must be parsed differently
                # see TableauExternalTableExtractor for more specifics
                if table['schema'] != "":
                    cluster, database = self._conf.get_string(TableauGraphQLApiExtractor.CLUSTER), self._conf.get_string(TableauGraphQLApiExtractor.DATABASE)

                    # Tableau sometimes incorrectly assigns the "schema" value incorrectly
                    # based on how the datasource connection is used in a workbook.
                    # It will hide the REAL schema in the table name, like "real_schema.real_table",
                    # with a "schema" value of "fake_schema". In every case discovered so far, the "schema"
                    # value is incorrect, so when this happens, the "inner" schema is used instead.
                    if "." in table['name']:
                        schema, name = table['name'].split(".")
                    else:
                        schema, name = table['schema'], table['name']
                    schema, name = TableauDashboardUtils.sanitize_schema_name(schema), TableauDashboardUtils.sanitize_table_name(name)
                else:
                    cluster = self._conf.get_string(TableauGraphQLApiExtractor.EXTERNAL_CLUSTER_NAME)
                    database = TableauDashboardUtils.sanitize_database_name(
                        table['database']['connectionType']
                    )
                    schema = TableauDashboardUtils.sanitize_schema_name(table['database']['name'])
                    name = TableauDashboardUtils.sanitize_table_name(table['name'])

                table_id = table_id_format.format(
                    database=database,
                    cluster=cluster,
                    schema=schema,
                    table=name,
                )
                data['table_ids'].append(table_id)

            yield data
