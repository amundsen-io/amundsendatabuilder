import logging
import requests
import uuid

from pyhocon import ConfigTree, ConfigFactory  # noqa: F401
from typing import Any  # noqa: F401

from databuilder import Scoped

from databuilder.extractor.base_extractor import Extractor
from databuilder.extractor.dashboard.tableau.tableau_dashboard_utils import TableauDashboardUtils, TableauDashboardAuth

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


class TableauDashboardTableExtractor(Extractor):
    """
    """

    def init(self, conf):
        # type: (ConfigTree) -> None

        self._conf = conf

        self._auth = TableauDashboardAuth(self._conf)

        self._extractor = TableauDashboardUtils.create_tableau_metadata_extractor(self._conf, self._auth.token)

        record = self._extractor.extract()

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

        return 'extractor.tableau'

    def _get_top_level_projects(self):
        projects_url = 'https://{tableau_host}/api/{api_version}/sites/{site_id}/projects'\
        .format(tableau_host=self._conf.get_string(TABLEAU_HOST), api_version=self._conf.get_string(API_VERSION), site_id=self._auth._site_id)

        headers = {
            "X-Tableau-Auth": self._auth._current_token,
            "Accept": "application/json"
        }
        params = {
            "headers": headers,
            "verify": False
        }

        response = requests.get(url=projects_url, **params)

        project_names = [project['name'] for project in response.json()['projects']['project']]
        name_id_map = {}
        for name in project_names:
            name_id_map[name] = str(uuid.uuid4())

        return name_id_map
