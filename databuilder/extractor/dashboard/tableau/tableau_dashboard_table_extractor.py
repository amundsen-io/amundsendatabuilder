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

        # TODO: implement this
        self._extractor = None

    def extract(self):
        # type: () -> Any

        record = self._extractor.extract()
        if not record:
            return None

        return self._transformer.transform(record=record)

    def get_scope(self):
        # type: () -> str

        return 'extractor.tableau'
