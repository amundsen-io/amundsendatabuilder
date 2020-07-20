import logging

from pyhocon import ConfigTree, ConfigFactory  # noqa: F401
from typing import Any  # noqa: F401

from databuilder.extractor.base_extractor import Extractor
from databuilder.extractor.dashboard.tableau.tableau_dashboard_utils import TableauDashboardAuth

from databuilder.rest_api.rest_api_query import RestApiQuery  # noqa: F401
from databuilder.rest_api.base_rest_api_query import BaseRestApiQuery  # noqa: F401

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
