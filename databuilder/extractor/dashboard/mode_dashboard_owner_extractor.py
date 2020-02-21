import logging

from pyhocon import ConfigTree, ConfigFactory  # noqa: F401
from typing import Any  # noqa: F401

from databuilder import Scoped
from databuilder.extractor.base_extractor import Extractor
from databuilder.extractor.dashboard.mode_dashboard_utils import ModeDashboardUtils
from databuilder.extractor.restapi.rest_api_extractor import RestAPIExtractor, REST_API_QUERY, STATIC_RECORD_DICT
from databuilder.rest_api.rest_api_query import RestApiQuery

# CONFIG KEYS
ORGANIZATION = 'organization'
MODE_ACCESS_TOKEN = 'mode_user_token'
MODE_PASSWORD_TOKEN = 'mode_password_token'

LOGGER = logging.getLogger(__name__)


class ModeDashboardExecutionsExtractor(Extractor):
    """
    A Extractor that extracts run (execution) status and timestamp.

    """

    def init(self, conf):
        # type: (ConfigTree) -> None
        self._conf = conf

        restapi_query = self._build_restapi_query()
        self._extractor = RestAPIExtractor()
        rest_api_extractor_conf = Scoped.get_scoped_conf(conf, self._extractor.get_scope()).with_fallback(
            ConfigFactory.from_dict(
                {REST_API_QUERY: restapi_query,
                 STATIC_RECORD_DICT: {'product': 'mode'}
                 }
            )
        )
        self._extractor.init(conf=rest_api_extractor_conf)

    def extract(self):
        # type: () -> Any

        return self._extractor.extract()

    def get_scope(self):
        # type: () -> str
        return 'extractor.mode_dashboard_owner'

    def _build_restapi_query(self):
        """
        Build REST API Query. To get Mode Dashboard last execution, it needs to call three APIs (spaces API, reports
        API, and run API) joining together.
        :return: A RestApiQuery that provides Mode Dashboard execution (run)
        """
        # type: () -> RestApiQuery

        report_url_template = 'https://app.mode.com/api/{organization}/spaces/{dashboard_group_id}/reports'
        creator_url_template = 'https://app.mode.com{creator_resource_path}'

        spaces_query = ModeDashboardUtils.get_spaces_query_api(conf=self._conf)
        params = ModeDashboardUtils.get_auth_params(conf=self._conf)

        # Reports
        json_path = '(_embedded.reports[*].token) | (_embedded.reports[*]._links.creator.href)'
        field_names = ['dashboard_id', 'creator_resource_path']
        creator_resource_path_query = RestApiQuery(query_to_join=spaces_query, url=report_url_template, params=params,
                                                   json_path=json_path, field_names=field_names, skip_no_result=True,
                                                   json_path_contains_or=True)

        json_path = 'email'
        field_names = ['user_email']
        owner_email_query = RestApiQuery(query_to_join=creator_resource_path_query, url=creator_url_template, params=params,
                                         json_path=json_path, field_names=field_names, skip_no_result=True)

        return owner_email_query
