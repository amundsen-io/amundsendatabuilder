import logging

from pyhocon import ConfigTree, ConfigFactory  # noqa: F401
from requests.auth import HTTPBasicAuth
from typing import Any  # noqa: F401

from databuilder.extractor.base_extractor import Extractor
from databuilder.extractor.restapi.rest_api_extractor import RestAPIExtractor, REST_API_QUERY
from databuilder.rest_api.base_rest_api_query import RestApiQuerySeed
from databuilder.rest_api.rest_api_query import RestApiQuery

# CONFIG KEYS
ORGANIZATION = 'organization'
MODE_ACCESS_TOKEN = 'mode_user_token'
MODE_PASSWORD_TOKEN = 'mode_password_token'

LOGGER = logging.getLogger(__name__)


class ModeDashboardExtractor(Extractor):

    def init(self, conf):
        # type: (ConfigTree) -> None
        self._conf = conf

        restapi_query = self._build_restapi_query()
        self._conf = self._conf.with_fallback(ConfigFactory.from_dict({REST_API_QUERY: restapi_query}))

        self._extractor = RestAPIExtractor()
        self._extractor.init(conf=self._conf)

    def extract(self):
        # type: () -> Any

        return self._extractor.extract()

    def get_scope(self):
        # type: () -> str
        return 'extractor.mode_dashboard'

    def _build_restapi_query(self):
        """
        Build REST API Query. To get Mode Dashboard metadata, it needs to call two APIs (spaces API and reports
        API) joining together.
        :return: A RestApiQuery that provides Mode Dashboard metadata
        """
        # type: () -> RestApiQuery
        seed_record = [{'organization': self._conf.get_string(ORGANIZATION)}]
        seed_query = RestApiQuerySeed(seed_record=seed_record)

        # Spaces
        url = 'https://app.mode.com/api/{organization}/spaces?filter=all'
        params = {'auth': HTTPBasicAuth(self._conf.get_string(MODE_ACCESS_TOKEN),
                                        self._conf.get_string(MODE_PASSWORD_TOKEN))}

        json_path = '_embedded.spaces[*].[token,name,description]'
        field_names = ['space_token', 'space_name', 'space_description']
        spaces_query = RestApiQuery(query_to_join=seed_query, url=url, params=params, json_path=json_path,
                                    field_names=field_names)

        # Report metadata
        url = 'https://app.mode.com/api/{organization}/spaces/{space_token}/reports'
        json_path = '_embedded.reports[*].[token,name,description,created_at,updated_at]'
        field_names = ['report_token', 'report_name', 'report_description', 'report_created_at', 'report_updated_at']
        reports_query = RestApiQuery(query_to_join=spaces_query, url=url, params=params, json_path=json_path,
                                     field_names=field_names)

        return reports_query
