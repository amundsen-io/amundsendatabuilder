import logging

from pyhocon import ConfigTree, ConfigFactory  # noqa: F401
from requests.auth import HTTPBasicAuth
from typing import Any  # noqa: F401

from databuilder import Scoped
from databuilder.extractor.base_extractor import Extractor
from databuilder.extractor.restapi.rest_api_extractor import RestAPIExtractor, REST_API_QUERY
from databuilder.rest_api.base_rest_api_query import RestApiQuerySeed
from databuilder.rest_api.rest_api_query import RestApiQuery
from databuilder.transformer.timestamp_string_to_epoch import TimestampStringToEpoch, FIELD_NAME

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
                {REST_API_QUERY: restapi_query, }
            )
        )

        self._extractor.init(conf=rest_api_extractor_conf)
        self._timestamp_str_to_epoch_transformer = TimestampStringToEpoch()
        self._timestamp_str_to_epoch_transformer.init(
            conf=ConfigFactory.from_dict(
                {FIELD_NAME: 'execution_timestamp', }
            )
        )

    def extract(self):
        # type: () -> Any
        return self._timestamp_str_to_epoch_transformer.transform(record=self._extractor.extract())

    def get_scope(self):
        # type: () -> str
        return 'extractor.mode_dashboard_execution'

    def _build_restapi_query(self):
        """
        Build REST API Query. To get Mode Dashboard metadata, it needs to call two APIs (spaces API and reports
        API) joining together.
        :return: A RestApiQuery that provides Mode Dashboard metadata
        """
        # type: () -> RestApiQuery

        # Seed query record for next query api to join with
        seed_record = [{'organization': self._conf.get_string(ORGANIZATION)}]
        seed_query = RestApiQuerySeed(seed_record=seed_record)

        # Spaces
        url = 'https://app.mode.com/api/{organization}/spaces?filter=all'
        params = {'auth': HTTPBasicAuth(self._conf.get_string(MODE_ACCESS_TOKEN),
                                        self._conf.get_string(MODE_PASSWORD_TOKEN))}

        json_path = '_embedded.spaces[*].[token]'
        field_names = ['dashboard_group_id']
        spaces_query = RestApiQuery(query_to_join=seed_query, url=url, params=params, json_path=json_path,
                                    field_names=field_names)

        # Reports
        url = 'https://app.mode.com/api/{organization}/spaces/{dashboard_group_id}/reports'
        json_path = '(_embedded.reports[*].token) | (_embedded.reports[*]._links.last_run.href)'
        field_names = ['dashboard_id', 'last_run_resource_path']
        last_run_resource_path_query = RestApiQuery(query_to_join=spaces_query, url=url, params=params,
                                                    json_path=json_path, field_names=field_names, skip_no_result=True
                                                    , json_path_contains_or=True)

        url = 'https://app.mode.com{last_run_resource_path}'
        json_path = '[state,completed_at]'
        field_names = ['execution_state', 'execution_timestamp']
        last_run_state_query = RestApiQuery(query_to_join=last_run_resource_path_query, url=url, params=params,
                                            json_path=json_path, field_names=field_names, skip_no_result=True)

        return last_run_state_query
