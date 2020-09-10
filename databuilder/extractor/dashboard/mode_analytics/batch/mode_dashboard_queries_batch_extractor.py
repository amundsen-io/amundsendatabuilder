# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import logging

from databuilder.extractor.dashboard.mode_analytics.mode_dashboard_utils import ModeDashboardUtils
from databuilder.extractor.dashboard.mode_analytics.\
    mode_dashboard_queries_extractor import ModeDashboardQueriesExtractor
from databuilder.rest_api.mode_analytics.mode_paginated_rest_api_query import ModePaginatedRestApiQuery
from databuilder.rest_api.rest_api_query import RestApiQuery


LOGGER = logging.getLogger(__name__)


class ModeDashboardQueriesExtractor(ModeDashboardQueriesExtractor):
    """
    A Extractor that extracts Query information using Batch API

    """
    # config to include the charts from all space
    INCLUDE_ALL_SPACE = 'include_all_space'

    def get_scope(self) -> str:
        return 'extractor.mode_dashboard_query_batch'

    def _build_restapi_query(self) -> RestApiQuery:
        """
        Build REST API Query for gathering query info with batch API
        """
        params = ModeDashboardUtils.get_auth_params(conf=self._conf, discover_auth=True)

        spaces_query = ModeDashboardUtils.get_spaces_query_api(conf=self._conf)

        # Reports
        # https://mode.com/developer/api-reference/analytics/reports/#listReportsInSpace
        url = 'https://app.mode.com/api/{organization}/spaces/{dashboard_group_id}/reports'
        json_path = '(_embedded.reports[*].token)'
        field_names = ['dashboard_id']
        reports_query = ModePaginatedRestApiQuery(query_to_join=spaces_query, url=url, params=params,
                                                  json_path=json_path, field_names=field_names, skip_no_result=True)

        queries_url_template = 'http://app.mode.com/batch/{organization}/queries'
        if self._conf.get_bool(ModeDashboardQueriesExtractor.INCLUDE_ALL_SPACE, default=False):
            queries_url_template += '?include_spaces=all'
        json_path = 'queries[*].[token,name,raw_query]'
        field_names = ['query_id', 'query_name', 'query_text']
        query_names_query = ModePaginatedRestApiQuery(query_to_join=reports_query,
                                                      url=queries_url_template,
                                                      params=params,
                                                      json_path=json_path,
                                                      pagination_json_path=json_path,
                                                      field_names=field_names,
                                                      skip_no_result=True)

        return query_names_query
