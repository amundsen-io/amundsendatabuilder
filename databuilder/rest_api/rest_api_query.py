import copy
import logging

import requests
from jsonpath_rw import parse
from retrying import retry
from typing import List, Dict, Any  # noqa: F401

from databuilder.rest_api.base_rest_api_query import BaseRestApiQuery

LOGGER = logging.getLogger(__name__)


class RestApiQuery(BaseRestApiQuery):

    def __init__(self,
                 query_to_join,  # type: BaseRestApiQuery
                 url,  # type: str
                 params,  # type: Dict[str, Any]
                 json_path,  # type: str
                 field_names,  # type: List[str]
                 coalesce=False  # type: bool
                 ):
        self._inner_rest_api_query = query_to_join
        self._url = url
        self._params = params
        self._json_path = json_path
        self._jsonpath_expr = parse(self._json_path)

        self._coalesce = coalesce
        self._field_names = field_names
        self._more_pages = False

        if self._coalesce and len(self._field_names) > 1:
            raise Exception('Cannot have multiple fields performing coalesce')

    def execute(self):
        self._authenticate()

        for record_dict in self._inner_rest_api_query.execute():

            first_try = True  # To control pagination. Always pass the while loop on the first try
            while first_try or self._more_pages:
                first_try = False

                url = self._preprocess_url(record=record_dict)
                response = self._send_request(url=url)

                response_json = response.json()  # type: Dict[str, Any]
                result_list = [match.value for match in self._jsonpath_expr.find(response_json)]  # type: List[Any]

                if not result_list:
                    LOGGER.warning('No result from URL: {url}  , JSONPATH: {json_path} , response payload: {response}' \
                                   .format(url=self._url, json_path=self._json_path, response=response_json))
                    # TODO: configure if we want to fail or skip
                    yield copy.deepcopy(record_dict)

                if self._coalesce:
                    record_dict = copy.deepcopy(record_dict)
                    record_dict[self._field_name] = result_list
                    yield record_dict

                while result_list:
                    record_dict = copy.deepcopy(record_dict)
                    for field_name in self._field_names:
                        record_dict[field_name] = result_list.pop(0)
                    yield record_dict

                self._post_process(response)

    def _preprocess_url(self,
                        record,  # type: Dict[str, Any]
                        ):
        return self._url.format(**record)

    @retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000, wait_exponential_max=10000)
    def _send_request(self,
                      url  # type: str
                      ):
        # type: (...) -> requests.Response
        LOGGER.info('Calling URL {}'.format(url))
        response = requests.get(url, **self._params)
        response.raise_for_status()
        return response

    def _post_process(self,
                      response,  # type: requests.Response
                      ):
        """
        Extension point for post-processing such thing as pagination
        :return:
        """
        pass

    def _authenticate(self):
        """
        Extension point to support other authentication mechanism such as Oauth.
        Subclass this class and implement authentication process.

        This assumes that most of authentication process can work with updating member variable such as url and params
        :return: None
        """
        pass
