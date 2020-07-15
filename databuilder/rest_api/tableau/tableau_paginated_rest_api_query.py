import logging
import copy

import requests  # noqa: F401
from jsonpath_rw import parse
from typing import Any  # noqa: F401

from databuilder.rest_api.rest_api_query import RestApiQuery

#  How many records considers as full and indicating there might be next page? In list reports on space API, it's 30.
DEFAULT_MAX_RECORD_SIZE = 100
PAGE_SUFFIX_TEMPLATE = '?pageSize={}&pageNumber={}'
LIST_PROJECTS_JSONPATH = 'workbooks.workbook.[*]'  # So far this is the only paginated API that we need.

LOGGER = logging.getLogger(__name__)


class TableauPaginatedRestApiQuery(RestApiQuery):
    """
    """

    def __init__(self,
                 pagination_json_path=LIST_PROJECTS_JSONPATH,  # type: str
                 max_record_size=DEFAULT_MAX_RECORD_SIZE,  # type: int
                 **kwargs  # type: Any
                 ):
        # type (...) -> None
        super(TableauPaginatedRestApiQuery, self).__init__(**kwargs)

        self._original_url = self._url
        self._max_record_size = max_record_size
        self._current_page = 1
        self._total_available = None
        self._pagination_jsonpath_expr = parse(pagination_json_path)

    def _preprocess_url(self,
                        record,  # type: Dict[str, Any]
                        ):
        # type: (...) -> str
        """
        Updates URL with page information
        :param record:
        :return: a URL that is ready to be called.
        """
        page_suffix = PAGE_SUFFIX_TEMPLATE.format(self._max_record_size,self._current_page)  # example: ?page=2

        # example: http://foo.bar/resources?page=2
        self._url = self._original_url + '{page_suffix}'.format(original_url=self._original_url,
                                                                page_suffix=page_suffix)
        return self._url.format(**record)

    def execute(self):  # noqa: C901
        # type: () -> Iterator[Dict[str, Any]]
        self._authenticate()

        for record_dict in self._inner_rest_api_query.execute():

            first_try = True  # To control pagination. Always pass the while loop on the first try
            while first_try or self._more_pages:
                first_try = False

                url = self._preprocess_url(record=record_dict)

                try:
                    response = self._send_request(url=url)
                except Exception as e:
                    if self._can_skip_failure and self._can_skip_failure(exception=e):
                        continue
                    raise e

                response_json = response.json()  # type: Union[List[Any], Dict[str, Any]]
                for workbook in response_json['workbooks']['workbook']:
                    if "description" not in workbook:
                        workbook['description'] = ""

                # value extraction via JSON Path
                result_list = [match.value for match in self._jsonpath_expr.find(response_json)]  # type: List[Any]

                if not result_list:
                    log_msg = 'No result from URL: {url}  , JSONPATH: {json_path} , response payload: {response}' \
                        .format(url=self._url, json_path=self._json_path, response=response_json)
                    LOGGER.info(log_msg)

                    self._post_process(response)

                    if self._fail_no_result:
                        raise Exception(log_msg)

                    if self._skip_no_result:
                        continue

                    yield copy.deepcopy(record_dict)

                sub_records = RestApiQuery._compute_sub_records(result_list=result_list,
                                                                field_names=self._field_names,
                                                                json_path_contains_or=self._json_path_contains_or)

                # for record in sub_records:
                #     if record[0] in ["Tableau Samples"]:

                # print(sub_records)
                for sub_record in sub_records:
                    record_dict = copy.deepcopy(record_dict)
                    for field_name in self._field_names:
                        record_dict[field_name] = sub_record.pop(0)
                    yield record_dict

                self._post_process(response)

    def _post_process(self,
                      response,  # type: requests.Response
                      ):
        # type: (...) -> None
        """
        Updates trigger to pagination (self._more_pages) as well as current_page (self._current_page)
        Mode does not have explicit indicator that it just the number of records need to be certain number that
        implying that there could be more records on next page.
        :return:
        """

        if self._total_available is None:
            self._total_available = int(response.json()['pagination']['totalAvailable'])

        result_list = [match.value for match in self._pagination_jsonpath_expr.find(response.json())]

        if result_list and (self._current_page * DEFAULT_MAX_RECORD_SIZE) < self._total_available:
            self._more_pages = True
            self._current_page = self._current_page + 1
            return

        self._more_pages = False
        self._current_page = 1
