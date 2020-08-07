# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import abc

from requests.exceptions import HTTPError
from typing import Iterable, Union, List, Dict, Any, Optional  # noqa: F401


class BaseFailureHandler(object, metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def can_skip_failure(self,
                         exception: Exception,
                         ) -> bool:
        pass


class HttpFailureSkipOnStatus(BaseFailureHandler):

    def __init__(self,
                 status_codes_to_skip: Iterable[int],
                 ) -> None:
        self._status_codes_to_skip = {v for v in status_codes_to_skip}

    def can_skip_failure(self,
                         exception: Exception,
                         ) -> bool:

        if isinstance(exception, HTTPError):
            try:
                status_code: int = getattr(getattr(exception, 'response'), 'status_code')
                return status_code in self._status_codes_to_skip
            except AttributeError:
                pass

        return False
