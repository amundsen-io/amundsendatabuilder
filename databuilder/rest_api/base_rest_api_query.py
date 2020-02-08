import abc
import logging

import six
from typing import Iterable, Any, Dict, Iterator  # noqa: F401

LOGGER = logging.getLogger(__name__)


@six.add_metaclass(abc.ABCMeta)
class BaseRestApiQuery(object):

    @abc.abstractmethod
    def execute(self):
        # type: () -> Iterator[Dict[str, Any]]
        return iter([dict()])


class RestApiQuerySeed(BaseRestApiQuery):

    def __init__(self,
                 seed_record  # type: Iterable[Dict[str, Any]]
                 ):
        self._seed_record = seed_record
        LOGGER.info('self._seed_record: {}'.format(self._seed_record))

    def execute(self):
        return iter(self._seed_record)
