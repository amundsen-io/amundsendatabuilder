import abc

import six
from typing import Dict  # noqa: F401


@six.add_metaclass(abc.ABCMeta)
class RelationPreprocessor(object):
    """
    A Preprocessor for relations. Prior to publish Neo4j relations, RelationPreprocessor will be used for
    pre-processing.
    Neo4j Publisher will iterate through relation file and call preprocess_cypher to perform any pre-process requested.

    For example, if you need current job's relation data to be desired state, you can add delete statement in
    pre-process_cypher method. With preprocess_cypher defined, and with long transaction size, Neo4j publisher will
    atomically apply desired state.


    """

    @abc.abstractmethod
    def preprocess_cypher(self, start_label, end_label, relation, reverse_relation):
        # type: (str, str, str, str) -> Tuple[str, Dict[str, str]]
        """
        Provides a Cypher statement that will be executed before publishing relations.
        :param start_label:
        :param end_label:
        :param relation:
        :param reverse_relation:
        :return: A Cypher statement
        """
        pass

    def is_perform_preprocess(self):
        # type: () -> None
        """
        A method for Neo4j Publisher to determine whether to perform pre-processing or not.
        :return:
        """
        return False


class NoopRelationPreprocessor(RelationPreprocessor):

    def preprocess_cypher(self, start_label, end_label, relation, reverse_relation):
        pass
