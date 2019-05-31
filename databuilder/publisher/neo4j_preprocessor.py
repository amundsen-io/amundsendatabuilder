import abc

import logging
import six
from typing import Dict  # noqa: F401


LOGGER = logging.getLogger(__name__)


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
    def preprocess_cypher(self,
                          start_label,
                          end_label,
                          start_key,
                          end_key,
                          relation,
                          reverse_relation):
        # type: (str, str, str, str, str, str) -> Tuple[str, Dict[str, str]]
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
        # type: () -> bool
        """
        A method for Neo4j Publisher to determine whether to perform pre-processing or not.
        :return:
        """
        return False


class NoopRelationPreprocessor(RelationPreprocessor):

    def preprocess_cypher(self,
                          start_label,
                          end_label,
                          start_key,
                          end_key,
                          relation,
                          reverse_relation):
        # type: (str, str, str, str, str, str) -> Tuple[str, Dict[str, str]]
        pass


class DeleteRelationPreprocessor(RelationPreprocessor):
    RELATION_MERGE_TEMPLATE = """
    MATCH (n1:{start_label} {{key: $start_key }})-[r]-(n2:{end_label} {{key: $end_key }})
    WITH r LIMIT 2
    DELETE r
    RETURN count(*) as count;
    """

    def __init__(self, label_tuples=[]):
        super(DeleteRelationPreprocessor, self).__init__()
        self.label_tuples = set(label_tuples)

        reversed_label_tuples = [(t2, t1) for t1, t2 in label_tuples]
        self.label_tuples.update(reversed_label_tuples)

    def preprocess_cypher(self,
                          start_label,
                          end_label,
                          start_key,
                          end_key,
                          relation,
                          reverse_relation):
        # type: (str, str, str, str, str, str) -> Tuple[str, Dict[str, str]]
        if self.label_tuples and (start_label, end_label) not in self.label_tuples:
            return None

        if not (start_label or end_label or start_key or end_key):
            raise Exception('all labels and keys are required: {}'.format(locals()))

        params = {'start_key': start_key, 'end_key': end_key}
        return DeleteRelationPreprocessor.RELATION_MERGE_TEMPLATE.format(start_label=start_label,
                                                                         end_label=end_label), \
               params


    def is_perform_preprocess(self):
        # type: () -> bool
        return True