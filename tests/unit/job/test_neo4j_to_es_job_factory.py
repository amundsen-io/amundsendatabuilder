import unittest

from mock import patch
from pyhocon import ConfigTree

from databuilder.job.factory.neo4j_to_es_job_factory import Neo4jToEsJobFactory


class TestNeo4jToEsJobFactory(unittest.TestCase):

    @patch('databuilder.extractor.neo4j_search_data_extractor.Neo4jSearchDataExtractor', lambda x: None)
    def test_create_es_publisher_job(self):
        factory = Neo4jToEsJobFactory("es-endoint", "neo4j-endpoint", "neo4j-user", "neo4j-password", False)
        job = factory.create_es_publisher_job()

        conf = job.conf

        # set the client obj to the host address, to be able to test on a dict
        conf['publisher']['elasticsearch']['client'] = conf['publisher']['elasticsearch']['client'].transport.hosts[0][
            'host']

        expected = ConfigTree(
            [('extractor', ConfigTree([('search_data', ConfigTree([('extractor', ConfigTree([('neo4j', ConfigTree(
                [
                    ('graph_url', 'neo4j-endpoint'),
                    ('model_class', 'databuilder.models.table_elasticsearch_document.TableESDocument'),
                    ('neo4j_auth_user', 'neo4j-user'),
                    ('neo4j_auth_pw', 'neo4j-password')]))]))]))])),
             ('loader',
              ConfigTree(
                  [('filesystem',
                    ConfigTree(
                        [
                            ('elasticsearch',
                             ConfigTree([('file_path', '/tmp/amundsen/search_data.json'), ('mode', 'w')]))]))])),
             ('publisher',
              ConfigTree(
                  [
                      ('elasticsearch',
                       ConfigTree(
                           [
                               ('file_path', '/tmp/amundsen/search_data.json'),
                               ('mode', 'r'), ('client', 'es-endoint'),
                               ('new_index', 'tables'),
                               ('doc_type', 'table'),
                               ('alias', 'table_search_index')]))]))])

        self.assertDictEqual(job.conf, expected)

    @patch('databuilder.extractor.neo4j_search_data_extractor.Neo4jSearchDataExtractor', lambda x: None)
    def test_create_es__user_publisher_job(self):
        factory = Neo4jToEsJobFactory("es-endoint", "neo4j-endpoint", "neo4j-user", "neo4j-password", False)
        job = factory.create_user_es_publisher_job()

        conf = job.conf

        # set the client obj to the host address, to be able to test on a dict
        conf['publisher']['elasticsearch']['client'] = conf['publisher']['elasticsearch']['client'].transport.hosts[0][
            'host']

        print(job.conf)

        expected = ConfigTree(
            [('extractor', ConfigTree([('search_data', ConfigTree([('extractor', ConfigTree([('neo4j', ConfigTree(
                [('graph_url', 'neo4j-endpoint'),
                 ('model_class', 'databuilder.models.user_elasticsearch_document.UserESDocument'),
                 ('neo4j_auth_user', 'neo4j-user'),
                 ('neo4j_auth_pw', 'neo4j-password')]))])), ('cypher_query',
                    '\nMATCH (user:User)\nOPTIONAL MATCH (user)-[read:READ]->(a)\n'
                    'OPTIONAL MATCH (user)-[own:OWNER_OF]->(b)\n'
                    'OPTIONAL MATCH (user)-[follow:FOLLOWED_BY]->(c)\n'
                    'OPTIONAL MATCH (user)-[manage_by:MANAGE_BY]->(manager)\n'
                    'with user, a, b, c, read, own, follow, manager\n'
                    'where user.full_name is not null\n'
                    'return user.email as email, user.first_name as first_name, user.last_name as last_name,\n'
                    'user.full_name as name, user.github_username as github_username, user.team_name as team_name,\n'
                    'user.employee_type as employee_type, manager.email as manager_email, user.slack_id as slack_id,\n'
                    'user.is_active as is_active,\n'
                    'REDUCE(sum_r = 0, r in COLLECT(DISTINCT read)| sum_r + r.read_count) AS total_read,\n'
                    'count(distinct b) as total_own,\n'
                    'count(distinct c) AS total_follow\n'
                    'order by user.email\n')]))])),
             ('loader', ConfigTree([('filesystem', ConfigTree(
                 [('elasticsearch', ConfigTree([('file_path', '/tmp/amundsen/search_data.json'), ('mode', 'w')]))]))])),
             ('publisher', ConfigTree([('elasticsearch', ConfigTree(
                 [('file_path', '/tmp/amundsen/search_data.json'), ('mode', 'r'), ('client', 'es-endoint'),
                  ('new_index', 'tables'), ('doc_type', 'user'), ('alias', 'user_search_index'), ('mapping',
                    '\n'
                    '                {\n'
                    '                  "mappings":{\n'
                    '                    "user":'
                    '{\n'
                    ''
                    '                      "properties": {\n'
                    ''
                    '                        "email": {\n'
                    '                          "type":"text",\n'
                    '                          "analyzer": "simple",\n'
                    '                          "fields": {\n'
                    '                            "raw": {\n'
                    '                              "type": "keyword"\n'
                    '                            }\n'
                    '                          }\n'
                    '                        },\n'
                    '                        "first_name": {\n'
                    '                          "type":"text",\n'
                    '                          "analyzer": "simple",\n'
                    '                          "fields": {\n'
                    '                            "raw": {\n'
                    '                              "type": "keyword"\n'
                    '                            }\n'
                    '                          }\n                        },\n'
                    '                        "last_name": {\n'
                    '                          "type":"text",\n'
                    '                          "analyzer": "simple",\n'
                    '                          "fields": {\n'
                    '                            "raw": {\n'
                    '                              "type": "keyword"\n'
                    '                            }\n'
                    '                          }\n'
                    '                        },\n'
                    '                        "name": {\n'
                    '                          "type":"text",\n'
                    '                          "analyzer": "simple",\n'
                    '                          "fields": {\n'
                    '                            "raw": {\n'
                    '                              "type": "keyword"\n'
                    '                            }\n'
                    '                          }\n'
                    '                        },\n'
                    '                        "total_read":{\n'
                    '                          "type": "long"\n'
                    '                        },\n'
                    '                        "total_own": {\n'
                    '                          "type": "long"\n'
                    '                        },\n'
                    '                        "total_follow": {\n'
                    '                          "type": "long"\n'
                    '                        }\n'
                    '                      }\n'
                    '                    }\n'
                    '                  }\n'
                    '                }\n'
                    '            ')]))]))])  # noqa

        self.assertDictEqual(job.conf, expected)
