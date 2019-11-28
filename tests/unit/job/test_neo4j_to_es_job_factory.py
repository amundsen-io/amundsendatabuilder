import unittest

from mock import patch

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

        self.assertEqual(conf['extractor']['search_data']['extractor']['neo4j']['graph_url'], 'neo4j-endpoint')
        self.assertEqual(conf['extractor']['search_data']['extractor']['neo4j']['model_class'],
                         'databuilder.models.table_elasticsearch_document.TableESDocument')
        self.assertEqual(conf['extractor']['search_data']['extractor']['neo4j']['neo4j_auth_user'], 'neo4j-user')
        self.assertEqual(conf['extractor']['search_data']['extractor']['neo4j']['neo4j_auth_pw'], 'neo4j-password')
        self.assertEqual(conf['loader']['filesystem']['elasticsearch']['file_path'], '/tmp/amundsen/search_data.json')
        self.assertEqual(conf['loader']['filesystem']['elasticsearch']['mode'], 'w')
        self.assertEqual(conf['publisher']['elasticsearch']['file_path'], '/tmp/amundsen/search_data.json')
        self.assertEqual(conf['publisher']['elasticsearch']['mode'], 'r')
        self.assertEqual(conf['publisher']['elasticsearch']['client'], 'es-endoint')
        self.assertEqual(conf['publisher']['elasticsearch']['new_index'], 'tables')
        self.assertEqual(conf['publisher']['elasticsearch']['doc_type'], 'table')
        self.assertEqual(conf['publisher']['elasticsearch']['alias'], 'table_search_index')

    @patch('databuilder.extractor.neo4j_search_data_extractor.Neo4jSearchDataExtractor', lambda x: None)
    def test_create_es__user_publisher_job(self):
        factory = Neo4jToEsJobFactory("es-endoint", "neo4j-endpoint", "neo4j-user", "neo4j-password", False)
        job = factory.create_user_es_publisher_job()

        conf = job.conf

        # set the client obj to the host address, to be able to test on a dict
        conf['publisher']['elasticsearch']['client'] = conf['publisher']['elasticsearch']['client'].transport.hosts[0][
            'host']

        self.assertEqual(conf['extractor']['search_data']['extractor']['neo4j']['graph_url'], 'neo4j-endpoint')
        self.assertEqual(conf['extractor']['search_data']['extractor']['neo4j']['model_class'],
                         'databuilder.models.user_elasticsearch_document.UserESDocument')
        self.assertEqual(conf['extractor']['search_data']['extractor']['neo4j']['neo4j_auth_user'], 'neo4j-user')
        self.assertEqual(conf['extractor']['search_data']['extractor']['neo4j']['neo4j_auth_pw'], 'neo4j-password')
        self.assertEqual(conf['extractor']['search_data']['cypher_query'],
                         '\nMATCH (user:User)\nOPTIONAL MATCH (user)-[read:READ]->(a)\nOPTIONAL MATCH (user)-[own:OWNER_OF]->(b)\nOPTIONAL MATCH (user)-[follow:FOLLOWED_BY]->(c)\nOPTIONAL MATCH (user)-[manage_by:MANAGE_BY]->(manager)\nwith user, a, b, c, read, own, follow, manager\nwhere user.full_name is not null\nreturn user.email as email, user.first_name as first_name, user.last_name as last_name,\nuser.full_name as name, user.github_username as github_username, user.team_name as team_name,\nuser.employee_type as employee_type, manager.email as manager_email, user.slack_id as slack_id,\nuser.is_active as is_active,\nREDUCE(sum_r = 0, r in COLLECT(DISTINCT read)| sum_r + r.read_count) AS total_read,\ncount(distinct b) as total_own,\ncount(distinct c) AS total_follow\norder by user.email\n')  # noqa
        self.assertEqual(conf['loader']['filesystem']['elasticsearch']['file_path'], '/tmp/amundsen/search_data.json')
        self.assertEqual(conf['loader']['filesystem']['elasticsearch']['mode'], 'w')
        self.assertEqual(conf['publisher']['elasticsearch']['file_path'], '/tmp/amundsen/search_data.json')
        self.assertEqual(conf['publisher']['elasticsearch']['mode'], 'r')
        self.assertEqual(conf['publisher']['elasticsearch']['client'], 'es-endoint')
        self.assertEqual(conf['publisher']['elasticsearch']['new_index'], 'tables')
        self.assertEqual(conf['publisher']['elasticsearch']['doc_type'], 'user')
        self.assertEqual(conf['publisher']['elasticsearch']['alias'], 'user_search_index')
        self.assertEqual(conf['publisher']['elasticsearch']['mapping'],
                         """
                {
                  "mappings":{
                    "user":{
                      "properties": {
                        "email": {
                          "type":"text",
                          "analyzer": "simple",
                          "fields": {
                            "raw": {
                              "type": "keyword"
                            }
                          }
                        },
                        "first_name": {
                          "type":"text",
                          "analyzer": "simple",
                          "fields": {
                            "raw": {
                              "type": "keyword"
                            }
                          }
                        },
                        "last_name": {
                          "type":"text",
                          "analyzer": "simple",
                          "fields": {
                            "raw": {
                              "type": "keyword"
                            }
                          }
                        },
                        "name": {
                          "type":"text",
                          "analyzer": "simple",
                          "fields": {
                            "raw": {
                              "type": "keyword"
                            }
                          }
                        },
                        "total_read":{
                          "type": "long"
                        },
                        "total_own": {
                          "type": "long"
                        },
                        "total_follow": {
                          "type": "long"
                        }
                      }
                    }
                  }
                }
            """)
