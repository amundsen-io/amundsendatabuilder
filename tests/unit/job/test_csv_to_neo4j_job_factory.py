import unittest

from pyhocon import ConfigTree

from databuilder.job.factory.csv_to_neo4j_job_factory import CsvToNeo4jJobFactory


class TestCsvToNeo4jJobFactory(unittest.TestCase):

    def test_create_model_csv_job(self):
        factory = CsvToNeo4jJobFactory("neo4j-endpoint", "neo4j-user", "neo4j-password", False)
        job = factory.create_model_job("file-location", "metadata-name", "model")

        print(job.conf)

        expected = ConfigTree(
            [('extractor',
              ConfigTree([('csv', ConfigTree([('file_location', 'file-location'), ('model_class', 'model')]))])),
             ('loader', ConfigTree([('filesystem_csv_neo4j', ConfigTree(
                 [('node_dir_path', '/tmp/amundsen/metadata-name/nodes'),
                  ('relationship_dir_path', '/tmp/amundsen/metadata-name/relationships'),
                  ('delete_created_directories', True)
                  ]))])),
             ('publisher', ConfigTree([('neo4j', ConfigTree(
                 [('node_files_directory', '/tmp/amundsen/metadata-name/nodes'),
                  ('relation_files_directory', '/tmp/amundsen/metadata-name/relationships'),
                  ('neo4j_endpoint', 'neo4j-endpoint'),
                  ('neo4j_user', 'neo4j-user'),
                  ('neo4j_password', 'neo4j-password'),
                  ('job_publish_tag', 'unique_tag')
                  ]))]))])

        self.assertDictEqual(job.conf, expected)

    def test_create_last_updated_job(self):
        factory = CsvToNeo4jJobFactory("neo4j-endpoint", "neo4j-user", "neo4j-password", False)
        job = factory.create_last_updated_job()

        print(job.conf)

        expected = ConfigTree(
            [('extractor',
              ConfigTree([('neo4j_es_last_updated', ConfigTree(
                  [('model_class', 'databuilder.models.neo4j_es_last_updated.Neo4jESLastUpdated')]))])),
             ('loader', ConfigTree([('filesystem_csv_neo4j', ConfigTree(
                 [('node_dir_path', '/tmp/amundsen/last_updated_data/nodes'),
                  ('relationship_dir_path', '/tmp/amundsen/last_updated_data/relationships')]
             ))])),
             ('publisher', ConfigTree([('neo4j', ConfigTree(
                 [('node_files_directory', '/tmp/amundsen/last_updated_data/nodes'),
                  ('relation_files_directory', '/tmp/amundsen/last_updated_data/relationships'),
                  ('neo4j_endpoint', 'neo4j-endpoint'), ('neo4j_user', 'neo4j-user'),
                  ('neo4j_password', 'neo4j-password'),
                  ('job_publish_tag', 'unique_lastupdated_tag')]))]))])

        self.assertDictEqual(job.conf, expected)
