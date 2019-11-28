import unittest

from databuilder.job.factory.csv_to_neo4j_job_factory import CsvToNeo4jJobFactory


class TestCsvToNeo4jJobFactory(unittest.TestCase):

    def test_create_model_csv_job(self):
        factory = CsvToNeo4jJobFactory("neo4j-endpoint", "neo4j-user", "neo4j-password", False)
        job = factory.create_model_job("file-location", "metadata-name", "model")

        conf = job.conf

        self.assertEqual(conf['extractor']['csv']['file_location'], 'file-location')
        self.assertEqual(conf['extractor']['csv']['model_class'], 'model')
        self.assertEqual(conf['loader']['filesystem_csv_neo4j']['node_dir_path'], '/tmp/amundsen/metadata-name/nodes')
        self.assertEqual(conf['loader']['filesystem_csv_neo4j']['relationship_dir_path'],
                         '/tmp/amundsen/metadata-name/relationships')
        self.assertEqual(conf['loader']['filesystem_csv_neo4j']['delete_created_directories'], True)
        self.assertEqual(conf['publisher']['neo4j']['node_files_directory'], '/tmp/amundsen/metadata-name/nodes')
        self.assertEqual(conf['publisher']['neo4j']['relation_files_directory'],
                         '/tmp/amundsen/metadata-name/relationships')
        self.assertEqual(conf['publisher']['neo4j']['neo4j_endpoint'], 'neo4j-endpoint')
        self.assertEqual(conf['publisher']['neo4j']['neo4j_user'], 'neo4j-user')
        self.assertEqual(conf['publisher']['neo4j']['neo4j_password'], 'neo4j-password')
        self.assertEqual(conf['publisher']['neo4j']['job_publish_tag'], 'unique_tag')

    def test_create_last_updated_job(self):
        factory = CsvToNeo4jJobFactory("neo4j-endpoint", "neo4j-user", "neo4j-password", False)
        job = factory.create_last_updated_job()

        conf = job.conf
        self.assertEqual(conf['extractor']['neo4j_es_last_updated']['model_class'],
                         'databuilder.models.neo4j_es_last_updated.Neo4jESLastUpdated')
        self.assertEqual(conf['loader']['filesystem_csv_neo4j']['node_dir_path'],
                         '/tmp/amundsen/last_updated_data/nodes')
        self.assertEqual(conf['loader']['filesystem_csv_neo4j']['relationship_dir_path'],
                         '/tmp/amundsen/last_updated_data/relationships')
        self.assertEqual(conf['publisher']['neo4j']['node_files_directory'], '/tmp/amundsen/last_updated_data/nodes')
        self.assertEqual(conf['publisher']['neo4j']['relation_files_directory'],
                         '/tmp/amundsen/last_updated_data/relationships')
        self.assertEqual(conf['publisher']['neo4j']['neo4j_endpoint'], 'neo4j-endpoint')
        self.assertEqual(conf['publisher']['neo4j']['neo4j_user'], 'neo4j-user')
        self.assertEqual(conf['publisher']['neo4j']['neo4j_password'], 'neo4j-password')
        self.assertEqual(conf['publisher']['neo4j']['job_publish_tag'], 'unique_lastupdated_tag')
