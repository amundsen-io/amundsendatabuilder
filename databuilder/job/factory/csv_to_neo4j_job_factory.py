import uuid

from pyhocon import ConfigFactory

from databuilder.extractor.csv_extractor import CsvExtractor
from databuilder.extractor.neo4j_es_last_updated_extractor import Neo4jEsLastUpdatedExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import NoopTransformer


class CsvToNeo4jJobFactory():
    """
    This is a convenience class, used to populate neo4j from csv files.
    """

    def __init__(self,
                 neo4j_endpoint,
                 neo4j_user,
                 neo4j_password,
                 use_unique_id=True):
        # type: (str, str, str, bool) -> None
        self.neo4j_endpoint = neo4j_endpoint
        self.neo4j_user = neo4j_user
        self.neo4j_password = neo4j_password
        self.use_unique_id = use_unique_id

    def create_model_job(self, file_loc, metadata_name, model):
        # type: (str, str, str) -> DefaultJob
        tmp_folder = '/tmp/amundsen/{metadata_name}'.format(metadata_name=metadata_name)
        node_files_folder = '{tmp_folder}/nodes'.format(tmp_folder=tmp_folder)
        relationship_files_folder = '{tmp_folder}/relationships'.format(tmp_folder=tmp_folder)

        csv_extractor = CsvExtractor()
        csv_loader = FsNeo4jCSVLoader()

        task = DefaultTask(extractor=csv_extractor,
                           loader=csv_loader,
                           transformer=NoopTransformer())

        job_config = ConfigFactory.from_dict({
            'extractor.csv.{}'.format(CsvExtractor.FILE_LOCATION): file_loc,
            'extractor.csv.model_class': model,
            'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.NODE_DIR_PATH):
                node_files_folder,
            'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.RELATION_DIR_PATH):
                relationship_files_folder,
            'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.SHOULD_DELETE_CREATED_DIR):
                True,
            'publisher.neo4j.{}'.format(neo4j_csv_publisher.NODE_FILES_DIR):
                node_files_folder,
            'publisher.neo4j.{}'.format(neo4j_csv_publisher.RELATION_FILES_DIR):
                relationship_files_folder,
            'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_END_POINT_KEY):
                self.neo4j_endpoint,
            'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_USER):
                self.neo4j_user,
            'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_PASSWORD):
                self.neo4j_password,
            'publisher.neo4j.{}'.format(neo4j_csv_publisher.JOB_PUBLISH_TAG):
                'unique_tag' + (str(uuid.uuid4()) if self.use_unique_id else "")
        })

        return DefaultJob(conf=job_config,
                          task=task,
                          publisher=Neo4jCsvPublisher())

    def create_last_updated_job(self):
        # type: (None) -> DefaultJob

        # loader saves data to these folders and publisher reads it from here
        tmp_folder = '/tmp/amundsen/last_updated_data'
        node_files_folder = '{tmp_folder}/nodes'.format(tmp_folder=tmp_folder)
        relationship_files_folder = '{tmp_folder}/relationships'.format(tmp_folder=tmp_folder)

        task = DefaultTask(extractor=Neo4jEsLastUpdatedExtractor(),
                           loader=FsNeo4jCSVLoader())

        job_config = ConfigFactory.from_dict({
            'extractor.neo4j_es_last_updated.model_class':
                'databuilder.models.neo4j_es_last_updated.Neo4jESLastUpdated',

            'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.NODE_DIR_PATH):
                node_files_folder,
            'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.RELATION_DIR_PATH):
                relationship_files_folder,

            'publisher.neo4j.{}'.format(neo4j_csv_publisher.NODE_FILES_DIR):
                node_files_folder,
            'publisher.neo4j.{}'.format(neo4j_csv_publisher.RELATION_FILES_DIR):
                relationship_files_folder,
            'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_END_POINT_KEY):
                self.neo4j_endpoint,
            'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_USER):
                self.neo4j_user,
            'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_PASSWORD):
                self.neo4j_password,
            'publisher.neo4j.{}'.format(neo4j_csv_publisher.JOB_PUBLISH_TAG):
                'unique_lastupdated_tag' + (str(uuid.uuid4()) if self.use_unique_id else "")
        })

        return DefaultJob(conf=job_config,
                          task=task,
                          publisher=Neo4jCsvPublisher())
