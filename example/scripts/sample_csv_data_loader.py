import sys

from databuilder.job.factory.neo4j_to_es_job_factory import Neo4jToEsJobFactory
from databuilder.job.factory.csv_to_neo4j_job_factory import CsvToNeo4jJobFactory

es_host = None
neo_host = None
if len(sys.argv) > 1:
    es_host = sys.argv[1]
if len(sys.argv) > 2:
    neo_host = sys.argv[2]

NEO4J_ENDPOINT = 'bolt://{}:7687'.format(neo_host if neo_host else 'localhost')
ES_ENDPOINT = es_host if es_host else 'localhost'

csv_job_factory = CsvToNeo4jJobFactory(NEO4J_ENDPOINT, 'neo4j', 'test')
es_job_factory = Neo4jToEsJobFactory(ES_ENDPOINT, NEO4J_ENDPOINT, 'neo4j', 'test')


def run_csv_job(file_loc, table_name, model):
    csv_job_factory.create_model_job(file_loc, table_name, model).launch()


if __name__ == "__main__":
    run_csv_job('example/sample_data/sample_table.csv', 'test_table_metadata',
                'databuilder.models.table_metadata.TableMetadata')
    run_csv_job('example/sample_data/sample_col.csv', 'test_col_metadata',
                'databuilder.models.standalone_column_model.StandaloneColumnMetadata')
    run_csv_job('example/sample_data/sample_table_column_stats.csv', 'test_table_column_stats',
                'databuilder.models.table_stats.TableColumnStats')
    run_csv_job('example/sample_data/sample_watermark.csv', 'test_watermark_metadata',
                'databuilder.models.watermark.Watermark')
    run_csv_job('example/sample_data/sample_table_owner.csv', 'test_table_owner_metadata',
                'databuilder.models.table_owner.TableOwner')
    run_csv_job('example/sample_data/sample_column_usage.csv', 'test_usage_metadata',
                'databuilder.models.column_usage_model.ColumnUsageModel')
    run_csv_job('example/sample_data/sample_user.csv', 'test_user_metadata',
                'databuilder.models.user.User')
    run_csv_job('example/sample_data/sample_application.csv', 'test_application_metadata',
                'databuilder.models.application.Application')
    run_csv_job('example/sample_data/sample_source.csv', 'test_source_metadata',
                'databuilder.models.table_source.TableSource')
    run_csv_job('example/sample_data/sample_table_last_updated.csv', 'test_table_last_updated_metadata',
                'databuilder.models.table_last_updated.TableLastUpdated')

    neo_last_updated_job = csv_job_factory.create_last_updated_job()
    neo_last_updated_job.launch()

    es_table_job = es_job_factory.create_es_publisher_job()
    es_table_job.launch()

    es_user_job = es_job_factory.create_user_es_publisher_job()
    es_user_job.launch()
