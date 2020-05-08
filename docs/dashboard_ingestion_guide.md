# Dashboard Ingestion guidance 
(Currently this guidance is about using Databuilder to ingest Dashboard metadata into Neo4j and Elasticsearch)

Dashboard ingestion consists of multiple Databuilder jobs and it can be described in four steps:

 1. Ingest base data to Neo4j.
 2. Ingest additional data and decorate Neo4j over base data.
 3. Update Elasticsearch index using Neo4j data
 4. Remove stale data

Note that Databuilder jobs need to be sequenced as 1 -> 2 -> 3 -> 4. To sequencing these jobs, Lyft uses Airflow to orchestrate the job, but Databuilder is not limited to Airflow and you can also simply use Python script to sequence it -- not recommended for production though.

Also, step 1, 3, 4 is expected to have one Databuilder job where Step 2 is expected to have **multiple** Databuilder jobs and number of Databuilder jobs in step 2 is expected to grow as we add more metadata into Dashboard. To improve performance, it is recommended, but not required, to execute Databuilder jobs in step 2 concurrently.

Here this documentation will be using [Mode Dashboard](https://app.mode.com/) as concrete example to show how to ingest Dashboard metadata. However, this ingestion process not limited to Mode Dashboard and any other Dashboard can follow this flow.

### 1. Ingest base data to Neo4j.
Using [ModeDashboardExtractor](../README.md#modedashboardextractor) along with [FsNeo4jCSVLoader](../README.md#fsneo4jcsvloader) and [Neo4jCsvPublisher](../README.md#neo4jcsvpublisher) to add base information such as Dashboard group name, Dashboard group id, Dashboard group description, Dashboard name, Dashboard id, Dashboard description to Neo4j. Use [this job configuration](../README.md#modedashboardextractor) example to configure the job.

### 2. Ingest additional data and decorate Neo4j over base data.

Use other Mode dashboard's extractors in create & launch multiple Databuilder jobs. Note that it all Databuilder job here will use [FsNeo4jCSVLoader](../README.md#fsneo4jcsvloader) and [Neo4jCsvPublisher](../README.md#neo4jcsvpublisher) where their configuration should be almost the same except the `NODE_FILES_DIR` and `RELATION_FILES_DIR` that is being used for temporary location to hold data.

List of other Extractors can be found [here](../README.md#mode-dashboard-extractor)

#### 2.1. Ingest Dashboard usage data and decorate Neo4j over base data.
Mode provide usage data (view count) per Dashboard, but this is accumulated usage data. The main use case of usage is search ranking and `accumulated usage` is not that much useful for Amundsen as we don't want to show certain Dashboard that was popular years ago and potentially deprecated.

To bring recent usage information, we can `snapshot` accumulated usage per report daily and extract recent usage information (past 30 days, 60 days, 90 days that fits our view of recency). 

##### 2.1.1. Ingest `accumulated usage` into Data warehouse (e.g: Hive, BigQuery, Redshift, Postgres, etc)

In this step, you can use ModeDashboardUsageExtractor to extract `accumulated_view_count` and load into Data warehouse of your choice by using GenericLoader.

Note that GenericLoader just takes a callback function, and you need to provide a function that `INSERT` record into your Dataware house.

```python
extractor = ModeDashboardUsageExtractor()
loader = GenericLoader()
task = DefaultTask(extractor=extractor,
				   loader=loader)

job_config = ConfigFactory.from_dict({
	'{}.{}'.format(extractor.get_scope(), ORGANIZATION): organization,
	'{}.{}'.format(extractor.get_scope(), MODE_ACCESS_TOKEN): mode_token,
	'{}.{}'.format(extractor.get_scope(), MODE_PASSWORD_TOKEN): mode_password,
	'{}.{}'.format(loader.get_scope(), 'callback_function'): mode_dashboard_usage_loader_callback_function,
})

job = DefaultJob(conf=job_config, task=task)
job.launch()

```
Step 2. Extract past ? days usage data from your Data warehouse and publish it to Neo4j.
You could use [existing extractors](../README.md#list-of-extractors) to achieve this with [DashboardUsage model](./models.md#dashboardusage) along with [FsNeo4jCSVLoader](../README.md#fsneo4jcsvloader) and [Neo4jCsvPublisher](../README.md#neo4jcsvpublisher).

### 3. Update Elasticsearch index using Neo4j data

TBD

### 4. Remove stale data
Dashboard ingestion, like Table ingestion, is UPSERT (CREATE OR UPDATE) operation and there could be some data deleted on source. Not removing it in Neo4j basically leaving a stale data in Amundsen.

You can use ### [Neo4jStalenessRemovalTask](../README.md#removing-stale-data-in-neo4j----neo4jstalenessremovaltask) to remove stale data.

There are two strategies to remove stale data. One is to use `job_publish_tag` and the other one is to use `milliseconds_to_expire`.

For example, you could use `job_publish_tag` to remove stale `DashboardGroup`, `Dashboard`, and  `Query` nodes.  And you could use `milliseconds_to_expire` on `Timestamp` node,  `READ` relation, and `READ_BY`.  One of the main reasons to use `milliseconds_to_expire` is to avoid race condition and it is explained more [here](./README.md#using-publisher_last_updated_epoch_ms-to-remove-stale-data)
