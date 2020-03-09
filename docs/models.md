# Amundsen Models

## Overview

These are the python classes that live in [databuilder/models/](../databuilder/models/).

Models represent the data structures that live in either neo4j (if the model extends Neo4jSerializable) or in elasticsearch.

Models that extend Neo4jSerializable have methods to create:
- the nodes
- the relationships

In this way, amundsendatabuilder pipelines can create python objects that can then be loaded into neo4j / elastic search
without developers needing to know the internals of the neo4j schema. 

-----

## The Models

###TableMetadata
[python class](../databuilder/models/table_metadata.py)

*What datasets does my org have?*

####Description
This corresponds to a dataset in amundsen and is the core building block.
In addition to ColumnMetadata, tableMetadata is one of the first datasets you should extract as
almost everything else depends on these being populated. 

#### Extraction
In general, for Table and Column Metadata, you should be able to use one of the pre-made extractors
in the [extractor package](../databuilder/extractor)


### Watermark 
[python class](../databuilder/models/watermark.py)

*What is the earliest data that this table has? What is the latest data?*
This is NOT the same as when the data was last updated.

####Description
Corresponds to the earliest and latest date that a dataset has. Only makes
sense if the dataset is timeseries data.
For example, a given table may have data from 2019/01/01 -> 2020/01/01
In that case the low watermark is 2019/01/01 and the high watermark is 2020/01/01.

#### Extraction
Depending on the datastore of your dataset, you would extract this by:
- a query on the minimum and maximum partition (hive)
- a query for the minimum and maximum record of a given timestamp column



### ColumnUsageModel
[python class](../databuilder/models/column_usage_model.py)

*How many queries is a given column getting? By which users?*

####Description
Has query counts per a given column per a user. This can help identify 
who uses given datasets so people can contact them if they have questions
on how to use a given dataset or if a dataset is changing. It is also used as a 
search boost so that the most used tables are put to the top of the search results.

####Extraction
For more traditional databases, there should be system tables where you can obtain 
these sorts of usage statistics.

In other cases, you may need to use audit logs which could require a custom solution.

Finally, for none traditional data lakes, getting this information exactly maybe difficult and you may need to rely
on a heuristic.

