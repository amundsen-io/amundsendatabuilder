# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import textwrap

# Documentation: https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html
# Setting type to "text" for all fields that would be used in search
# Using Simple Analyzer to convert all text into search terms
# https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-simple-analyzer.html
# Standard Analyzer is used for all text fields that don't explicitly specify an analyzer
# https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-standard-analyzer.html
# TODO use amundsencommon for this when this project is updated to py3
DASHBOARD_ELASTICSEARCH_INDEX_MAPPING = textwrap.dedent(
    """
    {
        "settings": {
          "analysis": {
            "normalizer": {
              "lowercase_normalizer": {
                "type": "custom",
                "char_filter": [],
                "filter": ["lowercase", "asciifolding"]
              }
            }
          }
        },
        "mappings":{
            "dashboard":{
              "properties": {
                "group_name": {
                  "type":"text",
                  "analyzer": "simple",
                  "fields": {
                    "raw": {
                      "type": "keyword",
                      "normalizer": "lowercase_normalizer"
                    }
                  }
                },
                "name": {
                  "type":"text",
                  "analyzer": "simple",
                  "fields": {
                    "raw": {
                      "type": "keyword",
                      "normalizer": "lowercase_normalizer"
                    }
                  }
                },
                "description": {
                  "type":"text",
                  "analyzer": "simple",
                  "fields": {
                    "raw": {
                      "type": "keyword"
                    }
                  }
                },
                "group_description": {
                  "type":"text",
                  "analyzer": "simple",
                  "fields": {
                    "raw": {
                      "type": "keyword"
                    }
                  }
                },
                "query_names": {
                  "type":"text",
                  "analyzer": "simple",
                  "fields": {
                    "raw": {
                      "type": "keyword"
                    }
                  }
                },
                "tags": {
                  "type": "keyword"
                },
                "badges": {
                  "type": "keyword"
                }
              }
            }
          }
        }
    """
)
