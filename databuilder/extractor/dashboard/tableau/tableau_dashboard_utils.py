import json
import requests
import re

from pyhocon import ConfigTree, ConfigFactory  # noqa: F401

from databuilder.extractor.base_extractor import Extractor
from databuilder.extractor.restapi.rest_api_extractor import STATIC_RECORD_DICT

import databuilder.extractor.dashboard.tableau.tableau_dashboard_constants as const

class TableauDashboardUtils():
    """
    Provides various utility functions specifc to the Tableau dashboard extractors.
    """

    API_VERSION = const.API_VERSION
    TABLEAU_HOST = const.TABLEAU_HOST
    SITE_NAME = const.SITE_NAME
    TABLEAU_ACCESS_TOKEN_NAME = const.TABLEAU_ACCESS_TOKEN_NAME
    TABLEAU_ACCESS_TOKEN_SECRET = const.TABLEAU_ACCESS_TOKEN_SECRET
    EXCLUDED_PROJECTS = const.EXCLUDED_PROJECTS
    EXTERNAL_CLUSTER_NAME = const.EXTERNAL_CLUSTER_NAME
    EXTERNAL_SCHEMA_NAME = const.EXTERNAL_SCHEMA_NAME
    CLUSTER = const.CLUSTER
    DATABASE = const.DATABASE

    @staticmethod
    def sanitize_schema_name(str):
        """
        Sanitizes a given string so that it can safely be used as a table's schema.
        Sanitization behaves as follows:
            - all spaces and periods are replaced by underscores
            - any [], (), -, &, and ? characters are deleted
        """
        # type: (str) -> str
        # this indentation looks a little odd, but otherwise the linter complains
        return re.sub(r' ', '_',
                      re.sub(r'\.', '_',
                             re.sub(r'(\[|\]|\(|\)|\-|\&|\?)', '', str)))

    @staticmethod
    def sanitize_database_name(str):
        """
        Sanitizes a given string so that it can safely be used as a table's database.
        Sanitization behaves as follows:
            - all hyphens are deleted
        """
        # type: (str) -> str
        return re.sub(r"-", "", str)

    @staticmethod
    def sanitize_table_name(str):
        """
        Sanitizes a given string so that it can safely be used as a table name.
        Replicates the current behavior of sanitize_workbook_name, but this is purely coincidental.
        As more breaking characters/patterns are found, each method should be updated to reflect the specifics.
        Sanitization behaves as follows:
            - all forward slashes and single quotes characters are deleted
        """
        # type: (str) -> str
        return re.sub(r'(\/|\')', '', str)

    @staticmethod
    def sanitize_workbook_name(str):
        """
        Sanitizes a given string so that it can safely be used as a workbook ID.
        Mimics the current behavior of sanitize_table_name for now, but is purely coincidental.
        As more breaking characters/patterns are found, each method should be updated to reflect the specifics.
        Sanitization behaves as follows:
            - all forward slashes and single quotes characters are deleted
        """
        # type: (str) -> str
        return re.sub(r'(\/|\')', '', str)


class TableauGraphQLApiExtractor(Extractor):
    """
    Base class for querying the Tableau Metdata API, which uses a GraphQL schema.
    """

    API_VERSION = const.API_VERSION
    TABLEAU_HOST = const.TABLEAU_HOST
    SITE_NAME = const.SITE_NAME
    TABLEAU_ACCESS_TOKEN_NAME = const.TABLEAU_ACCESS_TOKEN_NAME
    TABLEAU_ACCESS_TOKEN_SECRET = const.TABLEAU_ACCESS_TOKEN_SECRET
    EXCLUDED_PROJECTS = const.EXCLUDED_PROJECTS
    EXTERNAL_CLUSTER_NAME = const.EXTERNAL_CLUSTER_NAME
    EXTERNAL_SCHEMA_NAME = const.EXTERNAL_SCHEMA_NAME
    EXTERNAL_TABLE_TYPES = const.EXTERNAL_TABLE_TYPES
    CLUSTER = const.CLUSTER
    DATABASE = const.DATABASE

    def init(self, conf, auth_token, query, query_variables={}):
        self._conf = conf
        self._auth_token = auth_token
        self._query = query
        self._query_variables = query_variables
        self._iterator = None
        self._static_dict = conf.get(STATIC_RECORD_DICT, dict())
        self._metadata_url = 'https://{TABLEAU_HOST}/api/metadata/graphql'.format(
            TABLEAU_HOST=self._conf.get_string(TableauGraphQLApiExtractor.TABLEAU_HOST)
        )

    def execute_query(self):
        """
        Executes the extractor's given query and returns the data from the results.
        """
        # type: () -> dict
        query_payload = json.dumps({
            "query": self._query,
            "variables": self._query_variables
        })
        headers = {
            "Content-Type": "application/json",
            "X-Tableau-Auth": self._auth_token
        }
        params = {
            "data": query_payload,
            "headers": headers,
            "verify": False
        }

        response = requests.post(url=self._metadata_url, **params)
        return response.json()['data']

    def execute(self):
        """
        Must be overriden by any extractor using this class. This should parse the result and yield each entity's
        metadata one by one. 
        """
        pass

    def extract(self):
        """
        Fetch one result at a time from the generator created by self.execute(), updating using the
        static record values if needed.
        """
        if not self._iterator:
                self._iterator = self.execute()

        try:
            record = next(self._iterator)
        except StopIteration:
            return None

        if self._static_dict:
            record.update(self._static_dict)

        return record


class TableauDashboardAuth():
    """
    Attempts to authenticate agains the Tableau REST API using the provided personal access token credentials.
    When successful, it will create a valid token that must be used on all subsequent requests.
    https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_concepts_auth.htm
    """

    API_VERSION = const.API_VERSION
    TABLEAU_HOST = const.TABLEAU_HOST
    SITE_NAME = const.SITE_NAME
    TABLEAU_ACCESS_TOKEN_NAME = const.TABLEAU_ACCESS_TOKEN_NAME
    TABLEAU_ACCESS_TOKEN_SECRET = const.TABLEAU_ACCESS_TOKEN_SECRET
    EXCLUDED_PROJECTS = const.EXCLUDED_PROJECTS
    EXTERNAL_CLUSTER_NAME = const.EXTERNAL_CLUSTER_NAME
    EXTERNAL_SCHEMA_NAME = const.EXTERNAL_SCHEMA_NAME
    CLUSTER = const.CLUSTER
    DATABASE = const.DATABASE

    def __init__(self, conf):
        self.site_id = None
        self._token = None
        self._conf = conf
        self._site_name = self._conf.get_string(TableauDashboardAuth.SITE_NAME)
        self._tableau_host = self._conf.get_string(TableauDashboardAuth.TABLEAU_HOST)
        self._api_version = self._conf.get_string(TableauDashboardAuth.API_VERSION)
        self._access_token_name = self._conf.get_string(TableauDashboardAuth.TABLEAU_ACCESS_TOKEN_NAME)
        self._access_token_secret = self._conf.get_string(TableauDashboardAuth.TABLEAU_ACCESS_TOKEN_SECRET)

    @property
    def token(self):
        if not self._token:
            self._token = self._authenticate()
        return self._token

    def _authenticate(self):
        """
        Queries the auth/signin endpoint for the given Tableau instance using a personal access token.
        The API version differs with your version of Tableau.
        See https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_concepts_versions.htm
        for details or ask your Tableau server administrator.
        """
        self._auth_url = "https://{tableau_host}/api/{api_version}/auth/signin".format(
            tableau_host=self._tableau_host,
            api_version=self._api_version
        )
        payload = json.dumps({
            "credentials": {
                "personalAccessTokenName": self._access_token_name,
                "personalAccessTokenSecret": self._access_token_secret,
                "site": {
                    "contentUrl": self._site_name
                }
            }
        })
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        # verify = False is needed bypass occasional (valid) self-signed cert errors. TODO: actually fix it!!
        params = {
            "headers": headers,
            "verify": False
        }

        response_json = requests.post(url=self._auth_url, data=payload, **params).json()
        self.site_id = response_json['credentials']['site']['id']

        return response_json['credentials']['token']
