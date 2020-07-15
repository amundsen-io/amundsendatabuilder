import json
import requests
import html
import re
import xml.etree.ElementTree as ET

from pyhocon import ConfigTree, ConfigFactory  # noqa: F401
from requests.auth import HTTPBasicAuth

from databuilder.extractor.base_extractor import Extractor

from databuilder import Scoped
from databuilder.extractor.dashboard.tableau.tableau_dashboard_constants import *
from databuilder.extractor.restapi.rest_api_extractor import STATIC_RECORD_DICT
from databuilder.rest_api.base_rest_api_query import BaseRestApiQuery  # noqa: F401
from databuilder.rest_api.base_rest_api_query import RestApiQuerySeed
from databuilder.rest_api.rest_api_query import RestApiQuery  # noqa: F401

class TableauDashboardUtils():
    pass

class TableauGraphQLApiExtractor(Extractor):

    def init(self, conf, auth_token, query):
        self._conf = conf
        self._auth_token = auth_token
        self._query = query
        self._iterator = None
        self._static_dict = conf.get(STATIC_RECORD_DICT, dict())
        self._metadata_url = 'https://{TABLEAU_HOST}/api/metadata/graphql'.format(TABLEAU_HOST=self._conf.get_string(TABLEAU_HOST))

    def execute_query(self):
        query_payload = json.dumps({
            "query": self._query
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
        pass

    def extract(self):
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

    def __init__(self, conf):
        self._conf = conf
        self._site_name = self._conf.get_string(SITE_NAME)
        self._tableau_host = self._conf.get_string(TABLEAU_HOST)
        self._api_version = self._conf.get_string(API_VERSION)
        self._access_token_name = self._conf.get_string(TABLEAU_ACCESS_TOKEN_NAME)
        self._access_token_secret = self._conf.get_string(TABLEAU_ACCESS_TOKEN_SECRET)

        self.site_id = None
        self.token = None

        self._authenticate()

    def _authenticate(self):
        self._auth_url = "https://{tableau_host}/api/{api_version}/auth/signin".format(tableau_host=self._tableau_host, api_version=self._api_version)
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
            'Content-Type': 'application/json'
        }
        params = {
            "headers": headers,
            "verify": False
        }

        response = requests.post(url=self._auth_url, data=payload, **params)

        # TODO: use Accept application/json
        root = ET.fromstring(response.text)
        credentials = root.getchildren()[0]
        self.token = credentials.attrib['token']

        site = credentials.getchildren()[0]
        self.site_id = site.attrib['id']
