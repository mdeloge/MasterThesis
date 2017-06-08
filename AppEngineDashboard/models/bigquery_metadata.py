from oauth2client.contrib.appengine import AppAssertionCredentials
# from oauth2client.client import GoogleCredentials
from oauth2client.service_account import ServiceAccountCredentials
from httplib2 import Http
from apiclient.discovery import build
import os
from os import environ


class BQClient():

    def __init__(self):

        # You can only use AppAssertionCredentials credential objects in applications that are running on Google App Engine.
        # So when running locally, you need to add a json file containing the credentials.
        if environ['SERVER_SOFTWARE'].startswith('Development'):
            scopes = ['https://www.googleapis.com/auth/bigquery']
            # path_to_json = '../LKN-Muntstraat-38f5f268e477.json'
            path_to_json = os.path.join(
                os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)),
                'LKN-Muntstraat-38f5f268e477.json')
            credentials = ServiceAccountCredentials.from_json_keyfile_name(path_to_json, scopes=scopes)
            # credentials = GoogleCredentials.get_application_default()
            self.http_auth = credentials.authorize(Http())
            self.service = build('bigquery', 'v2', http=self.http_auth)

        # If it's not on the dev server, it's deployed on App Engine.
        else:
            credentials = AppAssertionCredentials('https://www.googleapis.com/auth/bigquery')

            self.http_auth = credentials.authorize(Http())

            self.service = build('bigquery', 'v2', http=self.http_auth)

    def query(self, query, project, timeout_ms=10000):
        query_config = {
            'query': query,
            'timeoutMs': timeout_ms
        }

        result_json = (self.service.jobs()
                       .query(projectId=project, body=query_config)
                       .execute(http=self.http_auth))
        return result_json
