from google.appengine.ext import ndb
from os import environ
import datetime as dt

from app.home import QUERY_TABLE
from models.bigquery_metadata import BQClient


class Business():
    DATASET = "Jouleboulevard"
    TABLE = "EnergieID_15min_4"
    QUERY_TABLE = ("select recordName, meterId, sensorType, max(datetime) "
                   "from  %s.%s "
                   "group by recordName "
                   "order by recordName desc;") % (DATASET, TABLE)

    @classmethod
    def get_business_list(cls):
        bq = BQClient()
        response = bq.query(QUERY_TABLE, "720523189352")
        results = {'list': []}
        for row in response["rows"]:
            temp = {
                'recordName': row["f"][0]["v"],
                'meterId': row["f"][3]["v"],
                'sensorType': row["f"][1]["v"]
            }
            results['list'].append(temp)
        return results

    @classmethod
    def get_error_businesses(cls):
        bq = BQClient()
        response = bq.query(QUERY_TABLE, "720523189352")
        results = {'list': []}
        for row in response["rows"]:
            td1 = row["f"][2]["v"]
            td2 = int(float(td1))
            td3 = dt.datetime.fromtimestamp(td2)
            temp = {
                'recordName': row["f"][0]["v"],
                'meterId': row["f"][3]["v"],
                'sensorType': row["f"][1]["v"],
                'datetime': td3
            }
            results['list'].append(temp)
        return results
