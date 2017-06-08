# This is the file that contains the RequestHandlers for our home page.

from framework.request_handler import CoreDashboardRequestHandler
from models.bigquery_metadata import BQClient
from models.users import Users

import datetime

BILLING_PROJECT_ID = "720523189352"
DATASET = "Jouleboulevard"
TABLE = "EnergieID_15min_4"
QUERY_TABLE = ("select recordName, sensorType, max(datetime), meterID "
               "from  %s.%s "
               "group by recordName, sensorType, meterID "
               "order by recordName asc;") % (DATASET, TABLE)


class Home(CoreDashboardRequestHandler):

    @CoreDashboardRequestHandler.analyst_required
    def get(self):
        # Get username
        userId = self.read_cookie('User')
        username = ""
        if userId:
            user = Users.get_by_id(int(userId))
            username = user.name

        bqclient = BQClient()
        bqdata = bqclient.query(QUERY_TABLE, BILLING_PROJECT_ID)

        results = [] # List of lists

        for row in bqdata["rows"]:
            td1 = row["f"][2]["v"]
            td2 = int(float(td1))
            td3 = datetime.datetime.fromtimestamp(td2)
            delta = datetime.datetime.now() - td3 #TODO toevoegen aan paper
            result = [row["f"][0]["v"], row["f"][1]["v"], td3, row["f"][3]["v"], str(row["f"][1]["v"]).replace(" ", ""), delta.days]

            results.append(result)

        self.render('home/home.html', results=results, name=username)