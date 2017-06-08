# This is the file that contains the RequestHandlers for our analysis page.
from framework.request_handler import CoreDashboardRequestHandler

from models.users import Users
from models.bigquery_metadata import BQClient

BILLING_PROJECT_ID = "720523189352"
DATASET = "Jouleboulevard"
TABLE = "EnergieID_15min_3"
END_DATE = '2017-05-29 13:00:00 UTC'

import os

class WaterAnalysis(CoreDashboardRequestHandler):
    @CoreDashboardRequestHandler.login_required
    def get(self, meter_id):
        userId = self.read_cookie('User')
        username = ""
        if userId:
            user = Users.get_by_id(int(userId))
            username = user.name

        analysis_list = []
        for filename in os.listdir('templates/analysis/partial_analysis_water'):
            if filename.endswith('.html'):
                analysis_list.append(os.path.join('analysis/partial_analysis_water', filename))
            else:
                continue

        analysis_list.sort()

        bqclient = BQClient()
        query = ("select datetime, consumption "
                 "from %s.%s "
                 "where meterID = '%s' "
                 "and datetime >= date_add(TIMESTAMP('%s'), -7, 'DAY') "
                 "ORDER BY datetime ASC") % (DATASET, TABLE, meter_id, END_DATE)

        bqdata = bqclient.query(query=query, project=BILLING_PROJECT_ID)

        water_chart_data_one = self.first_chart_data(bqdata=bqdata)
        water_chart_data_two = self.second_chart_data(bqdata=bqdata)

        self.render('analysis/analysis.html',
                    list_analysis=analysis_list,
                    type='water',
                    water_chart_data_one=water_chart_data_one,
                    water_chart_data_two=water_chart_data_two,
                    name=username)

    @classmethod
    def first_chart_data(cls, bqdata):
        data = cls.bq2linechart(bqdata=bqdata)
        # print data
        return data

    @classmethod
    def second_chart_data(cls, bqdata):
        return cls.bq2linechart(bqdata=bqdata)
