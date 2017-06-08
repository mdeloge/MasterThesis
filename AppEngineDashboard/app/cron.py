from framework.request_handler import CoreDashboardRequestHandler
from models.business import Business
from models.users import Users
from google.appengine.api import mail
from os import environ
import re   # import for regular expressions.
import datetime as dt


class Cron(CoreDashboardRequestHandler):

    def get(self):
        list_business = Business.get_error_businesses()
        error_businesses = []
        timestamp = dt.datetime.now() - dt.timedelta(days=7)
        for line in list_business['list']:
            if line['datetime'] < timestamp:
                error_businesses.append(line)

        for bus in error_businesses:
            print bus['recordName'] + "\t" + bus['sensorType'] + "\t" + str(bus['datetime'])
        self.send_mail(["matteus.deloge2@gmail.com"], error_businesses) #TODO add a way to fetch all analysts email adresses
        self.json_response(200)

    @classmethod
    def send_mail(cls, analysts, error_businesses):
        adresses = ""
        for temp in analysts:
            adresses += temp + ";"

        email_object = mail.EmailMessage(
            sender='noreply@lkn-muntstraat.appspotmail.com',
            subject='Daily error log',
            to=analysts[0]
        )

        email_parameters = {
            'domain': 'http://localhost:8080' if environ['SERVER_SOFTWARE'].startswith(
                'Development') else 'http://lkn-muntstraat.appspot.com',
            'error_businesses': error_businesses
        }

        html_from_template = cls.jinja_environment.get_template('email/error_update_email.html').render(
            email_parameters)

        email_object.html = html_from_template
        email_object.send()

