from framework.request_handler import CoreDashboardRequestHandler
from models.business import Business
from models.users import Users
from google.appengine.api import mail
from os import environ
import re   # import for regular expressions.

class RegisterUser(CoreDashboardRequestHandler):

    def get(self):
        list_business = Business.get_business_list()
        self.json_response(status_code=200, **list_business)

    @classmethod
    def send_mail(cls, to, user_id, confirmationcode):
        email_object = mail.EmailMessage(
            sender='noreply@lkn-muntstraat.appspotmail.com',
            subject='Confirm your account for the CORE dashboard',
            to=to
        )

        email_parameters = {
            'domain': 'http://localhost:8080' if environ['SERVER_SOFTWARE'].startswith('Development') else 'http://lkn-muntstraat.appspot.com',
            'user_id': user_id,
            'confirmation_code': confirmationcode
        }

        html_from_template = cls.jinja_environment.get_template('email/confirmation_email.html').render(email_parameters)

        email_object.html = html_from_template
        email_object.send()



    # In this handler, we're sending a post request.
    def post(self):
        email = self.request.get('email')
        password = self.request.get('password')
        name = self.request.get('name')
        role = self.request.get('role')
        business = self.request.get('business')
        sensors = self.request.get('sensor_data')

        business_check = True
        if role == 'Client' and not business:
            business_check = False

        status = 200

        # All of these variables are required.
        if email and password and name and role and business_check:
            email_validation_pattern = "(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)"

            if re.match(email_validation_pattern, email):

                user = Users.add_new_user(email, password, name, role, business, sensors)

                if user['created']:
                    html = self.jinja_environment.get_template('commons/register_modal_success.html').render()
                    json_response = {
                        'html': html
                    }

                    self.send_mail(to=email, user_id=user['user_id'], confirmationcode=user['confirmation_code'])
                else:
                    status = 400
                    json_response = user

            else:
                status = 400
                json_response = {
                    'created': False,
                    'title': 'The email is not valid',
                    'message': 'Please enter a valid email address'
                }

        else:
            status = 400
            json_response = {}

            if not email:
                json_response.update({
                    'title': 'Email field required',
                    'message': 'Please give a valid email address to continue'
                })

            if not password:
                json_response.update({
                    'title': 'Password required',
                    'message': 'Please fill in a password to continue'
                })

            if not name:
                json_response.update({
                    'title': 'Name required',
                    'message': 'Please fill in a name to continue'
                })

            if not role:
                json_response.update({
                    'title': 'Role required',
                    'message': 'Please select a role to continue'
                })

            if not business and role == 'Client':
                json_response.update({
                    'title': 'Business required when role is Client',
                    'message': 'Please select a business to continue'
                })

        # Call function from the CoreDashboardRequestHandler. json_response are the kwargs
        self.json_response(status_code=status, **json_response)

class ConfirmUser(CoreDashboardRequestHandler):
    def get(self, user_id, confirmation_code):
        user_id = user_id
        confirmation_code = confirmation_code

        if Users.check_confirmation(user_id=user_id, confirmation_code=confirmation_code):
            print "Correct!"
        else:
            print "Wrong confirmation code"

        self.redirect('/')
