from webapp2 import RequestHandler
from webapp2 import cached_property
import os
import jinja2



class CoreDashboardRequestHandler(RequestHandler):

    template_directory = os.path.join(
        os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)),
        'templates')

    jinja_environment = jinja2.Environment(
        loader=jinja2.FileSystemLoader(template_directory)
    )

    # Function used in all of our request handlers to render a template.
    # To render ANY possible template, we need to pass in 2 things, the template itself and the keyword arguments.
    def render(self, template, **kwargs):

        jinja_template = self.jinja_environment.get_template(template)
        html_from_template = jinja_template.render(kwargs)

        self.response.out.write(html_from_template)

    # Function for handling Ajax requests.
    def json_response(self, status_code=200, **kwargs):
        from json import dumps

        self.response.status = status_code
        # Tells browser that whatever it receives, it can't interpret it as HTML, but must interpret it as JSON
        self.response.headers['Content-Type'] = 'application/json'
        # We can't just write out the keyword arguments. First, we need to convert this dictionary to a string with 'dump string'
        self.response.out.write(dumps(kwargs))

    # Function to read cookies. Only accepts the name of the cookie
    def read_cookie(self, name):
        from framework.cookie_handler import check_cookie

        cookie_value = self.request.cookies.get(name)
        return check_cookie(cookie_value)

    def send_cookie(self, name, value):
        from framework.cookie_handler import sign_cookie

        signed_cookie_value = sign_cookie(value)
        # Cookies are passed in the header, so we need to add it there.
        self.response.headers.add_header('Set-Cookie', '%s=%s; Path=/' % (name, signed_cookie_value))

    # This wrapper will remember the outcome of the check_user_logged_in function.
    @cached_property
    def check_user_logged_in(self):
        # First, check if the cookie exists.
        if self.request.cookies.get('User'):
            user_id = self.read_cookie('User')
            # If the cookie is valid:
            if user_id:
                from models.users import Users

                # Whatever you get back from a cookie is always formatted as a string.
                # So we need to wrap this around the integer function.
                return Users.get_by_id(int(user_id))
            else:
                return None
        return None


    # Wrapper function. Need to pass in handler, which is the function we'll be wrapping around.
    # Defined a static function.
    @staticmethod
    def login_required(handler):
        # Define function inside of a function.
        # This is Python's way of defining a function that can accept any kind of values.
        def check_login(self, *args, **kwargs):
            # If the user is logged in ( = If the function returns anything), continue. If not, redirect to the login page.
            if self.check_user_logged_in:
                return handler(self, *args, **kwargs)
            else:
                return self.redirect('/login')

        # This is the way of designing a wrapper, create a function that accepts anything inside a function and return it.
        return check_login

    @staticmethod
    def analyst_required(handler):
        def check_analyst(self, *args, **kwargs):
            user = self.check_user_logged_in
            if user:
                if user.role == 'Analyst':
                    return handler(self, *args, **kwargs)
                else:
                    return self.redirect('/client_home')
            else:
                return self.redirect('/login')

        return check_analyst

    @staticmethod
    def bq2linechart(bqdata):
        # table = Table()

        data = []

        data.append(["Date", "Consumption"])

        # NameDate = bqdata["schema"]["fields"][0]["name"]
        # NameVal = bqdata["schema"]["fields"][1]["name"]
        # table.add_column(NameDate, datetime.datetime, NameDate)
        # table.add_column(NameVal, float, NameVal)


        # BigQuery query response comes back as JSON, but the Python API library interprets it into a Python data
        # structure for you.
        # Loop through it and append it to the 2d list.
        for row in bqdata["rows"]:
            td1 = row["f"][0]["v"]
            td2 = int(float(td1))
            # td3 = datetime.datetime.fromtimestamp(td2)
            # print float(row["f"][1]["v"])
            data.append([td2, float(row["f"][1]["v"])])


        print data

        return data
        # print data

        # sorted(table)
        # print table
        # # Convert data to a JSON string so that the template renderer can substitute the JSON string directly into the
        # # template.
        #
        # encoded = table.encode()
        # # print encoded
        # return encoded



