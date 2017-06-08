from framework.request_handler import CoreDashboardRequestHandler
from models.users import Users

class LoginUser(CoreDashboardRequestHandler):
    def get(self):

        self.render('login/login.html')

    def post(self):
        email = self.request.get("email")
        password = self.request.get("password")

        # status = 200
        # json_response = {}
        correct = False

        # Check whether or not email and password are valid.
        user_id = Users.check_password(email, password)

        # If the user is logged in, we can send the cookie with our user ID.
        if user_id:
            if user_id == 1:
                # In this case, account has not been confirmed.
                correct = 1
                print "Not confirmed"
                self.render('login/login.html', correct=correct, email=email)
                # status = 400
                # json_response.update({
                #     'loggedin': False,
                #     'title': 'Account not confirmed',  # We don't want the whole key, only the ID.
                #     'message': 'You have not yet confirmed your account. Check your inbox in order to get started.'
                # })
            else:
                # In this case, the user is correctly logged in.
                self.send_cookie(name='User', value=user_id)
                # html = self.jinja_environment.get_template('home/home.html').render()
                # json_response.update({
                #     'html': html
                # })
                self.redirect('/')
        else:
            # In this case, email or password are wrong.
            correct = 2
            print "Wrong credentials"
            self.render('login/login.html', correct=correct, email=email)
            # status = 400
            # json_response.update({
            #         'loggedin': False,
            #         'title': 'Wrong credentials',  # We don't want the whole key, only the ID.
            #         'message': 'Wrong email or password, please try again.'
            #     })

        # self.json_response(status_code=status, **json_response)


class Logout(CoreDashboardRequestHandler):
    def get(self):
        self.response.delete_cookie('User')
        self.redirect('/login')