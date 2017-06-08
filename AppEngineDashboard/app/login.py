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
                self.render('login/login.html', correct=correct, email=email)
            else:
                # In this case, the user is correctly logged in.
                self.send_cookie(name='User', value=user_id)
                self.redirect('/')
        else:
            # In this case, email or password are wrong.
            correct = 2
            self.render('login/login.html', correct=correct, email=email)



class Logout(CoreDashboardRequestHandler):
    def get(self):
        self.response.delete_cookie('User')
        self.redirect('/login')