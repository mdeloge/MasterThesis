from framework.request_handler import CoreDashboardRequestHandler

from models.users import Users


class UserAccount(CoreDashboardRequestHandler):

    # We only allow access to users who are logged in.
    @CoreDashboardRequestHandler.login_required
    def get(self):
        # Get username
        userId = self.read_cookie('User')
        username = ""
        email = ""
        business = ""
        if userId:
            user = Users.get_by_id(int(userId))
            username = user.name
            email = user.email
            business = user.business
        else:
            pass

        self.render('account/home.html', name=username, email=email, business=business)

    @CoreDashboardRequestHandler.login_required
    def post(self):
        name = self.request.get('name')
        email = self.request.get('email')
        status = 200
        userId = self.read_cookie('User')
        print userId

        json_response = {}
        if name:
            user = Users.update_user(name=name, email=email, user_id=userId)
            if user['updated']:
                json_response.update({
                    'title': 'Success!',
                    'message': 'Successfully changed your user information!'
                })
        else:
            status = 400
            json_response.update({
                'title': 'Error',
                'message': 'Something went wrong in the updating process.'
            })

        print json_response
        self.json_response(status_code=status, **json_response)
