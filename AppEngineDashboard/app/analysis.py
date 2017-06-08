# This is the file that contains the RequestHandlers for our analysis page.
from framework.request_handler import CoreDashboardRequestHandler
from models.users import Users


class ClientHome(CoreDashboardRequestHandler):
    @CoreDashboardRequestHandler.login_required
    def get(self):
        userId = self.read_cookie('User')
        username = ""
        business = ""
        if userId:
            user = Users.get_by_id(int(userId))
            username = user.name
            business = user.business


        self.render('analysis/client_home.html', name=username, business=business)