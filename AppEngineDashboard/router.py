from webapp2 import WSGIApplication
from webapp2 import Route

app = WSGIApplication(
    routes=[
        Route('/', handler='app.home.Home'),
        Route('/client_home', handler='app.analysis.ClientHome'),
        Route('/register', handler='app.register.RegisterUser'),
        Route('/login', handler='app.login.LoginUser'),
        Route('/logout', handler='app.login.Logout'),
        Route('/account', handler='app.account.UserAccount'),
        Route('/account/<user_id:[0-9]+>/confirm/<confirmation_code:[a-zA-Z0-9]{32}>', handler='app.register.ConfirmUser'),
        Route('/analysis/electricity/<meter_id:[-A-Z0-9]*>', handler='app.electricity_analysis.ElectricityAnalysis'),
        Route('/analysis/naturalgas/<meter_id:[-A-Z0-9]*>', handler='app.gas_analysis.GasAnalysis'),
        Route('/analysis/drinkingwater/<meter_id:[-A-Z0-9]*>', handler='app.water_analysis.WaterAnalysis'),
        Route('/tasks', handler='app.cron.Cron')
    ],
    debug=True
)