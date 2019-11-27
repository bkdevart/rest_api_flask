'''
This package sets up web app environment
'''

from flask import Flask, render_template
from flask_moment import Moment
from flask_login import LoginManager
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS, cross_origin
from config import config


moment = Moment()
db = SQLAlchemy()
login_manager = LoginManager()
login_manager.login_view = 'auth.login'

# application factory
def create_app(config_name):
    '''
    Creates instance of app based on config.py settings

    Parameters
    ----------
    config_name : str
        Options are development, testing, or production

    Returns
    -------
    app : flask app object
        Instance of the flask web app runtime
    '''
    app = Flask(__name__, template_folder='.')
    app.config.from_object(config[config_name])
    config[config_name].init_app(app)

    moment.init_app(app)
    # see if this initializes missing tables
    # db.create_all()
    db.init_app(app)
    login_manager.init_app(app)

    # attach routes and custom error pages here
    from .api import api as api_blueprint
    app.register_blueprint(api_blueprint, url_prefix='/api/v1')

    from .main import main as main_blueprint
    app.register_blueprint(main_blueprint)

    from .auth import auth as auth_blueprint
    app.register_blueprint(auth_blueprint, url_prefix='/auth')

    # TODO: re-enable this when Estevan is finished working
    # CORS(app, resources={r"/*": {"origins": "https://www.knightlifeindustries.com"}})
    CORS(app)

    return app