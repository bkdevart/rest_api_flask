from flask import send_from_directory
from . import main

@main.route('/<path:path>', methods=['GET'])
@main.route('/mainDashboard/<path:path>', methods=['GET'])
@main.route('/mainDashboard/activitySummary/<path:path>', methods=['GET'])
def static_proxy(path):
    return send_from_directory('../', path)

@main.route('/')
@main.route('/mainDashboard/')
@main.route('/mainDashboard/activitySummary')
def root():
    return send_from_directory('../', 'index.html')
