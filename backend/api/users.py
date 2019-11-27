from flask import jsonify, request, current_app, url_for
from . import api
from ..models import User

@api.route('/users/get_user', methods=['POST'])
def get_user():
    '''
    parameters
    ----------
    email: user email
    '''
    data = request.get_json()
    email = data['email']
    # password = data['password']

    user = User.query.filter_by(email=email).first()
    if user is not None:
        response = jsonify({'username': user.username})
        response.status_code = 200
        return response
    return jsonify({'username': 'False'})


@api.route('/users/get_id', methods=['POST'])
def get_id():
    '''
    parameters
    ----------
    email: user email
    '''
    data = request.get_json()
    email = data['email']

    user = User.query.filter_by(email=email).first()
    if user is not None:
        response = jsonify({'user_id': user.id})
        response.status_code = 200
        return response
    return jsonify({'user_id': 'False'})
