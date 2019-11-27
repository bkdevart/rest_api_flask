from flask import render_template, redirect, request, url_for, flash, jsonify
# from flask_login import login_user, login_required, logout_user, current_user
from . import auth
from ..models import User, AppleParser
from .forms import LoginForm, RegistrationForm
from .. import db


@auth.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    email = data['email']
    password = data['password']

    user = User.query.filter_by(email=email).first()
    if user is not None and user.verify_password(password):
        return jsonify(registered=True, username=user.username)
    return jsonify(registered=False, username='')


@auth.route('/register', methods=['POST'])
def register():
    data = request.get_json()
    email = data['email']
    username = data['username']
    password1 = data['password1']
    password2 = data['password2']
    
    if email is not None and username is not None and \
        password1 is not None and password2 is not None and \
        password1 == password2:
            user = User(email=email, username=username, password=password1)
            # check if user already exists before adding
            existing_user = User.query.filter_by(email=email).first()
            if existing_user is None:
                db.session.add(user)
                db.session.commit()  # TODO: this is crashing
                return jsonify(registered=True, username=user.username)
            else:
                return jsonify(registered=False, username='User already exists')
    return jsonify(registered=False, username='Form filled improperly')


@auth.route('/upload', methods=['POST'])
def process_file():
    # check if the post request has the file part
    if 'file' not in request.files:
        return jsonify({'status': 'No file part'})
    file = request.files['file']
    # if user does not select file, browser also
    # submit an empty part without filename
    if file.filename == '':
        return jsonify({'status': 'No selected file'})

    # take email and password, and return user id
    email = request.values.get('email')
    user = User.query.filter_by(email=email).first()

    if user is not None:
        user_id = user.id
    else:
        user_id = None
        # TODO: remove upload folders and end
        

    if user_id is not None:
        parser = AppleParser(file, user_id)
        if file and parser.allowed_file(file.filename):
            parser.parse_activity()
            return jsonify({'status': 'Database updated'})
    else:
        return jsonify({'status': 'Invalid user'})