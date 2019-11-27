'''
Contains definitions for database tables and contains the functions used to
parse the XML data uploaded from user's phones.
'''

import os
import zipfile, tempfile
from datetime import datetime
from time import time
import hashlib
import shutil
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from itsdangerous import TimedJSONWebSignatureSerializer as Serializer
import xml.etree.ElementTree as ET
from flask import current_app, request, url_for
from flask_login import UserMixin, AnonymousUserMixin
import numpy as np
import pandas as pd

from backend.exceptions import ValidationError
from . import db, login_manager


class User(UserMixin, db.Model):
    '''
    User information table
    '''
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(64), unique=True, index=True)
    username = db.Column(db.String(64), unique=True, index=True)
    password_hash = db.Column(db.String(128))

    @property
    def password(self):
        raise AttributeError('password is not a readable attribute')

    @password.setter
    def password(self, password):
        self.password_hash = generate_password_hash(password)

    def verify_password(self, password):
        return check_password_hash(self.password_hash, password)

    def __repr__(self):
        return '<User %r>' % self.username


class ActivityData(db.Model):
    '''
    Contains activity data from Apple Watch
    (Requirements 3.2.2, 3.4.1, and 3.4.2)
    '''
    __tablename__ = 'activity_data'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer)
    date = db.Column(db.Date)
    energy_burned = db.Column(db.Float)
    energy_burned_goal = db.Column(db.Integer)
    energy_burned_unit = db.Column(db.String(10))
    exercise_time = db.Column(db.Integer)
    exercise_time_goal = db.Column(db.Integer)
    stand_hours = db.Column(db.Integer)
    stand_hours_goal = db.Column(db.Integer)
    created_at = db.Column(db.Date)
    updated_at = db.Column(db.Date)
    last_updated_by = db.Column(db.String(32))


class WorkoutData(db.Model):
    '''
    Contains workout data from Apple Watch and iPhone
    (Requirements 3.2.3, 3.2.5, 3.4.1, and 3.4.2)
    '''
    __tablename__ = 'workout_data'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer)
    date = db.Column(db.Date)
    activity = db.Column(db.String(10))
    duration = db.Column(db.Float)
    duration_unit = db.Column(db.String(10))
    total_distance = db.Column(db.Float)
    total_distance_unit = db.Column(db.String(10))
    total_energy_burned = db.Column(db.Float)
    total_energy_burned_unit = db.Column(db.String(10))
    gadget = db.Column(db.String(15))
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)
    created_at = db.Column(db.Date)
    updated_at = db.Column(db.Date)
    last_updated_by = db.Column(db.String(32))


class ExerciseTime(db.Model):
    '''
    Contains exercise time data from Apple Watch and iPhone
    (Requirements 3.2.1, 3.4.1, and 3.4.2)
    '''
    __tablename__ = 'exercise_time'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer)
    date = db.Column(db.Date)
    exercise_time_type = db.Column(db.String(32))
    exercise_time_duration = db.Column(db.Float)
    exercise_time_durationUnit = db.Column(db.String(10))
    created_at = db.Column(db.Date)
    updated_at = db.Column(db.Date)
    last_updated_by = db.Column(db.String(32))


class HeartRate(db.Model):
    '''
    Contains heartrate data from Apple Watch
    (Requirements 3.2.4, 3.4.1, and 3.4.2)
    '''
    __tablename__ = 'heart_rate'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer)
    date = db.Column(db.Date)
    gadget = db.Column(db.String(15))
    avg = db.Column(db.Float)
    max = db.Column(db.Float)
    min = db.Column(db.Float)
    created_at = db.Column(db.Date)
    updated_at = db.Column(db.Date)
    last_updated_by = db.Column(db.String(32))


class AppleParser():
    '''
    contains functions for reading Apple's xml format for health data
    (Requirements 3.2 and 3.4)
    '''
    def __init__(self, file, user_id):
        self.file = file
        self.user_id = user_id

    def parse_activity(self):
        '''
        The full upload and parse process for placing user data in database
        '''
        print('Uploading file...')
        filepath = self.upload_file(self.file)
        print('Unzipping...')
        self.unzip_file(filepath)
        print('Creating csv data...')
        self.create_csv_data(filepath)
        print('Loading database...')
        self.db_load_csv(self.user_id, filepath)
        print(f'Removing temp folder ({filepath})...')
        shutil.rmtree(filepath)

    def allowed_file(self, filename):
        '''
        Checks file extension for allowed types
        '''
        return '.' in filename and \
            filename.rsplit('.', 1)[1].lower() in current_app.config['ALLOWED_EXTENSIONS']

    def upload_file(self, file):
        '''
        Simple upload script for Apple health data
        '''
        start = time()
        filename = secure_filename(file.filename)
        # modify filepath to include username to prevent conflict with multiple users
        filepath = os.path.join(current_app.config['UPLOAD_FOLDER'], str(self.user_id))
        # TODO: enable the following line once you've implemented all parsing methods
        # filepath = os.path.join(filepath, tempfile.mkdtemp())
        # create folder
        if not os.path.exists(filepath):
            os.makedirs(filepath)
        filepath_file = os.path.join(filepath, filename)
        file.save(filepath_file)
        print('Upload time:')
        print(time() - start)
        return filepath
    
    def unzip_file(self, filepath):
        '''
        Unzips export.zip and then deletes it
        '''
        start = time()
        filepath_file = os.path.join(filepath, 'export.zip')
        zip_ref = zipfile.ZipFile(filepath_file, 'r')
        zip_ref.extractall(filepath)
        zip_ref.close()
        os.remove(filepath_file)
        print('Unzip time:')
        print(time() - start)
    
    def create_csv_data(self, filepath):
        '''
        Creates individual csv files for each table to import into db
        '''
        start = time()
        print('Parsing activity data...')
        data = self.activity_summary(filepath)
        print('Cleaning up XML files...')
        # delete folder with XML files
        xml_folder = os.path.join(filepath, 'apple_health_export')
        shutil.rmtree(xml_folder)
        print('XML parse time:')
        print(time() - start)
    
    def db_load_csv(self, user_id, filepath):
        '''
        Loads csv data into database
        '''
        start = time()
        print('Loading activity summary...')
        file_path = filepath + 'activity_summary.csv'
        df = pd.read_csv(file_path)
        # TODO: for now, clear out all data of user - may optimize if there is time
        # check for data before trying to delete
        if db.session.query(ActivityData).filter(ActivityData.user_id==user_id).first() is not None:
            db.session.query(ActivityData).filter(ActivityData.user_id==user_id).\
                delete(synchronize_session=False)
            db.session.commit()
            db.session.close()
        df['user_id'] = user_id
        # grab user name from user table
        user_name = User.query.filter_by(id=user_id).first().username
        df['last_updated_by'] = user_name
        df.to_sql(name='activity_data', con=db.engine, if_exists='append', index=False)
        os.remove(file_path)

        print('Loading workout data...')
        file_path = filepath + 'workout.csv'
        df = pd.read_csv(file_path)
        # TODO: for now, clear out all data of user - may optimize if there is time
        # check for data before trying to delete
        if db.session.query(WorkoutData).filter(WorkoutData.user_id==user_id).first() is not None:
            db.session.query(WorkoutData).filter(WorkoutData.user_id==user_id).\
                delete(synchronize_session=False)
            db.session.commit()
            db.session.close()
        df['user_id'] = user_id
        # grab user name from user table
        user_name = User.query.filter_by(id=user_id).first().username
        df['last_updated_by'] = user_name
        df.to_sql(name='workout_data', con=db.engine, if_exists='append', index=False)
        os.remove(file_path)

        print('Loading exercise time data...')
        file_path = filepath + 'exercise_time.csv'
        df = pd.read_csv(file_path)
        # TODO: for now, clear out all data of user - may optimize if there is time
        # check for data before trying to delete
        if db.session.query(ExerciseTime).filter(ExerciseTime.user_id==user_id).first() is not None:
            db.session.query(ExerciseTime).filter(ExerciseTime.user_id==user_id).\
                delete(synchronize_session=False)
            db.session.commit()
            db.session.close()
        df['user_id'] = user_id
        # grab user name from user table
        user_name = User.query.filter_by(id=user_id).first().username
        df['last_updated_by'] = user_name
        df.to_sql(name='exercise_time', con=db.engine, if_exists='append', index=False)
        os.remove(file_path)

        '''
        print('Loading heartrate data...')
        file_path = filepath + 'heartrate.csv'
        df = pd.read_csv(file_path)
        # TODO: for now, clear out all data of user - may optimize if there is time
        # check for data before trying to delete
        if db.session.query(HeartRate).filter(HeartRate.user_id==user_id).first() is not None:
            db.session.query(HeartRate).filter(HeartRate.user_id==user_id).\
                delete(synchronize_session=False)
            db.session.commit()
            db.session.close()
        df['user_id'] = user_id
        # grab user name from user table
        user_name = User.query.filter_by(id=user_id).first().username
        df['last_updated_by'] = user_name
        df.to_sql(name='heart_rate', con=db.engine, if_exists='append', index=False)
        os.remove(file_path)
        '''
        
        print('DB load time:')
        print(time() - start)

    def activity_summary(self, file_path):
        '''
        Main XML parsing script, streams in file and proccesses each branch into
        lists, which get converted to data frames and exported to csv files for database upload
        '''
        # activity data
        date = []
        energy_burned = []
        energy_burned_goal = []
        energy_burned_unit = []
        exercise_time = []
        exercise_time_goal = []
        stand_hours = []
        stand_hours_goal = []
        file = file_path + '/apple_health_export/export.xml'

        # exercise time
        exercise_time_type = []
        exercise_time_date = []
        exercise_time_duration = []
        exercise_time_durationUnit = []

        # workout data
        workoutActivityType = []
        duration = []
        durationUnit = []
        totalDistance = []
        totalDistanceUnit = []
        totalEnergyBurned = []
        totalEnergyBurnedUnit = []
        sourceName = []
        sourceVersion = []
        device = []
        creationDate = []
        startDate = []
        endDate = []

        # heartrate data
        record_type = []
        record_unit = []
        record_value = []
        record_sourceName = []
        record_sourceVersion = []
        record_device = []
        record_creationDate = []
        record_startDate = []
        record_endDate = []

        for event, elem in ET.iterparse(file, events=("start", "end")):
            if event == 'end':
                # process the tag
                if elem.tag == 'ActivitySummary':
                    # import pdb;pdb.set_trace()
                    for item in elem.items():
                        if item[0] == 'dateComponents':
                            date.append(item[1])
                        elif item[0] == 'activeEnergyBurned':
                            energy_burned.append(item[1])
                        elif item[0] == 'activeEnergyBurnedGoal':
                            energy_burned_goal.append(item[1])
                        elif item[0] == 'activeEnergyBurnedUnit':
                            energy_burned_unit.append(item[1])
                        elif item[0] == 'appleExerciseTime':
                            exercise_time.append(item[1])
                        elif item[0] == 'appleExerciseTimeGoal':
                            exercise_time_goal.append(item[1])
                        elif item[0] == 'appleStandHours':
                            stand_hours.append(item[1])
                        elif item[0] == 'appleStandHoursGoal':
                            stand_hours_goal.append(item[1])
                if elem.tag == 'WorkoutEvent':
                    for item in elem.items():
                        if item[0] == 'type':
                            exercise_time_type.append(item[1])
                        elif item[0] == 'date':
                            exercise_time_date.append(item[1])
                        elif item[0] == 'duration':
                            exercise_time_duration.append(item[1])
                        elif item[0] == 'durationUnit':
                            exercise_time_durationUnit.append(item[1])
                if elem.tag == 'Workout':
                    for item in elem.items():
                        if item[0] == 'workoutActivityType':
                            workoutActivityType.append(item[1])
                        if item[0] == 'duration':
                            duration.append(item[1])
                        if item[0] == 'durationUnit':
                            durationUnit.append(item[1])
                        if item[0] == 'totalDistance':
                            totalDistance.append(item[1])
                        if item[0] == 'totalDistanceUnit':
                            totalDistanceUnit.append(item[1])
                        if item[0] == 'totalEnergyBurned':
                            totalEnergyBurned.append(item[1])
                        if item[0] == 'totalEnergyBurnedUnit':
                            totalEnergyBurnedUnit.append(item[1])
                        if item[0] == 'sourceName':
                            sourceName.append(item[1])
                        if item[0] == 'sourceVersion':
                            sourceVersion.append(item[1])
                        if item[0] == 'device':
                            device.append(item[1])
                        if item[0] == 'creationDate':
                            creationDate.append(item[1])
                        if item[0] == 'startDate':
                            startDate.append(item[1])
                        if item[0] == 'endDate':
                            endDate.append(item[1])
                '''
                if elem.tag == 'Record':
                    for item in elem.items():
                        if item[0] == 'type':
                            record_type.append(item[1])
                        if item[0] == 'unit':
                            record_unit.append(item[1])
                        if item[0] == 'value':
                            record_value.append(item[1])
                        if item[0] == 'sourceName':
                            record_sourceName.append(item[1])
                        if item[0] == 'sourceVersion':
                            record_sourceVersion.append(item[1])
                        if item[0] == 'device':
                            record_device.append(item[1])
                        if item[0] == 'creationDate':
                            record_creationDate.append(item[1])
                        if item[0] == 'startDate':
                            record_startDate.append(item[1])
                        if item[0] == 'endDate':
                            record_endDate.append(item[1])
                '''
                # this is the key to memory management on the server
                elem.clear()

        # create activity data data frame
        print('Creating activity data...')
        li = list(zip(date, energy_burned, energy_burned_goal,
                    energy_burned_unit, exercise_time,
                    exercise_time_goal, stand_hours, stand_hours_goal))
        df = pd.DataFrame(li, columns=['date',
                                    'energy_burned',
                                    'energy_burned_goal',
                                    'energy_burned_unit',
                                    'exercise_time',
                                    'exercise_time_goal',
                                    'stand_hours',
                                    'stand_hours_goal'])
        # remove dates before 2000-01-01
        df['datetime'] = pd.to_datetime(df['date'])
        df = df[df['datetime'] > '2000-01-01']
        # drop datetime column
        df = df.drop(['datetime'], axis=1)
        # add created_at, last_updated_by
        df['created_at'] = pd.to_datetime('now')
        df['updated_at'] = pd.to_datetime('now')
        df.fillna(0, inplace=True)

        # create exercise time data frame
        print('Creating exercise time data...')
        li = list(zip(exercise_time_date, exercise_time_type,
                    exercise_time_duration, exercise_time_durationUnit))
        exercise_time = pd.DataFrame(li,
                                    columns=['date',
                                            'exercise_time_type',
                                            'exercise_time_duration',
                                            'exercise_time_durationUnit'])
        # remove dates before 2000-01-01
        exercise_time['datetime'] = pd.to_datetime(exercise_time['date'])
        exercise_time = exercise_time[exercise_time['datetime'] > '2000-01-01']
        # drop datetime column
        exercise_time = exercise_time.drop(['datetime'], axis=1)
        # add created_at, last_updated_by
        exercise_time['created_at'] = pd.to_datetime('now')
        exercise_time['updated_at'] = pd.to_datetime('now')
        exercise_time.fillna(0, inplace=True)

        # create workout data frame
        print('Creating workout data...')
        li = list(zip(workoutActivityType, duration, durationUnit, totalDistance,
                    totalDistanceUnit, totalEnergyBurned, totalEnergyBurnedUnit,
                    sourceName, sourceVersion, device, creationDate, startDate,
                    endDate))
        workout = pd.DataFrame(li,
                            columns=['activity_type', 'duration',
                                        'duration_unit', 'total_distance',
                                        'total_distance_unit',
                                        'total_energy_burned',
                                        'total_energy_burned_unit',
                                        'source_name', 'source_version',
                                        'device', 'creation_date',
                                        'start_date', 'end_date'])
        # remove dates before 2000-01-01
        workout['creation_datetime'] = pd.to_datetime(workout['creation_date'])
        workout = workout[workout['creation_datetime'] > '2000-01-01']
        workout['start_datetime'] = pd.to_datetime(workout['start_date'])
        workout = workout[workout['start_datetime'] > '2000-01-01']
        workout['end_datetime'] = pd.to_datetime(workout['end_date'])
        workout = workout[workout['end_datetime'] > '2000-01-01']
        workout['date'] = workout['start_datetime'].dt.date

        # drop datetime column
        workout = workout.drop(['creation_datetime', 'start_datetime',
                                'end_datetime'], axis=1)
        # add created_at, last_updated_by
        workout['created_at'] = pd.to_datetime('now')
        workout['updated_at'] = pd.to_datetime('now')
        workout.fillna(0, inplace=True)

        # cleanup activity_type and device column text, date
        workout['gadget'] = np.where(workout['device'].str.contains('Apple Watch'),
                                    'Apple Watch', 'iPhone')
        # remove HKWorkoutActivityType from activity_type text
        workout['activity'] = (workout['activity_type'].str
                            .replace('HKWorkoutActivityType', ''))
        # remove unnecessary columns
        workout = workout[['date', 'activity', 'duration', 'duration_unit',
                        'total_distance', 'total_distance_unit',
                        'total_energy_burned', 'total_energy_burned_unit',
                        'gadget', 'start_date', 'end_date', 'created_at',
                        'updated_at']]

        '''
        # create heartrate data frame
        print('Creating heartrate data...')
        li = list(zip(record_type, record_unit, record_value, record_sourceName,
                    record_sourceVersion, record_device, record_creationDate,
                    record_startDate, record_endDate))
        record = pd.DataFrame(li,
                            columns=['type', 'unit',
                                    'value', 'source_name',
                                    'source_version', 'device',
                                    'creation_date', 'start_date',
                                    'end_date'])
        # remove dates before 2000-01-01
        record['creation_datetime'] = pd.to_datetime(record['creation_date'])
        record = record[record['creation_datetime'] > '2000-01-01']
        record['start_datetime'] = pd.to_datetime(record['start_date'])
        record = record[record['start_datetime'] > '2000-01-01']
        record['end_datetime'] = pd.to_datetime(record['end_date'])
        record = record[record['end_datetime'] > '2000-01-01']
        record['date'] = record['start_datetime'].dt.strftime('%Y-%m-%d')

        # drop datetime column
        record = record.drop(['creation_datetime', 'start_datetime',
                            'end_datetime'], axis=1)

        # filter down to heartrate
        record = record[record['type'] == 'HKQuantityTypeIdentifierHeartRate']
        # clean up device data (look for Apple Watch, iPhone)
        record['gadget'] = np.where(record['device'].str.contains('Apple Watch'),
                                    'Apple Watch', 'iPhone')
        # decrease columns to necessary info
        record = record[['date', 'gadget', 'value']]
        record['value'] = record['value'].astype(float)
        # aggregate this before adding to db (max, min, avg)
        record_avg = record.groupby(['date', 'gadget'], as_index=False).mean()
        record_max = record.groupby(['date', 'gadget'], as_index=False).max()
        record_min = record.groupby(['date', 'gadget'], as_index=False).min()
        # combine these into a single df for record
        record_avg.columns = ['date', 'gadget', 'avg']
        record_max.columns = ['date', 'gadget', 'max']
        record_min.columns = ['date', 'gadget', 'min']
        heartrate = record_avg.merge(record_max, on=['date', 'gadget'])
        heartrate = heartrate.merge(record_min, on=['date', 'gadget'])
        # add created_at, last_updated_by
        heartrate['created_at'] = pd.to_datetime('now')
        heartrate['updated_at'] = pd.to_datetime('now')
        heartrate.fillna(0, inplace=True)
        # import pdb; pdb.set_trace()
        '''
        # csv exports
        df.to_csv(file_path + 'activity_summary.csv', index=False)
        exercise_time.to_csv(file_path + 'exercise_time.csv', index=False)
        workout.to_csv(file_path + 'workout.csv', index=False)
        # heartrate.to_csv(file_path + 'heartrate.csv', index=False)
        return df
