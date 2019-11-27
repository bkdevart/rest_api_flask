#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 21 14:11:11 2019

@author: brandon
"""

import os
import xml.etree.ElementTree as ET
import numpy as np
import pandas as pd


def activity_summary(file_path):

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

            # this is the key to memory management
            elem.clear()

    # create activity data data frame
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

    # workout = workout.drop(['device', 'activity_type'], axis=1)

    # create record data frame
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

    # TODO: make sure to re-enable these for flask code base
    # df.to_csv(file_path + 'activity_summary.csv', index=False)
    # exercise_time.to_csv(file_path + 'exercise_time.csv', index=False)
    # workout.to_csv(file_path + 'workout.csv', index=False)
    # record.to_csv(file_path + 'workout.csv', index=False)

    return df, exercise_time, workout, heartrate


# %%


activity_data, exercise_time, workout, heartrate = \
    activity_summary(os.getcwd() + '/uploads')
