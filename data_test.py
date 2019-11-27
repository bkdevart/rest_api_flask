# coding=utf-8
import os
import pandas as pd
from backend.models import ActivityData, User, WorkoutData, ExerciseTime
from sqlalchemy.sql import select
from backend.api.activity import activity_summary


def get_activity():
    '''
    returns dataframe containing entire activity_data table
    '''
    basedir = os.getcwd()
    engine = 'sqlite:///' + os.path.join(basedir, 'data-dev.sqlite')
    s = select([ActivityData])
    df = pd.read_sql(s, con=engine)
    # TODO: this needs to be taken care of during import
    df['date'] = pd.to_datetime(df['date'])
    return df


def graph_activity_trend(df):
    '''
    plots activity trend data at all aggregates
    '''
    graph = activity_summary(df, 30, 'date', 'move')
    graph.plot(x='date')

    graph = activity_summary(df, 1000, 'week_start', 'move')
    graph.plot(x='date')

    graph = activity_summary(df, 365, 'month', 'move')
    graph.plot(x='date')

    graph = activity_summary(df, 365, 'year', 'move')
    graph.plot(x='date')


def graph_activity_current(df):
    '''
    plots activity current data at all aggregates
    note: the max() function here simulates the recent = 1 in server code
    '''
    # aggregate data based off of max() value at that aggregation
    graph = activity_summary(df, 1, 'date', 'move')
    graph = graph[graph['date'] == graph['date'].max()]
    graph.plot(x='date', kind='bar')
    
    graph = activity_summary(df, 1, 'week_start', 'move')
    graph = graph[graph['date'] == graph['date'].max()]
    graph.plot(x='date', kind='bar')
    
    graph = activity_summary(df, 1, 'month', 'move')
    graph = graph[graph['date'] == graph['date'].max()]
    graph.plot(x='date', kind='bar')
    
    graph = activity_summary(df, 1, 'year', 'move')
    graph = graph[graph['date'] == graph['date'].max()]
    graph.plot(x='date', kind='bar')


def get_workout():
    '''
    returns dataframe containing entire activity_data table
    '''
    basedir = os.getcwd()
    engine = 'sqlite:///' + os.path.join(basedir, 'data-dev.sqlite')
    s = select([WorkoutData])
    df = pd.read_sql(s, con=engine)
    # TODO: this needs to be taken care of during import
    df['date'] = pd.to_datetime(df['date'])
    return df
# In[main]


# df = get_activity()
df = get_workout()
# filter down to single user
df = df[df['user_id'] == 1]

# graph_activity_trend(df)
# graph_activity_current(df)

# TODO: write API function to pull list of available workout exercises
# TODO: write API function to pull data for specific workout exercises
# TODO: figure out how to show graphically - might be good just to show all



