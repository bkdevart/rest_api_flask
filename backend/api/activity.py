'''
Contains functions for filtering and aggregating data, as well as retrieving
activity data from the database for graphs
'''

from . import api
from . import users
from ..models import ActivityData, User, db
import pandas as pd
from flask import request
from sqlalchemy.sql import select


@api.route('/activity_current', methods=['POST'])
def pull_activity_current():
    '''
    Formats data for graphing most recent data (current day/week/month/year)
    (Requirement 3.4.1)

    Parameters
    ----------
        user_id: int
            user id to graph
        agg: string
            aggregate level for data - date, week_start, month, and year are options
        kind: string
            type of exercise (move, exercise, stand)
    '''
    data = request.get_json()
    user_id = data['user_id']
    recent = 1
    agg = data['agg']
    kind = data['kind']

    # original defaults
    s = select([ActivityData]).where(ActivityData.user_id==user_id)

    df = pd.read_sql(s, con=db.engine, parse_dates=['date'])
    graph = activity_summary(df, recent, agg, kind)
    graph = graph[graph['date'] == graph['date'].max()]
    # convert to json
    return graph.to_json(date_format='iso', index=False, orient='table')

@api.route('/activity_trend', methods=['POST'])
def pull_activity_trend():
    '''
    Formats data for graphing trend data.
    (Requirement 3.4.2)

    Parameters
    ----------
        user_id: int
            user id to graph
        recent: int
            number of data points to go back for graphs
        agg: string
            aggregate level for data - date, week_start, month, and year are options
        kind: string
            type of exercise (move, exercise, stand)
    '''
    data = request.get_json()
    user_id = data['user_id']
    recent = data['recent']
    agg = data['agg']
    kind = data['kind']

    # original defaults
    s = select([ActivityData]).where(ActivityData.user_id==user_id)

    df = pd.read_sql(s, con=db.engine, parse_dates=['date'])
    graph = activity_summary(df, recent, agg, kind)
    # convert to json
    return graph.to_json(date_format='iso', index=False, orient='table')


def activity_summary(df, recent, agg, kind):
    '''
    This function performs the data wrangling to return activity data for graphs
    (Requirement 3.2.2)

    Parameters
    ----------
    df : dataframe
        Source data to summarize
    recent : int
        how many data points to go back for goal data
    agg : string
        Aggregation level.  Values can be date, week_start, month, and year
    kind : string
        Exercise type.  Values can be move, exercise, or stand
    '''
    # pull totals
    if agg == 'week_start':
        activity = agg_week(df)
    elif agg == 'month':
        activity = agg_month(df)
    elif agg == 'year':
        activity = agg_year(df)
    elif agg == 'date':
        activity = agg_day(df)
    else:
        print('Invalid aggregate.  Defaulting to day view.')
        activity = agg_day(df)

    # check df length
    recent = check_data_limit(df, recent)
    # var_recent = check_data_limit(df, var_recent)

    # plot (for testing purposes)
    if kind == 'move':
        # energy
        # create energy line graph with actual/goal
        energy_goal = activity[['date', 'energy_burned',
                                'energy_burned_goal']]
        graph = energy_goal.tail(recent)
        
    elif kind == 'exercise':
        # exercise
        # create exercise line graph with actual/goal
        exercise_goal = activity[['date', 'exercise_time',
                                  'exercise_time_goal']]
        graph = exercise_goal.tail(recent)

    elif kind == 'stand':
        # stand
        # create stand line graph with actual/goal
        stand_goal = activity[['date', 'stand_hours', 'stand_hours_goal']]
        graph = stand_goal.tail(recent)

    else:
        d = {'Error': ['No data']}
        graph = pd.DataFrame(data=d)

    return graph

def agg_day(df):
    '''
    Returns df with daily goal difference and variance%
    (Requirements 3.4.1.1 and 3.4.2.1)
    '''
    df = calc_var(df)
    return df


def agg_week(df):
    '''
    Returns df with weekly totals, calculates goal differance and variance
    (Requirements 3.4.1.2 and 3.4.2.2)
    '''
    df['week_start'] = (df['date'] - pd.to_timedelta
                        (df['date'].dt.dayofweek, unit='d'))
    df = df.groupby(['week_start'], as_index=False).sum()
    df = calc_var(df)
    df['date'] = df['week_start']
    return df


def agg_month(df):
    '''
    Returns df with monthly totals, calculates goal differance and variance
    (Requirements 3.4.1.3 and 3.4.2.3)
    '''
    # modify date to be first date of month
    df['month'] = pd.to_datetime(df['date'].dt.strftime('%Y-%m') + '-01')
    df = df.groupby(['month'], as_index=False).sum()
    df = calc_var(df)
    df['date'] = df['month']
    return df


def agg_year(df):
    '''
    Returns df with yearly totals, calculates goal differance and variance
    (Requirements 3.4.1.4 and 3.4.2.4)
    '''
    # modify date to be January 1st of year
    df['year'] = pd.to_datetime(df['date'].dt.strftime('%Y') + '-01-01')
    df = df.groupby(['year'], as_index=False).sum()
    df = calc_var(df)
    df['date'] = df['year']
    return df


def calc_var(df):
    '''
    Calculates variances for aggregated dataframes
    (Requirement 3.2.2)
    '''
    df['energy_var#'] = df['energy_burned'] - df['energy_burned_goal']
    df['energy_var%'] = ((df['energy_burned'] - df['energy_burned_goal']) /
                         df['energy_burned_goal'])
    df['exercise_var#'] = df['exercise_time'] - df['exercise_time_goal']
    df['exercise_var%'] = ((df['exercise_time'] - df['exercise_time_goal'])
                           / df['exercise_time_goal'])
    df['stand_var#'] = df['stand_hours'] - df['stand_hours_goal']
    df['stand_var%'] = ((df['stand_hours'] - df['stand_hours_goal']) /
                        df['stand_hours_goal'])
    return df


def check_data_limit(df, data_size):
    '''
    Used to check if size specified is supported by data
    
    returns
    -------
        original data limit if acceptable, dataframe count if not
    '''
    if data_size > len(df.index):
        data_size = len(df.index)
    return data_size
