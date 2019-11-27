# from . import api
# from . import users
from models import ActivityData, User, db
import pandas as pd
from flask import request


def pull_activity(user_id, goal_recent=30, var_recent=30, agg='date',
                  kind='exercise'):
    '''
    formats data for graphing weekly aggregate data.
    parameters:
        user_id: user id to graph
        goal_recent: number of data points to go back for goal graphs
        var_recent: number of data points to go back for variance graphs
        agg: aggregate level for data - date, week_start, month, and year
             are options
    '''
    # TODO: pull dataframe filtered by user id
    user_id = request.values.get('user_id')
    s = ActivityData.query.filter_by(user_id=user_id).all()
    df = pd.read_sql(s, con=db.engine)
    # json = graph_agg(df)

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
        print('Invalid aggregate.  Defaulting to year view.')
        activity = agg_day(df)

    # check df length
    goal_recent = check_data_limit(df, goal_recent)
    var_recent = check_data_limit(df, var_recent)

    # plot (for testing purposes)
    if kind == 'move':
        # energy
        # create energy line graph with actual/goal
        energy_goal = activity[[agg, 'energy_burned',
                                'energy_burned_goal']]
        graph = energy_goal.tail(goal_recent)
        '''
        # create energy bar graph with var%
        # consider limiting this due to graph format
        energy_var = activity[[agg, 'energy_var%']]
        energy_var = energy_var.tail(var_recent)
        energy_goal.plot(x=agg, y=['energy_burned', 'energy_burned_goal'],
                         title=title_prefix + ' Energy Burned')
        energy_var.plot(x=agg, y='energy_var%', kind='bar',
                        title='Recent ' + title_prefix +
                        ' Energy Burned Variance to Goal')
        '''
    elif kind == 'exercise':
        # exercise
        # create exercise line graph with actual/goal
        exercise_goal = activity[[agg, 'exercise_time',
                                  'exercise_time_goal']]
        graph = exercise_goal.tail(goal_recent)
        '''
        # create exercise bar graph with var%
        exercise_var = activity[[agg, 'exercise_var%']]
        exercise_var = exercise_var.tail(var_recent)
        exercise_goal.plot(x=agg, y=['exercise_time',
                                     'exercise_time_goal'],
                           title=title_prefix + ' Exercise')
        exercise_var.plot(x=agg, y='exercise_var%', kind='bar',
                          title='Recent ' + title_prefix +
                          ' Exercise Variance to Goal')
        '''
    elif kind == 'stand':
        # stand
        # create stand line graph with actual/goal
        stand_goal = activity[[agg, 'stand_hours', 'stand_hours_goal']]
        graph = stand_goal.tail(goal_recent)
        '''
        # create stand bar graph with var%
        stand_var = activity[[agg, 'stand_var%']]
        stand_var = stand_var.tail(var_recent)
        stand_goal.plot(x=agg, y=['stand_hours', 'stand_hours_goal'],
                        title=title_prefix + ' Hours Standing')
        stand_var.plot(x=agg, y='stand_var%', kind='bar',
                       title='Recent ' + title_prefix +
                       ' Stand Variance to Goal')
        '''
    else:
        d = {'Error': ['No data']}
        graph = pd.DataFrame(data=d)

    # convert to json
    return graph.to_json(date_format='iso', index=False, orient='table')


def agg_day(df):
    '''
    returns df with daily goal difference and variance%
    '''
    df = calc_var(df)
    return df


def agg_week(df):
    '''
    returns df with weekly totals, calculates goal differance and variance
    '''
    df['week_start'] = (df['date'] - pd.to_timedelta
                        (df['date'].dt.dayofweek, unit='d'))
    df = df.groupby(['week_start'], as_index=False).sum()
    df = calc_var(df)
    return df


def agg_month(df):
    '''
    returns df with monthly totals, calculates goal differance and variance
    '''
    df['month'] = df['date'].dt.strftime('%Y-%m')
    df = df.groupby(['month'], as_index=False).sum()
    df = calc_var(df)
    return df


def agg_year(df):
    '''
    returns df with yearly totals, calculates goal differance and variance
    '''
    df['year'] = df['date'].dt.strftime('%Y')
    df = df.groupby(['year'], as_index=False).sum()
    df = calc_var(df)
    return df


def calc_var(df):
    '''
    calculates variances for aggregated dataframes
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
    used to check if size specified is supported by data
    returns:
        original data limit if acceptable, dataframe count if not
    '''
    if data_size > len(df.index):
        data_size = len(df.index)
    return data_size
