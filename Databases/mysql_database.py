import sqlalchemy
import pandas as pd
import traceback
from functools import wraps
from time import time


def measure(func):
    @wraps(func)
    def _time_it(*args, **kwargs):
        start = int(round(time() * 1000))
        try:
            return func(*args, **kwargs)
        finally:
            end_ = int(round(time() * 1000)) - start
            print("Total execution time: {end_ if end_ > 0 else 0} ms")
    return _time_it


def downloading(query, **kwargs):
    # connecting to mysql database via the previously declared
    user = kwargs['user']
    password = kwargs['password']
    host = kwargs['host']
    database = kwargs['db']

    # opens a cursor to write sql code through
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(
        user,
        password,
        host,
        database))
    try:
        raw_data = pd.read_sql(query, con=database_connection)
        print("data downloaded from server")
        database_connection.dispose()
        return raw_data
    except:
        database_connection.dispose()
        traceback.print_exc(limit=1)

# upload function


def uploading(upload_data=None, **kwargs):

    user = kwargs['user']
    password = kwargs['password']
    host = kwargs['host']
    database = kwargs['db']
    table = kwargs['table']

    # opens a cursor to write sql code through
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(
        user,
        password,
        host,
        database))
    try:
        upload_data.to_sql(
            name=table,
            con=database_connection,
            if_exists='append',
            index=True,
            chunksize=3000,
            method='multi')
        print("data uploaded to server")
        database_connection.dispose()
    except:
        database_connection.dispose()
        traceback.print_exc()
