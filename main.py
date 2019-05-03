from scraper import Scraper
import pandas as pd
from Databases.s3_transfer import s3_transfer
import sqlalchemy as db
from sqlalchemy_utils import database_exists, create_database
import config
import time


def get_database_host_user_password(aws_or_gcp):
    """Download the database credentials from aws s3. This part is specific and is subject to change
    on different bucket configuration.
    """
    assert aws_or_gcp == 'aws' or aws_or_gcp == 'gcp'

    credentials = s3_transfer(
        bucket_name=config.bucket_name,
        file_name=config.credential_name,
        direction='download',
        file_type='json')

    return [credentials['databases']['mysql'][aws_or_gcp]['host'],
            credentials['databases']['mysql'][aws_or_gcp]['user'],
            credentials['databases']['mysql'][aws_or_gcp]['password']]


def create_database_engine(user, password, host, database, create_database_bool):
    """Create the mysql database engine.
    """
    if not database_exists('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(user, password, host, database)):
        if create_database_bool:
            create_database(
                'mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(user, password, host, database))
        else:
            raise Exception('The database does not exist')
            exit(1)
    try:
        engine = db.create_engine(
            'mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(user, password, host, database))
        return engine
    except:
        print('Exception: cannot create connection with database: {}'.format(database))


def get_all_zipcodes(conn):
    """Get all the zipcodes that we are interested in searching from the database.

    Arguments:
        conn {class 'sqlalchemy.engine.base.Connection'} -- an object of the database connection
        user {string} -- the user of the database
        password {string} -- the password
        host {string} -- the host of the database
    """
    zipcodes = pd.read_sql(
        'SELECT zipcode from geocoded_zips', con=conn)
    return zipcodes


if __name__ == '__main__':
    # record the start time of the program
    start_time = time.time()

    # get the database credentials from aws bucket
    [gcp_host, gcp_user, gcp_password] = get_database_host_user_password('gcp')
    [aws_host, aws_user, aws_password] = get_database_host_user_password('aws')

    # create the scraper
    scraper = Scraper()

    # establish the database connection with the database zipcode_geocoded_data
    engine = create_database_engine(
        gcp_user, gcp_password, gcp_host, 'zipcode_geocoded_data', False)
    conn = engine.connect()
    cursor = conn.cursor()
    detail = False

    # create apartments table
    cursor.execute(
        'Create TABLE IF NOT EXISTS apartments (apartment_id VARCHAR(10), lat FLOAT, lon FLOAT, description TEXT, feature_json TEXT, datetime DATE);')

    # load the data for each zipcode of interest
    zipcodes = get_all_zipcodes(conn)
    empty_zipcodes = set()
    nonempty_zipcodes = set()
    for zipcode in zipcodes.values:
        print('Starting zipcode {} ...'.format(zipcode))
        data = scraper.get_apartments(zipcode[0])
        if data:
            scraper.append_search_results(data, zipcode[0], True)
            nonempty_zipcodes.add(zipcode[0])
        else:
            empty_zipcodes.add(zipcode[0])
            print('No venues found in zipcode {} ...'.format(zipcode))

    print('Finished retrieving data!')
    print('Transferring the data to the database...')
    # scraper.venues.to_sql('classpass_venues', create_database_engine(gcp_user, gcp_password, gcp_host, 'dev_test', True),
    #                       if_exists='append', index=False)
    print('Total rows retrieved: {}'.format(len(scraper.venues)))
    print('List of nonempty zipcodes:')
    print(list(nonempty_zipcodes))
    print('Time used: {}'.format(time.time() - start_time))