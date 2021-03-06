from scraper import Scraper
import pandas as pd
from Databases.s3_transfer import s3_transfer
import sqlalchemy as db
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database
import config
import time
from datetime import date


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
    zip_engine = create_database_engine(
        aws_user, aws_password, aws_host, 'zipcode_geocoded_data', False)
    zip_conn = zip_engine.connect()
    detail = False

    # establish the database connection with the database dev_test
    engine = create_database_engine(
        gcp_user, gcp_password, gcp_host, 'dev_test', True)
    Session = sessionmaker(bind=engine)
    conn = engine.connect()

    print('Creating table in the gcp database...')
    # create apartments table
    conn.execute(
        'Create TABLE IF NOT EXISTS apartments (id INT AUTO_INCREMENT PRIMARY KEY,lat FLOAT, lon FLOAT, description TEXT, feature_json TEXT, datetime DATE);')

    # load the data for each zipcode of interest
    zipcodes = get_all_zipcodes(zip_conn)
    empty_zipcodes = set()
    nonempty_zipcodes = set()
    for zipcode in zipcodes.values:
        print('Starting zipcode {} ...'.format(zipcode))
        conn = engine.connect()
        scraper.store_apartment_info(zipcode[0], conn)
        # ***************************************************************
        # The following code is only for getting the apartment locations.
        # This has much better performance.
        # id_lat_lon_array = scraper.get_apartment_ids(zipcode[0])
        # print(id_lat_lon_array)
        # if id_lat_lon_array:
        #     for id_lat_lon in id_lat_lon_array:
        #         (id, lat, lon) = id_lat_lon
        #         # check to see if the id already exists in the database
        #         print('Selecting existing id...')
        #         id_rows = conn.execute(
        #             'SELECT apartment_id FROM apartments WHERE apartment_id = "{}";'.format(id))
        #         empty = True
        #         for row in id_rows:
        #             empty = False
        #             break
        #         if empty:
        #             [description, feature_json] = scraper.scrape_apartment_info(
        #                 id, lat, lon)
        #             conn.execute('INSERT INTO apartments VALUES ({0},{1},{2},{3},{4},{5})'.format(
        #                 id, float(lat), float(lon), description, feature_json, date.today().strftime('%Y-%m-%d')))
        #             print('Finished inserting...')
        #         print(id, lat, lon)
        # **************************************************************
    print('Finished retrieving data!')
    print('Transferring the data to the database...')
    print('List of nonempty zipcodes:')
    print(list(nonempty_zipcodes))
    print('Time used: {}'.format(time.time() - start_time))
