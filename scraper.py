import requests
from random import uniform
import time
import pandas as pd
from datetime import date
import json


class Scraper():
    """Scraper class contains the basic information that is needed for scraping.
    """
    # max_iter defines the max number of reqeust attempts.
    # If the max number of attempt is reached, the requests might be blocked.
    max_iter = 10
    geography_url = 'https://www.apartments.com/services/geography/search/'
    pins_url = 'https://www.apartments.com/services/search/'
    request_header = {
        'method': 'POST',
        'Accept': 'application/json, text/javascript, */*',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en',
        'content-type': 'application/json',
        'Cache-Control': 'no-cache',
        'Host': 'www.apartments.com',
        'Origin': 'https://www.apartments.com',
        'Referer': 'https://www.apartments.com/',
        'platform': 'web',
        'referer': 'https://classpass.com/search/ladera-heights-ca-usa/fitness-classes/4VvdcJFiBgT',
        'X_CSRF_TOKEN': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYmYiOjE1NTY4Nzk0NDYsImV4cCI6MTU1Njk2NTg0NiwiaWF0IjoxNTU2ODc5NDQ2LCJpc3MiOiJodHRwczovL3d3dy5hcGFydG1lbnRzLmNvbSIsImF1ZCI6Imh0dHBzOi8vd3d3LmFwYXJ0bWVudHMuY29tIn0.WWLSfxr-vGLFQ6RKCWZxtEEZZ8vHG4-1YEszrmt1Tfc',
        'X-Requested-With': 'XMLHttpRequest',
        'User-agent': 'Mozilla/5.0 (Windows NT 10.0 Win64 x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/17.17134'
    }
    rows_retrieved = 0

    def __init__(self, category_idx=0):
        """
        Constructor of the Scraper class
        Arguments:
            category_idx {int} -- The category in which we
            lon {float} -- The longitude of the center of the search. Default to lon of center LA.
        """
        self.session = requests.Session()
        self.category_idx = category_idx
        # We store the following information:
        self.venues = pd.DataFrame(
            columns=['apartment_id', 'lat', 'lon', 'description', 'feature_json', 'datetime'])

    @staticmethod
    def random_sleep():
        """Allows the program to sleep for random amount of time to avoid blocked by the site
        Raises:
            Exception -- If the provided url is not valid (HTTP 404) or the network condition is off, an exception will be thrown.
        Returns:
            None
        """
        time.sleep(uniform(0, 2))

    def parse_ids(self, ids_raw):
        ids_array_raw = ids_raw.split('~')
        id_lat_lon_array = []
        for id_string in ids_array_raw:
            id_string_array = id_string.split('|')
            lat = id_string_array[3]
            lon = id_string_array[4]
            if id_string_array[2] != 'null':
                for id_string_sub in json.load(id_string_array[2]):
                    id_lat_lon_array.append(tuple((id_string_sub['ListingId'], lat, lon)))
            else:
                id_lat_lon_array.append(id_string_array[0], lat, lon)
        return id_lat_lon_array

    def get_apartment_ids(self, zipcode):
        """It returns the apartment ids that are within the area.
        """
        # sleep to avoid detection
        Scraper.random_sleep()

        # repeat the request for max_iter times just to avoid package loss or network glitches
        for _ in range(self.max_iter):
            Scraper.random_sleep()
            resp = self.session.post(
                Scraper.search_url, headers=self.request_header, data=json.dumps('{"t": {}}'.format(zipcode)), verify=False)
            if resp.status_code == 200:
                geography = json.loads(resp.text)
                if len(geography) == 0:
                    return None
            print('ERROR with status code {}'.format(resp))
            print('HTTP response body {}'.format(resp.text))
        raise Exception(
            'Request failed {} times. It is probably blocked.'.format(self.max_iter))

        geography_payload = {}
        geography_payload['Geography'] = geography[0]

        # starting to search for the apartment ids
        # repeat the request for max_iter times just to avoid package loss or network glitches
        for _ in range(self.max_iter):
            Scraper.random_sleep()
            resp = self.session.post(
                Scraper.pins_url, headers=self.request_header, data=json.dumps(geography_payload), verify=False)
            if resp.status_code == 200:
                result = json.loads(resp.text)
                if 'PinsState' not in result:
                    return None
                ids_raw = result['PinsState']['cl']
                return self.parse_ids(ids_raw)

    def append_search_results(self, data, zipcode, attach):
        """Given the data, it append each entry of the data to the existing dataframe in the scraper instance.

        Arguments:
            data {an array of json objects} -- each entry of the array is a json object containing the venue information
        """
        datetime = date.today().strftime('%Y-%m-%d')
        if not attach:
            venues = pd.DataFrame(columns=[
                'id', 'datetime', 'venue_name', 'zipcode', 'location_name', 'activities', 'display_rating_total', 'display_rating_average', 'description'])
        for entry in data:
            id = entry['venue_id']
            venue_name = entry['venue_name']
            location_name = entry['location_name'] if 'location_name' in entry else None
            activities = entry['activities'] if 'activities' in entry else None
            description = entry['description'] if 'description' in entry else None
            display_rating_total = entry['display_rating_total'] if 'display_rating_total' in entry else None
            display_rating_average = entry['display_rating_average'] if 'display_rating_average' in entry else None
            if attach:
                self.venues = self.venues.append({'id': id, 'datetime': datetime, 'venue_name': venue_name, 'zipcode': zipcode, 'location_name': location_name,
                                                  'activities': activities, 'display_rating_total': display_rating_total, 'display_rating_average': display_rating_average,
                                                  'description': description}, ignore_index=True)
            else:
                venues = venues.append(
                    {'id': id, 'datetime': datetime, 'venue_name': venue_name, 'zipcode': zipcode, 'location_name': location_name,
                     'activities': activities, 'display_rating_total': display_rating_total, 'display_rating_average': display_rating_average,
                     'description': description}, ignore_index=True)
        return

    def save_venues_to_pickle(self, path='scraped_venues.pkl'):
        """Save the current venues to a pickle file locally. The default path is 'scraped_venues.pkl'
        """
        pd.to_pickle(self.venues, path)
