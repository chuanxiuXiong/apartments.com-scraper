import requests
from random import uniform
import time
import pandas as pd
from datetime import date
import json
from bs4 import BeautifulSoup


class Scraper():
    """Scraper class contains the basic information that is needed for scraping.
    """
    # max_iter defines the max number of reqeust attempts.
    # If the max number of attempt is reached, the requests might be blocked.
    max_iter = 10
    geography_url = 'https://www.apartments.com/services/geography/search/'
    search_url = 'https://www.apartments.com/services/search/'
    request_header = {
        'Accept': "application/json, text/javascript, */*; q=0.01",
        'Accept-Encoding': "gzip, deflate, br",
        'Accept-Language': "en-US, en; q=0.8, zh-Hans-CN; q=0.5, zh-Hans; q=0.3",
        'Cache-Control': "no-cache",
        'Content-Type': "application/json",
        'Host': "www.apartments.com",
        'Origin': "https://www.apartments.com",
        'Referer': "https://www.apartments.com/",
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/17.17134",
        'X_CSRF_TOKEN': "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYmYiOjE1NTY4Nzk0NDYsImV4cCI6MTU1Njk2NTg0NiwiaWF0IjoxNTU2ODc5NDQ2LCJpc3MiOiJodHRwczovL3d3dy5hcGFydG1lbnRzLmNvbSIsImF1ZCI6Imh0dHBzOi8vd3d3LmFwYXJ0bWVudHMuY29tIn0.WWLSfxr-vGLFQ6RKCWZxtEEZZ8vHG4-1YEszrmt1Tfc",
        'X-Requested-With': "XMLHttpRequest",
        'Postman-Token': "6f9268f4-f473-40d8-b53e-70e363dd7b51"
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
        time.sleep(uniform(1, 5))

    def parse_ids(self, ids_raw):
        ids_array_raw = ids_raw.split('~')
        id_lat_lon_array = []
        for id_string in ids_array_raw:
            id_string_array = id_string.split('|')
            lat = id_string_array[3]
            lon = id_string_array[4]
            if id_string_array[2] != 'null':
                for id_string_sub in json.load(id_string_array[2]):
                    id_lat_lon_array.append(
                        tuple((id_string_sub['ListingId'], lat, lon)))
            else:
                id_lat_lon_array.append(tuple((id_string_array[0], lat, lon)))
        return id_lat_lon_array

    def scrape_apartment_info(self, url):
        headers = {'Cache-Control': 'no-cache', 'Accept': '*/*',
                   'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/17.17134"}
        print("Parsing Info...")
        print("URL {}".format(url))
        for _ in range(self.max_iter):
            Scraper.random_sleep()
            response = requests.request("GET", url, headers=headers)
            print(response)
            if response.status_code == 404:
                return None
            elif response.status_code == 200:
                # parse the info
                soup = BeautifulSoup(response.text, 'html.parser')
                info = {}
                info['lat'] = soup.find(
                    'meta', {'property': 'place:location:latitude'})['content']
                info['lon'] = soup.find(
                    'meta', {'property': 'place:location:longitude'})['content']
                info['neighborhood'] = soup.find(
                    'a', {'class': 'neighborhood'}).text
                info['price'] = soup.find('td', {'class': 'rent'}).text.replace(
                    '\r', '').replace('\n', '').split()[0]
                info['description'] = soup.find(
                    'section', {'class': 'descriptionSection'}).p.text.replace('\u2022', '').strip()
                features = soup.find_all('div', class_='specList')
                feature_list = []
                for feature in features:
                    # if there is only one feature, there are no <li> on the website
                    # so we do something special here
                    if not feature.find_all('li'):
                        feature_str = feature.find_all(
                            'span')[-1].text.replace('\r\n', '').replace('\u2022', '').strip()
                        if feature_str != '':
                            feature_list.append(feature_str)
                    else:
                        for li in feature.find_all('li'):
                            feature_str = li.text.replace('\u2022', '')
                            if feature_str != '':
                                feature_list.append(feature_str)
                info['feature_json'] = feature_list
                return info
        if _ == self.max_iter - 1:
            raise Exception(
                'Request failed {} times. It is probably blocked.'.format(self.max_iter))

    def store_apartment_info(self, zipcode, conn):
        """It returns the apartment infos that are within the area.
        """
        # sleep to avoid detection
        Scraper.random_sleep()

        # repeat the request for max_iter times just to avoid package loss or network glitches
        for _ in range(self.max_iter):
            Scraper.random_sleep()
            payload = {}
            payload['t'] = zipcode
            resp = requests.request(
                "POST", url=self.geography_url, data=json.dumps(payload), headers=self.request_header)
            if resp.status_code == 200:
                geography = json.loads(resp.text)
                if not geography:
                    return None
                if len(geography) == 0:
                    return None
                break
            print('ERROR with status code {}'.format(resp))
            print('HTTP response body {}'.format(resp.text))
        if _ == self.max_iter - 1:
            raise Exception(
                'Request failed {} times. It is probably blocked.'.format(self.max_iter))

        geography_payload = {}
        geography_payload['Geography'] = geography[0]
        paging_payload = {}
        paging_payload['CurrentPageListingKey'] = None
        print("Requesting Pages...")
        end = False
        page_idx = 1
        previous_url = ''  # records the first id of the last page
        while not end:
            print('Requesting Page {}'.format(page_idx))
            paging_payload['Page'] = page_idx
            geography_payload['Paging'] = paging_payload
            # starting to search for the apartment links
            # repeat the request for max_iter times just to avoid package loss or network glitches
            for _ in range(self.max_iter):
                Scraper.random_sleep()
                print('Requesting page iter...')
                resp = self.session.post(
                    Scraper.search_url, headers=self.request_header, data=json.dumps(geography_payload), verify=False)
                if resp.status_code == 200:
                    result = json.loads(resp.text)
                    if 'PlacardState' not in result:
                        return None
                    html_raw = result['PlacardState']['HTML']
                    soup = BeautifulSoup(html_raw, 'html.parser')
                    cards = soup.find_all(
                        'a', {'class': 'placardTitle js-placardTitle'})
                    # the apartments.com allows request with `page` exceeding the # of pages and returns by the last page
                    # Therefore, to check if it is the last page, we check if the current page's first url is the same as
                    # one in the last page
                    if not cards:
                        return None
                    if not cards[0]['href']:
                        print(cards[0])
                        print(
                            'href not in cards[0]... breaking the loop...')
                        return None
                    if cards[0]['href'][cards[0]['href'].find('http'):-1] == previous_url:
                        print('End of the region... Braking...')
                        end = True
                        break
                    previous_url = cards[0]['href'][cards[0]
                                                    ['href'].find('http'):-1]

                    for card in cards:
                        if not card['href']:
                            print(card)
                            print(
                                'href not in card... breaking the loop...')
                            return None
                        url = card['href'][card['href'].find('http'):-1]
                        print("Scraping url {}".format(url))
                        info = self.scrape_apartment_info(url)
                        if not info:
                            continue
                        print('Inserting into database...')
                        conn.execute('INSERT INTO apartments VALUES (NULL,{0},{1},"{2}","{3}","{4}");'.format(float(
                            info['lat']), float(info['lon']), info['description'], json.dumps(info['feature_json']).replace('"', '\''), date.today().strftime('%Y-%m-%d')))
                else:
                    return None
            page_idx += 1
