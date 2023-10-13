import json
import boto3
from botocore.exceptions import ClientError

import argparse
import json
import pprint
import requests
import sys
import urllib
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

import datetime

# This client code can run on Python 2.x or 3.x.  Your imports can be
# simpler if you only need one of those.
try:
    # For Python 3.0 and later
    from urllib.error import HTTPError
    from urllib.parse import quote
    from urllib.parse import urlencode
except ImportError:
    # Fall back to Python 2's urllib2 and urllib
    from urllib2 import HTTPError
    from urllib import quote
    from urllib import urlencode

API_KEY = 'i3LlxZb2eNydjhDKA9UHqxoax4gGGtjtkUZXCWrKltpY2OTPH9tXSgGhZHjt2VtG1UvuKIA1jkyP3dnjNbSiH39mfD1WTElVKACNmkOD07iSXayUke6ldSIb21YkZXYx'
API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/'  # Business ID will come after slash.

inserted_restaurants = set()  # for storing the restaurants inserted to db


def request(host, path, api_key, url_params=None):
    url_params = url_params or {}
    url = '{0}{1}'.format(host, quote(path.encode('utf8')))
    headers = {
        'Authorization': 'Bearer %s' % api_key,
    }

    response = requests.request('GET', url, headers=headers, params=url_params)
    print(f"request def:{response}")
    return response.json()


def query_api(params):
    response = request(API_HOST, SEARCH_PATH, API_KEY, url_params=params)
    businesses = response.get('businesses')
    if not businesses:
        print(u'No businesses for {0} in {1} found.'.format(params['term'], params['location']))
        return

    return businesses


def collectYelpData():
    cuisines = ['chinese', 'japanese', 'indian', 'mexican', 'thai', 'american', 'korean']
    for cuisine in cuisines:
        for offset in range(20):
            params = {
                'location': 'Manhattan',
                'offset': offset * 50,
                'limit': 50,
                'term': cuisine,
                'sort_by': 'best_match'
            }
            businesses = query_api(params)
            # print(businesses)
            response = insert_data(businesses, cuisine)
            # print(response)


host = 'search-restaurants-rvcpcwdylvui3wkdgt4fw4joii.us-east-1.es.amazonaws.com'
region = 'us-east-1'


def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
                    cred.secret_key,
                    region,
                    service,
                    session_token=cred.token)


client = OpenSearch(hosts=[{
    'host': host,
    'port': 443
}],
    http_auth=get_awsauth(region, 'es'),
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection)


def opensearch(restaurant_id, cuisine):
    os_insert = {
        "Restaurant": restaurant_id,
        "Type": cuisine
    }

    # Insert data into Elasticsearch
    res = client.index(index="restaurants", body=os_insert)
    return res


def insert_data(data_list, cuisine, db=None, table='yelp-restaurants'):
    if not db:
        db = boto3.resource('dynamodb', region_name='us-east-1')
    table = db.Table(table)
    # ignored if the same index is provided
    for data in data_list:
        if data['id'] not in inserted_restaurants:
            os_response = opensearch(data['id'], cuisine)
            print(f"Inserted into opensearch: {os_response}")

            response = table.put_item(Item={
                'insertedAtTimestamp': str(datetime.datetime.now()),
                'restaurantID': data['id'],
                'Name': data['name'],
                'Type': cuisine,
                'Address': data['location'],
                'Coordinates': str(data['coordinates']),
                'NumberofReviews': data['review_count'],
                'Rating': str(data['rating']),
                'ZipCode': data['location']['zip_code']
            })
            inserted_restaurants.add(data['id'])
            os_response = opensearch(data['id'], cuisine)
            print(f"Inserted into opensearch: {os_response}")
    # print('@insert_data: response', response)
    return response


def lambda_handler(event, context):
    # TODO implement
    collectYelpData()
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

