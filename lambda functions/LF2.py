import json
import os

import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from boto3.dynamodb.conditions import Key, Attr
import random

REGION = 'us-east-1'
HOST = 'search-restaurants-rvcpcwdylvui3wkdgt4fw4joii.us-east-1.es.amazonaws.com'
INDEX = 'restaurants'


def lambda_handler(event, context):
    response = connect()
    print(response)
    cuisine = response.get('cuisine')
    results = query(cuisine)
    # print(results)
    recommends = searchDB(results)
    print(recommends)
    emailSent = send_email(response, recommends, "yx2812@columbia.edu")


def connect():
    # connect sqs
    # pull sqs message
    query = {}
    sqs_client = boto3.client('sqs')
    sqs_url = 'https://sqs.us-east-1.amazonaws.com/924698316170/diningQueue'  # 这里需要一个建好后的url
    resp = sqs_client.receive_message(
        QueueUrl=sqs_url,
        AttributeNames=['SentTimestamp'],
        MessageAttributeNames=['All'],
        VisibilityTimeout=0,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=0
    )

    try:
        if 'Messages' in resp:
            message_attribute = resp['Messages'][0]
            query = json.loads(message_attribute['Body'])  # get cuisine from message later
            print("check point 2: query")
            print(query)
            receipt_handle = message_attribute['ReceiptHandle']

            sqs_client.delete_message(
                QueueUrl=sqs_url,
                ReceiptHandle=receipt_handle
            )
    except:
        print("Pull sqs message failed")
    return query


def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
                    cred.secret_key,
                    region,
                    service,
                    session_token=cred.token)


def query(term):
    randSeed = random.randrange(1000)
    query = {
        "size": 20,
        "query": {
            "function_score": {
                "query": {"query_string": {"query": str(term)}},
                "random_score": {"seed": randSeed}
            }
        }
    }

    client = OpenSearch(hosts=[{
        'host': HOST,
        'port': 443
    }],
        http_auth=get_awsauth(REGION, 'es'),
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection)

    res = client.search(index=INDEX, body=query)
    print(res)

    hits = res['hits']['hits']
    results = []
    for hit in hits:
        results.append(hit['_source'])

    return results


def searchDB(res):
    # connect dynamdb
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('yelp-restaurants')
    cnt = 0
    recommends = dict()

    for restaurant in res:
        id = restaurant.get('Restaurant')
        if cnt == 3:
            break
        response = table.scan(FilterExpression=Attr('restaurantID').eq(id))
        try:
            item = response['Items'][0]
            name = item.get("Name")
            address = item.get("Address").get("address1")
            recommends[name] = address
            cnt += 1

        except:
            print("dynamdb Response Failed")

    return recommends


def send_email(query, recommends, from_address="yx2812@columbia.edu"):
    client = boto3.client('ses')
    # {'date': '2023-10-11', 'city': 'manhattan', 'cuisine': 'japanese', 'time': '14:00', 'peopleNum': '2', 'email': 'zx2462@columbia.edu'}
    body_text = "Hello! Here are my {} restaurant suggestions for {} people, for {} at {}. ".format(
        query.get('cuisine'), query.get('peopleNum'), query.get('date'), query.get('time'))
    i = 1
    for key, value in recommends.items():
        body_text += "{}. {}, located at {}. ".format(i, key, value)
        i = i + 1
    body_text += "Enjoy your meal!"
    subject = "Email from DiningConcierge"
    to_addresses = [query.get('email')]

    response = client.send_email(
        Source=from_address,
        Destination={
            'ToAddresses': to_addresses,
        },
        Message={
            'Subject': {
                'Data': subject,
                'Charset': 'UTF-8'
            },
            'Body': {
                'Text': {
                    'Data': body_text,
                    'Charset': 'UTF-8'
                },
            }
        }
    )

    return response