from audioop import reverse
from boto3 import resource
import config
import os
import csv
from boto3.dynamodb.conditions import Key, Attr

AWS_ACCESS_KEY_ID = config.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY
REGION_NAME = config.REGION_NAME
ENDPOINT_URL = config.ENDPOINT_URL

resource = resource(
    'dynamodb',
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=REGION_NAME
)

def create_table_user():
    table = resource.create_table(
        TableName='User',  # Name of the table
        KeySchema=[
            {
                'AttributeName': 'username',
                'KeyType': 'HASH'  # HASH = partition key, RANGE = sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'username',  # Name of the attribute
                'AttributeType': 'S'   # N = Number (S = String, B= Binary)
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    return table

UserTable = resource.Table('User')

def add_user(username,password):
    response = UserTable.put_item(
        Item = {
            'username':username,
            'password':password
        }
    )
    return response

def get_user(username):
    response = UserTable.get_item(
        Key = {
            'username':username
        },
        AttributesToGet = ['username','password']
    )
    return response

def delete_movie(username):
    response = UserTable.delete_item(
        Key={
            'username': username
        }
    )

    return response