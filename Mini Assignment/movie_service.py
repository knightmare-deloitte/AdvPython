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


def create_table_movie():
    table = resource.create_table(
        TableName='Movie',  # Name of the table
        KeySchema=[
            {
                'AttributeName': 'imdb_title_id',
                'KeyType': 'HASH'  # HASH = partition key, RANGE = sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'imdb_title_id',  # Name of the attribute
                'AttributeType': 'S'   # N = Number (S = String, B= Binary)
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 60,
            'WriteCapacityUnits': 60
        }
    )
    return table


MovieTable = resource.Table('Movie')

# Add movie to db


def add_movie(imdb_title_id, title,
              original_title, year, date_published, genre,
              duration, country, language, director, writer, production_company, actors, description,
              avg_vote, votes, budget,
              usa_gross_income,
              worldwide_gross_income,
              metascore, reviews_from_users, reviews_from_critics
              ):
    response = MovieTable.put_item(
        Item={
            'imdb_title_id': imdb_title_id,
            'title': title,
            'original_title': original_title,
            'year': year,
            'date_published': date_published,
            'genre': genre,
            'duration': duration,
            'country': country,
            'language': language,
            'director': director,
            'writer': writer,
            'production_company': production_company,
            'actors': actors,
            'description': description,
            'avg_vote': avg_vote,
            'votes': votes,
            'budget': budget,
            'usa_gross_income': usa_gross_income,
            'worldwide_gross_income': worldwide_gross_income,
            'metascore': metascore,
            'reviews_from_users': reviews_from_users,
            'reviews_from_critics': reviews_from_critics,
        }
    )
    return response


# Get movie from db by id

def get_movie(id):
    response = MovieTable.get_item(
        Key={
            'imdb_title_id': id
        },
        AttributesToGet=['avg_vote', 'title', 'year', 'budget',
                         'reviews_from_users', 'country', 'genre', 'duration', 'director']
    )
    return response

# Delete movie from db by id


def delete_movie(id):
    response = MovieTable.delete_item(
        Key={
            'imdb_title_id': id
        }
    )

    return response


# Load data from csv to db

csv_file = '/home/abhikushwaha/Documents/Adv Python/code/movies.csv'


def load_csv():
    with MovieTable.batch_writer() as batch:
        with open(csv_file, 'r') as file:
            next(file)
            f = csv.reader(file)
            for data in f:
                # data = line.rstrip().split(',')
                # for i in range(len(data)):
                #     if not len(data[i]):
                #         data[i]="na"
                response = batch.put_item(
                    Item={
                        'imdb_title_id': data[0],
                        'title': data[1],
                        'original_title': data[2],
                        'year': data[3],
                        'date_published': data[4],
                        'genre': data[5],
                        'duration': data[6],
                        'country': data[7],
                        'language': data[8],
                        'director': data[9],
                        'writer': data[10],
                        'production_company': data[11],
                        'actors': data[12],
                        'description': data[13],
                        'avg_vote': data[14],
                        'votes': data[15],
                        'budget': data[16].replace(',',''),
                        'usa_gross_income': data[17],
                        'worldwide_gross_income': data[18],
                        'metascore': data[19],
                        'reviews_from_users': data[20],
                        'reviews_from_critics': data[21],
                    }
                )
    return response


# Table scan director and year

def m1(director, year1, year2):
    response = MovieTable.scan(
        FilterExpression=Attr('director').eq(
            director) & Attr('year').between(year1, year2)

    )
    return response

# Table scan by language and review_by_users sorted by review_by_users


def m2(language, review):
    response = MovieTable.scan(
        FilterExpression=Attr('language').eq(
            language) & Attr('reviews_from_users').gt(review)

    )
    def comp(e):
        return e['reviews_from_users']
    response['Items'].sort(reverse=True, key=comp)
    return response

# Table scan for highest budget by country and year
def m3(year, country):
    response = MovieTable.scan(
        FilterExpression=Attr('country').eq(country) & Attr('year').eq(year)
    )
    
    def comp(e):
        return e['budget']
    response['Items'].sort(reverse=True,key=comp)
    return response
