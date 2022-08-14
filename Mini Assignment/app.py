from unicodedata import decimal
from flask import Flask, request , jsonify, make_response, Response
from werkzeug.security import generate_password_hash, check_password_hash
import jwt
import datetime
import movie_service as dynamodb
from functools import wraps
from timeit import default_timer
import user_service as userdb
import uuid
from urllib import response

app = Flask(__name__)

app.config['SECRET_KEY'] = 'knightmare'

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None

        if 'x-access-token' in request.headers:
            token = request.headers['x-access-token']

        if not token:
            return jsonify({'message' : 'Token is missing!'}), 401

        # data = jwt.decode(token, 'knightmare',algorithms=["HS256"])
        # print(data)
        try: 
            data = jwt.decode(token, 'knightmare',algorithms=["HS256"])
            
            # current_user = userdb.get_user(username=data['username'])
        except:
            return jsonify({'message' : 'Token is invalid!'}), 401

        return f(*args, **kwargs)

    return decorated

def timer(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        start_time = default_timer()
        response = f(*args, **kwargs)
        time_to_execute = default_timer() - start_time
        print(f"{time_to_execute}-TIME-TO-EXECUTE")
        response["time_to_execute"] = time_to_execute
        return response

    return wrapper

# @app.route('/')
# # @token_required
# @timer
# def home():
#     return "Hello"


@app.route('/create_table_user')
@timer
def user_table():
    userdb.create_table_user()
    return 'Table Created'

@app.route('/add_user', methods=['POST'])
@timer
def add_user():
    data = request.get_json()
    hashed_password = generate_password_hash(data['password'], method='sha256')
    response = userdb.add_user(data['username'],hashed_password)

    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        return {
            'msg': 'Added Successfully',
            'response': response
        }
    return {
        'msg': 'Error',
        'response': response
    }



@app.route('/user/<string:username>', methods=['GET'])
@token_required
@timer
def get_user(username):
    response = userdb.get_user(username)
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        if ('Item' in response):
            return {'Item': response['Item']}
        return {'msg': 'Item not found!'}
    return {
        'msg': 'Some error occured',
        'response': response
    }

@app.route('/user/<string:username>', methods=['DELETE'])
@timer
def delete_user(username):
    response = userdb.delete_movie(username)
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        return {
            'msg': 'Deleted successfully',
        }
    return {
        'msg': 'Some error occcured',
        'response': response
    }

@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data['username']
    password = data['password']
    response = None
    if not username or not password:
        response = make_response('Could not verify, Incorrect username or password', 401, {'WWW-Authenticate' : 'Basic realm="Login required!"'})

    user = userdb.get_user(username)
    # dbname = user['Item']['username']
    # dbpassword = user['Item']['password']
    if not 'Item' in user:
        response = make_response('Could not verify', 401, {'WWW-Authenticate' : 'Basic realm="Login required!"'})

    if check_password_hash(user['Item']['password'], password):
        token = jwt.encode({'username' : user['Item']['username'], 'exp' : datetime.datetime.utcnow() + datetime.timedelta(minutes=120)}, 'knightmare')

        result = jsonify({'token' : token})

    return result

@app.route('/create_table_movie')
@timer
def root_route():
    dynamodb.create_table_movie()
    return 'Table Created'


@app.route('/add_movie', methods=['POST'])
@token_required
@timer
def add_movie():
    data = request.get_json()
    response = dynamodb.add_movie(data['imdb_title_id'], data['title'], data['original_title'], data['year'], data['date_published'], data['genre'], data['duration'], data['country'], data['language'], data['director'], data['writer'], data['production_company'], data['actors'], data['description'], int(data['avg_vote']*10), data['votes'], data['budget'], data['usa_gross_income'], data['worldwide_gross_income'], data['metascore'], data['reviews_from_users'], data['reviews_from_critics']
                                  )
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        return {
            'msg': 'Added Successfully',
            'response': response
        }
    return {
        'msg': 'Error',
        'response': response
    }


@app.route('/load_csv')
@token_required
@timer
def load_csv():
    response = dynamodb.load_csv()
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        return {
            'msg': 'All Movies Added Successfully',
            'response': response
        }
    return {
        'msg': 'Error',
        'response': response
    }


@app.route('/movie/<string:id>', methods=['GET'])
@token_required
@timer
def get_movie(id):
    response = dynamodb.get_movie(id)
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        if ('Item' in response):
            return {'Item': response['Item']}
        return {'msg': 'Item not found!'}
    return {
        'msg': 'Some error occured',
        'response': response
    }


@app.route('/movie/<string:id>', methods=['DELETE'])
@token_required
@timer
def delete_movie(id):
    response = dynamodb.delete_movie(id)
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        return {
            'msg': 'Deleted successfully',
        }
    return {
        'msg': 'Some error occcured',
        'response': response
    }


@app.route('/m1/<string:director>/<string:year1>/<string:year2>')
@token_required
@timer
def scan_year_director(director, year1, year2):
    response = dynamodb.m1(director, year1, year2)

    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        if ('Items' in response):
            return {'Items': response['Items']}
        return {'msg': 'Item not found!'}
    return {
        'msg': 'Some error occured',
        'response': response
    }


@app.route('/m2/<string:language>/<string:review>')
@token_required
@timer
def scan_language_review(language, review):
    response = dynamodb.m2(language, review)
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        if ('Items' in response):
            return {'Items': response['Items']}
        return {'msg': 'Item not found!'}
    return {
        'msg': 'Some error occured',
        'response': response
    }


@app.route('/m3/<string:year>/<string:country>')
@token_required
@timer
def scan_highes_budget_year_country(year, country):
    response = dynamodb.m3(year, country)
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        if ('Items' in response):
            return {'Item': response['Items'][0]}
        return {'msg': 'Item not found!'}
    return {
        'msg': 'Some error occured',
        'response': response
    }


if __name__ == '__main__':
    app.run(port=5000)
