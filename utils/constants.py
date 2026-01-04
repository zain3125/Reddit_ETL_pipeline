import configparser
import os

parser = configparser.ConfigParser()

parser.read(os.path.join(os.path.dirname(__file__), "../config/config.conf"))

# Reddit
SECRET = parser.get('api_keys', 'reddit_secret_key')

CLIENT_ID = parser.get('api_keys', 'reddit_client_id')
USER_AGENT = parser.get('api_keys', 'user_agent')

# MongoDB
MONGO_URI = parser.get('mongodb','mongo_uri')
MONGO_DB = parser.get('mongodb','mongo_db')

RAW_COLLECTION = parser.get('mongodb','raw_collection')
CLEAN_COLLECTION = parser.get('mongodb','clean_collection')
