import configparser
import os

parser = configparser.ConfigParser()

parser.read(os.path.join(os.path.dirname(__file__), "../config/config.conf"))

SECRET = parser.get('api_keys', 'reddit_secret_key')

CLIENT_ID = parser.get('api_keys', 'reddit_client_id')
USER_AGENT = parser.get('api_keys', 'user_agent')

# MongoDB
MONGO_URI = parser.get('mongodb','mongo_uri')
MONGO_DB = parser.get('mongodb','mongo_db')

RAW_COLLECTION = parser.get('mongodb','raw_collection')
CLEAN_COLLECTION = parser.get('mongodb','clean_collection')

DATABASE_HOST = parser.get('database', 'database_host')
DATABASE_NAME = parser.get('database', 'database_name')
DATABASE_PORT = parser.get('database', 'database_port')
DATABASE_USER = parser.get('database', 'database_username')
DATABASE_PASSWORD = parser.get('database', 'database_password')

PG_PARAMS = {
    "dbname": DATABASE_NAME,
    "user": DATABASE_USER,
    "password": DATABASE_PASSWORD,
    "host": DATABASE_HOST,
    "port": DATABASE_PORT
}

INPUT_PATH = parser.get('file_paths', 'input_path')
OUTPUT_PATH = parser.get('file_paths', 'output_path')
