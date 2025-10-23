import configparser
import os

parser = configparser.ConfigParser()

parser.read(os.path.join(os.path.dirname(__file__), "../config/config.conf"))

SECRET = parser.get('api_keys', 'reddit_secret_key')

CLIENT_ID = parser.get('api_keys', 'reddit_client_id')
USER_AGENT = parser.get('api_keys', 'user_agent')

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

POST_FIELDS = (
    'id',
    'subreddit_name_prefixed',
    'title',
    'link_flair_text',
    'selftext',
    'score',
    'num_comments',
    'author',
    'created_utc',
    'url',
    'over_18',
    'edited',
    'spoiler',
    'stickied'
)
