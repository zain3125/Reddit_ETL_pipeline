import praw
from pymongo import MongoClient
from utils.constants import MONGO_URI

# Connect to Reddit API
def connect_to_reddit(api_key, api_secret, user_agent):
    try:
        reddit = praw.Reddit(client_id=api_key,
                            client_secret=api_secret,
                            user_agent=user_agent)
        print("Connected to Reddit API successfully")
        return reddit
    except Exception as e:
        print(f"Error connecting to Reddit: {e}")    

# Connect to MongoDB
def get_mongo_client():
    mongo_uri = MONGO_URI
    return MongoClient(mongo_uri)
