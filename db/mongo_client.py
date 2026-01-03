from pymongo import MongoClient
import os

def get_mongo_client():
    mongo_uri = os.getenv(
        "MONGO_URI",
        "mongodb://mongodb:27017"
    )
    return MongoClient(mongo_uri)

def insert_raw_posts(posts, db_name="reddit", collection_name="posts"):
    client = get_mongo_client()
    db = client[db_name]
    collection = db[collection_name]

    if posts:
        collection.insert_many(posts)

    client.close()
