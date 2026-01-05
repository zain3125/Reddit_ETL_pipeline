from airflow.exceptions import AirflowException
import logging
from utils.constants import CLIENT_ID, SECRET, USER_AGENT, MONGO_DB, RAW_COLLECTION, CLEAN_COLLECTION
from utils.connections import connect_to_reddit
from elts.reddit_elt import (
    extract_reddit_posts, load_posts_to_mongo, 
    load_comments_to_mongo, get_mongo_client, merge_posts_and_comments_in_mongo, 
    transform_reddit_data, get_active_post_ids, extract_comments_for_ids
)

def extract_reddit_posts_data(subreddits, time_filter='day', limit=None):
    instance = connect_to_reddit(CLIENT_ID, SECRET, USER_AGENT)
    if not instance:
        raise AirflowException("Failed to connect to Reddit API during posts extraction")

    all_posts = []
    for subreddit in subreddits:
        try:
            posts = extract_reddit_posts(instance, subreddit, time_filter, limit)
            for post in posts:
                post["subreddit"] = subreddit
            all_posts.extend(posts)
        except Exception as e:
            logging.error(f"Error fetching posts from r/{subreddit}: {e}")
            raise AirflowException(f"Critical error extracting posts from {subreddit}")

    return all_posts

def load_raw_posts_to_mongo(**context):
    posts = context['ti'].xcom_pull(task_ids='extract_reddit_data')

    if not posts:
        raise AirflowException("Error: Extraction returned no posts. Nothing to load.")

    client = get_mongo_client()
    try:
        load_posts_to_mongo(client, MONGO_DB, RAW_COLLECTION, posts)
        logging.info(f"Successfully loaded {len(posts)} posts to MongoDB.")
    except Exception as e:
        logging.exception("Failed to load posts into Mongo")
        raise AirflowException(f"Critical failure in load_raw_posts_to_mongo: {e}")
    finally:
        client.close()

def extract_reddit_comments_data():
    instance = connect_to_reddit(CLIENT_ID, SECRET, USER_AGENT)
    client = get_mongo_client()
    
    try:
        post_ids = get_active_post_ids(client, MONGO_DB, RAW_COLLECTION)
        if not post_ids:
            logging.info("No active posts need comment updates.")
            return {"comments": [], "updated_post_ids": []}

        comments = extract_comments_for_ids(instance, post_ids)
        return {"comments": comments, "updated_post_ids": post_ids}
    finally:
        client.close()

# Updated for Smart Sync
def load_raw_comments_to_mongo(**context):
    data = context['ti'].xcom_pull(task_ids='extract_comments_task')

    if not data or not data.get('comments'):
        logging.warning("No comments found in XCom.")
        return

    client = get_mongo_client()
    try:
        load_comments_to_mongo(client, MONGO_DB, 'raw_comments', data['comments'], data.get('updated_post_ids'))
        logging.info(f"Successfully loaded comments to MongoDB.")
    except Exception as e:
        logging.exception("Failed to load comments into Mongo")
        raise AirflowException(f"Critical failure in load_raw_comments_to_mongo: {e}")
    finally:
        client.close()

def run_mongo_aggregation():
    try:
        merge_posts_and_comments_in_mongo()
        logging.info("Successfully merged posts and comments.")
    except Exception as e:
        logging.exception("Aggregation failed")
        raise AirflowException(f"MongoDB Aggregation (Merge) failed: {e}")

def run_transform_pipeline():
    client = get_mongo_client()
    try:
        transform_reddit_data(
            client, 
            MONGO_DB, 
            'merged_reddit_data',
            CLEAN_COLLECTION
        )
        logging.info("Remove not needed fields step completed successfully.")
    except Exception as e:
        logging.exception("Transformation failed")
        raise AirflowException(f"Critical error during transformation: {e}")
    finally:
        client.close()
