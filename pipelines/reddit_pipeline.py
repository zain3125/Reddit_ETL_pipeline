from airflow.exceptions import AirflowException
import logging
from utils.constants import CLIENT_ID, SECRET, USER_AGENT, MONGO_DB, RAW_COLLECTION
from elts.reddit_elt import (
    connect_to_reddit, extract_reddit_posts, load_posts_to_mongo, 
    load_comments_to_mongo, get_mongo_client, extract_reddit_comments, 
    merge_posts_and_comments_in_mongo, transform_reddit_data
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

def extract_reddit_comments_data(subreddits, time_filter='day', limit=None):
    instance = connect_to_reddit(CLIENT_ID, SECRET, USER_AGENT)
    if not instance:
        raise AirflowException("Failed to connect to Reddit API during comments extraction")

    all_comments = []
    for subreddit in subreddits:
        try:
            comments = extract_reddit_comments(instance, subreddit, time_filter, limit)
            all_comments.extend(comments)
        except Exception as e:
            logging.error(f"Error fetching comments from r/{subreddit}: {e}")
            raise AirflowException(f"Critical error extracting comments from {subreddit}")

    return all_comments

def load_raw_comments_to_mongo(**context):
    comments = context['ti'].xcom_pull(task_ids='extract_comments_task')

    if not comments:
        logging.warning("No comments found in XCom.")

    client = get_mongo_client()
    try:
        load_comments_to_mongo(client, MONGO_DB, 'raw_comments', comments)
        logging.info(f"Successfully loaded {len(comments)} comments to MongoDB.")
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
            'processed_reddit_data',
            'needed fields'
        )
        logging.info("Remove not needed fields step completed successfully.")
    except Exception as e:
        logging.exception("Transformation failed")
        raise AirflowException(f"Critical error during transformation: {e}")
    finally:
        client.close()