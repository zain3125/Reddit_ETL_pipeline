from utils.constants import CLIENT_ID, SECRET, USER_AGENT, MONGO_DB, RAW_COLLECTION
from etls.reddit_etl import (connect_to_reddit, extract_reddit_posts,
                              get_mongo_client, extract_reddit_comments)

def extract_reddit_posts_data(subreddits, time_filter='day', limit=None):
    # Connect to Reddit API
    instance = connect_to_reddit(CLIENT_ID, SECRET, USER_AGENT)
    all_posts = []

    # Extract data from Reddit
    for subreddit in subreddits:
        print(f"Fetching posts from r/{subreddit}...")
        posts = extract_reddit_posts(instance, subreddit, time_filter, limit)
        for post in posts:
            post["subreddit"] = subreddit
        all_posts.extend(posts)

    # Load data to the desired destination
    print(f"Extracted {len(all_posts)} posts from Reddit.")
    
    return all_posts

def load_raw_posts_to_mongo(**context):
    posts = context['ti'].xcom_pull(task_ids='extract_reddit_data')

    if not posts:
        print("No data received from extract_reddit_data.")
        return

    client = get_mongo_client()
    db = client[MONGO_DB]
    collection = db[RAW_COLLECTION]

    for post in posts:
        collection.update_one(
            {"_id": f"t3_{post.get('id')}"},
            {"$set": post},
            upsert=True
        )

    client.close()
    print(f"Inserted/Updated {len(posts)} posts into MongoDB.")

def extract_reddit_comments_data(subreddits, time_filter='day', limit=None):
    instance = connect_to_reddit(CLIENT_ID, SECRET, USER_AGENT)
    all_comments = []

    for subreddit in subreddits:
        print(f"Fetching comments from r/{subreddit}...")
        comments = extract_reddit_comments(instance, subreddit, time_filter, limit)
        all_comments.extend(comments)

    print(f"Extracted {len(all_comments)} comments from Reddit.")
    return all_comments

def load_raw_comments_to_mongo(**context):
    comments = context['ti'].xcom_pull(task_ids='extract_comments_task')

    if not comments:
        print("No comments data received.")
        return

    client = get_mongo_client()
    db = client[MONGO_DB]
    collection = db['raw_comments']

    for comment in comments:
        collection.update_one(
            {"_id": comment.get('id')},
            {"$set": comment},
            upsert=True
        )

    client.close()
    print(f"Inserted/Updated {len(comments)} comment trees into MongoDB.")