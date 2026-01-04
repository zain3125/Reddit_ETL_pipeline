from utils.constants import CLIENT_ID, SECRET, USER_AGENT, MONGO_DB, RAW_COLLECTION
from elts.reddit_elt import (connect_to_reddit, extract_reddit_posts, load_posts_to_mongo, load_comments_to_mongo,
                              get_mongo_client, extract_reddit_comments, merge_posts_and_comments_in_mongo)

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
    
    if not posts: return
    client = get_mongo_client()
    load_posts_to_mongo(client, MONGO_DB, RAW_COLLECTION, posts)
    client.close()

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
    if not comments: return

    client = get_mongo_client()
    load_comments_to_mongo(client, MONGO_DB, 'raw_comments', comments)
    client.close()

def run_mongo_aggregation():
    merge_posts_and_comments_in_mongo()
