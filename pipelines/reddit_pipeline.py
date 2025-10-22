import pandas as pd
from utils.constants import CLIENT_ID, SECRET, USER_AGENT, OUTPUT_PATH
from etls.reddit_etl import connect_to_reddit, extract_reddit_posts, transform_data, load_data_to_csv

def extract_reddit_data(file_name: str, subreddits: list, time_filter='day', limit=None):
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

    post_df = pd.DataFrame(all_posts)

    # Transform data as needed
    post_df = transform_data(post_df)

    # Load data to the desired destination
    file_path = (f"{OUTPUT_PATH}/{file_name}.csv")
    load_data_to_csv(post_df, file_path)

    return file_path
