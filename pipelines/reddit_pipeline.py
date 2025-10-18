from utils.constants import CLIENT_ID, SECRET, USER_AGENT
from etls.reddit_etl import connect_to_reddit, extract_reddit_posts

def extract_reddit_data(file_name: str, subreddit: str, time_filter='day', limit=None):
    # Connect to Reddit API
    instance = connect_to_reddit(CLIENT_ID, SECRET, USER_AGENT)
    
    # Extract data from Reddit
    posts = extract_reddit_posts(instance, subreddit, time_filter, limit)
    # Transform data as needed

    # Load data to the desired destination
