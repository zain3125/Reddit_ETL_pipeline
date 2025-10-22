import numpy as np
import pandas as pd
import praw
from praw import Reddit
from utils.constants import POST_FIELDS

# Function to connect to Reddit API
def connect_to_reddit(api_key, api_secret, user_agent):
    try:
        reddit = praw.Reddit(client_id=api_key,
                            client_secret=api_secret,
                            user_agent=user_agent)
        print("Connected to Reddit API successfully")
        return reddit
    except Exception as e:
        print(f"Error connecting to Reddit: {e}")    

# Function to extract Reddit data
def extract_reddit_posts(reddit_instance: Reddit, subreddit: str, time_filter: str, limit=None):
    subreddit = reddit_instance.subreddit(subreddit)
    posts = subreddit.top(time_filter=time_filter, limit=limit)

    posts_lists = []

    for post in posts:
        post_dict = vars(post)
        
        post = {key: post_dict[key] for key in POST_FIELDS}
        posts_lists.append(post)

    return posts_lists

# Function to transform Reddit data
def transform_data(post_df: pd.DataFrame):
    post_df['created_utc'] = pd.to_datetime(post_df['created_utc'],unit='s')
    post_df['over_18'] = np.where((post_df['over_18']== True), True, False)
    post_df['author'] = post_df['author'].astype(str)
    post_df['edited'] = post_df['edited'].where(post_df['edited'] == True, False)
    post_df['num_comments'] = post_df['num_comments'].astype(int)
    post_df['score'] = post_df['score'].astype(int)
    post_df['selftext'] = post_df['selftext'].astype(str)
    post_df['flair'] = post_df['link_flair_text'].where(post_df['link_flair_text'].notna(), None)
    post_df = post_df.drop(columns=['link_flair_text'])
    post_df['title'] = post_df['title'].astype(str)
    post_df['subreddit_name_prefixed'] = post_df['subreddit_name_prefixed'].str.replace('r/', '', regex=False)

    return post_df

# Function to load data to CSV
def load_data_to_csv(data: pd.DataFrame, path: str):
    data.to_csv(path, index=False)

