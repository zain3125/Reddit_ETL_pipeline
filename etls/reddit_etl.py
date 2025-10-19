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
