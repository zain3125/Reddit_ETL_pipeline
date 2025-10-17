import praw
from praw import Reddit

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
def extract_reddit_posts(reddit_instance: Reddit, subreddit: str, time_fillter: str, limit=None):
    subreddit = reddit_instance.subreddit(subreddit)
    posts = subreddit.top(time_filter=time_fillter, limit=limit)

    posts_lists = []

    print(posts)
    #for post in posts:
