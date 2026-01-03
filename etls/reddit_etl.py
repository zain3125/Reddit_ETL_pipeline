import praw
from pymongo import MongoClient
from utils.constants import MONGO_URI, MONGO_DB, RAW_COLLECTION

# Connect to Reddit API
def connect_to_reddit(api_key, api_secret, user_agent):
    try:
        reddit = praw.Reddit(client_id=api_key,
                            client_secret=api_secret,
                            user_agent=user_agent)
        print("Connected to Reddit API successfully")
        return reddit
    except Exception as e:
        print(f"Error connecting to Reddit: {e}")    

# Connect to MongoDB
def get_mongo_client():
    mongo_uri = MONGO_URI
    return MongoClient(mongo_uri)

# Extract data
def extract_reddit_posts(reddit_instance, subreddit, time_filter, limit=None):
    subreddit_obj = reddit_instance.subreddit(subreddit)
    posts = subreddit_obj.top(time_filter=time_filter, limit=limit)

    posts_list = []

    for post in posts:
        post_dict = vars(post)

        post_dict.pop('_reddit', None)
        post_dict.pop('subreddit', None)
        if post_dict.get('author'):
            post_dict['author'] = str(post_dict['author'])

        posts_list.append(post_dict)

    return posts_list

def process_comment(comment):
    comment_dict = vars(comment)
    
    data = {
        "id": comment.id,
        "body": comment.body,
        "author": str(comment.author) if comment.author else "[deleted]",
        "score": comment.score,
        "created_utc": comment.created_utc,
        "replies": []
    }

    if hasattr(comment, "replies"):
        comment.replies.replace_more(limit=0)
        for reply in comment.replies:
            data["replies"].append(process_comment(reply))
            
    return data

def extract_reddit_comments(reddit_instance, subreddit, time_filter, limit=None):
    subreddit_obj = reddit_instance.subreddit(subreddit)
    posts = subreddit_obj.top(time_filter=time_filter, limit=limit)

    all_comments_tree = []

    for post in posts:
        post.comments.replace_more(limit=0)
        
        for top_level_comment in post.comments:
            comment_data = process_comment(top_level_comment)
            comment_data['post_id'] = post.id
            all_comments_tree.append(comment_data)

    return all_comments_tree

def merge_posts_and_comments_in_mongo():
    client = get_mongo_client()
    db = client[MONGO_DB]
    
    pipeline = [
        {
            "$lookup": {
                "from": "raw_comments",
                "localField": "id",
                "foreignField": "post_id",
                "as": "comments_list"
            }
        },
        {
            "$out": "processed_reddit_data"
        }
    ]
    
    db[RAW_COLLECTION].aggregate(pipeline)
    client.close()
    print("✅ Aggregation completed: Posts and Comments merged into 'processed_reddit_data'")
