from pymongo import UpdateOne
from datetime import datetime
from utils.constants import MONGO_DB, RAW_COLLECTION
from utils.connections import get_mongo_client

# Extract data
def extract_reddit_posts(reddit_instance, subreddit, time_filter, limit=None):
    # Fetch posts from one subreddit
    subreddit_obj = reddit_instance.subreddit(subreddit)
    posts = subreddit_obj.top(time_filter=time_filter, limit=limit)

    posts_list = []

    for post in posts:
        # Convert post from praw.models.Submission to dict
        post_dict = vars(post)

        # Remove what we can't make it in dict
        post_dict.pop('_reddit', None)
        post_dict.pop('subreddit', None)
        if post_dict.get('author'):
            post_dict['author'] = str(post_dict['author'])

        posts_list.append(post_dict)

    return posts_list

def process_comment(comment):
    # Comment schema
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
        # Recursion for more replies
        for reply in comment.replies:
            data["replies"].append(process_comment(reply))
            
    return data

# --- New Addition for Smart Sync ---
def get_active_post_ids(mongo_client, db_name, collection_name):
    db = mongo_client[db_name]
    current_ts = datetime.utcnow().timestamp()
    query = {
        "created_utc": {"$gt": current_ts - (30 * 24 * 60 * 60)}, # Posts from last 30 days
        "$or": [
            {"last_sync_utc": {"$lt": current_ts - (24 * 60 * 60)}}, # Not synced in 24h
            {"last_sync_utc": {"$exists": False}}
        ]
    }
    return [post['id'] for post in db[collection_name].find(query, {"id": 1})]

def extract_comments_for_ids(reddit_instance, post_ids):
    all_comments_tree = []
    for post_id in post_ids:
        try:
            submission = reddit_instance.submission(id=post_id)
            submission.comments.replace_more(limit=0)
            for top_level_comment in submission.comments:
                comment_data = process_comment(top_level_comment)
                comment_data['post_id'] = post_id
                all_comments_tree.append(comment_data)
        except: continue
    return all_comments_tree

# Load data
def load_posts_to_mongo(mongo_client, db_name, collection_name, posts):
    db = mongo_client[db_name]
    collection = db[collection_name]

    operations = []
    now = datetime.utcnow()
    sync_ts = now.timestamp()

    for post in posts:
        post["ingested_at"] = now
        post["last_sync_utc"] = sync_ts

        operations.append(
            UpdateOne(
                {"_id": f"t3_{post.get('id')}"},
                {"$set": post},
                upsert=True
            )
        )

    if operations:
        collection.bulk_write(operations)

def load_comments_to_mongo(mongo_client, db_name, collection_name, comments, post_ids_updated=None):
    db = mongo_client[db_name]
    collection = db[collection_name]

    operations = []
    now = datetime.utcnow()

    for comment in comments:
        comment["ingested_at"] = now

        operations.append(
            UpdateOne(
                {"_id": f"t1_{comment.get('id')}"},
                {"$set": comment},
                upsert=True
            )
        )

    if operations:
        collection.bulk_write(operations)
    
    # Update last_sync_utc for the posts we just updated [Added]
    if post_ids_updated:
        db[RAW_COLLECTION].update_many(
            {"id": {"$in": post_ids_updated}},
            {"$set": {"last_sync_utc": now.timestamp()}}
        )

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
            "$out": "merged_reddit_data"
        }
    ]
    
    db[RAW_COLLECTION].aggregate(pipeline)
    client.close()
    print("Aggregation completed: Posts and Comments merged into 'merged_reddit_data'")

# Transform data
def transform_reddit_data(mongo_client, db_name, source_collection, target_collection):
    db = mongo_client[db_name]
    FIELDS_TO_REMOVE = [
                    "_additional_fetch_params",
                    "_comments_by_id",
                    "_fetched",
                    "created",
                    "name",
                    "secure_media_embed",
                    "subreddit_id",
                    "subreddit_name_prefixed",
                    "subreddit_subscribers",
                    "subreddit_type",
                    "author_flair_background_color",
                    "author_flair_css_class",
                    "author_flair_richtext",
                    "author_flair_template_id",
                    "author_flair_text",
                    "author_flair_text_color",
                    "author_flair_type",
                    "selftext_html",
                    "link_flair_background_color",
                    "link_flair_css_class",
                    "link_flair_richtext",
                    "link_flair_template_id",
                    "link_flair_text_color",
                    "link_flair_type",
                    "url_overridden_by_dest",
                    "preview.images.resolutions",
                    "preview.images.variants",
                    "preview.images.id",
                    "comments_list.post_id",
                    "comments_list._id"
                ]

    pipeline = [
            {
                "$unset": FIELDS_TO_REMOVE
            },
            {
                "$set": {
                    "root_as_array": {
                        "$filter": {
                            "input": { "$objectToArray": "$$ROOT" },
                            "as": "item",
                            "cond": { "$ne": ["$$item.v", None] }
                        }
                    }
                }
            },
            {
                "$replaceRoot": {
                    "newRoot": { "$arrayToObject": "$root_as_array" }
                }
            },
            {
                "$merge": {
                    "into": target_collection,
                    "whenMatched": "merge",
                    "whenNotMatched": "insert"
                    }
            }
        ]
    db[source_collection].aggregate(pipeline)
    print(f"✅ Data cleaned using unified helper and moved to {target_collection}")
