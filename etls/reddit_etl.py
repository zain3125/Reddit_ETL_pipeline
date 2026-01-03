import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import praw
from pymongo import MongoClient
from utils.constants import PG_PARAMS, MONGO_URI

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

# Extract Reddit posts
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

# Transform Reddit data
def transform_data(post_df: pd.DataFrame):
    TARGET_COLUMNS_ORDER = [
        'id', 'subreddit_name_prefixed', 'title', 'flair', 'selftext', 
        'score', 'num_comments', 'author', 'created_utc', 'url', 
        'over_18', 'edited', 'spoiler', 'stickied'
    ]
    post_df['created_utc'] = pd.to_datetime(
        post_df['created_utc'], unit='s'
    ).dt.strftime('%Y-%m-%d %H:%M:%S')
    post_df['over_18'] = np.where((post_df['over_18'] == True), True, False)
    post_df['author'] = post_df['author'].astype(str)
    post_df['edited'] = post_df['edited'].where(post_df['edited'] == True, False)
    post_df['num_comments'] = post_df['num_comments'].astype(int)
    post_df['score'] = post_df['score'].astype(int)
    post_df['selftext'] = post_df['selftext'].astype(str)
    post_df['flair'] = post_df['link_flair_text'].where(post_df['link_flair_text'].notna(), None)
    post_df = post_df.drop(columns=['link_flair_text'])    
    post_df['title'] = post_df['title'].astype(str)
    post_df['subreddit_name_prefixed'] = post_df['subreddit_name_prefixed'].str.replace('r/', '', regex=False)
    
    missing_cols = [col for col in TARGET_COLUMNS_ORDER if col not in post_df.columns]
    if missing_cols:
        print(f"Warning: Missing columns {missing_cols} in DataFrame before final order.")

    post_df = post_df[TARGET_COLUMNS_ORDER]

    return post_df

# Connect to Postgres
def get_db_connection():
    try:    
        conn = psycopg2.connect(**PG_PARAMS)
        print(f"pg armes is {PG_PARAMS}")
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{PG_PARAMS['dbname']}'")
        exists = cur.fetchone()
        if not exists:
            cur.execute(f"CREATE DATABASE {PG_PARAMS['dbname']}")
            print(f"Database '{PG_PARAMS['dbname']}' created successfully.")
        else:
            print(f"Database '{PG_PARAMS['dbname']}' already exists.")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error while creating database: {e}")
        return None, None

    # Connect to the target database
    try:
        conn = psycopg2.connect(**PG_PARAMS)
        cur = conn.cursor()
        print(f"Connected to database '{PG_PARAMS['dbname']}'")
        return cur, conn
    except Exception as e:
        print(f"Error connecting to database '{PG_PARAMS['dbname']}': {e}")
        return None, None

def load_to_postgres(cur, conn, dataframe=None):
    # Create table if not exists
    create_table_query = """
    CREATE TABLE IF NOT EXISTS reddit_posts (
        id TEXT PRIMARY KEY,
        subreddit_name_prefixed TEXT,
        title TEXT,
        flair TEXT,
        selftext TEXT,
        score INT,
        num_comments INT,
        author TEXT,
        created_utc TIMESTAMP,
        url TEXT,
        over_18 BOOLEAN,
        edited BOOLEAN,
        spoiler BOOLEAN,
        stickied BOOLEAN
    );
    """
    try:
        cur.execute(create_table_query)
        conn.commit()
        print("Table 'reddit_posts' is ready.")
    except Exception as e:
        print(f"Error creating table: {e}")
        conn.rollback()
        cur.close()
        conn.close()
        return

    # Load data from DataFrame
    if dataframe is not None:
        df = dataframe
        print("Loaded data from DataFrame.")
    else:
        print("No data DataFrame provided.")
        cur.close()
        conn.close()
        return

    # Insert data
    insert_query = """
    INSERT INTO reddit_posts (
        id, subreddit_name_prefixed, title, flair, selftext, score, num_comments,
        author, created_utc, url, over_18, edited, spoiler, stickied
    )
    VALUES %s
    ON CONFLICT (id) DO NOTHING;
    """

    records = [tuple(row[col] for col in df.columns) for _, row in df.iterrows()]

    try:
        psycopg2.extras.execute_values(cur, insert_query, records)
        conn.commit()
        print(f"Inserted {len(df)} rows into 'reddit_posts'.")
    except Exception as e:
        print(f"Error inserting data: {e}")
        conn.rollback()

    # Close connection
    cur.close()
    conn.close()
    print("Connection closed.")

# Connect to MongoDB
def get_mongo_client():
    mongo_uri = MONGO_URI
    return MongoClient(mongo_uri)

def extract_reddit_comments(reddit_instance, subreddit, time_filter, limit=None):
    subreddit_obj = reddit_instance.subreddit(subreddit)
    comments = subreddit_obj.top(time_filter=time_filter, limit=limit)

