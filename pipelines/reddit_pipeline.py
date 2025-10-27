from datetime import datetime
import pandas as pd
import os
from utils.constants import CLIENT_ID, SECRET, USER_AGENT, OUTPUT_PATH
from etls.reddit_etl import (connect_to_reddit, extract_reddit_posts, transform_data, load_data_to_csv
                             ,get_db_connection, load_to_postgres)

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
    print(f"Extracted {len(post_df)} posts from Reddit.")
    
    return post_df.to_json(orient="records")
    
def load_data_to_database(**context):
    json_data = context['ti'].xcom_pull(task_ids='extract_reddit_data')
    
    if not json_data:
        print("No data received from extract_reddit_data.")
        return
    
    df = pd.read_json(json_data)
    df['created_utc'] = pd.to_datetime(df['created_utc'], errors='coerce') 

    def format_datetime_for_postgres(dt):
        if pd.isna(dt):
            return None
        return dt.strftime('%Y-%m-%d %H:%M:%S')

    df['created_utc'] = df['created_utc'].apply(format_datetime_for_postgres)
    
    print(f"Loaded DataFrame with {len(df)} rows.")

    cur, conn = get_db_connection()
    if not cur or not conn:
        print("Database connection failed.")
        return

    load_to_postgres(cur=cur, conn=conn, dataframe=df)

def load_data_to_csv_task(**context):
    json_data = context['ti'].xcom_pull(task_ids='extract_reddit_data')
    if not json_data:
        print("❌ No data received from extract_reddit_data.")
        return

    # Convert JSON back to DataFrame
    df = pd.read_json(json_data)

    # Define file path dynamically
    file_path = f"{OUTPUT_PATH}/reddit_posts_{datetime.now().strftime('%Y%m%d')}.csv"

    # Ensure the output directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # Save to CSV
    df.to_csv(file_path, index=False)
    print(f"✅ Data saved to CSV at: {file_path}")

