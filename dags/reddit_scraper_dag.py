from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from datetime import datetime, timedelta
import praw
import pandas as pd
from pmaw import PushshiftAPI
import json
from typing import List

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def initialize_reddit():
    """Initialize Reddit API client"""
    return praw.Reddit(
        client_id=Variable.get("reddit_id"),
        client_secret=Variable.get("reddit_secret_key"),
        user_agent=Variable.get("reddit_user_agent")
    )

def fetch_reddit_data(subreddit_name: str, start_date: datetime, end_date: datetime, **context) -> List[dict]:
    """Fetch Reddit data for a given date range"""
    reddit = initialize_reddit()
    api = PushshiftAPI()
    stories = []
    
    print(f"Fetching data for {subreddit_name} from {start_date} to {end_date}")
    
    try:
        # Recent data from Reddit API
        if (datetime.now() - start_date).days < 90:
            subreddit = reddit.subreddit(subreddit_name)
            for submission in subreddit.new(limit=1000):
                submission_time = datetime.fromtimestamp(submission.created_utc)
                
                if start_date <= submission_time < end_date:
                    if not submission.removed_by_category and submission.selftext:
                        story = {
                            'title': submission.title,
                            'text': submission.selftext,
                            'author': str(submission.author),
                            'score': submission.score,
                            'created_utc': submission_time.isoformat(),
                            'url': submission.url,
                            'id': submission.id,
                            'num_comments': submission.num_comments,
                            'upvote_ratio': submission.upvote_ratio
                        }
                        stories.append(story)
        
        # Older data from Pushshift
        if not stories:
            start_timestamp = int(start_date.timestamp())
            end_timestamp = int(end_date.timestamp())
            
            submissions = api.search_submissions(
                subreddit=subreddit_name,
                after=start_timestamp,
                before=end_timestamp,
                limit=None
            )
            
            for submission in submissions:
                try:
                    reddit_submission = reddit.submission(id=submission['id'])
                    if not reddit_submission.removed_by_category and reddit_submission.selftext:
                        story = {
                            'title': reddit_submission.title,
                            'text': reddit_submission.selftext,
                            'author': str(reddit_submission.author),
                            'score': reddit_submission.score,
                            'created_utc': datetime.fromtimestamp(reddit_submission.created_utc).isoformat(),
                            'url': reddit_submission.url,
                            'id': reddit_submission.id,
                            'num_comments': reddit_submission.num_comments,
                            'upvote_ratio': reddit_submission.upvote_ratio
                        }
                        stories.append(story)
                except Exception as e:
                    print(f"Error processing submission {submission['id']}: {str(e)}")
                    continue
        
        return stories
    
    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        raise

def upload_to_s3(stories: List[dict], subreddit_name: str, date_str: str, **context):
    """Upload processed data to S3"""
    if not stories:
        print(f"No stories to upload for {date_str}")
        return
    
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = Variable.get("s3_bucket_name")
    
    df = pd.DataFrame(stories)
    json_str = df.to_json(orient='records', date_format='iso')
    
    s3_key = f'reddit_data/{subreddit_name}/{date_str}.json'
    s3_hook.load_string(
        string_data=json_str,
        key=s3_key,
        bucket_name=bucket_name,
        replace=True
    )
    
    print(f"Uploaded {len(stories)} stories to s3://{bucket_name}/{s3_key}")

with DAG(
    'reddit_scraper',
    default_args=default_args,
    description='Scrape Reddit data and store in S3',
    schedule_interval='@daily',
    catchup=True,
    max_active_runs=3
) as dag:
    
    # Dynamic task generation for date range
    subreddit_name = Variable.get("subreddit_name")
    processing_date = "{{ ds }}"  # Airflow's execution date
    next_date = "{{ next_ds }}"  # Next execution date
    
    # Fetch Reddit data
    fetch_task = PythonOperator(
        task_id=f'fetch_reddit_data',
        python_callable=fetch_reddit_data,
        op_kwargs={
            'subreddit_name': subreddit_name,
            'start_date': "{{ execution_date.start_of('day') }}",
            'end_date': "{{ next_execution_date.start_of('day') }}"
        }
    )
    
    # Upload to S3
    upload_task = PythonOperator(
        task_id=f'upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'subreddit_name': subreddit_name,
            'date_str': "{{ ds_nodash }}"
        }
    )
    
    fetch_task >> upload_task