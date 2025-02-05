import praw
import pandas as pd
import time
import os
from datetime import datetime, timedelta
from pmaw import PushshiftAPI
import boto3
from io import StringIO
from dotenv import load_dotenv

def initialize_reddit():
    """Initialize and return a Reddit API client instance.
    
    This function initializes a Reddit API client using credentials stored in environment 
    variables. It requires the following environment variables to be set:
    - reddit_id: The Reddit API client ID
    - secret_key: The Reddit API client secret
    - user_agent: A unique user agent string identifying your application
    
    Returns:
        praw.Reddit: An authenticated Reddit API client instance
        
    Raises:
        praw.exceptions.ClientException: If there are issues with the credentials
        praw.exceptions.PRAWException: For other PRAW-related errors
    """
    # Load environment variables
    load_dotenv()
    
    # You'll need to create a Reddit app and get these credentials
    reddit = praw.Reddit(
        client_id=os.environ['reddit_id'],
        client_secret=os.environ['secret_key'],
        user_agent=os.environ['user_agent']
    )
    return reddit

def initialize_s3():
    """Initialize and return an S3 client instance.
    
    This function initializes an S3 client using credentials stored in environment variables.
    It requires the following environment variables to be set:
    - AWS_ACCESS_KEY_ID: The AWS access key ID
    - AWS_SECRET_ACCESS_KEY: The AWS secret access key
    - AWS_REGION: The AWS region (default is 'us-east-1')
    
    Returns:
        boto3.client: An authenticated S3 client instance
    """
    return boto3.client('s3')

def get_date_range(start_date, end_date):
    """Generate a sequence of daily date ranges between start_date and end_date.
    
    This function creates an iterator that yields pairs of datetime objects representing
    the start and end of each day in the range. Each pair consists of:
    - The start of the day (00:00:00)
    - The start of the next day (00:00:00)

    Args:
        start_date (datetime): The starting date/time
        end_date (datetime): The ending date/time (exclusive)
        
    Yields:
        tuple: A pair of datetime objects (current_date, next_date) for each day
        
    Example:
        >>> start = datetime(2023, 1, 1)
        >>> end = datetime(2023, 1, 3) 
        >>> for start, end in get_date_range(start, end):
        ...     print(f"{start} to {end}")
        2023-01-01 00:00:00 to 2023-01-02 00:00:00
        2023-01-02 00:00:00 to 2023-01-03 00:00:00
    """

    current_date = start_date
    while current_date < end_date:
        next_date = current_date + timedelta(days=1)
        yield current_date, next_date
        current_date = next_date

def upload_to_s3(df, bucket_name, subreddit_name, date_str):
    """Upload a DataFrame to S3 as a JSON file.
    
    This function takes a pandas DataFrame, converts it to a JSON string, and uploads it to 
    the specified S3 bucket and path. It handles errors and prints debug information to help 
    with troubleshooting.
    
    Args:
        df (pd.DataFrame): The DataFrame to upload
        bucket_name (str): The name of the S3 bucket
        subreddit_name (str): The name of the subreddit
        date_str (str): The date string in the format 'YYYYMMDD'
    """

    s3_client = initialize_s3()
    
    # Debug print
    print(f"Preparing to upload data for {date_str}. DataFrame size: {len(df)}")
    
    # Convert DataFrame to JSON
    json_str = df.to_json(orient='records', date_format='iso')
    
    # Generate S3 path with date
    s3_path = f'reddit_data/{subreddit_name}/{date_str}.json'
    
    try:
        # Debug print before upload
        print(f"Attempting to upload to s3://{bucket_name}/{s3_path}")
        
        response = s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_path,
            Body=json_str,
            ContentType='application/json'
        )
        
        # Check if upload was successful
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print(f"Successfully uploaded data to s3://{bucket_name}/{s3_path}")
        else:
            print(f"Upload might have failed. Response: {response}")
            
    except Exception as e:
        print(f"Error uploading to S3: {str(e)}")
        # Print more detailed error information
        import traceback
        print(traceback.format_exc())

def download_subreddit_by_date(subreddit_name, bucket_name, start_date, end_date):
    reddit = initialize_reddit()
    api = PushshiftAPI()
    
    # Debug print at start
    print(f"Starting download process for r/{subreddit_name}")
    print(f"Date range: {start_date.date()} to {end_date.date()}")
    print(f"Target S3 bucket: {bucket_name}")
    
    for start, end in get_date_range(start_date, end_date):
        print(f"\nProcessing date: {start.date()}")
        
        stories = []  # Move stories list outside the try block
        
        try:
            # Try Reddit API directly for recent dates first (last 90 days)
            if (datetime.now() - start).days < 90:
                print("Using Reddit API for recent data...")
                subreddit = reddit.subreddit(subreddit_name)
                
                for submission in subreddit.new(limit=1000):  # Limit to prevent infinite loop
                    submission_time = datetime.fromtimestamp(submission.created_utc)
                    
                    if start <= submission_time < end:
                        if not submission.removed_by_category and submission.selftext:
                            story = {
                                'title': submission.title,
                                'text': submission.selftext,
                                'author': str(submission.author),
                                'score': submission.score,
                                'created_utc': submission_time,
                                'url': submission.url,
                                'id': submission.id,
                                'num_comments': submission.num_comments,
                                'upvote_ratio': submission.upvote_ratio
                            }
                            stories.append(story)
                            print(f"Found post: {submission.title[:50]}...")
                    
                    time.sleep(0.5)  # Respect rate limits
            
            # If no stories found or older date, try Pushshift
            if not stories:
                print("Using Pushshift API...")
                start_timestamp = int(start.timestamp())
                end_timestamp = int(end.timestamp())
                
                submissions = api.search_submissions(
                    subreddit=subreddit_name,
                    after=start_timestamp,
                    before=end_timestamp,
                    limit=None
                )
                
                submission_ids = [sub['id'] for sub in submissions]
                print(f"Found {len(submission_ids)} posts in Pushshift")
                
                for submission_id in submission_ids:
                    try:
                        submission = reddit.submission(id=submission_id)
                        if not submission.removed_by_category and submission.selftext:
                            story = {
                                'title': submission.title,
                                'text': submission.selftext,
                                'author': str(submission.author),
                                'score': submission.score,
                                'created_utc': datetime.fromtimestamp(submission.created_utc),
                                'url': submission.url,
                                'id': submission.id,
                                'num_comments': submission.num_comments,
                                'upvote_ratio': submission.upvote_ratio
                            }
                            stories.append(story)
                            print(f"Found post: {submission.title[:50]}...")
                        
                        time.sleep(0.5)
                    except Exception as e:
                        print(f"Error processing submission {submission_id}: {str(e)}")
                        continue
            
            # Upload if we have stories
            if stories:
                print(f"Found {len(stories)} stories for {start.date()}")
                df = pd.DataFrame(stories)
                date_str = start.strftime('%Y%m%d')
                upload_to_s3(df, bucket_name, subreddit_name, date_str)
            else:
                print(f"No stories found for {start.date()}")
            
            # Wait between days to avoid rate limits
            time.sleep(2)
            
        except Exception as e:
            print(f"Error processing date {start.date()}: {str(e)}")
            import traceback
            print(traceback.format_exc())
            continue

if __name__ == "__main__":

    # Configuration
    SUBREDDIT_NAME = os.environ['SUBREDDIT_NAME']
    BUCKET_NAME = os.environ['S3_BUCKET_NAME']
    
    # Define date range (example: last 5 years)
    END_DATE = datetime.now()
    START_DATE = END_DATE - timedelta(days=5)
    
    print("Starting Reddit data collection script...")
    print(f"Subreddit: r/{SUBREDDIT_NAME}")
    print(f"Date range: {START_DATE.date()} to {END_DATE.date()}")
    print(f"S3 Bucket: {BUCKET_NAME}/Historical")
    
    # Download and upload data
    download_subreddit_by_date(
        subreddit_name=SUBREDDIT_NAME,
        bucket_name=BUCKET_NAME,
        start_date=START_DATE,
        end_date=END_DATE
    )
    