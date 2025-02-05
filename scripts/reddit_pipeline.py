# reddit_pipeline.py
import os
import json
from datetime import datetime
import praw
import boto3
from dotenv import load_dotenv
import logging
import requests


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RedditS3Pipeline:
    def __init__(self):
        
        # Load environment variables
        load_dotenv()
        
        # Initialize Reddit client
        self.reddit = praw.Reddit(
            client_id=os.getenv('reddit_id'),
            client_secret=os.getenv('secret_key'),
            user_agent=os.getenv('user_agent')
        )
        
        # Initialize S3 client
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION', 'us-east-1')
        )
        
        self.bucket_name = os.getenv('S3_BUCKET_NAME')
        self.subreddit_name = os.getenv('SUBREDDIT_NAME')
    
    def fetch_stories(self, limit=100):
        """Fetch stories from specified subreddit"""
        try:
            subreddit = self.reddit.subreddit(self.subreddit_name)
            stories = []
            
            for submission in subreddit.hot(limit=limit):
                story = {
                    'id': submission.id,
                    'title': submission.title,
                    'author': str(submission.author),
                    'created_utc': submission.created_utc,
                    'score': submission.score,
                    'upvote_ratio': submission.upvote_ratio,
                    'url': submission.url,
                    'selftext': submission.selftext,
                    'num_comments': submission.num_comments,
                }
                stories.append(story)
            
            logger.info(f"Successfully fetched {len(stories)} stories from r/{self.subreddit_name}")
            return stories
            
        except Exception as e:
            logger.error(f"Error fetching stories: {str(e)}")
            raise
    
    def upload_to_s3(self, stories):
        """Upload stories to S3 bucket"""
        try:
            # Create filename with current date
            current_date = datetime.now().strftime('%Y-%m-%d')
            filename = f"reddit_stories_{self.subreddit_name}_{current_date}.json"
            
            # Convert stories to JSON
            stories_json = json.dumps(stories, indent=2)
            
            # Upload to S3
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=f"raw/{filename}",
                Body=stories_json,
                ContentType='application/json'
            )
            
            logger.info(f"Successfully uploaded {filename} to S3 bucket {self.bucket_name}")
            
        except Exception as e:
            logger.error(f"Error uploading to S3: {str(e)}")
            raise
    
    def run_pipeline(self):
        """Run the complete pipeline"""
        try:
            logger.info("Starting Reddit to S3 pipeline")
            stories = self.fetch_stories()
            self.upload_to_s3(stories)
            logger.info("Pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise

if __name__ == "__main__":
    pipeline = RedditS3Pipeline()
    pipeline.run_pipeline()