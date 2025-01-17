import json
import praw
import boto3
import logging
import os
from datetime import datetime

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class RedditS3Pipeline:
    def __init__(self):
        # Initialize Reddit client
        self.reddit = praw.Reddit(
            client_id=os.environ['REDDIT_CLIENT_ID'],
            client_secret=os.environ['REDDIT_CLIENT_SECRET'],
            user_agent=os.environ['REDDIT_USER_AGENT']
        )
        
        # Initialize S3 client
        self.s3 = boto3.client('s3')
        self.bucket_name = os.environ['S3_BUCKET_NAME']
        self.subreddit_name = os.environ['SUBREDDIT_NAME']
        
        # Add tracking for processed IDs
        self.processed_ids_file = "processed_ids.json"
        self.processed_ids = self.load_processed_ids()

    # ... existing methods (load_processed_ids, save_processed_ids, fetch_stories) ...

    def run_pipeline(self):
        """Run the complete pipeline"""
        try:
            logger.info("Starting Reddit to S3 pipeline")
            stories = self.fetch_stories()
            if stories:
                self.upload_to_s3(stories)
            logger.info("Pipeline completed successfully")
            return {
                'statusCode': 200,
                'body': json.dumps(f'Successfully processed {len(stories)} stories')
            }
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            return {
                'statusCode': 500,
                'body': json.dumps(f'Pipeline failed: {str(e)}')
            }

def lambda_handler(event, context):
    """AWS Lambda entry point"""
    pipeline = RedditS3Pipeline()
    return pipeline.run_pipeline() 