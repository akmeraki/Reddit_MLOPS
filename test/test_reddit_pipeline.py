# test_reddit_pipeline.py
import pytest
from unittest.mock import Mock, patch, MagicMock
import json
from datetime import datetime
from reddit_pipeline import RedditS3Pipeline

@pytest.fixture
def mock_reddit_submission():
    """Create a mock Reddit submission"""
    submission = Mock()
    submission.id = "abc123"
    submission.title = "Test Post"
    submission.author = "test_user"
    submission.created_utc = 1641234567
    submission.score = 100
    submission.upvote_ratio = 0.95
    submission.url = "https://reddit.com/r/test/comments/abc123"
    submission.selftext = "Test content"
    submission.num_comments = 10
    return submission

@pytest.fixture
def mock_pipeline():
    """Create a pipeline instance with mocked Reddit and S3 clients"""
    with patch('praw.Reddit') as mock_reddit, \
         patch('boto3.client') as mock_s3:
        
        # Mock the subreddit and its hot posts
        mock_subreddit = Mock()
        mock_reddit.return_value.subreddit.return_value = mock_subreddit
        
        # Mock S3 client
        mock_s3_client = Mock()
        mock_s3.return_value = mock_s3_client
        
        pipeline = RedditS3Pipeline()
        
        # Add mocks to pipeline for testing
        pipeline.mock_reddit = mock_reddit
        pipeline.mock_s3_client = mock_s3_client
        pipeline.mock_subreddit = mock_subreddit
        
        yield pipeline

class TestRedditS3Pipeline:
    
    def test_init(self):
        """Test pipeline initialization"""
        with patch('praw.Reddit') as mock_reddit, \
             patch('boto3.client') as mock_s3:
            
            pipeline = RedditS3Pipeline()
            
            # Verify Reddit client initialization
            mock_reddit.assert_called_once()
            
            # Verify S3 client initialization
            mock_s3.assert_called_once_with(
                's3',
                aws_access_key_id=None,  # None in tests as env vars aren't set
                aws_secret_access_key=None,
                region_name='us-east-1'
            )
    
    def test_fetch_stories(self, mock_pipeline, mock_reddit_submission):
        """Test fetching stories from Reddit"""
        # Setup mock subreddit to return our mock submission
        mock_pipeline.mock_subreddit.hot.return_value = [mock_reddit_submission]
        
        # Fetch stories
        stories = mock_pipeline.fetch_stories(limit=1)
        
        # Verify the result
        assert len(stories) == 1
        story = stories[0]
        assert story['id'] == 'abc123'
        assert story['title'] == 'Test Post'
        assert story['author'] == 'test_user'
        assert story['created_utc'] == 1641234567
        assert story['score'] == 100
        assert story['upvote_ratio'] == 0.95
        assert story['url'] == 'https://reddit.com/r/test/comments/abc123'
        assert story['selftext'] == 'Test content'
        assert story['num_comments'] == 10
        
        # Verify subreddit.hot() was called with correct limit
        mock_pipeline.mock_subreddit.hot.assert_called_once_with(limit=1)
    
    def test_fetch_stories_error(self, mock_pipeline):
        """Test error handling in fetch_stories"""
        # Setup mock to raise an exception
        mock_pipeline.mock_subreddit.hot.side_effect = Exception("Reddit API Error")
        
        # Verify the exception is raised
        with pytest.raises(Exception) as exc_info:
            mock_pipeline.fetch_stories()
        
        assert str(exc_info.value) == "Reddit API Error"
    
    def test_upload_to_s3(self, mock_pipeline):
        """Test uploading stories to S3"""
        # Test data
        test_stories = [{
            'id': 'abc123',
            'title': 'Test Post'
        }]
        
        # Mock datetime to get consistent filename
        mock_date = datetime(2025, 1, 9)
        with patch('reddit_pipeline.datetime') as mock_datetime:
            mock_datetime.now.return_value = mock_date
            
            # Upload stories
            mock_pipeline.upload_to_s3(test_stories)
            
            # Verify S3 put_object was called correctly
            mock_pipeline.mock_s3_client.put_object.assert_called_once()
            call_args = mock_pipeline.mock_s3_client.put_object.call_args[1]
            
            assert call_args['Bucket'] == mock_pipeline.bucket_name
            assert call_args['Key'] == 'raw/reddit_stories_None_2025-01-09.json'
            assert call_args['ContentType'] == 'application/json'
            
            # Verify uploaded JSON content
            uploaded_content = json.loads(call_args['Body'])
            assert uploaded_content == test_stories
    
    def test_upload_to_s3_error(self, mock_pipeline):
        """Test error handling in upload_to_s3"""
        # Setup mock to raise an exception
        mock_pipeline.mock_s3_client.put_object.side_effect = Exception("S3 Upload Error")
        
        # Verify the exception is raised
        with pytest.raises(Exception) as exc_info:
            mock_pipeline.upload_to_s3([])
        
        assert str(exc_info.value) == "S3 Upload Error"
    
    def test_run_pipeline(self, mock_pipeline, mock_reddit_submission):
        """Test running the complete pipeline"""
        # Setup mocks
        mock_pipeline.mock_subreddit.hot.return_value = [mock_reddit_submission]
        
        # Run pipeline
        mock_pipeline.run_pipeline()
        
        # Verify both fetch_stories and upload_to_s3 were called
        mock_pipeline.mock_subreddit.hot.assert_called_once()
        mock_pipeline.mock_s3_client.put_object.assert_called_once()
    
    def test_run_pipeline_error(self, mock_pipeline):
        """Test error handling in run_pipeline"""
        # Setup mock to raise an exception
        mock_pipeline.mock_subreddit.hot.side_effect = Exception("Pipeline Error")
        
        # Verify the exception is raised
        with pytest.raises(Exception) as exc_info:
            mock_pipeline.run_pipeline()
        
        assert str(exc_info.value) == "Pipeline Error"

if __name__ == "__main__":
    pytest.main(["-v"])