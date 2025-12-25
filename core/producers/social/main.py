import asyncio
import json
import os
from datetime import datetime
from loguru import logger
from pydantic import BaseModel, ValidationError
from aiokafka import AIOKafkaProducer
import praw

# 1. Validation Schema
class RedditPost(BaseModel):
    title: str
    text: str
    subreddit: str
    created_utc: float
    post_id: str

# 2. Setup Logging
logger.add("/app/logs/social_errors.log", rotation="10 MB")

class SocialProducer:
    def __init__(self):
        self.producer = None
        self.reddit = praw.Reddit(
            client_id=os.getenv("REDDIT_CLIENT_ID"),
            client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
            user_agent="Glasshouse v1.0 by /u/yourname"
        )
        self.topic = "social_raw"

    async def start(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BROKERS", "localhost:9092")
        )
        await self.producer.start()
        logger.info("Social Producer started and connected to Redpanda.")

    def get_reddit_stream(self):
        """Synchronous generator for Reddit submissions."""
        # Targeting high-alpha subreddits
        target_subreddits = "Bitcoin+CryptoCurrency+EthTrader"
        return self.reddit.subreddit(target_subreddits).stream.submissions(pause_after=None)

    async def run(self):
        await self.start()
        
        # We use run_in_executor to keep the Reddit stream from blocking the event loop
        loop = asyncio.get_event_loop()
        
        # Professional standard: Iterate over the stream in a non-blocking way
        # Note: In a true prod env, you'd use 'asyncpraw', but 'praw' in a thread is common.
        stream = self.get_reddit_stream()
        
        while True:
            try:
                # Wrap the blocking next(stream) call
                submission = await loop.run_in_executor(None, next, stream)
                
                if submission is None:
                    continue

                # Validate data before sending to Redpanda
                post = RedditPost(
                    title=submission.title,
                    text=submission.selftext[:500], # Truncate for efficiency; we only need the gist for sentiment
                    subreddit=str(submission.subreddit),
                    created_utc=submission.created_utc,
                    post_id=submission.id
                )

                await self.producer.send_and_wait(
                    self.topic, 
                    post.model_dump_json().encode('utf-8')
                )
                
                # Update Health Heartbeat for Docker Healthcheck
                with open("/tmp/heartbeat_social", "w") as f:
                    f.write(str(datetime.now().timestamp()))
                
                logger.debug(f"Dispatched post from r/{post.subreddit}: {post.title[:30]}...")

            except ValidationError as ve:
                logger.error(f"Schema mismatch on post: {ve}")
            except Exception as e:
                logger.error(f"Stream error: {e}")
                await asyncio.sleep(5) # Backoff before retry

if __name__ == "__main__":
    producer = SocialProducer()
    try:
        asyncio.run(producer.run())
    except KeyboardInterrupt:
        logger.info("Social Producer shut down manually.")