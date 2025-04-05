import os
import sys
from datetime import datetime
import logging
from logging.handlers import TimedRotatingFileHandler
import asyncio
import random
import signal
from dotenv import load_dotenv

from tweepy.asynchronous import AsyncClient
from pymongo import MongoClient
import pytz

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        TimedRotatingFileHandler(
            "twitter_bot.log", when="midnight", interval=1, backupCount=7
        ),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger("TwitterBot")

load_dotenv()

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
ACCESS_SECRET = os.getenv("ACCESS_SECRET")
BEARER_TOKEN = os.getenv("BEARER_TOKEN")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")

mongo_client = MongoClient(MONGO_URI)
db = mongo_client["twitter_db"]
tweets_zico_collection = db["tweets_zico"]
posted_tweets_zico_collection = db["posted_tweets_zico"]

async def get_new_tweet():
    """Get a tweet from the database."""
    try:
        tweet_data = tweets_zico_collection.find_one(
            {"posted": False}, sort=[("created_at_datetime", -1)]
        )

        if not tweet_data:
            print("No unposted tweets found in tweets_zico_collection")
            return None

        for part in tweet_data.get("parts", []):
            existing_posted_tweet = posted_tweets_zico_collection.find_one(
                {"text": part}
            )
            if existing_posted_tweet:
                print(
                    f'Part "{part[:30]}..." already exists in posted_tweets_zico_collection. Skipping tweet...'
                )
                tweets_zico_collection.update_one(
                    {"_id": tweet_data["_id"]}, {"$set": {"posted": True}}
                )
                return None

        return {"parts": tweet_data.get("parts", []), "tweet_id": tweet_data["_id"]}
    except Exception as e:
        logging.error(f"Error in get_new_tweet: {str(e)}")
        return None

async def post_tweet(client):
    """Post a tweet using the API v2."""
    try:
        tweet_data = await get_new_tweet()

        if tweet_data:
            last_tweet_id = None
            for part in tweet_data["parts"]:
                max_attempts = 3
                attempt = 0
                post_success = False

                while attempt < max_attempts and not post_success:
                    attempt += 1
                    try:
                        print(
                            f"Posting tweet part (attempt {attempt}/{max_attempts})..."
                        )
                        new_tweet = await client.create_tweet(
                            text=part, in_reply_to_tweet_id=last_tweet_id
                        )
                        tweet_id = new_tweet.data["id"]

                        if new_tweet and tweet_id:
                            last_tweet_id = tweet_id
                            print(f"Tweet part posted successfully (attempt {attempt})")
                            post_success = True
                            human_delay = random.uniform(5, 8)
                            print(
                                f"Waiting {human_delay:.2f} seconds before next post..."
                            )
                            await asyncio.sleep(human_delay)
                        else:
                            print(
                                f"Tweet post attempt {attempt} failed: No valid tweet ID returned"
                            )
                            if attempt < max_attempts:
                                print(f"Waiting 10 seconds before retry...")
                                await asyncio.sleep(10)

                    except Exception as e:
                        print(f"Error posting tweet (attempt {attempt}): {str(e)}")
                        if attempt < max_attempts:
                            print(f"Waiting 10 seconds before retry...")
                            await asyncio.sleep(10)

            if not post_success:
                raise Exception(
                    f"Failed to post tweet part after {max_attempts} attempts"
                )

            tweets_zico_collection.update_one(
                {"_id": tweet_data["tweet_id"]}, {"$set": {"posted": True}}
            )

    except Exception as e:
        print(f"Error in tweet job: {str(e)}")

async def auth_v2():
    """Authenticates with the Twitter API v2."""
    try:
        client = AsyncClient(
            bearer_token=BEARER_TOKEN,
            consumer_key=API_KEY,
            consumer_secret=API_SECRET,
            access_token=ACCESS_TOKEN,
            access_token_secret=ACCESS_SECRET,
            wait_on_rate_limit=True
        )
        logger.info("Authentication successful!")
        return client
    except Exception as e:
        logger.error(f"Error during authentication: {str(e)}")
        return None


async def job():
    """Job function that will be executed periodically."""
    client = await auth_v2()
    if client:
        try:
            await post_tweet(client)
        finally:
            await client.aclose()

async def main():
    def signal_handler(sig, frame):
        print("Exiting gracefully...")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        utc_now = datetime.now(pytz.UTC)
        current_hour = utc_now.hour

        if current_hour in [6, 12]:
            await job()
            print(f"Job executed successfully at UTC {current_hour}:00")
        else:
            print(f"Not the right time to run. Current UTC hour: {current_hour}")
            print("This script should run at UTC 6:00 or 12:00")

    except Exception as e:
        logging.error(f"Fatal error in main: {str(e)}")
