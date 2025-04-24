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
from tweepy import OAuth1UserHandler
from pymongo import MongoClient
from gridfs import GridFS
from bson.objectid import ObjectId
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

def get_image_from_gridfs(image_id: str) -> bytes:
    """
    Retrieves an image from GridFS by ID
    """
    try:
        fs = GridFS(db)
        # Converter string ID para ObjectId
        object_id = ObjectId(image_id)
        
        if not fs.exists(object_id):
            logger.warning(f"Image {image_id} not found in GridFS")
            return None
        
        return fs.get(object_id).read()
    except Exception as e:
        logger.error(f"Error retrieving image from GridFS: {e}")
        return None
    
async def prepare_post_image(image_id):
    """
    Prepare image for posting by retrieving from GridFS and uploading to Twitter.
    """
    try:
        temp_image_path = None
        media_id = None
        
        if image_id:
            image_data = get_image_from_gridfs(image_id)
            if image_data:
                temp_image_path = f"/tmp/tweet_{image_id}.png"
                with open(temp_image_path, 'wb') as f:
                    f.write(image_data)
            else:
                logger.warning(f"Skipping image upload for missing image {image_id}")
                return None
        
        if temp_image_path:
            try:
                v1_client = auth_v1()
                if not v1_client:
                    logger.error("Failed to authenticate with Twitter V1 API")
                    return None
                    
                media = v1_client.media_upload(filename=temp_image_path)
                media_id = media.media_id
            finally:
                if os.path.exists(temp_image_path):
                    os.remove(temp_image_path)
        
        return media_id
    except Exception as e:
        logger.error(f"Error preparing image for tweet: {e}")
        if temp_image_path and os.path.exists(temp_image_path):
            os.remove(temp_image_path)
        return None

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

        return {"parts": tweet_data.get("parts", []), "tweet_id": tweet_data["_id"], "image_id": tweet_data.get("image_id")}
    except Exception as e:
        logging.error(f"Error in get_new_tweet: {str(e)}")
        return None

async def post_tweet(client):
    """Post a tweet using the API v2."""
    try:
        tweet_data = await get_new_tweet()
        media_id = None

        if tweet_data:
            last_tweet_id = None
            if "image_id" in tweet_data:
                media_id = await prepare_post_image(tweet_data["image_id"])
            
            for part in tweet_data["parts"]:
                max_attempts = 3
                attempt = 0
                post_success = False

                while attempt < max_attempts and not post_success:
                    attempt += 1
                    try:
                        print(f"Posting tweet part (attempt {attempt}/{max_attempts})...")
                        if media_id:
                            new_tweet = await client.create_tweet(
                                text=part, in_reply_to_tweet_id=last_tweet_id, media_ids=[media_id]
                            )
                            if new_tweet:
                                media_id = None
                        else:
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
        
def auth_v1():
    try:
        """Get twitter conn 1.1"""

        auth = OAuth1UserHandler(API_KEY, API_SECRET)
        auth.set_access_token(
            ACCESS_TOKEN,
            ACCESS_SECRET,
        )
        return OAuth1UserHandler(API_KEY, API_SECRET)
    except Exception as e:
        logger.error(f"Error during authentication: {str(e)}")
        return None
    
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
    try:
        client = await auth_v2()
        if client:
            try:
                await post_tweet(client)
            except Exception as e:
                logger.error(f"Error posting tweet: {e}")
                raise
    except Exception as e:
        logger.error(f"Error in job: {str(e)}")
        raise

async def main():
    try:
        await job()
        print("Job executed successfully")
    except Exception as e:
        logging.error(f"Fatal error in main: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())
