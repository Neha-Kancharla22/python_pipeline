import tweepy
import pandas as pd
import os
from dotenv import load_dotenv
from connectt import get_db_connection, read_config
import re
import time
from datetime import datetime

# Toggle this to False once your API access resets
USE_MOCK_DATA = True

# Load Twitter credentials
load_dotenv(dotenv_path=r'C:\Users\Neha\Documents\twitterapi1\config\config.env')
BEARER_TOKEN = os.getenv("BEARER_TOKEN")

if not BEARER_TOKEN:
    raise ValueError("Bearer token not found. Please check your .env file.")

# Initialize Twitter client
client = tweepy.Client(bearer_token=BEARER_TOKEN)

# Mock tweet class for local testing
class MockTweet:
    def __init__(self, id, text, created_at, author_id):
        self.id = id
        self.text = text
        self.created_at = created_at
        self.author_id = author_id

def fetch_tweets(query="iphone15 -is:retweet lang:en", max_results=100):
    """Fetch tweets using Tweepy client or mock tweets when capped"""
    if USE_MOCK_DATA:
        print(" Twitter API cap hit — using mock tweets for testing.")
        return [
            MockTweet("mock_101", "Awesome review of iPhone 15!", datetime.utcnow(), "user001"),
            MockTweet("mock_102", "Should I buy the new iPhone 15?", datetime.utcnow(), "user002"),
            MockTweet("mock_103", "iPhone 15 camera is awesome!", datetime.utcnow(), "user003"),
        ]
    else:
        try:
            response = client.search_recent_tweets(
                query=query,
                max_results=max_results,
                tweet_fields=["created_at", "text", "author_id"]
            )
            return response.data if response.data else []
        except tweepy.TooManyRequests:
            print(" Rate limit or monthly cap exceeded. Switch to mock mode to continue testing.")
            return []

def clean_tweet(text):
    """Remove URLs, mentions, hashtags, emojis, and extra spaces"""
    text = re.sub(r"http\S+", "", text)
    text = re.sub(r"@\w+", "", text)
    text = re.sub(r"#\w+", "", text)
    text = re.sub(r"[^\x00-\x7F]+", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def filter_tweet(text):
    """Filter tweets based on keywords"""
    keywords = ['buy', 'review', 'awesome']
    return any(word in text.lower() for word in keywords)

def save_to_sql(tweets):
    """Store cleaned and filtered tweets in SQL Server"""
    config = read_config()
    conn = get_db_connection(config)
    cursor = conn.cursor()

    inserted_count = 0

    for tweet in tweets:
        cleaned = clean_tweet(tweet.text)
        if filter_tweet(cleaned):
            try:
                cursor.execute("""
                    INSERT INTO Tweets (tweet_id, text, created_at, author_id)
                    VALUES (?, ?, ?, ?)
                """, tweet.id, cleaned, tweet.created_at, tweet.author_id)
                inserted_count += 1
            except Exception as e:
                print(f" Could not insert tweet {tweet.id}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print(f"{inserted_count} tweets inserted into SQL Server.")

def main():
    tweets = fetch_tweets()
    if tweets:
        save_to_sql(tweets)
    else:
        print("No tweets to process.")

if __name__ == "__main__":
    main()