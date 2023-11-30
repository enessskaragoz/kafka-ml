import os
import json
import tweepy
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

# Twitter API credentials
consumer_key = os.environ['TWITTER_CONSUMER_KEY']
consumer_secret = os.environ['TWITTER_CONSUMER_SECRET']
access_token = os.environ['TWITTER_ACCESS_TOKEN']
access_token_secret = os.environ['TWITTER_ACCESS_TOKEN_SECRET']

# Kafka topic and schema registry URL
topic = 'tweets'
schema_registry_url = 'http://localhost:8081'

# Avro schema for tweet value
value_schema_str = """
{
   "namespace": "twitter",
   "name": "tweet",
   "type": "record",
   "fields" : [
       {"name" : "id", "type" : "long"},
       {"name" : "created_at", "type" : "string"},
       {"name" : "text", "type" : "string"},
       {"name" : "user_id", "type" : "long"},
       {"name" : "user_name", "type" : "string"},
       {"name" : "user_location", "type" : ["null", "string"]},
       {"name" : "user_followers_count", "type" : "int"},
       {"name" : "user_friends_count", "type" : "int"},
       {"name" : "user_verified", "type" : "boolean"},
       {"name" : "lang", "type" : "string"},
       {"name" : "retweet_count", "type" : "int"},
       {"name" : "favorite_count", "type" : "int"}
   ]
}
"""

# Create an Avro producer
value_schema = avro.loads(value_schema_str)
avro_producer = AvroProducer({
    'bootstrap.servers': 'localhost:9092',
    'schema.registry.url': schema_registry_url
    }, default_value_schema=value_schema)

# Create a tweepy stream listener
class TweetListener(tweepy.StreamListener):

    def on_data(self, data):
        try:
            # Decode the JSON data
            tweet = json.loads(data)

            # Extract relevant information from the tweet
            value = {
                "id": tweet["id"],
                "created_at": tweet["created_at"],
                "text": tweet["text"],
                "user_id": tweet["user"]["id"],
                "user_name": tweet["user"]["name"],
                "user_location": tweet["user"]["location"],
                "user_followers_count": tweet["user"]["followers_count"],
                "user_friends_count": tweet["user"]["friends_count"],
                "user_verified": tweet["user"]["verified"],
                "lang": tweet["lang"],
                "retweet_count": tweet["retweet_count"],
                "favorite_count": tweet["favorite_count"]
            }

            # Produce the tweet to Kafka
            avro_producer.produce(topic=topic, value=value)
            avro_producer.flush()

            # Print the tweet
            print(value)

        except Exception as e:
            print("Error: " + str(e))
            return False

        return True

    def on_error(self, status):
        print(status)
        return False

# Create a tweepy stream
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
listener = TweetListener()
stream = tweepy.Stream(auth, listener)

# Filter the stream for tweets containing the keyword 'kafka'
stream.filter(track=['kafka'])
