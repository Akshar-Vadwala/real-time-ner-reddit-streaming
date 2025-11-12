import os
import praw
import json
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()


def connect_to_reddit():
    """
    Function to connect to Reddit using credentials

    Returns:
        obj: reddit object 
    """

    reddit = praw.Reddit(
        client_id = os.getenv("CLIENT_ID"), # Use your own client_id
        client_secret = os.getenv("CLIENT_SECRET"), # Use your own client_secret
        user_agent = os.getenv("USER_AGENT") # Use your own user_agent
    )
   
    print("Connected Successfully to Reddit...")

    return reddit


def stream_to_kafka():
    """
    Function to use the reddit object and produce the data to submit to Kafka Broker 
 
    """

    producer = KafkaProducer(
        bootstrap_servers = ['localhost:9092'],
        value_serializer = lambda v: json.dumps(v).encode('utf-8')
    ) 

    reddit = connect_to_reddit()

    for submission in reddit.subreddit("wallstreetbets").stream.comments(skip_existing = True):
        message = {
            "id": submission.id,
            "body": submission.body,
            "created_utc": submission.created_utc
        }

        producer.send("reddit-stream", value=message)
        print(f"Comment {submission.id} sent to Kafka")


if __name__ == "__main__":
    stream_to_kafka()
    
