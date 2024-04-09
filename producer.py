import praw
import json
import time
from kafka import KafkaProducer

from config import CLIENT_ID, CLIENT_SECRET, KAFKA_BOOTSTRAP_SERVER, INPUT_TOPIC

producer = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

reddit = praw.Reddit(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    user_agent="my user agent",
)


def send_to_kafka(subreddit="all"):
    for comment in reddit.subreddit(subreddit).stream.comments():
        try:
            data = {'author': str(comment.author), 'body': comment.body}
            producer.send(INPUT_TOPIC, value=data)
            print(f"Sent: {data}")
        except Exception as e:
            print(f"Error: {e}")
    time.sleep(10)


if __name__ == "__main__":
    start_id = 0

    print(reddit.read_only)

    send_to_kafka()
