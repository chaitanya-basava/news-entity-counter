import json
import time
from kafka import KafkaProducer
from newsapi import NewsApiClient

from config import CATEGORY, API_KEY, KAFKA_BOOTSTRAP_SERVER, INPUT_TOPIC

producer = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

news_api = NewsApiClient(api_key=API_KEY)


def read_headlines(page=1):
#     top_headlines = news_api.get_top_headlines(category=CATEGORY, language='en', country='us', page_size=100, page=page)
    top_headlines = news_api.get_everything(q="IPL", language='en', page_size=100, sort_by='relevancy', from_param='2025-04-01', page=page)
#     print(top_headlines)
    if top_headlines["status"] == "ok":
        return [x["description"] for x in top_headlines["articles"] if x["description"] != "[Removed]"]
    return []


def send_to_kafka():
    page = 1
    while True:
        for headline in read_headlines():
            if not headline:
                continue
            print(f"Sending: {headline}")
            producer.send(INPUT_TOPIC, value=headline)
            time.sleep(1)
        page += 1
        print(f"Waiting for 10 seconds... next up page: {page}")
        time.sleep(10)


if __name__ == "__main__":
    send_to_kafka()