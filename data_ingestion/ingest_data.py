import requests
import json
from kafka import KafkaProducer

def fetch_air_quality_data(api_url):
    response = requests.get(api_url)
    data = response.json()
    return data

def produce_messages(producer, topic, data):
    for record in data:
        producer.send(topic, json.dumps(record).encode('utf-8'))

if __name__ == "__main__":
    api_url = "http://api.example.com/air_quality"
    kafka_topic = "air_quality"
    kafka_producer = KafkaProducer(bootstrap_servers='localhost:9092')

    data = fetch_air_quality_data(api_url)
    produce_messages(kafka_producer, kafka_topic, data)
