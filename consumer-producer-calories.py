from sys import api_version
from time import sleep
import json
import re

from bs4 import BeautifulSoup
from kafka import KafkaProducer, KafkaConsumer

def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = json.dumps(value).encode('utf-8')
        print(type(value_bytes))
        print(type(key_bytes))
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))

def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

def parse(markup):
    title = "-"
    calories = 0
    rec = {}

    soup = BeautifulSoup(markup, 'lxml')
    
    title_section = soup.select('.elementFont__display')
    title = title_section[0].text
    
    nutrition_section = soup.select('.recipeNutritionSectionBlock > .section-body')
    calories = re.findall('\d+ calories',nutrition_section[0].text)[0]

    rec = {'title': title ,'calories': calories}

    return rec

print("Running consumer: ")
parsed_records = []
topic_name = 'raw_recipes'
parsed_topic_name = 'parsed_recipes'

consumer = KafkaConsumer(topic_name, auto_offset_reset = 'earliest', consumer_timeout_ms = 1000, bootstrap_servers=['localhost:9092'], api_version=(0,10))

for message in consumer:
    print("Consuming message...")
    html = message.value
    result = parse(html)
    parsed_records.append(result)

consumer.close()

if len(parsed_records) > 0:
    producer = connect_kafka_producer()
    for record in parsed_records:
        print("Publishing message...")
        publish_message(producer, parsed_topic_name, 'parsed', record)