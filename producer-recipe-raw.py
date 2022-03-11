import re
import requests
from bs4 import BeautifulSoup

from ensurepip import bootstrap
from sys import api_version
from kafka import KafkaProducer, KafkaConsumer

def fetch_raw(recipe_url):
    html = None
    print('Processing..{}'.format(recipe_url))
    try:
        r = requests.get(recipe_url)
        if r.status_code == 200:
            html = r.text
    except Exception as ex:
        print('Exception while accessing raw html')
        print(str(ex))
    finally:
        return html.strip()

def get_recipes():
    recipes = []
    url = 'https://www.allrecipes.com/recipes/96/salad/'
    print("Accessing list: ")

    try: 
        r = requests.get(url)
        if r.status_code == 200:
            html = r.text
        soup = BeautifulSoup(html, 'lxml')
        links = soup.select("[class~=elementFont__titleLink]")
        idx = 0
        for link in links:
            recipe = fetch_raw(link['href'])
            recipes.append(recipe)
            idx += 1
            if idx > 1:
                break
    except Exception as e:
        print('Exception in get_recipes')
        print(str(e))
    finally:
        return recipes
        
def publish_message(producer_instance, topic_name, key, value):
    key_bytes = bytes(key, encoding='utf-8')
    value_bytes = bytes(value, encoding='utf-8')
    producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
    producer_instance.flush()
    print("Message published successfully!")

def connect_kafka_producer():
    _producer = None
    _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    return _producer

all_recipes = get_recipes()
if len(all_recipes) > 0:
    producer = connect_kafka_producer()
    for recipe in all_recipes:
        publish_message(producer, 'raw_recipes', 'raw', recipe.strip())
