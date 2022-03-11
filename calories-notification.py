import json
from sys import api_version
from kafka import KafkaConsumer

parsed_topic_name = "parsed_recipes"
calories_threhold = 200

consumer = KafkaConsumer(parsed_topic_name, auto_offset_reset='earliest', bootstrap_servers=['localhost:9092'], api_version=(0,10), consumer_timeout_ms=1000)

print("Consuming messages...")
for message in consumer:
    record = json.loads(message.value)
    title = record['title']
    calories = int(record['calories'].split()[0])
    
    if calories > calories_threhold:
        print("Alert: {} has {} calories!".format(title, calories))

consumer.close()