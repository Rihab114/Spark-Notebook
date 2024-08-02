from kafka import KafkaProducer
import json 
import requests
import time 

producer = KafkaProducer(bootstrap_servers='localhost:9092',
        value_serializer= lambda v: json.dumps(v).encode('utf-8'))
topic= 'velib_bikes'
url= 'https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/records?limit=100'

def get_api_data(url):
    try:
        response = requests.get(url).json()
        return response
    except ConnectionError:
        return "Failed to get data from api"

def send_to_kafka(topic,data):
    producer.send(topic, value=data)
    producer.flush()


while True:
    data = get_api_data(url)
    if data: 
        send_to_kafka(topic, data)
    time.sleep(10)
