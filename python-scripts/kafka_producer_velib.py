from kafka import KafkaProducer
import requests
import time
import json
import io

#Initialize the producer
producer1 = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer= lambda v: json.dumps(v).encode('utf-8'))

#Produce kafka messages from the API

#utility functions

def read_data_from_api(url):
    try: 
        response = requests.get(url).json()
        return response
    except ConnectionError:
        return "Connection not established correctly and get api failed"
    
    
def send_to_kafka(topic,data):
    producer1.send(topic, value= data)
    producer1.flush()
    
def fetch_data(limit,offset,topic):
    
    while True:
        url=f"https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/records?limit={limit}&offset={offset}"
        data=read_data_from_api(url)
        if data['results']:
            send_to_kafka(topic, data)
            offset+=limit
            #time.sleep(1)
        else:
            break
def main():
    #Initializing variables
    topic= 'velib' # Kafka topic to send message to
    limit=100 #number of recrds per request
    offset=0  # starting point
    fetch_data(limit,offset,topic)  # c

if __name__=='__main__':
    main()
