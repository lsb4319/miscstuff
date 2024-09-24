from datetime import datetime
import json
import time
from confluent_kafka import Producer
import requests
import logging
import uuid
import logging

KAFKA_HOST = "100.25.125.23"
KAFKA_PORT = "9092"
KAFKA_ISS_TOPIC = "iss"
DATETIME_FORMAT = '%m/%d/%y %H:%M:%S'

class kafka_iss:
    def __init__():
        pass
    
    def get_location(self):
        try:
            endpoint = "https://api.wheretheiss.at/v1/satellites/25544"
            response = requests.get(endpoint)
            if response.status_code == 200:
                result = json.loads(response.text)
            else:
                result = "error"
            return result
        except Exception as ex:
            logging.exception(ex)
        
    def format_json(self, location):
        try:
            json_str = json.dumps(location)
            escaped_json_str = json_str.replace("\\", "\\\\")
            out = escaped_json_str
            out.encode("ascii")
            return out
        except Exception as ex:
            logging.exception(ex)
        
    def send(self, producer, topic, bytes):
        try:
            producer.begin_transaction()
            producer.produce(topic = topic, value= bytes)
            producer.commit_transaction()
        except Exception as ex:
            logging.exception(ex)
            
    def get_producer(self, kafka_broker, transaction_id):
        try:
            producer_config = {"bootstrap.servers": kafka_broker,"enable.idempotence": True, "transactional.id":transaction_id}
            producer = Producer(producer_config)
            producer.init_transactions()
            return producer
        except Exception as ex:
            logging.exception(ex)
    
    def get_google_map_url(self, location):
        try:
            latitude = location["latitude"]
            longitude = location["longitude"]
            url = "https://www.google.com/maps/search/?api=1&query={0}%2c{1}".format(latitude,longitude)
            location["url"]=url
            return location
        except Exception as ex:
            logging.exception(ex)

def main():
    log_time = str(datetime.now())
    log_time = log_time.replace(" ","_")
    log_time = log_time.replace(":","-")
    log_file = "/home/ubuntu/iss/logs/{0}.log".format(log_time)
    logging.basicConfig(level=logging.INFO,filename=log_file)
    logging.info("Started at: {0}".format(log_time))
    ki = kafka_iss
    kafka_broker = "{0}:{1}".format(KAFKA_HOST, KAFKA_PORT)
    while True:
        transaction_id = uuid.uuid4().int
        producer = ki.get_producer(ki, kafka_broker, transaction_id)
        location = ki.get_location(ki)
        location = ki.get_google_map_url(ki,location)
        location = ki.format_json(ki, location)
        ki.send(ki, producer, KAFKA_ISS_TOPIC, location)
        time.sleep(2)
        
if __name__ == '__main__':
    main()