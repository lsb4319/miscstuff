from datetime import datetime
import json
import time
from confluent_kafka import Producer
import requests
import logging
import uuid
import threading

KAFKA_HOST = "100.25.125.23"
KAFKA_PORT = "9092"
KAFKA_JSON_TOPIC = "articles-json"
KAFKA_TSV_TOPIC = "articles-tsv"
API_KEY = "MjkeGAA5taQ4CZYIVB3PakRRbQXs9Fxh"
DATETIME_FORMAT = '%m/%d/%y %H:%M:%S'
LIMIT = 500


class KafkaProducerException(Exception):
    pass

class kafka_producer:
    last_date = None
    round = 0
    records = 0
    new_date = None
    def __init__(self):
        self.last_date = datetime.fromisoformat("2020-08-03T11:55:09-04:00") 
      
    def on_delivery(self,err,msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to topic {msg.topic()} partition {msg.partition()} offset {msg.offset()}")

    def send(self,producer, topic, bytes):
        try:
            producer.begin_transaction()
            producer.produce(topic = topic, value= bytes)
            producer.commit_transaction()
            
        except Exception as e:
            print("Failed to send to topic %s: %s" % (topic, bytes))
            raise KafkaProducerException(e)

    def get_articles(self,key, limit, offset):
        endpoint = "https://api.nytimes.com/svc/news/v3/content/all/all.json?offset={0}&limit={1}&api-key={2}".format(offset, limit, key)
        response = requests.get(endpoint).json()
        return response

    def send_json(self, producer, small_result):
        json_str = json.dumps(small_result)
        escaped_json_str = json_str.replace("\\", "\\\\")
        out = escaped_json_str
        self.send(producer, KAFKA_JSON_TOPIC, out.encode("ascii"))

    def send_tsv(self, producer, small_result):
        out = "\t".join([
            str(small_result["section"]),
            str(small_result["subsection"]),
            str(small_result["title"]),
            str(small_result["abstract"]),
            str(small_result["url"]),
            str(small_result["item_type"]),
            str(small_result["updated_date"]),
            str(small_result["created_date"]),
            str(small_result["published_date"]),
            str(small_result["first_published_date"])]
        )
        self.send(producer, KAFKA_TSV_TOPIC, out.encode("UTF-8"))

    def run(self, round):
        offset = 0
        kafka_broker = "{0}:{1}".format(KAFKA_HOST, KAFKA_PORT)
        transaction_id = uuid.uuid4().int
        try:
            producer_config = {"bootstrap.servers": kafka_broker,"enable.idempotence": True, "transactional.id":transaction_id}
            producer = Producer(producer_config)
            producer.init_transactions()
        except Exception as e:
            logging.exception(e)
            time.sleep(5)
        
        return_text = self.get_articles(API_KEY,LIMIT,offset)
        try:
            results = return_text["results"]
            keys_to_keep = ["section","subsection","title","abstract","url","item_type","updated_date","created_date","published_date","first_published_date"]
            for result in results: 
                small_result = {k: result[k] for k in keys_to_keep}
                if small_result["updated_date"].startswith("0000"):
                    self.new_date = datetime.fromisoformat("2000-08-03T11:55:09-04:00")
                else:
                    self.new_date = datetime.fromisoformat(small_result["updated_date"])
                if round != 0:
                    if self.new_date > self.last_date:
                        self.last_date = self.new_date
                        self.send_json(producer, small_result)
                        self.send_tsv(producer, small_result)
                        self.records+=1
                elif round == 0:
                    self.send_json(producer, small_result)
                    self.send_tsv(producer, small_result)
                    self.records += 1
                time.sleep(.4)
        except Exception as e:
            logging.exception(e)

    def show_status(self):
        logging.info("{4} Round: {0}, Records: {1}, Last Date: {2}, New Date: {3}  ".format(self.round, self.records, self.last_date, self.new_date, datetime.now()))
    

if __name__ == '__main__':
    kp=kafka_producer()
    logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger()
    log_time = str(datetime.now())
    log_time = log_time.replace(" ","_")
    log_time = log_time.replace(":","-")
    log_file = "/home/ubuntu/kafka/logs/{0}.log".format(log_time)
    handler = logging.FileHandler(log_file, mode='a')
    logger.addHandler(handler)
    logging.info("Connected to Kafka broker: {0} at {1}".format("100.25.125.23:9092",datetime.now()))
    logging.info("Using Kafka topics: {0}, {1}".format(KAFKA_JSON_TOPIC,KAFKA_TSV_TOPIC))
    while True:
        try:
            kp.show_status()
            kp.run(kp.round)
            kp.round += 1
        except KafkaProducerException as e:
            logging.exception(e)
            time.sleep(5)
