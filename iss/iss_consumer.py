from kafka import KafkaConsumer
import time
from datetime import datetime
import logging

def kafka_consume():
    consumer = KafkaConsumer(
        'iss',
        bootstrap_servers='100.25.125.23:9092',
        auto_offset_reset='latest'
    )
    messages = 0
    elapsed_time = list()
    previous_timestamp = 0
    current_timestamp = 0
    try:
        for message in consumer:
            if messages != 0 and messages <=10:
                messages+=1
                previous_timestamp = current_timestamp
                current_timestamp = message.timestamp
                time_diff = current_timestamp - previous_timestamp
                elapsed_time.append(time_diff)
            elif messages > 10:
                average_time = sum(elapsed_time)/len(elapsed_time)
                average_time_as_text = str(average_time)
                logging.info(str(messages) + " Messages," + average_time_as_text + " average time")
                messages = 0
            else:
                current_timestamp = message.timestamp
                messages+=1 
                

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
    log_time = str(datetime.now())
    log_time = log_time.replace(" ","_")
    log_time = log_time.replace(":","-")
    log_file = "logs/{0}.log".format(log_time)
    logger = logging.getLogger()
    handler = logging.FileHandler(log_file, mode='a')
    logger.addHandler(handler)
    kafka_consume()