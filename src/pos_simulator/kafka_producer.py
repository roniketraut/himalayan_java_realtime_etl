import json
import time
import uuid
import random
import pandas as pd
from datetime import datetime, timedelta
from confluent_kafka import Producer
from order_generator import random_timestamp_generator, random_order_generator
import logging
logging.basicConfig(format='%(levelname)s-%(message)s')
from dotenv import load_dotenv
import os

load_dotenv()
EVENTHUB_CONNECTION_STRING = os.getenv("EVENTHUB_CONNECTION_STRING")

producer = Producer({
    "bootstrap.servers": "himalayanjavahubstd.servicebus.windows.net:9093",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": "$ConnectionString",   
    "sasl.password": EVENTHUB_CONNECTION_STRING,  
    "client.id": "coffeehouse-producer"
})

TOPIC_NAME = 'coffeehouse_orders'

def wait_for_kafka():
    logging.info("Waiting fro Kafka broker")

    while True:
        try:
            # testing connection by sending empty message to test topic"
            producer.produce(TOPIC_NAME, value=b"healthcheck")
            producer.flush()
            logging.info("Kafka is ready")
            return
        except Exception as e:
            logging.warning(f"Kafka not ready yet: {e}")
            time.sleep(5)

def produce_orders_forever(delay_seconds=2):
    wait_for_kafka()
    logging.info("Streaming orders into Kafka")

    while True:
        order = random_order_generator()
        producer.produce(TOPIC_NAME, json.dumps(order, default=str).encode("utf-8"))
        producer.flush()

        print(f"Sent: {order['order_id']}, Total amount: {order['total_order_amount']}")
        time.sleep(delay_seconds)

if __name__ == "__main__":
    produce_orders_forever(delay_seconds=2)

