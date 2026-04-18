import os
import json
import time
from confluent_kafka import Producer

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def test_producer():
    broker = os.getenv('KAFKA_BROKER', 'kafka:29092')
    conf = {'bootstrap.servers': broker}
    producer = Producer(conf)
    
    dummy_data = {
        "symbol": "FOLLOWER_STOCK",
        "price": 82000,
        "volume": 1500,
        "type": "trade"
    }
    
    print(f"Sending dummy data to market.data via {broker}...")
    producer.produce("market.data", json.dumps(dummy_data).encode('utf-8'), callback=delivery_report)
    producer.flush()

if __name__ == "__main__":
    test_producer()
