import os
import json
from confluent_kafka import Consumer

def test_consumer():
    broker = os.getenv('KAFKA_BROKER', 'kafka:29092')
    conf = {
        'bootstrap.servers': broker,
        'group.id': 'test_windows_vm_group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe(['signal.trade'])
    
    print(f"Listening on signal.trade via {broker}...")
    try:
        # poll briefly to grab the signal that was just emitted
        msg = consumer.poll(3.0)
        if msg is None:
            print("No signal received.")
        elif msg.error():
            print(f"Error: {msg.error()}")
        else:
            print(f"Received Signal on Windows VM side: {msg.value().decode('utf-8')}")
    finally:
        consumer.close()

if __name__ == "__main__":
    test_consumer()
