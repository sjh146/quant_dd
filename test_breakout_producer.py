import os
import json
import time
from confluent_kafka import Producer

def delivery_report(err, msg):
    pass

def test_breakout():
    broker = os.getenv('KAFKA_BROKER', 'kafka:29092')
    conf = {'bootstrap.servers': broker}
    producer = Producer(conf)
    
    # 1. Fake Breakout (Low volume, negative OFI)
    fake_breakout = {
        "symbol": "FAKE_STOCK",
        "price": 50000,
        "volume": 1000,
        "avg_volume_5m": 800,  # Only 1.25x volume shock (Needs 2.0x)
        "bid_size_1": 100,
        "ask_size_1": 500   # Negative OFI (More selling pressure)
    }
    
    # 2. True Breakout (High volume shock, positive OFI)
    true_breakout = {
        "symbol": "REAL_STOCK",
        "price": 60000,
        "volume": 5000,
        "avg_volume_5m": 1500, # 3.3x volume shock (Pass)
        "bid_size_1": 800,     # Strong buying pressure
        "ask_size_1": 200      # OFI = (800-200)/1000 = +0.6 (Pass)
    }
    
    print(f"Sending FAKE breakout data...")
    producer.produce("market.data", json.dumps(fake_breakout).encode('utf-8'), callback=delivery_report)
    
    time.sleep(1)
    
    print(f"Sending TRUE breakout data...")
    producer.produce("market.data", json.dumps(true_breakout).encode('utf-8'), callback=delivery_report)
    
    producer.flush()

if __name__ == "__main__":
    test_breakout()
