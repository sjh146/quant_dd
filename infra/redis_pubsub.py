import json
import logging
import redis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class QuantRedisProducer:
    def __init__(self, redis_url="redis://localhost:6379/0"):
        self.redis_client = redis.from_url(redis_url)

    def send_signal(self, channel, signal_data):
        try:
            json_data = json.dumps(signal_data)
            self.redis_client.publish(channel, json_data)
            logger.debug(f"Message published to {channel}")
        except Exception as e:
            logger.error(f"Failed to publish message: {e}")

    def flush(self):
        pass

class QuantRedisConsumer:
    def __init__(self, redis_url="redis://localhost:6379/0", channel="market.data"):
        self.redis_client = redis.from_url(redis_url)
        self.pubsub = self.redis_client.pubsub()
        self.pubsub.subscribe(channel)
        self.channel = channel

    def poll_messages(self, timeout=1.0):
        msg = self.pubsub.get_message(timeout=timeout)
        
        if msg is None:
            return None
            
        if msg['type'] == 'message':
            try:
                data = json.loads(msg['data'].decode('utf-8'))
                return data
            except json.JSONDecodeError:
                logger.error(f"Failed to decode JSON from message: {msg['data']}")
                return None
        return None

    def close(self):
        self.pubsub.close()
        self.redis_client.close()
