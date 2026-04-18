import json
import logging
from confluent_kafka import Consumer, Producer, KafkaError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class QuantKafkaProducer:
    def __init__(self, broker_url):
        conf = {
            'bootstrap.servers': broker_url,
            'client.id': 'quant_ubuntu_producer'
        }
        self.producer = Producer(conf)

    def delivery_report(self, err, msg):
        """ Callback triggered by poll() or flush() for each message. """
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def send_signal(self, topic, signal_data):
        """ Send JSON serialized dictionary to Kafka topic """
        try:
            json_data = json.dumps(signal_data)
            self.producer.produce(topic, json_data.encode('utf-8'), callback=self.delivery_report)
            self.producer.poll(0) # Trigger callbacks
        except Exception as e:
            logger.error(f"Failed to produce message: {e}")

    def flush(self):
        self.producer.flush()


class QuantKafkaConsumer:
    def __init__(self, broker_url, topic, group_id='quant_ai_group'):
        conf = {
            'bootstrap.servers': broker_url,
            'group.id': group_id,
            'auto.offset.reset': 'latest' # Only process new data in live trading
        }
        self.consumer = Consumer(conf)
        self.consumer.subscribe([topic])
        self.topic = topic

    def poll_messages(self, timeout=1.0):
        """ Polls for new messages and yields them as parsed JSON dictionaries """
        msg = self.consumer.poll(timeout)

        if msg is None:
            return None
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                return None
            else:
                logger.error(f"Consumer error: {msg.error()}")
                return None

        try:
            data = json.loads(msg.value().decode('utf-8'))
            return data
        except json.JSONDecodeError:
            logger.error(f"Failed to decode JSON from message: {msg.value()}")
            return None

    def close(self):
        self.consumer.close()
