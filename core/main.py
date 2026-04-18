import os
import time
import logging
from infra.kafka_client import QuantKafkaConsumer, QuantKafkaProducer
from infra.db_client import DBManager
from agents.pipeline import PipelineManager
from core.strategy_engine import StrategyEngine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting AI Trading System (Ubuntu Node)...")
    
    # Environment Variables
    KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
    MARKET_DATA_TOPIC = "market.data"
    SIGNAL_TRADE_TOPIC = "signal.trade"

    logger.info(f"Connecting to Kafka broker: {KAFKA_BROKER}")
    
    # 1. Initialize Connections
    consumer = QuantKafkaConsumer(broker_url=KAFKA_BROKER, topic=MARKET_DATA_TOPIC)
    producer = QuantKafkaProducer(broker_url=KAFKA_BROKER)
    db_manager = DBManager()
    
    # 2. Initialize Core Logic Engines
    pipeline = PipelineManager()
    strategy_engine = StrategyEngine()

    logger.info(f"Waiting for market data from [{MARKET_DATA_TOPIC}]...")

    try:
        while True:
            # 1. Consume Market Data (Price, Orderbook, News)
            market_data = consumer.poll_messages(timeout=1.0)
            
            if market_data:
                logger.info(f"Received market data: {market_data.get('symbol', 'UNKNOWN')}")
                
                # 2. Run Pipeline (Momentum -> Breakout -> Liquidity -> LeadLag/Theme)
                candidates = pipeline.run_pipeline(market_data)
                
                if candidates and isinstance(candidates, list):
                    # 3. Strategy Engine Fusion & Position Sizing
                    final_orders = strategy_engine.fuse_signals(candidates)
                    
                    # 4. Emit Order Signals and Log to DB
                    for order in final_orders:
                        if not isinstance(order, dict): continue
                        
                        order_signal = {
                            "action": "BUY",
                            "symbol": order.get("symbol"),
                            "price_type": "MARKET",
                            "quantity": order.get("execution_quantity", 0),
                            "timestamp": time.time(),
                            "reason": order.get('reason', 'Pipeline Validation Passed'),
                            "confidence": order.get('llm_analysis', {}).get('confidence', 0.5)
                        }
                        
                        logger.info(f"Emitting execution signal: {order_signal}")
                        producer.send_signal(SIGNAL_TRADE_TOPIC, order_signal)
                        
                        # 5. Log to PostgreSQL
                        db_manager.log_signal(
                            signal_data=order_signal,
                            breakout_metrics=order.get('breakout_metrics'),
                            llm_data=order.get('llm_analysis')
                        )

    except KeyboardInterrupt:
        logger.info("Shutting down cleanly...")
    finally:
        consumer.close()
        producer.flush()

if __name__ == "__main__":
    main()
