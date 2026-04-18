import os
import json
import time
import logging
from confluent_kafka import Consumer, KafkaError

# ---------------------------------------------------------
# [WARNING] 
# This code is meant to be run on the WINDOWS VM.
# It requires `pip install confluent-kafka pywin32`
# ---------------------------------------------------------
try:
    import win32com.client
    CREON_AVAILABLE = True
except ImportError:
    CREON_AVAILABLE = False
    print("win32com is not available. Creon API features will be mocked.")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("CreonExecutionEngine")

class CreonExecutionEngine:
    def __init__(self):
        self.is_connected = False
        if CREON_AVAILABLE:
            self.init_creon()

    def init_creon(self):
        """Initialize Creon COM objects and check connection."""
        try:
            # CpCybos: Check Connection Status
            self.cpStatus = win32com.client.Dispatch('CpUtil.CpCybos')
            self.cpTradeUtil = win32com.client.Dispatch('CpTrade.CpTdUtil')
            
            if self.cpStatus.IsConnect == 0:
                logger.error("Creon Plus is NOT connected. Please login to Creon HTS/API first.")
                return
            
            if self.cpTradeUtil.TradeInit(0) != 0:
                logger.error("TradeInit failed. Check your Cybos Plus password/cert.")
                return

            self.acc = self.cpTradeUtil.AccountNumber[0] # First Account
            self.accFlag = self.cpTradeUtil.GoodsList(self.acc, 1)[0] # Account Flag (e.g., Stock)
            
            # CpTd0311: Order Object
            self.cpOrder = win32com.client.Dispatch('CpTrade.CpTd0311')
            self.is_connected = True
            logger.info(f"Creon API Connected Successfully. Account: {self.acc}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Creon API: {e}")

    def execute_order(self, signal: dict):
        """
        Translates the Kafka JSON signal to a Creon API order.
        """
        symbol = signal.get('symbol')
        action = signal.get('action')
        qty = signal.get('quantity')
        
        # Creon requires "A" prefix for stock codes generally
        if not symbol.startswith('A'):
            creon_symbol = f"A{symbol}"
        else:
            creon_symbol = symbol

        logger.info(f"Execution Triggered: {action} {qty} shares of {creon_symbol} (Reason: {signal.get('reason')})")

        if not self.is_connected:
            logger.warning("Creon API not connected. Mocking order execution.")
            return True

        try:
            # Set order values
            self.cpOrder.SetInputValue(0, "2")        # 2: Buy, 1: Sell
            if action.upper() == "SELL":
                self.cpOrder.SetInputValue(0, "1")
                
            self.cpOrder.SetInputValue(1, self.acc)       # Account Number
            self.cpOrder.SetInputValue(2, self.accFlag)   # Account Flag
            self.cpOrder.SetInputValue(3, creon_symbol)   # Stock Code
            self.cpOrder.SetInputValue(4, qty)            # Quantity
            self.cpOrder.SetInputValue(5, 0)              # Price (0 for Market Order)
            self.cpOrder.SetInputValue(8, "03")           # 03: Market Price (시장가)

            # Block until execution
            result = self.cpOrder.BlockRequest()
            
            if result != 0:
                logger.error(f"Order BlockRequest failed with code: {result}")
                return False
                
            rqStatus = self.cpOrder.GetDibStatus()
            rqRet = self.cpOrder.GetDibMsg1()
            
            if rqStatus != 0:
                logger.error(f"Order Failed: {rqRet}")
                return False
                
            order_num = self.cpOrder.GetHeaderValue(8)
            logger.info(f"Order Success! Order Number: {order_num}")
            return True
            
        except Exception as e:
            logger.error(f"Order Execution Exception: {e}")
            return False

def main():
    # In Windows, point this to the Ubuntu VM's IP address
    REDIS_URL = os.getenv('REDIS_URL', 'redis://192.168.0.50:6379/0') 
    TOPIC = 'signal.trade'
    
    logger.info(f"Starting Execution Node. Connecting to Redis: {REDIS_URL}")
    
    try:
        redis_client = redis.from_url(REDIS_URL)
        pubsub = redis_client.pubsub()
        pubsub.subscribe(TOPIC)
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        return

    engine = CreonExecutionEngine()
    
    logger.info(f"Listening for execution signals on Redis channel: {TOPIC}...")
    
    try:
        for msg in pubsub.listen():
            if msg['type'] == 'message':
                try:
                    signal_data = json.loads(msg['data'].decode('utf-8'))
                    logger.info(f"Received Signal from AI System: {signal_data}")
                    
                    # Execute the trade via Creon API
                    engine.execute_order(signal_data)
                    
                except json.JSONDecodeError:
                    logger.error("Failed to parse message")
                except Exception as e:
                    logger.error(f"Error during order execution: {e}")
                    
    except KeyboardInterrupt:
        logger.info("Shutting down Execution Node...")
    finally:
        pubsub.close()
        redis_client.close()

if __name__ == "__main__":
    main()