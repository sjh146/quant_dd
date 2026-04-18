import os
import pandas as pd
import numpy as np
import logging
from agents.lead_lag import LeadLagEngine

logging.basicConfig(level=logging.INFO)

def run_test():
    redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
    engine = LeadLagEngine(redis_url)
    
    # 1. Create dummy historical price data
    # Leader is a sine wave (e.g., strong trend/cycle)
    np.random.seed(42)
    x = np.linspace(0, 50, 100)
    leader_price = np.sin(x) + np.random.normal(0, 0.1, 100)
    
    # Follower lags by 5 periods exactly
    follower_price = np.roll(leader_price, 5)
    follower_price[:5] = 0 # zero out the shifted part to mimic no early info
    
    # Noise stock (unrelated)
    noise_price = np.random.normal(0, 1, 100)
    
    df = pd.DataFrame({
        "LEADER_STOCK": leader_price,
        "FOLLOWER_STOCK": follower_price,
        "NOISE_STOCK": noise_price
    })
    
    # 2. Compute the Lead-Lag tensor and save to Redis Graph
    print("\n--- Computing Lead-Lag Graph ---")
    engine.compute_and_store_graph(df, max_lag=10, corr_threshold=0.8)
    
    # 3. Retrieve from Redis and verify
    print("\n--- Retrieving from Redis ---")
    print("Leaders for FOLLOWER_STOCK:", engine.get_leaders_for("FOLLOWER_STOCK"))
    print("Leaders for NOISE_STOCK:", engine.get_leaders_for("NOISE_STOCK"))
    print("Leaders for LEADER_STOCK:", engine.get_leaders_for("LEADER_STOCK"))

if __name__ == "__main__":
    run_test()
