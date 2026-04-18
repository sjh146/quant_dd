import json
import logging
import numpy as np
import pandas as pd
import redis

logger = logging.getLogger(__name__)

class LeadLagEngine:
    def __init__(self, redis_url="redis://localhost:6379/0"):
        """
        Initialize Lead-Lag Engine with Redis connection for storing the correlation tensor/graph.
        """
        self.redis_client = redis.from_url(redis_url)
        self.redis_key = "lead_lag_graph"

    def lead_lag_corr(self, a: pd.Series, b: pd.Series, max_lag: int = 30):
        """
        Finds the maximum cross-correlation between series `a` and shifted series `b`.
        Returns a tuple of (best_lag, max_correlation).
        """
        results = []
        for lag in range(1, max_lag):
            corr = a.corr(b.shift(lag))
            if pd.notna(corr):
                results.append((lag, corr))
        
        if not results:
            return 0, 0.0
            
        return max(results, key=lambda x: x[1])

    def compute_and_store_graph(self, price_df: pd.DataFrame, max_lag: int = 30, corr_threshold: float = 0.7):
        """
        Compute Lead-Lag relationships across a universe of stocks and store the directed graph in Redis.
        :param price_df: DataFrame where columns are stock symbols and rows are time series prices/returns.
        """
        graph = {}
        symbols = price_df.columns

        for leader in symbols:
            for follower in symbols:
                if leader == follower:
                    continue
                
                lag, corr = self.lead_lag_corr(price_df[leader], price_df[follower], max_lag)

                # If strong correlation is found with a valid lag
                if corr > corr_threshold and 1 <= lag <= max_lag:
                    if follower not in graph:
                        graph[follower] = []
                    graph[follower].append({
                        "leader": leader,
                        "lag": lag,
                        "corr": float(corr)  # Convert for JSON serialization
                    })
        
        # Sort the leaders for each follower by highest correlation
        for follower in graph:
            graph[follower] = sorted(graph[follower], key=lambda x: x['corr'], reverse=True)

        # Store to Redis
        try:
            self.redis_client.set(self.redis_key, json.dumps(graph))
            logger.info(f"Lead-Lag graph updated in Redis. Discovered leaders for {len(graph)} stocks.")
        except Exception as e:
            logger.error(f"Failed to save Lead-Lag graph to Redis: {e}")

    def get_leaders_for(self, symbol: str):
        """
        Retrieve identified leaders for a given stock symbol from Redis.
        """
        try:
            data = self.redis_client.get(self.redis_key)
            if data:
                graph = json.loads(data)
                return graph.get(symbol, [])
        except Exception as e:
            logger.error(f"Failed to fetch Lead-Lag graph from Redis: {e}")
        return []
