import logging

logger = logging.getLogger(__name__)

class BreakoutValidator:
    def __init__(self, volume_shock_threshold: float = 2.0, ofi_threshold: float = 0.5):
        """
        Validate breakout candidates using market microstructure features.
        
        :param volume_shock_threshold: Multiple of average volume required for a valid breakout.
        :param ofi_threshold: Order Flow Imbalance threshold (-1.0 to 1.0) indicating aggressive buying.
        """
        self.volume_shock_threshold = volume_shock_threshold
        self.ofi_threshold = ofi_threshold

    def calculate_ofi(self, market_data: dict) -> float:
        """
        Calculate Order Flow Imbalance (OFI).
        Requires bid/ask price and size updates.
        Simplified version for prototype.
        """
        # In a real scenario, this uses tick-by-tick orderbook changes
        # Example dummy calculation based on provided fields if they exist
        best_bid_size = market_data.get('bid_size_1', 0)
        best_ask_size = market_data.get('ask_size_1', 0)
        
        total_size = best_bid_size + best_ask_size
        if total_size == 0:
            return 0.0
            
        # Positive OFI means more buying pressure (stronger bids relative to asks)
        ofi = (best_bid_size - best_ask_size) / total_size
        return ofi

    def validate(self, candidate: dict) -> bool:
        """
        Validates if the candidate's current market condition represents a true breakout.
        """
        symbol = candidate.get('symbol', 'UNKNOWN')
        current_volume = candidate.get('volume', 0)
        avg_volume_5m = candidate.get('avg_volume_5m', current_volume) # Fallback to 1.0 ratio if missing
        
        # 1. Volume Shock Check
        volume_ratio = current_volume / avg_volume_5m if avg_volume_5m > 0 else 1.0
        has_volume_shock = volume_ratio >= self.volume_shock_threshold

        # 2. Order Flow Imbalance (OFI) Check
        ofi = self.calculate_ofi(candidate)
        has_buy_pressure = ofi >= self.ofi_threshold

        # 3. Microstructure Validation
        if has_volume_shock and has_buy_pressure:
            candidate['breakout_validated'] = True
            candidate['breakout_metrics'] = {
                'volume_ratio': round(volume_ratio, 2),
                'ofi': round(ofi, 2)
            }
            logger.info(f"[{symbol}] Breakout Validated: VolShock={volume_ratio:.1f}x, OFI={ofi:.2f}")
            return True
        else:
            logger.debug(f"[{symbol}] Breakout Failed: VolShock={volume_ratio:.1f}x, OFI={ofi:.2f}")
            return False
