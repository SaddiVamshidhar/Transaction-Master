"""
Utility functions for parsing data from various sources.
"""
import logging
from app.schemas import TradeType

logger = logging.getLogger(__name__)

def parse_fyers_symbol(symbol: str) -> tuple[str, TradeType | None]:
    """
    Parses a Fyers symbol string (e.g., 'NSE:SBIN-EQ', 'NSE:NIFTY25DECFUT')
    into exchange and trade type.
    
    Returns (exchange, trade_type) or (exchange, None) if type is unknown.
    """
    try:
        parts = symbol.split(':')
        if len(parts) != 2:
            logger.warning(f"Could not parse exchange from symbol: {symbol}")
            return "UNKNOWN", None # Or raise an error if exchange is mandatory

        exchange = parts[0]
        identifier = parts[1]

        # Determine trade type based on common Fyers suffixes
        if identifier.endswith("-EQ"):
            trade_type = TradeType.EQUITY
        elif identifier.endswith("FUT"):
            trade_type = TradeType.FUTURES
        elif identifier.endswith("CE"):
            trade_type = TradeType.OPTIONS_CALL
        elif identifier.endswith("PE"):
            trade_type = TradeType.OPTIONS_PUT
        # Add more specific checks if needed for CUR, COM etc.
        # This might require more examples of Fyers symbols for those types.
        else:
            logger.warning(f"Could not determine trade type from symbol identifier: {identifier}")
            trade_type = None # Mark as unknown if suffix doesn't match

        return exchange, trade_type
    except Exception as e:
        logger.error(f"Error parsing symbol '{symbol}': {e}", exc_info=True)
        return "ERROR", None