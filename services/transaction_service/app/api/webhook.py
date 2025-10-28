import logging
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, status
from app.schemas import RawTrade, FyersTradeWebhook, TradeSide, TradeType
from app.kafka.producer import KafkaProducer, get_kafka_producer
from app.core.config import settings
from app.utils.parsers import parse_fyers_symbol  # <--- IMPORTED FROM NEW LOCATION

router = APIRouter()
logger = logging.getLogger(__name__)

# --- The parse_fyers_symbol function has been REMOVED from this file ---
# ... (it is now in app/utils/parsers.py) ...


@router.post("/trade", status_code=status.HTTP_202_ACCEPTED)
async def accept_trade(
    fyers_trade: FyersTradeWebhook,
    producer: KafkaProducer = Depends(get_kafka_producer)
):
    """
    Accepts a Fyers-specific trade payload, validates it, transforms it
    to our internal format (including parsing symbol), and enqueues it to Kafka.
    """
    try:
        # --- Transformation Logic ---
        trade_side = TradeSide.BUY if fyers_trade.side == 1 else TradeSide.SELL
        trade_timestamp = datetime.strptime(fyers_trade.orderDateTime, "%d-%b-%Y %H:%M:%S")

        # NEW: Parse exchange and trade_type from the symbol
        exchange, trade_type = parse_fyers_symbol(fyers_trade.symbol)

        # Handle case where trade_type could not be determined
        if trade_type is None:
            # Decide strategy: error out, or proceed with None?
            # For now, let's proceed but log clearly. Downstream needs to handle None.
            logger.warning(f"Proceeding with unknown trade type for symbol: {fyers_trade.symbol}")

        internal_trade = RawTrade(
            external_id=fyers_trade.id,
            client_id=fyers_trade.clientId,
            instrument_token=fyers_trade.symbol, # Store original symbol
            side=trade_side,
            price=fyers_trade.tradedPrice,
            quantity=fyers_trade.filledQty,
            timestamp=trade_timestamp,
            executing_broker="Fyers",
            source="fyers_webhook",
            exchange=exchange,       # Populate parsed exchange
            trade_type=trade_type,   # Populate parsed trade type
            event_type=None          # Fyers webhook sends trades, not events
        )

        logger.info(f"Received and transformed Fyers trade with external_id: {internal_trade.external_id}")

        await producer.send(
            topic=settings.KAFKA_INCOMING_TOPIC,
            value=internal_trade.model_dump(mode="json")
        )

        return {"message": "Trade accepted for processing."}
    except Exception as e:
        logger.error(f"Failed to process Fyers trade: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not queue trade for processing. Please try again later."
        )