import logging
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, status
from app.schemas import RawTrade, FyersTradeWebhook, TradeSide
from app.kafka.producer import KafkaProducer, get_kafka_producer
from app.core.config import settings

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/trade", status_code=status.HTTP_202_ACCEPTED)
async def accept_trade(
    # 1. Expect the Fyers-specific model
    fyers_trade: FyersTradeWebhook,
    producer: KafkaProducer = Depends(get_kafka_producer)
):
    """
    Accepts a Fyers-specific trade payload, validates it, transforms it
    to our internal format, and enqueues it to Kafka.
    """
    try:
        # 2. Transform the Fyers data into our internal RawTrade format
        
        # Convert side from number to string
        trade_side = TradeSide.BUY if fyers_trade.side == 1 else TradeSide.SELL

        # Convert timestamp from Fyers format to standard datetime object
        # Fyers format: "06-Oct-2025 11:44:04"
        trade_timestamp = datetime.strptime(fyers_trade.order_date_time, "%d-%b-%Y %H:%M:%S")

        internal_trade = RawTrade(
            external_id=fyers_trade.id,
            client_id=fyers_trade.client_id,
            instrument_token=fyers_trade.symbol,
            side=trade_side,
            price=fyers_trade.traded_price,
            quantity=fyers_trade.filled_qty,
            timestamp=trade_timestamp,
            executing_broker="Fyers",  # We can hard-code this
            source="fyers_webhook"
        )

        logger.info(f"Received and transformed Fyers trade with external_id: {internal_trade.external_id}")
        
        # 3. Send our internal model to Kafka
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