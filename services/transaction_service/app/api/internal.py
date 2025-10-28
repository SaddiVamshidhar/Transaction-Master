import logging
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, status

from app.schemas import RawTrade, InternalEventPayload
from app.kafka.producer import KafkaProducer, get_kafka_producer
from app.core.config import settings
from app.utils.parsers import parse_fyers_symbol  # <--- IMPORT THE SHARED PARSER

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/event", status_code=status.HTTP_202_ACCEPTED)
async def accept_internal_event(
    event: InternalEventPayload,
    producer: KafkaProducer = Depends(get_kafka_producer)
):
    """
    Accepts an internal corporate action or event, transforms it
    to the internal RawTrade format, and enqueues it for processing.
    
    This endpoint is for internal services (e.g., Event Master).
    """
    try:
        # --- Transformation Logic ---
        
        # We can reuse the symbol parser to get exchange/trade_type
        # It's okay if trade_type is None for a pure event.
        exchange, trade_type = parse_fyers_symbol(event.instrument_token)

        # Transform the internal event payload into the standardized RawTrade model
        # This is the same model the /webhook/trade endpoint produces
        internal_trade = RawTrade(
            external_id=event.event_id,
            client_id=event.client_id,
            instrument_token=event.instrument_token,
            
            # Events don't have a "side"
            side=None, 
            
            price=event.price,       # Use price for dividend amount, etc.
            quantity=event.quantity, # Use quantity for bonus shares, etc.
            
            timestamp=event.timestamp,
            executing_broker="SYSTEM", # Clearly mark this as an internal event
            source=event.source,
            
            exchange=exchange,
            trade_type=trade_type,
            
            # This is the most important part!
            event_type=event.event_type 
        )

        logger.info(f"Received and transformed internal event: {internal_trade.external_id}")

        # Send the standardized message to the *same* incoming topic
        await producer.send(
            topic=settings.KAFKA_INCOMING_TOPIC,
            value=internal_trade.model_dump(mode="json")
        )

        return {"message": "Event accepted for processing."}
        
    except Exception as e:
        logger.error(f"Failed to process internal event: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not queue event for processing."
        )