from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict
from enum import Enum
from typing import Optional, List

# --- Core Domain Models ---

class TradeSide(str, Enum):
    BUY = "Buy"
    SELL = "Sell"

class TradeType(str, Enum):
    EQUITY = "EQ"
    FUTURES = "FUT"
    OPTIONS_CALL = "CE"
    OPTIONS_PUT = "PE"
    CURRENCY = "CUR"
    COMMODITY = "COM"

class EventType(str, Enum):
    DIVIDEND = "DIVIDEND"
    SPLIT = "SPLIT"
    BONUS = "BONUS"
    RIGHTS = "RIGHTS"
    MERGER = "MERGER"

class RawTrade(BaseModel):
    """
    The standardized internal representation of any incoming event or trade,
    before it is fully processed and saved. This is what goes into the
    KAFKA_INCOMING_TOPIC.
    """
    external_id: str
    client_id: str
    instrument_token: Optional[str] = None
    side: Optional[TradeSide] = None
    price: Optional[float] = None
    quantity: Optional[float] = None
    timestamp: datetime
    executing_broker: str
    version: Optional[str] = None
    source: Optional[str] = None
    exchange: Optional[str] = None
    trade_type: Optional[TradeType] = None
    event_type: Optional[EventType] = None


# --- External API Models ---

class FyersTradeWebhook(BaseModel):
    """Payload from the external Fyers trade webhook."""
    id: str
    clientId: str
    symbol: str
    side: int
    tradedPrice: float
    filledQty: int
    orderDateTime: str
    model_config = ConfigDict(extra='ignore')


class InternalEventPayload(BaseModel):
    """
    Payload expected from the internal Event Master service.
    """
    event_id: str                   # The unique ID from the event system (for idempotency)
    client_id: str                  # The client this event applies to
    instrument_token: str           # The security this event applies to
    event_type: EventType           # e.g., "DIVIDEND", "SPLIT"
    timestamp: datetime             # When the event occurred or is effective
    price: Optional[float] = None     # e.g., cash amount for a dividend
    quantity: Optional[float] = None  # e.g., new shares for a split/bonus
    source: str = "event_master"
    
    model_config = ConfigDict(extra='ignore')


# --- Database & Messaging Models ---

class DBTransactionCreate(BaseModel):
    external_id: str
    client_id: str
    security_id: int
    side: Optional[TradeSide] = None
    price: Optional[float] = None
    quantity: Optional[float] = None
    timestamp: datetime
    executing_broker: str
    version: Optional[bytes] = None
    source: Optional[str] = None
    exchange: Optional[str] = None
    trade_type: Optional[TradeType] = None
    event_type: Optional[EventType] = None

class EnrichedTrade(BaseModel):
    """
    The final, enriched data model. This is what is saved to the DB
    (via DBTransactionCreate) and published to KAFKA_ENRICHED_TOPIC.
    """
    transaction_id: int
    external_id: str
    client_id: str
    security_id: int
    side: Optional[TradeSide] = None
    price: Optional[float] = None
    quantity: Optional[float] = None
    timestamp: datetime
    executing_broker: str
    source: Optional[str] = None
    version: Optional[str] = None
    created_at: datetime
    exchange: Optional[str] = None
    trade_type: Optional[TradeType] = None
    event_type: Optional[EventType] = None

class DLEnvelope(BaseModel):
    error_message: str
    error_service: str = "transaction_master_service"
    failed_at: datetime
    original_payload: RawTrade # Consider if this should hold Fyers payload on Fyers failure


# --- Security Master Client Models (REVISED) ---

class SecurityMasterDetail(BaseModel):
    """Represents the object inside the 'data' list."""
    id: int # This is the crucial security_id
    company_name: Optional[str] = None
    isin: Optional[str] = None
    market_code1: Optional[str] = None
    symbol1: Optional[str] = None
    # Add any other fields you might need from the response later
    # Be careful with types, some numbers are strings
    face_value: Optional[str] = None
    outstanding_shares: Optional[str] = None

    # Allow extra fields we don't explicitly define
    model_config = ConfigDict(extra='ignore')

class SecurityMasterResponse(BaseModel):
    """Represents the overall structure of the Security Master API response."""
    count: int
    data: List[SecurityMasterDetail] # It's a list containing detail objects
    status: str # Expecting "success" or potentially an error status

    # Allow extra fields we don't explicitly define
    model_config = ConfigDict(extra='ignore')