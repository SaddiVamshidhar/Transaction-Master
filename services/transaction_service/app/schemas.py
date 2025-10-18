from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict
from enum import Enum
from typing import Optional

# --- Core Domain Models ---

class TradeSide(str, Enum):
    BUY = "Buy"
    SELL = "Sell"

class RawTrade(BaseModel):
    """Our internal, standardized representation of a raw trade, used for Kafka messages."""
    external_id: str
    client_id: str
    instrument_token: str
    side: TradeSide
    price: float
    quantity: float
    timestamp: datetime
    executing_broker: str
    version: Optional[str] = None
    source: Optional[str] = None


# --- External API Models ---

class FyersTradeWebhook(BaseModel):
    """Pydantic model to validate the incoming webhook payload from Fyers."""
    # Use Field(alias=...) to map Fyers's JSON field names (like 'id') to our preferred Python attribute names.
    id: str = Field(..., alias='id')
    client_id: str = Field(..., alias='clientId')
    symbol: str = Field(..., alias='symbol')
    side: int  # Fyers sends a number (-1 for sell, 1 for buy)
    traded_price: float = Field(..., alias='tradedPrice')
    filled_qty: int = Field(..., alias='filledQty')
    order_date_time: str = Field(..., alias='orderDateTime')
    
    # This tells Pydantic to ignore any extra fields Fyers might send that we don't use.
    model_config = ConfigDict(extra='ignore')


# --- Database & Messaging Models ---

class DBTransactionCreate(BaseModel):
    """Schema for creating a new record in the database."""
    external_id: str
    client_id: str
    security_id: Optional[int]
    side: TradeSide
    price: float
    quantity: float
    timestamp: datetime
    executing_broker: str
    version: Optional[bytes] = None
    source: Optional[str] = None

class EnrichedTrade(BaseModel):
    """Schema for the outgoing message to the 'enriched' topic and the query API response."""
    transaction_id: int
    external_id: str
    client_id: str
    security_id: Optional[int]
    side: TradeSide
    price: float
    quantity: float
    timestamp: datetime
    executing_broker: str
    source: Optional[str] = None
    version: Optional[str] = None
    created_at: datetime

class DLEnvelope(BaseModel):
    """Schema for messages sent to the Dead-Letter Queue."""
    error_message: str
    error_service: str = "transaction_master_service"
    failed_at: datetime
    original_payload: RawTrade # Note: In a real system, you might want this to hold the raw Fyers payload.


# --- Security Master Client Models ---

class SecurityMasterData(BaseModel):
    id: int
    nse_symbol: Optional[str] = None
    company_name: str

class SecurityMasterResponse(BaseModel):
    success: bool
    data: Optional[SecurityMasterData] = None