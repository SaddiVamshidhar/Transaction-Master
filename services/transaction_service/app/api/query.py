import base64
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import List, Optional

from app.db.models import Transaction
from app.db.session import get_db_session
# Ensure Enums are imported if needed for type hints or explicit conversion
from app.schemas import EnrichedTrade, TradeSide, TradeType, EventType

router = APIRouter()

@router.get("/transactions", response_model=List[EnrichedTrade])
async def get_transactions(
    # Add optional filters for new fields if desired
    client_id: Optional[str] = Query(None, description="Filter by client ID"),
    exchange: Optional[str] = Query(None, description="Filter by exchange"),
    trade_type: Optional[TradeType] = Query(None, description="Filter by trade type"),
    event_type: Optional[EventType] = Query(None, description="Filter by event type"),
    skip: int = Query(0, ge=0, description="Number of records to skip for pagination"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return"),
    session: AsyncSession = Depends(get_db_session)
):
    """
    Retrieves a list of historical transactions from the database,
    including new fields like exchange, trade_type, and event_type.
    """
    # Start building the query
    query = select(Transaction).order_by(Transaction.timestamp.desc())

    # Apply filters if provided
    if client_id:
        query = query.where(Transaction.client_id == client_id)
    if exchange:
        query = query.where(Transaction.exchange == exchange)
    if trade_type:
        query = query.where(Transaction.trade_type == trade_type)
    if event_type:
        query = query.where(Transaction.event_type == event_type)

    # Apply pagination
    query = query.offset(skip).limit(limit)

    # Execute the query
    result = await session.execute(query)
    transactions_from_db = result.scalars().all()

    # --- Convert DB objects to Pydantic schema, including new fields ---
    response_data = []
    for trans in transactions_from_db:
        # Handle potential None values before accessing .value for Enums
        side_enum = TradeSide(trans.side.value) if trans.side else None
        trade_type_enum = TradeType(trans.trade_type.value) if trans.trade_type else None
        event_type_enum = EventType(trans.event_type.value) if trans.event_type else None
        version_str = base64.b64encode(trans.version).decode('utf-8') if trans.version else None

        response_data.append(
            EnrichedTrade(
                transaction_id=trans.id,
                external_id=trans.external_id,
                client_id=trans.client_id,
                security_id=trans.security_id,
                side=side_enum,          # Pass nullable enum
                price=trans.price,       # Pass nullable float
                quantity=trans.quantity, # Pass nullable float
                timestamp=trans.timestamp,
                executing_broker=trans.executing_broker,
                source=trans.source,
                version=version_str,
                created_at=trans.created_at,
                exchange=trans.exchange,          # NEW: Map exchange
                trade_type=trade_type_enum,      # NEW: Map trade_type
                event_type=event_type_enum       # NEW: Map event_type
            )
        )
    return response_data