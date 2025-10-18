import base64
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import List, Optional

from app.db.models import Transaction
from app.db.session import get_db_session
from app.schemas import EnrichedTrade

router = APIRouter()

@router.get("/transactions", response_model=List[EnrichedTrade])
async def get_transactions(
    client_id: Optional[str] = Query(None, description="Filter by client ID"),
    skip: int = Query(0, ge=0, description="Number of records to skip for pagination"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return"),
    session: AsyncSession = Depends(get_db_session)
):
    """
    Retrieves a list of historical transactions from the database.
    """
    # Start building the query
    query = select(Transaction).order_by(Transaction.timestamp.desc())

    # If a client_id is provided, add a filter
    if client_id:
        query = query.where(Transaction.client_id == client_id)

    # Apply pagination
    query = query.offset(skip).limit(limit)

    # Execute the query
    result = await session.execute(query)
    transactions_from_db = result.scalars().all()

    # FastAPI needs the data to be converted into the Pydantic schema format.
    # A more advanced setup would do this automatically, but a simple loop is very clear.
    response_data = []
    for trans in transactions_from_db:
        # The version is stored as bytes in the DB, so we encode it for the JSON response
        version_str = base64.b64encode(trans.version).decode('utf-8') if trans.version else None
        
        response_data.append(
            EnrichedTrade(
                transaction_id=trans.id,
                external_id=trans.external_id,
                client_id=trans.client_id,
                security_id=trans.security_id,
                side=trans.side,
                price=trans.price,
                quantity=trans.quantity,
                timestamp=trans.timestamp,
                executing_broker=trans.executing_broker,
                source=trans.source,
                version=version_str,
                created_at=trans.created_at
            )
        )
    return response_data