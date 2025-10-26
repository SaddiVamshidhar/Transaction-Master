from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Enum as SQLAlchemyEnum, LargeBinary,
    UniqueConstraint, Index
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func
# Import the Enums from schemas to ensure consistency
from app.schemas import TradeSide, TradeType, EventType

Base = declarative_base()

class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, index=True)
    external_id = Column(String(255), unique=True, nullable=False, index=True)
    client_id = Column(String(255), nullable=False, index=True)
    # CHANGED: Made non-nullable as Security Master is live
    security_id = Column(Integer, nullable=False, index=True)

    # CHANGED: Made nullable to support non-trade events (e.g., dividends)
    # The Enum name 'trade_side_enum' MUST remain the same unless you drop the type in PG
    side = Column(SQLAlchemyEnum(TradeSide, name="trade_side_enum"), nullable=True)
    # CHANGED: Made nullable to support events without a price (e.g., splits)
    price = Column(Float, nullable=True)
    # CHANGED: Made nullable (though less common, could support events affecting shares differently)
    quantity = Column(Float, nullable=True)

    timestamp = Column(DateTime(timezone=True), nullable=False)
    executing_broker = Column(String(255), nullable=False)
    source = Column(String(255))
    version = Column(LargeBinary, nullable=True)

    # --- NEW COLUMNS ---
    # NEW: Added exchange, nullable=True allows for non-exchange events
    exchange = Column(String(50), nullable=True, index=True)
    # NEW: Added trade_type, nullable=True allows for non-trade events
    # Ensure a unique name for the new Enum type in PostgreSQL
    trade_type = Column(SQLAlchemyEnum(TradeType, name="trade_type_enum"), nullable=True, index=True)
    # NEW: Added event_type, nullable=True as most transactions are not events
    # Ensure a unique name for the new Enum type in PostgreSQL
    event_type = Column(SQLAlchemyEnum(EventType, name="event_type_enum"), nullable=True, index=True)
    # --- END NEW COLUMNS ---

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint('external_id', name='uq_external_id'),
        Index('ix_transactions_client_id', 'client_id'),
        Index('ix_transactions_security_id', 'security_id'),
        # Add indexes for new queryable fields
        Index('ix_transactions_exchange', 'exchange'),
        Index('ix_transactions_trade_type', 'trade_type'),
        Index('ix_transactions_event_type', 'event_type'),
    )