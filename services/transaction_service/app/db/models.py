from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Enum as SQLAlchemyEnum, LargeBinary,
    UniqueConstraint, Index
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func
from app.schemas import TradeSide

Base = declarative_base()

class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, index=True)
    external_id = Column(String(255), unique=True, nullable=False, index=True)
    client_id = Column(String(255), nullable=False, index=True)
    security_id = Column(Integer, nullable=True, index=True) # Nullable for failed enrichment
    
    side = Column(SQLAlchemyEnum(TradeSide, name="trade_side_enum"), nullable=False)
    price = Column(Float, nullable=False)
    quantity = Column(Float, nullable=False)
    
    timestamp = Column(DateTime(timezone=True), nullable=False)
    executing_broker = Column(String(255), nullable=False)
    source = Column(String(255))
    version = Column(LargeBinary, nullable=True)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint('external_id', name='uq_external_id'),
        Index('ix_transactions_client_id', 'client_id'),
        Index('ix_transactions_security_id', 'security_id'),
    )