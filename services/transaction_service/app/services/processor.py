import asyncio
import base64
import json
import logging
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from pydantic import ValidationError
# Tenacity is not used currently, but kept for potential future retry logic
# from tenacity import retry, stop_after_attempt, wait_exponential

from app.core.config import settings
from app.db.models import Transaction
from app.db.session import AsyncSessionFactory
from app.schemas import RawTrade, DBTransactionCreate, EnrichedTrade, DLEnvelope, TradeSide, TradeType, EventType
# Correctly import the real client
from app.clients.security_master_client import security_master_client


logger = logging.getLogger(__name__)

class TransactionProcessor:
    def __init__(self):
        self.consumer = AIOKafkaConsumer(
            settings.KAFKA_INCOMING_TOPIC,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id=settings.KAFKA_CONSUMER_GROUP,
            enable_auto_commit=False,
            auto_offset_reset='earliest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        self.producer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
        )
        self._running = False

    async def start(self):
        logger.info("Starting Transaction Processor...")
        await self.consumer.start()
        await self.producer.start()
        self._running = True
        asyncio.create_task(self.process_messages())

    async def stop(self):
        logger.info("Stopping Transaction Processor...")
        self._running = False
        # Gracefully stop consumer and producer
        if self.consumer._running:
             await self.consumer.stop()
        if self.producer and self.producer._started: # Check if producer exists before checking _started
             await self.producer.stop()
        logger.info("Transaction Processor stopped.")

    async def process_messages(self):
        try:
            async for msg in self.consumer:
                if not self._running: break
                logger.info(f"Processing message from offset {msg.offset} in partition {msg.partition}")
                try:
                    # Parse the message using RawTrade schema which now includes new fields
                    raw_trade = RawTrade.model_validate(msg.value)
                    await self.handle_message(raw_trade)
                    # Only commit if handle_message succeeds
                    await self.consumer.commit()
                except ValidationError as e:
                    logger.error(f"Validation error for incoming Kafka message: {e} - Payload: {msg.value}")
                    # Log and commit offset to avoid blocking on bad messages
                    await self.consumer.commit()
                except Exception as e:
                    # Catch errors from handle_message (enrichment, DB, publish, DLQ send)
                    logger.error(f"Unhandled error processing message offset {msg.offset}: {e}. Message will NOT be committed.", exc_info=True)
                    # DO NOT COMMIT offset here. Kafka consumer group will retry after timeout.
                    # Consider adding specific handling for DLQ send failures if critical
        finally:
            logger.info("Message processing loop finished.")

    async def handle_message(self, raw_trade: RawTrade):
        """Processes a single RawTrade message. Raises exception on internal failure."""
        security_id = None # Initialize security_id
        try:
            # --- Enrichment ---
            if raw_trade.instrument_token:
                # --- THIS IS THE CORRECTED METHOD CALL ---
                security_id = await security_master_client.get_security_id_from_token(raw_trade.instrument_token)
                # ----------------------------------------

            # --- Fail Hard if Enrichment Fails ---
            if security_id is None:
                error_msg = f"Enrichment failed: Could not find security_id for instrument {raw_trade.instrument_token}"
                logger.error(error_msg)
                await self.send_to_dlq(error_msg, raw_trade)
                return # Stop processing THIS message successfully (commit will happen in process_messages)

            # --- Persistence ---
            async with AsyncSessionFactory() as session:
                transaction = await self.upsert_transaction(session, raw_trade, security_id)
                await session.commit() # Commit DB transaction

            # --- Publish Enriched Event ---
            await self.publish_enriched_trade(transaction)

        except Exception as e:
             # Catch potential exceptions during DB or publish steps
             logger.error(f"Internal error processing trade {raw_trade.external_id} after enrichment: {e}", exc_info=True)
             # Attempt to send to DLQ after internal failure
             await self.send_to_dlq(f"Internal processing error: {e}", raw_trade)
             # Re-raise the exception to signal failure to process_messages loop
             raise e

    async def upsert_transaction(self, session: AsyncSession, trade: RawTrade, security_id: int) -> Transaction:
        """Saves the transaction to the database idempotently."""
        version_bytes = base64.b64decode(trade.version) if trade.version else None

        db_payload = DBTransactionCreate(
            external_id=trade.external_id,
            client_id=trade.client_id,
            security_id=security_id,
            side=trade.side,
            price=trade.price,
            quantity=trade.quantity,
            timestamp=trade.timestamp,
            executing_broker=trade.executing_broker,
            source=trade.source,
            version=version_bytes,
            exchange=trade.exchange,
            trade_type=trade.trade_type,
            event_type=trade.event_type
        )

        stmt = insert(Transaction).values(**db_payload.model_dump()).on_conflict_do_nothing(
            index_elements=['external_id']
        )
        await session.execute(stmt)

        result = await session.execute(select(Transaction).where(Transaction.external_id == trade.external_id))
        db_record = result.scalar_one_or_none() # Use scalar_one_or_none for safety
        if db_record is None:
             # This should ideally not happen with upsert, but handle defensively
             logger.error(f"Upsert failed to return a record for external_id: {trade.external_id}")
             raise RuntimeError(f"Database upsert failed for external_id {trade.external_id}")
        return db_record


    async def publish_enriched_trade(self, transaction: Transaction):
        """Publishes the final enriched trade message to Kafka."""
        version_str = base64.b64encode(transaction.version).decode('utf-8') if transaction.version else None

        # Handle enums safely
        side_enum = TradeSide(transaction.side.value) if transaction.side else None
        trade_type_enum = TradeType(transaction.trade_type.value) if transaction.trade_type else None
        event_type_enum = EventType(transaction.event_type.value) if transaction.event_type else None

        enriched_payload = EnrichedTrade(
            transaction_id=transaction.id,
            external_id=transaction.external_id,
            client_id=transaction.client_id,
            security_id=transaction.security_id,
            side=side_enum,
            price=transaction.price,
            quantity=transaction.quantity,
            timestamp=transaction.timestamp,
            executing_broker=transaction.executing_broker,
            source=transaction.source,
            version=version_str,
            created_at=transaction.created_at,
            exchange=transaction.exchange,
            trade_type=trade_type_enum,
            event_type=event_type_enum
        )
        await self.producer.send(
            settings.KAFKA_ENRICHED_TOPIC,
            value=enriched_payload.model_dump(mode="json"),
            headers=[("source_service", b"transaction_master")]
        )
        logger.info(f"Published enriched trade for external_id: {transaction.external_id}")

    async def send_to_dlq(self, error_msg: str, payload: RawTrade):
        """Sends a message envelope to the Dead-Letter Queue."""
        dlq_envelope = DLEnvelope(
            error_message=error_msg,
            failed_at=datetime.now(timezone.utc),
            original_payload=payload
        )
        try:
            await self.producer.send(settings.KAFKA_DLQ_TOPIC, value=dlq_envelope.model_dump(mode="json"))
            logger.info(f"Message for external_id {payload.external_id} sent to DLQ: {error_msg}")
        except Exception as e:
            # If sending to DLQ fails, log critically but don't crash the processor loop.
            # The message offset won't be committed, allowing Kafka to retry later.
            logger.critical(f"FATAL: Could not send message to DLQ for external_id {payload.external_id}: {e}", exc_info=True)
            # Re-raise the exception to signal failure back to process_messages
            raise e

# Singleton instance
transaction_processor = TransactionProcessor()