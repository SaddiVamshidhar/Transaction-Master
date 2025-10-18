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
from tenacity import retry, stop_after_attempt, wait_exponential

from app.core.config import settings
from app.db.models import Transaction
from app.db.session import AsyncSessionFactory
from app.schemas import RawTrade, DBTransactionCreate, EnrichedTrade, DLEnvelope, TradeSide
from .security_master_mock import security_master_client

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
        await self.consumer.stop()
        await self.producer.stop()
        logger.info("Transaction Processor stopped.")

    async def process_messages(self):
        try:
            async for msg in self.consumer:
                if not self._running: break
                logger.info(f"Processing message from offset {msg.offset} in partition {msg.partition}")
                try:
                    await self.handle_message(msg.value)
                    await self.consumer.commit()
                except Exception as e:
                    logger.error(f"Unhandled error in message handler: {e}. Message will not be committed.", exc_info=True)
        finally:
            logger.info("Message processing loop finished.")

    @retry(stop=stop_after_attempt(settings.PROCESSOR_MAX_RETRIES), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def handle_message(self, message_data: dict):
        try:
            raw_trade = RawTrade.model_validate(message_data)
        except ValidationError as e:
            logger.error(f"Validation error for message: {e}")
            await self.send_to_dlq("Validation Error", RawTrade.model_validate(message_data, strict=False))
            return

        try:
            # --- MODIFIED SECTION START ---
            
            # 1. Call the updated security master client
            security_details_response = await security_master_client.get_security_details(raw_trade.instrument_token)
            
            # 2. Check the response and extract the ID
            security_id = None
            if security_details_response and security_details_response.get("success"):
                security_id = security_details_response["data"]["id"]
            else:
                logger.warning(f"Enrichment failed for instrument {raw_trade.instrument_token}. Proceeding with NULL security_id.")
            
            # --- MODIFIED SECTION END ---

            async with AsyncSessionFactory() as session:
                transaction = await self.upsert_transaction(session, raw_trade, security_id)
                await session.commit()
            
            await self.publish_enriched_trade(transaction)

        except Exception as e:
            logger.error(f"Failed to process trade {raw_trade.external_id}: {e}", exc_info=True)
            current_retry = self.handle_message.retry.statistics.get('attempt_number', 1)
            if current_retry >= settings.PROCESSOR_MAX_RETRIES:
                logger.error(f"Max retries reached for trade {raw_trade.external_id}. Sending to DLQ.")
                await self.send_to_dlq(str(e), raw_trade)
            else:
                raise

    async def upsert_transaction(self, session: AsyncSession, trade: RawTrade, security_id: int | None) -> Transaction:
        version_bytes = base64.b64decode(trade.version) if trade.version else None
        
        db_payload = DBTransactionCreate(
            external_id=trade.external_id, client_id=trade.client_id,
            security_id=security_id, side=trade.side, price=trade.price,
            quantity=trade.quantity, timestamp=trade.timestamp,
            executing_broker=trade.executing_broker, source=trade.source,
            version=version_bytes
        )

        stmt = insert(Transaction).values(**db_payload.model_dump()).on_conflict_do_nothing(
            index_elements=['external_id']
        )
        await session.execute(stmt)
        
        result = await session.execute(select(Transaction).where(Transaction.external_id == trade.external_id))
        return result.scalar_one()

    async def publish_enriched_trade(self, transaction: Transaction):
        version_str = base64.b64encode(transaction.version).decode('utf-8') if transaction.version else None
        
        enriched_payload = EnrichedTrade(
            transaction_id=transaction.id, external_id=transaction.external_id,
            client_id=transaction.client_id, security_id=transaction.security_id,
            side=TradeSide(transaction.side.value), price=transaction.price,
            quantity=transaction.quantity, timestamp=transaction.timestamp,
            executing_broker=transaction.executing_broker, source=transaction.source,
            version=version_str, created_at=transaction.created_at
        )
        await self.producer.send(
            settings.KAFKA_ENRICHED_TOPIC, 
            value=enriched_payload.model_dump(mode="json"), 
            headers=[("source_service", b"transaction_master")]
        )
        logger.info(f"Published enriched trade for external_id: {transaction.external_id}")

    async def send_to_dlq(self, error_msg: str, payload: RawTrade):
        dlq_envelope = DLEnvelope(
            error_message=error_msg,
            failed_at=datetime.now(timezone.utc),
            original_payload=payload
        )
        try:
            await self.producer.send(settings.KAFKA_DLQ_TOPIC, value=dlq_envelope.model_dump(mode="json"))
            logger.info(f"Message for external_id {payload.external_id} sent to DLQ.")
        except Exception as e:
            logger.critical(f"FATAL: Could not send message to DLQ: {e}", exc_info=True)

transaction_processor = TransactionProcessor()