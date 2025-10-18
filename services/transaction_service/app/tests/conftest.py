import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock
from app.kafka.producer import KafkaProducer

@pytest_asyncio.fixture
async def mock_kafka_producer():
    producer = MagicMock(spec=KafkaProducer)
    producer.send = AsyncMock()
    return producer