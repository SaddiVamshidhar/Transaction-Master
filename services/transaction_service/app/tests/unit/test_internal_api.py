import pytest
from httpx import AsyncClient
from fastapi import FastAPI
from unittest.mock import AsyncMock, MagicMock

# Import the router we are testing
from app.api import internal as internal_api

# Import the dependency we need to override
from app.kafka.producer import get_kafka_producer, KafkaProducer

# Import schemas for payload creation
from app.schemas import EventType
from app.core.config import settings # Need this for the topic name


@pytest.fixture
def test_app(mock_kafka_producer: MagicMock) -> FastAPI:
    """
    Creates a FastAPI app instance for testing, including the internal router
    and overriding the Kafka producer dependency.
    """
    app = FastAPI()
    app.include_router(internal_api.router, prefix="/internal")
    
    # Override the dependency to use our mock
    app.dependency_overrides[get_kafka_producer] = lambda: mock_kafka_producer
    return app

@pytest.mark.asyncio
async def test_accept_internal_event_dividend_success(
    test_app: FastAPI, 
    mock_kafka_producer: MagicMock
):
    """
    Tests the successful submission of a DIVIDEND event.
    Verifies 202 status and checks the exact message sent to Kafka.
    """
    payload = {
        "event_id": "evt_div_12345",
        "client_id": "client_abc",
        "instrument_token": "NSE:INFY-EQ",
        "event_type": EventType.DIVIDEND,
        "timestamp": "2025-11-01T09:00:00Z",
        "price": 10.5,  # Represents dividend amount per share
        "quantity": None,
        "source": "event_master_test"
    }

    async with AsyncClient(app=test_app, base_url="http://test") as ac:
        response = await ac.post("/internal/event", json=payload)

    # 1. Check for 202 Accepted response
    assert response.status_code == 202
    assert response.json() == {"message": "Event accepted for processing."}

    # 2. Verify Kafka producer was called correctly
    mock_kafka_producer.send.assert_called_once()
    
    # 3. Inspect the arguments passed to producer.send
    call_args = mock_kafka_producer.send.call_args
    sent_topic = call_args.kwargs['topic']
    sent_value = call_args.kwargs['value']

    # Check the topic is correct
    assert sent_topic == settings.KAFKA_INCOMING_TOPIC

    # Check the transformed RawTrade payload
    assert sent_value['external_id'] == "evt_div_12345"
    assert sent_value['client_id'] == "client_abc"
    assert sent_value['instrument_token'] == "NSE:INFY-EQ"
    assert sent_value['event_type'] == "DIVIDEND"
    assert sent_value['trade_type'] == "EQ"      # Correctly parsed
    assert sent_value['exchange'] == "NSE"       # Correctly parsed
    assert sent_value['price'] == 10.5
    assert sent_value['quantity'] is None
    assert sent_value['side'] is None
    assert sent_value['executing_broker'] == "SYSTEM"
    assert sent_value['source'] == "event_master_test"

@pytest.mark.asyncio
async def test_accept_internal_event_split_success(
    test_app: FastAPI, 
    mock_kafka_producer: MagicMock
):
    """
    Tests the successful submission of a SPLIT event.
    """
    payload = {
        "event_id": "evt_split_67890",
        "client_id": "client_xyz",
        "instrument_token": "NSE:TCS-EQ",
        "event_type": EventType.SPLIT,
        "timestamp": "2025-11-02T09:00:00Z",
        "price": None,
        "quantity": 2.0,  # Represents 2-for-1 split
    } # Note: 'source' is optional in schema, defaults to 'event_master'

    async with AsyncClient(app=test_app, base_url="http://test") as ac:
        response = await ac.post("/internal/event", json=payload)

    assert response.status_code == 202

    # Verify the payload sent to Kafka
    mock_kafka_producer.send.assert_called_once()
    sent_value = mock_kafka_producer.send.call_args.kwargs['value']
    
    assert sent_value['external_id'] == "evt_split_67890"
    assert sent_value['event_type'] == "SPLIT"
    assert sent_value['price'] is None
    assert sent_value['quantity'] == 2.0
    assert sent_value['source'] == "event_master" # Check default

@pytest.mark.asyncio
async def test_accept_internal_event_validation_error(
    test_app: FastAPI, 
    mock_kafka_producer: MagicMock
):
    """
    Tests that a 422 Unprocessable Entity is returned for an invalid payload
    (e.g., missing required fields like 'event_id' or 'event_type').
    """
    payload = {
        # "event_id": "missing", # <-- Missing required field
        "client_id": "client_abc",
        "instrument_token": "NSE:INFY-EQ",
        # "event_type": "DIVIDEND", # <-- Missing required field
        "timestamp": "2025-11-01T09:00:00Z",
    }

    async with AsyncClient(app=test_app, base_url="http://test") as ac:
        response = await ac.post("/internal/event", json=payload)

    # 1. Check for 422 status code
    assert response.status_code == 422

    # 2. Check that the producer was NOT called
    mock_kafka_producer.send.assert_not_called()

@pytest.mark.asyncio
async def test_accept_internal_event_kafka_failure(
    test_app: FastAPI, 
    mock_kafka_producer: MagicMock
):
    """
    Tests that a 503 Service Unavailable is returned if the Kafka
    producer fails to send the message.
    """
    # 1. Configure the mock producer to raise an exception
    mock_kafka_producer.send = AsyncMock(
        side_effect=Exception("Kafka is down for testing!")
    )

    # 2. Send a perfectly valid payload
    payload = {
        "event_id": "evt_fail_111",
        "client_id": "client_fail",
        "instrument_token": "NSE:WIPRO-EQ",
        "event_type": EventType.BONUS,
        "timestamp": "2025-11-03T09:00:00Z",
        "quantity": 1
    }

    async with AsyncClient(app=test_app, base_url="http://test") as ac:
        response = await ac.post("/internal/event", json=payload)

    # 3. Check for 503 status code
    assert response.status_code == 503
    assert response.json() == {"detail": "Could not queue event for processing."}

    # 4. Ensure the mock was still called
    mock_kafka_producer.send.assert_called_once()