import pytest
from httpx import AsyncClient
from fastapi import FastAPI
from app.api.webhook import router as webhook_router
from app.kafka.producer import get_kafka_producer

@pytest.fixture
def test_app(mock_kafka_producer):
    app = FastAPI()
    app.include_router(webhook_router)
    app.dependency_overrides[get_kafka_producer] = lambda: mock_kafka_producer
    return app

@pytest.mark.asyncio
async def test_accept_trade_success(test_app, mock_kafka_producer):
    payload = {
        "external_id": "trade_123", "client_id": "client_abc",
        "instrument_token": "INFY", "side": "Buy",
        "price": 18500.50, "quantity": 50,
        "timestamp": "2025-09-30T10:00:00Z",
        "executing_broker": "Fyers", "source": "fyers_webhook"
    }
    async with AsyncClient(app=test_app, base_url="http://test") as ac:
        response = await ac.post("/trade", json=payload)

    assert response.status_code == 202
    assert response.json() == {"message": "Trade accepted for processing."}
    mock_kafka_producer.send.assert_called_once()

@pytest.mark.asyncio
async def test_accept_trade_validation_error(test_app):
    payload = { "external_id": "trade_123" } # Incomplete payload
    async with AsyncClient(app=test_app, base_url="http://test") as ac:
        response = await ac.post("/trade", json=payload)
    assert response.status_code == 422