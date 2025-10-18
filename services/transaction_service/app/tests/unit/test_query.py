import pytest
from httpx import AsyncClient
from fastapi import FastAPI
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timezone

# Import what we need to test and mock
from app.api.query import router as query_router
from app.db.session import get_db_session
from app.db.models import Transaction
from app.schemas import TradeSide

# 1. Create a sample Transaction object, just like one from a real database
fake_transaction = Transaction(
    id=1,
    external_id="test_query_001",
    client_id="client_abc",
    security_id=1001,
    side=TradeSide.BUY,
    price=150.0,
    quantity=10.0,
    timestamp=datetime.now(timezone.utc),
    executing_broker="TestBroker",
    source="test_source",
    version=b'v1',
    created_at=datetime.now(timezone.utc)
)

# 2. Create a fake database session
@pytest.fixture
def mock_db_session():
    # Create a mock for the async session
    session = AsyncMock()
    
    # Mock the execute method to return a result
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [fake_transaction]
    session.execute.return_value = mock_result
    
    # This makes the session usable in an 'async with' block
    async def get_session():
        yield session
        
    return get_session

# 3. Create our test application, but override the real DB with our fake one
@pytest.fixture
def test_app_with_db(mock_db_session):
    app = FastAPI()
    app.include_router(query_router)
    app.dependency_overrides[get_db_session] = mock_db_session
    return app


# 4. The actual test
@pytest.mark.asyncio
async def test_get_transactions_success(test_app_with_db):
    async with AsyncClient(app=test_app_with_db, base_url="http://test") as ac:
        response = await ac.get("/transactions")

    # Assert that the API call was successful
    assert response.status_code == 200
    
    # Assert that the response contains one record
    response_data = response.json()
    assert len(response_data) == 1
    
    # Assert that the data in the response matches our fake data
    assert response_data[0]["external_id"] == "test_query_001"
    assert response_data[0]["security_id"] == 1001