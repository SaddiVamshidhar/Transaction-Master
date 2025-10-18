import asyncio
import json
import uuid
from datetime import datetime, timezone
import httpx
from aiokafka import AIOKafkaConsumer

# Configuration
API_URL = "http://transaction_service:8000/webhook/trade"
# In docker-compose, our service is on the same network as 'kafka'.
# The internal listener is on port 29092.
KAFKA_INTERNAL_BOOTSTRAP_SERVERS = "kafka:29092"
ENRICHED_TOPIC = "transactions.enriched"
CONSUMER_TIMEOUT_S = 10

async def main():
    external_id = f"smoke_test_{uuid.uuid4()}"
    trade_payload = {
        "external_id": external_id,
        "client_id": "smoke_test_client",
        "instrument_token": "TCS-EQ",
        "side": "Buy",
        "price": 3500.0,
        "quantity": 25,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "executing_broker": "Fyers",
        "version": "MQ==", # "1" in base64
        "source": "smoke_test_script"
    }

    # 1. Send trade to webhook
    # The script runs inside the service container, so it can call the API directly.
    print(f"Sending trade with external_id: {external_id} to {API_URL}")
    async with httpx.AsyncClient() as client:
        response = await client.post(API_URL, json=trade_payload)
        response.raise_for_status()
        print(f"Webhook response: {response.status_code}")

    # 2. Consume from enriched topic to verify
    print(f"Waiting for message on topic '{ENRICHED_TOPIC}'...")
    consumer = AIOKafkaConsumer(
        ENRICHED_TOPIC,
        bootstrap_servers=KAFKA_INTERNAL_BOOTSTRAP_SERVERS,
        group_id=f"smoke_test_consumer_{uuid.uuid4()}", # Unique group to read from beginning
        auto_offset_reset='earliest',
        consumer_timeout_ms=CONSUMER_TIMEOUT_S * 1000,
    )
    await consumer.start()

    found_message = False
    try:
        async for msg in consumer:
            data = json.loads(msg.value.decode('utf-8'))
            if data.get("external_id") == external_id:
                print("\n--- Smoke Test SUCCESS ---")
                print("Found matching enriched message:")
                print(json.dumps(data, indent=2))
                assert data['security_id'] == 3003 # Check for correct enrichment
                assert data['transaction_id'] is not None
                found_message = True
                break
    finally:
        await consumer.stop()

    if not found_message:
        print(f"\n--- Smoke Test FAILED ---")
        print(f"Did not find message with external_id '{external_id}' within {CONSUMER_TIMEOUT_S} seconds.")
        exit(1)

if __name__ == "__main__":
    asyncio.run(main())