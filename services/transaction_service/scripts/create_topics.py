import asyncio
import logging
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError
# Note: This import will fail on your local machine, but it will work inside the Docker container
# because of how we will structure the 'app' directory later.
from app.core.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def create_topics():
    """Creates the necessary Kafka topics idempotently."""
    # A simple retry loop for initial connection
    for attempt in range(5):
        try:
            admin_client = AIOKafkaAdminClient(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS
            )
            await admin_client.start()
            logger.info("Successfully connected to Kafka.")
            break
        except Exception as e:
            if attempt == 4:
                logger.error(f"Failed to connect to Kafka after 5 attempts: {e}")
                raise
            logger.warning(f"Kafka not ready yet, retrying in 5 seconds... ({e})")
            await asyncio.sleep(5)
    
    topics = [
        NewTopic(name=settings.KAFKA_INCOMING_TOPIC, num_partitions=3, replication_factor=1),
        NewTopic(name=settings.KAFKA_ENRICHED_TOPIC, num_partitions=3, replication_factor=1),
        NewTopic(name=settings.KAFKA_DLQ_TOPIC, num_partitions=1, replication_factor=1),
    ]
    
    try:
        for topic in topics:
            try:
                await admin_client.create_topics([topic])
                logger.info(f"Topic '{topic.name}' created successfully.")
            except TopicAlreadyExistsError:
                logger.warning(f"Topic '{topic.name}' already exists.")
            except Exception as e:
                logger.error(f"Failed to create topic '{topic.name}': {e}")
                raise
    finally:
        await admin_client.close()

if __name__ == "__main__":
    logger.info("Attempting to create Kafka topics...")
    # This script will eventually run from the app's startup command in docker-compose.yml
    # To run it manually inside the container for testing:
    # docker-compose run --rm transaction_service python /app/scripts/create_topics.py
    try:
        asyncio.run(create_topics())
        logger.info("Topic creation process finished.")
    except Exception as e:
        logger.critical(f"Could not run topic creation: {e}")