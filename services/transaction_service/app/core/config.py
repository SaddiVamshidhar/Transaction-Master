from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    # Kafka settings
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_INCOMING_TOPIC: str = "transactions.incoming"
    KAFKA_ENRICHED_TOPIC: str = "transactions.enriched"
    KAFKA_DLQ_TOPIC: str = "transactions.dlq"
    KAFKA_CONSUMER_GROUP: str = "transaction_service_group"

    # Database settings
    DB_URL: str = "postgresql+asyncpg://user:password@localhost:5432/transactions_db"

    # App settings
    LOG_LEVEL: str = "INFO"
    PROCESSOR_MAX_RETRIES: int = 3
    
settings = Settings()