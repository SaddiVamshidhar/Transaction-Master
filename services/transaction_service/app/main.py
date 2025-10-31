import logging
from contextlib import asynccontextmanager
from sqlalchemy import text 

from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from aiokafka.errors import KafkaConnectionError
from prometheus_fastapi_instrumentator import Instrumentator

# --- IMPORT ALL ROUTERS ---
from app.api import webhook
from app.api import query as query_api
from app.api import internal as internal_api  # <--- IMPORT THE NEW ROUTER
# ---------------------------

from app.core.config import settings
from app.db.session import get_db_session, engine
from app.kafka.producer import kafka_producer
from app.services.processor import transaction_processor
from app.utils.logging import setup_logging


# Setup logging first
setup_logging()
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting up service...")
    await kafka_producer.start()
    await transaction_processor.start()
    yield
    # Shutdown
    logger.info("Shutting down service...")
    await transaction_processor.stop()
    await kafka_producer.stop()
    await engine.dispose()
    logger.info("Shutdown complete.")

app = FastAPI(
    title="Transaction Master Service",
    description="Service for processing and mastering trade transactions.",
    version="0.1.0",
    lifespan=lifespan
)

# Instrument for Prometheus metrics
Instrumentator().instrument(app).expose(app)

# --- INCLUDE ALL ROUTERS ---
app.include_router(webhook.router, prefix="/webhook", tags=["Webhook (External)"])
app.include_router(query_api.router, prefix="/query", tags=["Query"])
app.include_router(internal_api.router, prefix="/internal", tags=["Internal"]) # <--- ADD THIS LINE
# ---------------------------


@app.get("/health", tags=["Health"])
async def health_check(session: AsyncSession = Depends(get_db_session)):
    """
    Performs a health check on the service and its dependencies (DB, Kafka).
    """
    # Check DB connection
    try:
        # A simple query to check DB connectivity
        await session.execute(text("SELECT 1"))
        db_status = "ok"
    except Exception as e:
        logger.error(f"Health check failed: DB connection error - {e}")
        raise HTTPException(status_code=503, detail={"database": "error"})

    # Check Kafka producer connection
    try:
        # aiokafka doesn't have a simple "ping", but sending metadata request is a good check
        await kafka_producer.producer.partitions_for(settings.KAFKA_INCOMING_TOPIC)
        kafka_status = "ok"
    except KafkaConnectionError as e:
        logger.error(f"Health check failed: Kafka connection error - {e}")
        raise HTTPException(status_code=503, detail={"kafka": "error"})

    return {"status": "ok", "dependencies": {"database": db_status, "kafka": "ok"}}