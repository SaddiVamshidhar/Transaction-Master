import logging
from pythonjsonlogger import jsonlogger
from app.core.config import settings

def setup_logging():
    """Sets up structured JSON logging."""
    log_handler = logging.StreamHandler()
    formatter = jsonlogger.JsonFormatter(
        '%(asctime)s %(name)s %(levelname)s %(message)s'
    )
    log_handler.setFormatter(formatter)
    
    root_logger = logging.getLogger()
    # Clear any existing handlers to avoid duplicate logs
    if root_logger.hasHandlers():
        root_logger.handlers.clear()
        
    root_logger.addHandler(log_handler)
    root_logger.setLevel(settings.LOG_LEVEL.upper())

    # Suppress verbose logs from third-party libraries
    logging.getLogger("uvicorn").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)