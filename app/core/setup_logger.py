"""
Centralized logger factory for the application
Provides separate loggers for API, Worker, and other components
"""
import logging

from app.core.logger import setup_logging

# API Logger - for FastAPI application
api_logger = setup_logging(
    log_level=logging.INFO,
    log_dir='logs',
    app_name='api',
    backup_count=30
)

# Worker Logger - for background job processing
worker_logger = setup_logging(
    log_level=logging.INFO,
    log_dir='logs',
    app_name='worker',
    backup_count=30
)

# Database Logger - for database operations (optional)
db_logger = setup_logging(
    log_level=logging.WARNING,  # Only log warnings and errors
    log_dir='logs',
    app_name='db',
    backup_count=30
)


def get_logger(name: str):
    """
    Get a logger by name

    Args:
        name: Logger name ('api', 'worker', 'database')

    Returns:
        Logger instance
    """
    loggers = {
        'api': api_logger,
        'worker': worker_logger,
        'database': db_logger
    }

    return loggers.get(name, api_logger)  # Default to api_logger


