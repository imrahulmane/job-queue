"""
Job Handler Registry
Maps job_type to handler instances
"""

from typing import Dict, Callable
from app.workers.job_handlers import EmailHandler, ReportHandler, ImageHandler
from app.workers.job_handlers.base_handler import BaseJobHandler
from app.core.setup_logger import worker_logger
from app.core.logger import info

# Initialize handler instances
email_handler = EmailHandler()
report_handler = ReportHandler()
image_handler = ImageHandler()

# Handler Registry - maps job_type string to handler instance
HANDLERS: Dict[str, BaseJobHandler] = {
    email_handler.job_type: email_handler,
    report_handler.job_type: report_handler,
    image_handler.job_type: image_handler,
}


def get_handler(job_type: str) -> BaseJobHandler:
    """
    Get handler instance for a given job type

    Args:
        job_type: The type of job (e.g., 'emails', 'report_generation')

    Returns:
        Handler instance

    Raises:
        ValueError: If job_type is not registered
    """
    handler = HANDLERS.get(job_type)

    if not handler:
        available_types = ", ".join(HANDLERS.keys())
        raise ValueError(
            f"No handler registered for job_type: '{job_type}'. "
            f"Available types: {available_types}"
        )

    return handler


def register_handler(handler: BaseJobHandler) -> None:
    """
    Register a new handler

    Args:
        handler: Handler instance (must inherit from BaseJobHandler)
    """
    if not isinstance(handler, BaseJobHandler):
        raise TypeError("Handler must inherit from BaseJobHandler")

    job_type = handler.job_type
    info(worker_logger, f"Registering handler for job_type: {job_type}")
    HANDLERS[job_type] = handler


def list_handlers() -> list:
    """Get list of all registered job types"""
    return list(HANDLERS.keys())