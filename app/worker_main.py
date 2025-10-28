

"""
Worker Entry Point
This is the main entry point for the background job worker
Run with: python -m app.worker_main
"""

import asyncio
import os
import sys
import socket
from app.db import database
from app.repositories.job_repository import JobRepository
from app.workers.worker import Worker
from app.core.setup_logger import worker_logger
from app.core.logger import info, critical

repo = JobRepository()

def get_job_repository() -> JobRepository:
    return repo


def load_config() -> dict:
    """
    Load worker configuration from environment variables

    Returns:
        Configuration dictionary
    """
    config = {
        "worker_id": os.getenv("WORKER_ID", socket.gethostname()),
        "poll_interval": float(os.getenv("POLL_INTERVAL", "1.0")),
        "max_poll_interval": float(os.getenv("MAX_POLL_INTERVAL", "30.0")),
        "backoff_factor": float(os.getenv("BACKOFF_FACTOR", "1.5")),
        "max_concurrent_jobs": int(os.getenv("MAX_CONCURRENT_JOBS", "3")),
        "queues": os.getenv("WORKER_QUEUES", "default").split(","),
    }

    # Clean up queue names (remove whitespace)
    config["queues"] = [q.strip() for q in config["queues"]]

    info(worker_logger, "Configuration loaded", context=config)

    return config


async def main():
    """
    Main function to start the worker
    """
    info(worker_logger, "Worker process starting...")

    try:
        # Load configuration
        config = load_config()

        # Initialize database
        info(worker_logger, "Initializing database connection...")
        await database.init_database()
        info(worker_logger, "Database connection initialized")

        # Create worker instance
        worker = Worker(
            worker_id=config["worker_id"],
            queues=config["queues"],
            poll_interval=config["poll_interval"],
            max_poll_interval=config["max_poll_interval"],
            backoff_factor=config["backoff_factor"],
            job_repository=get_job_repository(),
            max_concurrent_jobs=config["max_concurrent_jobs"],
        )

        info(worker_logger, "Worker created successfully", context={
            "worker_id": config["worker_id"],
            "queues": config["queues"],
            "max_concurrent_jobs": config["max_concurrent_jobs"],
        })

        # Start the worker (this blocks until shutdown)
        await worker.start()

    except KeyboardInterrupt:
        info(worker_logger, "Worker interrupted by user (Ctrl+C)")

    except Exception as e:
        critical(worker_logger, "Worker failed to start", context={
            "error": str(e),
            "error_type": type(e).__name__
        })
        sys.exit(1)

    info(worker_logger, "Worker process terminated")


if __name__ == "__main__":
    """
    Entry point when running: python -m app.worker_main
    or: python app/worker_main.py
    """
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        info(worker_logger, "Worker stopped by user")
    except Exception as e:
        critical(worker_logger, "Fatal error", context={
            "error": str(e)
        })
        sys.exit(1)