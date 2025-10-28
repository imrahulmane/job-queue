"""
Base handler class for all job handlers
Provides common functionality and interface
"""

from abc import ABC, abstractmethod
from typing import Dict, Any
from app.core.setup_logger import worker_logger


class BaseJobHandler(ABC):
    """
    Base class for all job handlers
    All handlers should inherit from this class
    """

    def __init__(self):
        self.logger = worker_logger

    @abstractmethod
    async def execute(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the job handler

        Args:
            payload: Job payload data

        Returns:
            Result dictionary
        """
        pass

    @property
    @abstractmethod
    def job_type(self) -> str:
        """Return the job type this handler processes"""
        pass