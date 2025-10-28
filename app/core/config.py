from functools import lru_cache
from typing import List
from pydantic_settings import BaseSettings
from pydantic import ConfigDict


class Settings(BaseSettings):
    API_V1_STR: str = "/api/v1"
    VERSION: str = "0.1.0"
    PROJECT_NAME: str = "job_queue"

    # Security
    SECRET_KEY: str
    DEBUG: bool = False

    # Database settings
    DB_URL: str

    # Worker Configuration
    POLL_INTERVAL: float = 1.0
    MAX_POLL_INTERVAL: float = 30.0
    BACKOFF_FACTOR: float = 1.5
    MAX_CONCURRENT_JOBS: int = 3  # ← Fixed: Added 'S'
    WORKER_QUEUES: str = "default"

    # Application
    ENVIRONMENT: str = "development"

    # CORS settings
    CORS_ORIGINS: List[str] = ["*"]
    CORS_METHODS: List[str] = ["*"]
    CORS_HEADERS: List[str] = ["*"]
    ALLOW_CREDENTIALS: bool = True

    model_config = ConfigDict(
        env_file=".env",
        case_sensitive=False
    )

    @property
    def database_url(self) -> str:
        return f"postgresql://{self.DB_URL}"

    @property
    def async_database_url(self) -> str:
        """Async version for asyncpg/worker processes."""
        return f"postgresql+asyncpg://{self.DB_URL}"


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()