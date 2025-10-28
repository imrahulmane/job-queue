import asyncio
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import text

from app.core.config import settings


async def connect_with_retry(retries=5, delay=3):
    """Create async engine with retry logic."""
    for attempt in range(retries):
        try:
            engine = create_async_engine(
                settings.async_database_url,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=10,
                echo=settings.DEBUG  # Show SQL queries in debug mode
            )
            # Test the connection
            async with engine.begin() as connection:
                await connection.execute(text("SELECT 1"))
            return engine
        except Exception as e:
            if attempt == retries - 1:
                raise
            print(f"Database connection attempt {attempt + 1} failed, retrying in {delay} seconds...")
            await asyncio.sleep(delay)


# Create async engine and session factory
engine = None
SessionLocal = None
Base = declarative_base()


async def init_database():
    """Initialize database connection."""
    global engine, SessionLocal

    if engine is None:
        engine = await connect_with_retry()
        SessionLocal = async_sessionmaker(
            bind=engine,
            class_=AsyncSession,
            expire_on_commit=False
        )
        print("Async database connection initialized")


async def close_database():
    """Close database connections."""
    global engine
    if engine:
        await engine.dispose()
        print("Database connections closed")


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency for async database sessions."""
    if SessionLocal is None:
        await init_database()

    async with SessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise