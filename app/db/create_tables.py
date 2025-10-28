# app/db/create_tables.py
"""
Simple script to create database tables.
Run this once to set up the database schema.
"""
import asyncio

from sqlalchemy.ext.asyncio.engine import create_async_engine

from app.core import settings
from app.db.database import init_database, Base

async def create_tables():
    engine = create_async_engine(
        settings.async_database_url,
        pool_pre_ping=True,
        echo=True  # Show SQL commands
    )

    """Create all database tables."""
    await init_database()

    print("Creating database tables...")

    # Create all tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    print("Database tables created successfully!")


if __name__ == "__main__":
    asyncio.run(create_tables())