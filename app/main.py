
from fastapi import FastAPI, Depends
from sqlalchemy.ext.asyncio.session import AsyncSession
from app.api.v1 import api_v1_router
from app.core.logger import info
from app.db import get_db
from app.core import settings
from sqlalchemy import text
from fastapi.middleware.cors import CORSMiddleware
from app.core.setup_logger import api_logger

app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

app.include_router(api_v1_router, prefix=settings.API_V1_STR)

info(api_logger, "FastAPI application starting...")

@app.get("/")
def health_check():
    return {"status": "ok", "message": "Application running"}

@app.get("/db-health")
async def db_health_check(db: AsyncSession = Depends(get_db)):
    try:
        result = await db.execute(text('SELECT 1'))
        _ = result.scalar()
        return {"status": "ok", "message": "Database running"}
    except Exception as e:
        return {"status": "error", "message": str(e)}