from fastapi import APIRouter

from app.api.v1.endpoints import tasks

router = APIRouter()

router.include_router(
    tasks.router,
    prefix="/queue",
    tags=["Job Queue"]
)