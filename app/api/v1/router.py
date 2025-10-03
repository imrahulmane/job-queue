from fastapi import APIRouter

from app.api.v1.endpoints import mqtt_shadow

router = APIRouter()

router.include_router(mqtt_shadow.router, prefix="/mqtt", tags=["mqtt"])