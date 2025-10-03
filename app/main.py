import logging
import os
from datetime import time, datetime
from pathlib import Path

from fastapi import FastAPI, Body
from fastapi.middleware.cors import CORSMiddleware

from app.core.logger import setup_logging
from app.services import MqttShadowService
from dotenv import load_dotenv
from app.api.v1 import api_v1_router

load_dotenv()

timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
log_file = Path(f"logs/mqtt_service-{timestamp}.log")

logger = setup_logging(
    log_level=logging.DEBUG if os.environ.get("DEBUG") == "True" else logging.INFO,
    log_file=log_file
)

app = FastAPI(
    title="mqtt_service",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

app.include_router(api_v1_router, prefix="/api/v1")
@app.get("/")
def health_check():
    return {"status": "ok", "message": "Application running"}

@app.post("/topic")
async def login(
        data: dict = Body(...),
):
    mqtt_service = MqttShadowService(data)
    result = mqtt_service.update_device_shadow()
    return {"status": "ok", "message": result}
