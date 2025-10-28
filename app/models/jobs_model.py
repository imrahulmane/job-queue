from sqlalchemy.sql.functions import func
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import String, DateTime, Integer, JSON

from app.models.base_model import BaseModel


class Jobs(BaseModel):
    __tablename__ = "jobs"

    #core
    queue_name = Column(String(100), nullable=False, default="default", index=True)
    job_type = Column(String(100), nullable=False, index=True)

    #Job Data
    payload = Column(JSON, nullable=False, default=dict)

    # Job status and scheduling
    status = Column(String(20), nullable=False, default="pending", index=True)
    scheduled_at = Column(DateTime(timezone=True), nullable=False,
                          default=func.now(), index=True)

    # Retry logic
    attempts = Column(Integer, nullable=False, default=0, index=True)
    max_tries = Column(Integer, nullable=False, default=3, index=True)