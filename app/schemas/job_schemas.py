from datetime import datetime
from typing import Dict, Any, Optional, List

from app.constants.job_types import JobTypes
from app.constants.queue_status import QueueStatus
from pydantic import BaseModel, ConfigDict, Field

class JobCreate(BaseModel):
    """Schema for Job Creation"""

    job_type: JobTypes
    payload: Dict[str, Any]
    queue_name: str
    scheduled_at: Optional[datetime] = None
    max_tries: int

class JobResponse(BaseModel):
    """Schema for job response."""
    model_config = ConfigDict(from_attributes=True)

    id: int
    job_type: JobTypes
    payload: Dict[str, Any]
    queue_name: str
    status: str
    scheduled_at: datetime
    attempts: int
    max_tries: int
    created_at: datetime
    updated_at: Optional[datetime] = None


class JobUpdate(BaseModel):
    """Schema for updating a job."""
    payload: Optional[Dict[str, Any]] = None
    scheduled_at: Optional[datetime] = None
    max_tries: Optional[int] = Field(None, ge=1, le=10)


class JobStats(BaseModel):
    """Schema for job queue statistics."""
    pending_count: int
    running_count: int
    completed_count: int
    failed_count: int
    queue_counts: Dict[str, Any]


class BulkJobCreate(BaseModel):
    """Schema for creating multiple jobs at once."""
    jobs: List[JobCreate] = Field(..., min_length=1, max_length=100, description="List of jobs to create")


class BulkJobResponse(BaseModel):
    """Schema for bulk job creation response."""
    created_jobs: List[JobResponse]
    total_created: int


class JobListQuery(BaseModel):
    """Schema for job listing query parameters."""
    queue_name: Optional[str] = None
    status: Optional[QueueStatus] = Field(None)
    job_type: Optional[JobTypes] = None
    skip: int = Field(default=0, ge=0)
    limit: int = Field(default=50, ge=1, le=1000)


# Helper functions to get enum values for validation
def get_valid_job_types() -> List[str]:
    """Get list of valid job types for validation."""
    return [job_type.value for job_type in JobTypes]


def get_valid_statuses() -> List[str]:
    """Get list of valid statuses for validation."""
    return [status.value for status in QueueStatus]