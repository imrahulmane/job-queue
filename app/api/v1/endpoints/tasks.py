
from typing import List, Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.repositories.job_repository import JobRepository
from app.schemas.job_schemas import (
    JobCreate,
    JobResponse,
    JobUpdate,
    JobStats,
    BulkJobCreate,
    BulkJobResponse
)
from app.constants.queue_status import QueueStatus
from app.constants.job_types import JobTypes
from app.services.job_service import JobService

router = APIRouter()

repo = JobRepository()
service = JobService(repo)

def get_service() -> JobService:
    return service

@router.post("/jobs", response_model=JobResponse, status_code=201)
async def create_job(
        job_data: JobCreate,
        db: AsyncSession = Depends(get_db),
        svc: JobService = Depends(get_service)
):
    return await JobService.create_job(svc, job_data, db)


@router.post("/jobs/bulk", response_model=BulkJobResponse, status_code=201)
async def create_jobs_bulk(
        bulk_data: BulkJobCreate,
        db: AsyncSession = Depends(get_db),
        svc: JobService = Depends(get_service)
):
    return await JobService.create_jobs_bulk(svc, bulk_data, db)

@router.get("/jobs", response_model=List[JobResponse])
async def list_jobs(
        queue_name: Optional[str] = Query(None, description="Filter by queue name"),
        status: Optional[QueueStatus] = Query(None, description="Filter by job status"),
        job_type: Optional[JobTypes] = Query(None, description="Filter by job type"),
        skip: int = Query(0, ge=0, description="Number of jobs to skip"),
        limit: int = Query(50, ge=1, le=1000, description="Maximum jobs to return"),
        db: AsyncSession = Depends(get_db),
        svc: JobService = Depends(get_service)
):
    return await JobService.list_jobs(svc, queue_name, status, job_type, skip, limit, db)


@router.get("/jobs/{job_id}", response_model=JobResponse)
async def get_job(
        job_id: int,
        db: AsyncSession = Depends(get_db),
        svc: JobService = Depends(get_service)
):
    return await JobService.get_job(svc, job_id, db)

@router.put("/jobs/{job_id}", response_model=JobResponse)
async def update_job(
        job_id: int,
        job_update: JobUpdate,
        db: AsyncSession = Depends(get_db),
        svc: JobService = Depends(get_service)
):
    return await JobService.update_job(svc, job_id, job_update, db)

@router.delete("/jobs/{job_id}")
async def cancel_job(
        job_id: int,
        db: AsyncSession = Depends(get_db),
        svc: JobService = Depends(get_service)
):
    return await JobService.cancel_job(svc, job_id, db)

@router.post("/jobs/{job_id}/retry", response_model=JobResponse)
async def retry_job(
        job_id: int,
        reset_attempts: bool = Query(False, description="Reset attempt count to 0"),
        svc: JobService = Depends(get_service),
        db: AsyncSession = Depends(get_db),
):
    return await JobService.retry_job(svc, job_id, reset_attempts, db)

@router.get("/jobs/stats/overview", response_model=JobStats)
async def get_job_stats(
        db: AsyncSession = Depends(get_db),
        svc: JobService = Depends(get_service)
):
    return await JobService.get_job_stats(svc, db)


@router.get("/jobs/queue/{queue_name}", response_model=List[JobResponse])
async def get_jobs_by_queue(
        queue_name: str,
        status: Optional[QueueStatus] = Query(None, description="Filter by status"),
        limit: int = Query(50, ge=1, le=1000),
        db: AsyncSession = Depends(get_db),
        svc: JobService = Depends(get_service)
):
    return await JobService.get_jobs_by_queue(svc, queue_name, status, limit, db)

# Health check endpoints
@router.get("/jobs/health/pending-count")
async def get_pending_jobs_count(
        queue_name: Optional[str] = Query(None),
        db: AsyncSession = Depends(get_db),
        svc: JobService = Depends(get_service)
):
    return await JobService.get_pending_jobs_count(svc, queue_name, db)


@router.get("/jobs/health/running-count")
async def get_running_jobs_count(
        queue_name: Optional[str] = Query(None),
        db: AsyncSession = Depends(get_db),
        svc: JobService = Depends(get_service)
):
    return JobService.get_running_jobs_count(svc, queue_name, db)


# Admin endpoints
@router.post("/admin/jobs/reset-stale")
async def reset_stale_jobs(
        timeout_minutes: int = Query(30, ge=1, description="Minutes after which running jobs are considered stale"),
        db: AsyncSession = Depends(get_db),
        svc: JobService = Depends(get_service)
):
    return JobService.reset_stale_jobs(svc, timeout_minutes, db)

@router.delete("/admin/jobs/cleanup")
async def cleanup_completed_jobs(
        older_than_days: int = Query(7, ge=1, description="Delete completed jobs older than this many days"),
        db: AsyncSession = Depends(get_db),
        svc: JobService = Depends(get_service)
):
    return JobService.cleanup_completed_jobs(svc, older_than_days, db)