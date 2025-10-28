from typing import Optional
from sqlalchemy.ext.asyncio.session import AsyncSession
from app.constants.job_types import JobTypes
from app.constants.queue_status import QueueStatus
from app.core import db_logger
from app.core.logger import error
from app.db import get_db
from app.repositories.job_repository import JobRepository
from fastapi import  Depends, HTTPException, Query

from app.schemas import JobCreate, JobResponse, BulkJobCreate, BulkJobResponse, JobUpdate, JobStats


class JobService:
    def __init__(self, repo: JobRepository):
        self.repo = repo

    async def create_job(
            self,
            job_data: JobCreate,
            db: AsyncSession = Depends(get_db)
    ):
        try:
            job = await self.repo.enqueue_job(
                db,
                job_type=job_data.job_type,
                payload=job_data.payload,
                queue_name=job_data.queue_name,
                scheduled_at=job_data.scheduled_at,
                max_tries=job_data.max_tries
            )

            await db.commit()
            return JobResponse.model_validate(job)
        except Exception as e:
            await db.rollback()
            raise HTTPException(status_code=500, detail=f"Failed to create job: {str(e)}")

    async def create_jobs_bulk(
            self,
            bulk_data: BulkJobCreate,
            db: AsyncSession = Depends(get_db)
    ):
        """
           Create multiple jobs at once.

           Useful for batch operations like sending multiple emails.
           """

        try:
            jobs = []
            for job_data in bulk_data.jobs:
                job = await self.repo.enqueue_job(
                db,
                job_type=job_data.job_type,
                payload=job_data.payload,
                queue_name=job_data.queue_name,
                scheduled_at=job_data.scheduled_at,
                max_tries=job_data.max_tries
            )
                jobs.append(job)
            await db.commit()
            return BulkJobResponse(
                created_jobs=[JobResponse.model_validate(job) for job in jobs],
                total_created=len(jobs),
            )
        except Exception as e:
            await db.rollback()
            raise HTTPException(status_code=500, detail=f"Failed to create jobs: {str(e)}")


    async def list_jobs(
            self,
            queue_name: Optional[str],
            status: Optional[QueueStatus],
            job_type: Optional[JobTypes],
            skip: int,
            limit: int,
            db: AsyncSession = Depends(get_db)
    ):
        """
           List jobs with optional filtering.

           Filter by queue, status, or job type. Supports pagination.
           """

        try:
            conditions={}
            if queue_name:
                conditions["queue_name"] = queue_name
            if status:
                conditions["status"] = status.value
            if job_type:
                conditions["job_type"] = job_type.value


            if conditions:
                jobs = await self.repo.get_by_condition(
                    db,
                    conditions,
                    limit=limit,
                )

                jobs = jobs[skip:skip + limit if skip > 0 else jobs]
            else:
                jobs = await self.repo.get_paginated(
                    db,
                    skip=skip,
                    limit=limit
                )

            return [JobResponse.model_validate(job) for job in jobs]
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to list jobs: {str(e)}")

    async def get_job(
            self,
            job_id: int,
            db: AsyncSession = Depends(get_db)
    ):
        """
        Get a specific job by ID.
        """
        job = await self.repo.get(db, job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        return JobResponse.model_validate(job)

    async def update_job(
            self,
            job_id: int,
            job_update: JobUpdate,
            db: AsyncSession = Depends(get_db)
    ):
        """
        Update a job's properties.

        Can update payload, schedule, or retry count.
        NOTE: Status cannot be updated here - use specific action endpoints like /cancel or /retry.
        """
        try:
            # Convert to dict and remove None values
            update_data = job_update.model_dump(exclude_unset=True)

            # Additional safety check - ensure status is never updated via this endpoint
            if "status" in update_data:
                raise HTTPException(
                    status_code=400,
                    detail="Cannot update job status via this endpoint. Use /cancel or /retry endpoints."
                )

            job = await self.repo.update(db, id=job_id, obj_in=update_data)
            if not job:
                raise HTTPException(status_code=404, detail="Job not found")

            await db.commit()
            return JobResponse.model_validate(job)
        except HTTPException:
            raise
        except Exception as e:
            await db.rollback()
            raise HTTPException(status_code=500, detail=f"Failed to update job: {str(e)}")


    async def cancel_job(
            self,
            job_id: int,
            db: AsyncSession = Depends(get_db)
    ):
        """
        Cancel a job (specific action endpoint).

        This is a SPECIFIC ACTION that safely transitions job state.
        Sets status to 'cancelled' and marks as deleted.
        Only works on 'pending' jobs to avoid conflicts with running workers.
        """
        try:
            # Get the job first to check its current status
            job = await (self.repo.get(db, job_id))
            if not job:
                raise HTTPException(status_code=404, detail="Job not found")

            # Only allow canceling pending jobs to avoid worker conflicts
            if job.status not in [QueueStatus.pending.value]:
                raise HTTPException(
                    status_code=400,
                    detail=f"Cannot cancel job with status '{job.status}'. Only 'pending' jobs can be cancelled."
                )

            # Update status to cancelled (this is safe because job is pending)
            job = await self.repo.update(
                db,
                id=job_id,
                obj_in={"status": QueueStatus.cancelled.value}
            )

            # Then soft delete
            success = await self.repo.delete(db, id=job_id)
            if not success:
                raise HTTPException(status_code=500, detail="Failed to cancel job")

            await db.commit()
            return {"message": f"Job {job_id} cancelled successfully"}
        except HTTPException:
            raise
        except Exception as e:
            await db.rollback()
            raise HTTPException(status_code=500, detail=f"Failed to cancel job: {str(e)}")


    async def retry_job(
            self,
            job_id: int,
            reset_attempts: bool,
            db: AsyncSession = Depends(get_db)
    ):
        """
        Manually retry a failed job.

        Resets the job status to 'pending' and optionally resets the attempt counter.
        """
        try:
            job = await self.repo.retry_failed_job(
                db,
                job_id=job_id,
                reset_attempts=reset_attempts
            )
            if not job:
                raise HTTPException(
                    status_code=404,
                    detail="Job not found or not in failed status"
                )

            await db.commit()
            return JobResponse.model_validate(job)
        except HTTPException:
            raise
        except Exception as e:
            await db.rollback()
            raise HTTPException(status_code=500, detail=f"Failed to retry job: {str(e)}")


    async def get_job_stats(
            self,
            db: AsyncSession = Depends(get_db)
    ):
        """
        Get overall job queue statistics.

        Returns counts by status and by queue.
        """
        try:
            stats = await self.repo.get_job_stats(db)

            return JobStats(**stats)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to get job stats: {str(e)}")


    async def get_jobs_by_queue(
            self,
            queue_name: str,
            status: Optional[QueueStatus],
            limit: int,
            db: AsyncSession = Depends(get_db)
    ):
        """
        Get jobs from a specific queue.

        Optionally filter by status within that queue.
        """
        try:
            status_value = status.value if status else None
            jobs = await self.repo.get_jobs_by_queue(
                db,
                queue_name=queue_name,
                status=status_value,
                limit=limit
            )
            return [JobResponse.model_validate(job) for job in jobs]
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to get jobs from queue: {str(e)}")


    async def get_pending_jobs_count(
            self,
            queue_name: Optional[str],
            db: AsyncSession = Depends(get_db)
    ):
        """
        Get count of pending jobs for monitoring.
        """
        try:
            count = await self.repo.get_pending_jobs_count(db, queue_name)
            return {"pending_jobs": count, "queue": queue_name or "all"}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to get pending jobs count: {str(e)}")


    async def get_running_jobs_count(
            self,
            queue_name: Optional[str],
            db: AsyncSession = Depends(get_db)
    ):
        """
        Get count of running jobs for monitoring.
        """
        try:
            count = await self.repo.get_running_jobs_count(db, queue_name)
            return {"running_jobs": count, "queue": queue_name or "all"}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to get running jobs count: {str(e)}")


    # Admin endpoints
    async def reset_stale_jobs(
            self,
            timeout_minutes: int,
            db: AsyncSession = Depends(get_db)
    ):
        """
        Admin endpoint: Reset stale running jobs back to pending.

        Useful for recovering from worker crashes.
        """
        try:
            count = await self.repo.reset_stale_jobs(db, timeout_minutes)
            await db.commit()
            return {"message": f"Reset {count} stale jobs"}
        except Exception as e:
            await db.rollback()
            raise HTTPException(status_code=500, detail=f"Failed to reset stale jobs: {str(e)}")


    async def cleanup_completed_jobs(
            self,
            older_than_days: int,
            db: AsyncSession = Depends(get_db)
    ):
        """
        Admin endpoint: Clean up old completed jobs.

        Permanently deletes completed jobs older than the specified days.
        """
        try:
            count = await self.repo.cleanup_completed_jobs(db, older_than_days)
            await db.commit()
            return {"message": f"Deleted {count} completed jobs older than {older_than_days} days"}
        except Exception as e:
            await db.rollback()
            raise HTTPException(status_code=500, detail=f"Failed to cleanup jobs: {str(e)}")