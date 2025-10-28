from typing import List, Optional, Dict, Any
from datetime import datetime, timezone, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, and_, text
from sqlalchemy.exc import SQLAlchemyError

from app.constants.job_types import JobTypes
from app.constants.queue_status import QueueStatus
from app.repositories.base_repository import AsyncBaseRepository
from app.models.jobs_model import Jobs
from app.schemas.job_schemas import get_valid_statuses


class JobRepository(AsyncBaseRepository[Jobs]):
    def __init__(self):
        super().__init__(Jobs)

    async def enqueue_job(
            self,
            db: AsyncSession,
            *,
            job_type: JobTypes,
            payload: Dict[str, Any],
            queue_name: str = "default",
            scheduled_at: Optional[datetime] = None,
            max_tries: int = 3
    ) -> Jobs:
        """
        Enqueue a new job.
        """
        if scheduled_at is None:
            scheduled_at = datetime.now(timezone.utc)

        job_data = {
            "job_type": job_type.value,
            "payload": payload,
            "queue_name": queue_name,
            "status": QueueStatus.pending.value,
            "scheduled_at": scheduled_at,
            "attempts": 0,
            "max_tries": max_tries
        }

        return await self.create(db, obj_in=job_data)

    async def claim_next_job(
            self,
            db: AsyncSession,
            queue_names: List[str] = None,
            worker_id: str = None
    ) -> Optional[Jobs]:
        """
        Claim the next available job using FOR UPDATE SKIP LOCKED.
        This ensures atomic job claiming without conflicts.
        """
        try:
            if queue_names is None:
                queue_names = ["default"]

            # Build the query with FOR UPDATE SKIP LOCKED
            query = text("""
                UPDATE jobs 
                SET status = :running_status, 
                    attempts = attempts + 1,
                    updated_at = :updated_at
                WHERE id = (
                    SELECT id FROM jobs 
                    WHERE status = :pending_status
                    AND queue_name = ANY(:queue_names)
                    AND scheduled_at <= :now
                    AND (is_deleted = false OR is_deleted IS NULL)
                    ORDER BY scheduled_at ASC, id ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                RETURNING *;
            """)

            result = await db.execute(query, {
                "pending_status": QueueStatus.pending.value,
                "running_status": QueueStatus.running.value,
                "queue_names": queue_names,
                "now": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc)
            })

            row = result.fetchone()
            if row:
                # Convert row to Jobs object
                job = Jobs()
                for key, value in row._mapping.items():
                    setattr(job, key, value)
                return job

            return None

        except SQLAlchemyError as e:
            await db.rollback()
            raise

    async def mark_job_completed(
            self,
            db: AsyncSession,
            job_id: int,
            result_data: Optional[Dict[str, Any]] = None
    ) -> Optional[Jobs]:
        """
        Mark a job as completed.
        """
        update_data = {
            "status": QueueStatus.completed.value,
            "updated_at": datetime.now(timezone.utc)
        }

        # Optionally store result data in payload
        if result_data:
            job = await self.get(db, job_id)
            if job and job.payload:
                job.payload.update({"result": result_data})
                update_data["payload"] = job.payload

        return await self.update(db, id=job_id, obj_in=update_data)

    async def mark_job_failed(
            self,
            db: AsyncSession,
            job_id: int,
            error_message: str,
            retry: bool = True
    ) -> Optional[Jobs]:
        """
        Mark a job as failed and potentially retry it.
        """
        job = await self.get(db, job_id)
        if not job:
            return None

        # Check if we should retry
        should_retry = retry and job.attempts < job.max_tries

        update_data = {
            "status": QueueStatus.pending.value if should_retry else QueueStatus.failed.value,
            "updated_at": datetime.now(timezone.utc)
        }

        # Add error info to payload
        if job.payload:
            if "errors" not in job.payload:
                job.payload["errors"] = []
            job.payload["errors"].append({
                "attempt": job.attempts,
                "error": error_message,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
            update_data["payload"] = job.payload

        # If retrying, schedule for later (exponential backoff)
        if should_retry:
            delay_minutes = 2 ** (job.attempts - 1)  # 1, 2, 4, 8 minutes...
            update_data["scheduled_at"] = datetime.now(timezone.utc) + timedelta(minutes=delay_minutes)

        return await self.update(db, id=job_id, obj_in=update_data)

    async def get_jobs_by_status(
            self,
            db: AsyncSession,
            status: str,
            queue_name: Optional[str] = None,
            limit: int = 100
    ) -> List[Jobs]:
        """
        Get jobs by status and optionally by queue.
        """
        condition = {"status": status}
        if queue_name:
            condition["queue_name"] = queue_name

        return await self.get_by_condition(db, condition, limit=limit)

    async def get_jobs_by_queue(
            self,
            db: AsyncSession,
            queue_name: str,
            status: Optional[str] = None,
            limit: int = 100
    ) -> List[Jobs]:
        """
        Get jobs by queue and optionally by status.
        """
        condition = {"queue_name": queue_name}
        if status:
            condition["status"] = status

        return await self.get_by_condition(db, condition, limit=limit)

    async def get_pending_jobs_count(
            self,
            db: AsyncSession,
            queue_name: Optional[str] = None
    ) -> int:
        """
        Get count of pending jobs.
        """
        condition = {"status": QueueStatus.pending.value}
        if queue_name:
            condition["queue_name"] = queue_name

        return await self.count(db, condition)

    async def get_running_jobs_count(
            self,
            db: AsyncSession,
            queue_name: Optional[str] = None
    ) -> int:
        """
        Get count of running jobs.
        """
        condition = {"status": QueueStatus.running.value}
        if queue_name:
            condition["queue_name"] = queue_name

        return await self.count(db, condition)

    async def reset_stale_jobs(
            self,
            db: AsyncSession,
            stale_timeout_minutes: int = 30
    ) -> int:
        """
        Reset jobs that have been running too long back to pending.
        This handles cases where workers crash or jobs hang.
        """
        try:
            cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=stale_timeout_minutes)

            count = await self.bulk_update(
                db,
                condition={
                    "status": QueueStatus.running.value,
                },
                values={
                    "status": QueueStatus.pending.value,
                    "scheduled_at": datetime.now(timezone.utc)
                }
            )

            # Additional SQL to only reset jobs older than cutoff
            stmt = update(Jobs).where(
                and_(
                    Jobs.status == QueueStatus.pending.value,  # Just updated to pending
                    Jobs.updated_at < cutoff_time  # But were updated before cutoff
                )
            ).values(status=QueueStatus.pending.value)

            result = await db.execute(stmt)
            return result.rowcount

        except SQLAlchemyError as e:
            await db.rollback()
            raise

    async def get_job_stats(self, db: AsyncSession) -> Dict[str, Any]:
        """
        Get overall job queue statistics.
        """
        stats = {}

        # Count by status
        for status in get_valid_statuses():
            stats[f"{status}_count"] = await self.count(db, {"status": status})

        # Count by queue
        stmt = select(Jobs.queue_name, text("COUNT(*) as count")).group_by(Jobs.queue_name)
        if hasattr(Jobs, 'is_deleted'):
            stmt = stmt.where(Jobs.is_deleted == False)

        result = await db.execute(stmt)

        queue_stats = {row[0]: row[1] for row in result}
        stats["queue_counts"] = queue_stats

        return stats

    async def cleanup_completed_jobs(
            self,
            db: AsyncSession,
            older_than_days: int = 7
    ) -> int:
        """
        Clean up completed jobs older than specified days.
        """
        try:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=older_than_days)

            # Use hard delete for completed jobs
            stmt = select(Jobs).where(
                and_(
                    Jobs.status == QueueStatus.completed.value,
                    Jobs.updated_at < cutoff_date
                )
            )

            result = await db.execute(stmt)
            jobs_to_delete = list(result.scalars().all())

            count = 0
            for job in jobs_to_delete:
                await db.delete(job)
                count += 1

            await db.flush()
            return count

        except SQLAlchemyError as e:
            await db.rollback()
            raise

    async def retry_failed_job(
            self,
            db: AsyncSession,
            job_id: int,
            reset_attempts: bool = False
    ) -> Optional[Jobs]:
        """
        Manually retry a failed job.
        """
        job = await self.get(db, job_id)
        if not job or job.status != QueueStatus.failed.value:
            return None

        update_data = {
            "status": QueueStatus.pending.value,
            "scheduled_at": datetime.now(timezone.utc)
        }

        if reset_attempts:
            update_data["attempts"] = 0

        return await self.update(db, id=job_id, obj_in=update_data)

