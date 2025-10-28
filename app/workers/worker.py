"""
Main Worker Class
Handles background job processing with exponential backoff polling
"""
import asyncio
import signal
import sys
from datetime import datetime
from typing import List, Optional
from zoneinfo import available_timezones

from sqlalchemy.ext.asyncio import AsyncSession

from app.db import database
from app.db.database import get_db, SessionLocal
from app.repositories import job_repository
from app.repositories.job_repository import JobRepository
from app.workers.handlers import get_handler
from app.core.setup_logger import worker_logger
from app.core.logger import info, debug, warning, error, critical


class Worker:
    """
    Background job worker with exponential backoff polling
    Processes jobs from the queue using FOR UPDATE SKIP LOCKED
    """

    def __init__(
            self,
            worker_id: str,
            queues: List[str],
            poll_interval: float = 1.0,
            max_poll_interval: float = 30.0,
            backoff_factor: float = 1.5,
            job_repository: JobRepository = JobRepository,
            max_concurrent_jobs: int = 1,
    ):
        """
        Initialize the worker

        Args:
            worker_id: Unique identifier for this worker instance
            queues: List of queue names to process
            poll_interval: Initial polling interval in seconds
            max_poll_interval: Maximum polling interval in seconds
            backoff_factor: Backoff multiplier when no jobs found
        """

        self.worker_id = worker_id
        self.queues = queues
        self.poll_interval = poll_interval
        self.max_poll_interval = max_poll_interval
        self.backoff_factor = backoff_factor
        self.job_repository = job_repository
        self.max_concurrent_jobs = max_concurrent_jobs

        #track active jobs
        self.active_jobs: set = set()

        # Current polling interval (starts at poll_interval, increases with backoff)
        self.current_poll_interval = poll_interval

        # Shutdown flag
        self.should_shutdown = False

        # Statistics
        self.jobs_processed = 0
        self.jobs_failed = 0
        self.jobs_succeeded = 0

        info(worker_logger, "Worker initialized", context={
            "worker_id": self.worker_id,
            "queues": self.queues,
            "poll_interval": self.poll_interval,
            "max_poll_interval": self.max_poll_interval,
            "backoff_factor": self.backoff_factor,
            "max_concurrent_jobs": self.max_concurrent_jobs
        })


    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""

        def signal_handler(signum, frame):
            signal_name = signal.Signals(signum).name
            warning(worker_logger, f"Received {signal_name} signal, initiating graceful shutdown...")
            self.should_shutdown = True

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

        info(worker_logger, "Signal handlers registered (SIGTERM, SIGINT)")

    async def start(self):
        """
        Start the worker and begin processing jobs
        Main entry point for the worker
        """
        info(worker_logger, "Worker starting...", context={
            "worker_id": self.worker_id,
            "queues": self.queues
        })

        try:
            # Setup signal handlers for graceful shutdown
            self.setup_signal_handlers()

            # Main processing loop
            await self._processing_loop()

        except Exception as e:
            critical(worker_logger, "Worker crashed with unexpected error", context={
                "worker_id": self.worker_id,
                "error": str(e),
                "error_type": type(e).__name__
            })
            raise
        finally:
            await self._shutdown()

    # async def _processing_loop(self):
    #     """
    #     Main processing loop
    #     Continuously polls for jobs and processes them
    #     """
    #     info(worker_logger, "Entering main processing loop")
    #
    #     while not self.should_shutdown:
    #         try:
    #             # Get database session
    #             async with database.SessionLocal() as db:
    #                 # Try to claim and process a job
    #                 job_processed = await self._process_single_job(db)
    #
    #                 if job_processed:
    #                     # Reset poll interval when job was found
    #                     self.current_poll_interval = self.poll_interval
    #                     debug(worker_logger, "Job processed, resetting poll interval", context={
    #                         "poll_interval": self.current_poll_interval
    #                     })
    #                 else:
    #                     # No job found, apply exponential backoff
    #                     await self._apply_backoff()
    #
    #         except Exception as e:
    #             error(worker_logger, "Error in processing loop", context={
    #                 "error": str(e),
    #                 "error_type": type(e).__name__
    #             })
    #             # Wait before retrying to avoid tight error loops
    #             await asyncio.sleep(self.poll_interval)
    #
    #     info(worker_logger, "Processing loop terminated")
    #
    # async def _process_single_job(self, db: AsyncSession) -> bool:
    #     """
    #     Attempt to claim and process a single job
    #
    #     Args:
    #         db: Database session
    #
    #     Returns:
    #         True if a job was processed, False if no job available
    #     """
    #     # Claim next available job
    #     job = await self.job_repository.claim_next_job(
    #         db=db,
    #         queue_names=self.queues,
    #         worker_id=self.worker_id
    #     )
    #
    #     if not job:
    #         debug(worker_logger, "No jobs available", context={
    #             "queues": self.queues,
    #             "worker_id": self.worker_id
    #         })
    #         return False
    #
    #     # Job claimed successfully
    #     info(worker_logger, "Job claimed", context={
    #         "job_id": job.id,
    #         "job_type": job.job_type,
    #         "queue_name": job.queue_name,
    #         "attempts": job.attempts,
    #         "worker_id": self.worker_id
    #     })
    #
    #     # Process the job
    #     start_time = datetime.now()
    #
    #     try:
    #         # Get handler for this job type
    #         handler = get_handler(job.job_type)
    #
    #         info(worker_logger, "Executing job handler", context={
    #             "job_id": job.id,
    #             "job_type": job.job_type,
    #             "handler": handler.__class__.__name__
    #         })
    #
    #         # Execute handler
    #         result = await handler.execute(job.payload)
    #
    #         # Mark job as completed
    #         await self.job_repository.mark_job_completed(db, job.id, result)
    #         await db.commit()
    #
    #         # Calculate duration
    #         duration = (datetime.now() - start_time).total_seconds()
    #
    #         # Update statistics
    #         self.jobs_processed += 1
    #         self.jobs_succeeded += 1
    #
    #         info(worker_logger, "Job completed successfully", context={
    #             "job_id": job.id,
    #             "job_type": job.job_type,
    #             "duration_seconds": round(duration, 2),
    #             "attempts": job.attempts,
    #             "result": result
    #         })
    #
    #         return True
    #
    #     except ValueError as e:
    #         # Handler not found or invalid job type
    #         error(worker_logger, "Invalid job type or handler not found", context={
    #             "job_id": job.id,
    #             "job_type": job.job_type,
    #             "error": str(e)
    #         })
    #
    #         # Mark as permanently failed (no retry for invalid job types)
    #         await self.job_repository.mark_job_failed(
    #             db=db,
    #             job_id=job.id,
    #             error_message=f"Invalid job type: {str(e)}",
    #             retry=False
    #         )
    #         await db.commit()
    #
    #         self.jobs_processed += 1
    #         self.jobs_failed += 1
    #
    #         return True
    #
    #     except Exception as e:
    #         # Handler execution failed
    #         duration = (datetime.now() - start_time).total_seconds()
    #
    #         error(worker_logger, "Job processing failed", context={
    #             "job_id": job.id,
    #             "job_type": job.job_type,
    #             "error": str(e),
    #             "error_type": type(e).__name__,
    #             "duration_seconds": round(duration, 2),
    #             "attempts": job.attempts
    #         })
    #
    #         # Mark job as failed (will retry if attempts < max_tries)
    #         await self.job_repository.mark_job_failed(
    #             db=db,
    #             job_id=job.id,
    #             error_message=str(e),
    #             retry=True
    #         )
    #         await db.commit()
    #
    #         self.jobs_processed += 1
    #         self.jobs_failed += 1
    #
    #         return True

    async def _processing_loop(self):
        """
        Main loop for processing jobs
        Continuously polls for jobs and process them
        """
        info(worker_logger, "Entering main processing loop...", context={
            "max_concurrent_jobs": self.max_concurrent_jobs,
        })

        while not self.should_shutdown:
            try:
                # Cleanup completed tasks to free up slots
                await self._cleanup_completed_tasks()

                # check how many slots are available
                available = self._available_slots()

                debug(worker_logger, "Loop iteration", context={
                    "active_jobs": len(self.active_jobs),
                    "available_slots": available,
                    "max_concurrent_jobs": self.max_concurrent_jobs,
                })

                if available > 0:
                    async with database.SessionLocal() as db:
                        job = await self.job_repository.claim_next_job(
                            db=db,
                            queue_names=self.queues,
                            worker_id=self.worker_id,
                        )

                        if job:
                            #Job claimed! task to process it

                            info(worker_logger, "Job claimed, creating background task", context={
                                "job_id": job.id,
                                "job_type": job.job_type,
                                "queue_name": job.queue_name,
                                "active_jobs": len(self.active_jobs),
                                "available_slots": available-1,
                            })

                            await db.commit()

                            #create background task
                            task = asyncio.create_task(
                                self.__process_job_async(job.id)
                            )

                            self.active_jobs.add(task)

                            #Resent poll interval since we've found a job
                            self.current_poll_interval = self.poll_interval

                        else:
                            #No Jobs available

                            debug(worker_logger, "No jobs available in queue", context={
                                "queues" : self.queues,
                                "active_jobs": len(self.active_jobs),
                            })

                            #Only apply exponential backoff if NO jobs are running
                            if len(self.active_jobs) == 0:
                                await self._apply_backoff()
                            else:
                                #Jobs are running check again soon
                                await asyncio.sleep(self.poll_interval)

                else:
                    #No slots are available, wait ....
                    debug(worker_logger, "All slots are occupied, waiting...", context={
                        "active_jobs": len(self.active_jobs),
                        "max_concurrent": self.max_concurrent_jobs,
                    })

                    await asyncio.sleep(self.poll_interval)
            except Exception as e:
                error(worker_logger, "Worker crashed with unexpected error", context={
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "worker_id": self.worker_id,
                    "active_jobs": len(self.active_jobs),
                })

                await asyncio.sleep(self.poll_interval)

        info(worker_logger, "Exiting main processing loop")

        if self.active_jobs:
            warning(worker_logger, f"Waiting for {len(self.active_jobs)} active jobs to complete")
            await asyncio.wait(self.active_jobs, timeout=60)

            remaining_jobs = [task for task in self.active_jobs if not task.done()]
            if remaining_jobs:
                warning(worker_logger, f"Forcefully cancelling {len(remaining_jobs)} remaining jobs after timeout")
                for task in remaining_jobs:
                    task.cancel()

    async def __process_job_async(self, job_id: int):
        """
        Process a single job asynchronously as background task

        This method is run as an asyncio task for concurrent processing.
        It handles the complete job lifecycle. execute handler, mark complete/failed

        Args:
             :param job_id: ID of the job to process

        """

        start_time = datetime.now()
        job = None

        try:
            async with database.SessionLocal() as db:
                job = await self.job_repository.get(db, job_id)

                if not job:
                    warning(worker_logger, "Job not found", context={
                        "job_id": job_id
                    })
                    return

                #get handler for the job
                try:
                    handler = get_handler(job.job_type)
                except ValueError as e:
                    #invalid job type - mark permanantly failed
                    error(worker_logger, "Invalid job type or handler not found", context={
                        "job_id": job_id,
                        "job_type": job.job_type,
                        "error": str(e),
                    })

                    await self.job_repository.mark_job_failed(
                        db=db,
                        job_id=job_id,
                        error_message=f"Invalid job type: {str(e)}",
                        retry=False
                    )

                    await db.commit()

                    self.jobs_failed += 1
                    return

            #execute the handler
            info(worker_logger, "Processing job", context={
                "job_id": job_id,
                "job_type": job.job_type,
                "handler": handler.__class__.__name__,
                "active_jobs": len(self.active_jobs),
            })

            try:
                result = await handler.execute(job.payload)

                #Mark job as completed
                await self.job_repository.mark_job_completed(db, job_id, result)
                await db.commit()

                duration = (datetime.now() - start_time).total_seconds()

                self.jobs_succeeded += 1

                info(worker_logger, "Job completed successfully", context={
                    "job_id": job_id,
                    "job_type": job.job_type,
                    "duration_seconds": round(duration, 2),
                    "attempts": job.attempts,
                    "active_jobs": len(self.active_jobs)-1,
                    "result": result
                })
            except Exception as e:
                #handler execution failed
                duration = (datetime.now() - start_time).total_seconds()

                error(worker_logger, "Job processing failed", context={
                    "job_id": job_id,
                    "job_type" : job.job_type,
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "duration_seconds": round(duration, 2),
                    "attempts": job.attempts
                })

                await self.job_repository.mark_job_failed(
                    db=db,
                    job_id=job_id,
                    error_message=str(e),
                    retry=True  #we will try with exponential backoff
                )

                await db.commit()
                self.jobs_failed += 1
        except Exception as e:
            error(worker_logger, "Unexpected error in job task", context={
                "job_id": job_id,
                "error": str(e),
                "error_type": type(e).__name__,
            })

            self.jobs_failed += 1
        finally:
            self.jobs_processed += 1


    async def _apply_backoff(self):
        """
        Apply exponential backoff when no jobs are available
        Gradually increases wait time up to max_poll_interval
        """
        old_interval = self.current_poll_interval

        # Increase poll interval with backoff factor
        self.current_poll_interval = min(
            self.current_poll_interval * self.backoff_factor,
            self.max_poll_interval
        )

        debug(worker_logger, "Applying backoff", context={
            "old_interval": round(old_interval, 2),
            "new_interval": round(self.current_poll_interval, 2),
            "max_interval": self.max_poll_interval
        })

        # Wait for current poll interval
        await asyncio.sleep(self.current_poll_interval)

    async def _shutdown(self):
        """
        Perform graceful shutdown
        Log final statistics and cleanup
        """
        warning(worker_logger, "Worker shutting down...", context={
            "worker_id": self.worker_id
        })

        # Log final statistics
        info(worker_logger, "Worker statistics", context={
            "worker_id": self.worker_id,
            "jobs_processed": self.jobs_processed,
            "jobs_succeeded": self.jobs_succeeded,
            "jobs_failed": self.jobs_failed,
            "success_rate": f"{(self.jobs_succeeded / self.jobs_processed * 100) if self.jobs_processed > 0 else 0:.2f}%"
        })

        info(worker_logger, "Worker stopped gracefully", context={
            "worker_id": self.worker_id
        })

    async def stop(self):
        """
        Stop the worker gracefully
        Can be called programmatically to stop the worker
        """
        warning(worker_logger, "Stop requested", context={
            "worker_id": self.worker_id
        })
        self.should_shutdown = True

    def _available_slots(self) -> int:
        """
        Calculates how many job slots are available
        :return:
            Number of slots available (0 if all slots are full)
        """
        available = self.max_concurrent_jobs - len(self.active_jobs)
        return available

    async def _cleanup_completed_tasks(self):
        """
        Find and remove completed jobs from active_jobs set
        This frees up slots for jobs
        """
        completed = {task for task in self.active_jobs if task.done()}

        if completed:
            debug(worker_logger, f"Cleaning up {len(completed)} completed tasks", context={
                "completed_count": len(completed),
                "remaining_active": len(self.active_jobs) - len(completed)
            })

            for task in completed:
                try:
                    # This will raise exception if task failed
                    await task
                except Exception as e:
                    pass

            # Remove completed tasks from active set
            self.active_jobs -= completed











