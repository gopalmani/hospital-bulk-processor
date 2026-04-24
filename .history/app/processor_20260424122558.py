import asyncio
import uuid
import time
import logging
import httpx
from typing import List

from .store import jobs
from .models import JobStatus, HospitalResult, JobStatusEnum
from .hospital_client import create_hospital, activate_batch, delete_batch
from .config import CONCURRENT_REQUESTS

logger = logging.getLogger(__name__)


async def process_bulk(rows: List, batch_id: str) -> JobStatus:
    """
    Process bulk hospital creation in background.
    Updates job state incrementally as rows complete.
    """
    job = jobs[batch_id]

    logger.info(f"Starting bulk processing for batch {batch_id} with {len(rows)} hospitals")
    job.status = JobStatusEnum.PROCESSING

    sem = asyncio.Semaphore(CONCURRENT_REQUESTS)
    start = time.time()

    async with httpx.AsyncClient() as client:

        async def worker(index: int, row) -> HospitalResult:
            """Process a single hospital row with concurrency control."""
            async with sem:
                try:
                    data = await create_hospital(client, row, batch_id)
                    hospital_id = data.get("id")

                    result = HospitalResult(
                        row=index + 1,
                        hospital_id=hospital_id,
                        name=row.name,
                        status="created"
                    )

                    # Thread-safe update of job state
                    job.processed_hospitals += 1
                    job.hospitals.append(result)
                    logger.info(f"Row {index + 1}: Created hospital '{row.name}' (ID: {hospital_id})")
                    return result

                except Exception as e:
                    job.failed_hospitals += 1
                    error_msg = str(e)
                    result = HospitalResult(
                        row=index + 1,
                        name=row.name,
                        status="failed",
                        error=error_msg
                    )
                    job.hospitals.append(result)
                    logger.error(f"Row {index + 1}: Failed to create '{row.name}': {error_msg}")
                    return result

        # Process all rows concurrently with semaphore limiting
        await asyncio.gather(
            *(worker(i, row) for i, row in enumerate(rows))
        )

        # Check if all rows succeeded
        if job.failed_hospitals == 0:
            logger.info(f"All {job.total_hospitals} hospitals created successfully, activating batch")
            try:
                await activate_batch(client, batch_id)
                job.batch_activated = True
                job.status = JobStatusEnum.COMPLETED

                # Update hospital statuses to reflect activation
                for h in job.hospitals:
                    h.status = "created_and_activated"

                logger.info(f"Batch {batch_id} activated successfully")

            except Exception as e:
                logger.error(f"Failed to activate batch {batch_id}: {e}")
                # Rollback on activation failure
                await rollback_batch(client, batch_id, job)

        else:
            logger.warning(f"Batch {batch_id} has {job.failed_hospitals} failed rows, initiating rollback")
            await rollback_batch(client, batch_id, job)

    # Record processing time
    elapsed = round(time.time() - start, 2)
    job.processing_time_seconds = elapsed
    logger.info(f"Batch {batch_id} processing completed in {elapsed}s")

    return job


async def rollback_batch(client: httpx.AsyncClient, batch_id: str, job: JobStatus) -> None:
    """
    Rollback a batch by deleting all created hospitals.
    """
    try:
        await delete_batch(client, batch_id)
        job.status = JobStatusEnum.ROLLED_BACK
        logger.info(f"Batch {batch_id} rolled back successfully")
    except Exception as e:
        job.status = JobStatusEnum.FAILED
        logger.error(f"Failed to rollback batch {batch_id}: {e}")


def create_job(rows: List) -> tuple[str, JobStatus]:
    """
    Create a new job entry in the store.
    Returns the batch_id and job status.
    """
    batch_id = str(uuid.uuid4())

    job = JobStatus(
        batch_id=batch_id,
        status=JobStatusEnum.PROCESSING,
        total_hospitals=len(rows),
        processed_hospitals=0,
        failed_hospitals=0,
        hospitals=[]
    )

    jobs[batch_id] = job
    logger.info(f"Created new job {batch_id} for {len(rows)} hospitals")

    return batch_id, job