from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks
from typing import List
import logging

from .csv_service import parse_csv, CSVValidationError
from .processor import process_bulk, create_job
from .store import jobs
from .models import BulkUploadResponse, JobStatus

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post(
    "/hospitals/bulk",
    response_model=BulkUploadResponse,
    status_code=202,
    responses={
        202: {"description": "Bulk job accepted for processing"},
        400: {"description": "Validation error"},
        500: {"description": "Internal server error"}
    }
)
async def bulk_create(
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = None
) -> BulkUploadResponse:
    """
    Accept CSV upload and create async background job.
    
    Returns 202 Accepted immediately while processing happens in background.
    """
    # Validate file type
    if not file.filename.endswith(".csv"):
        logger.warning(f"Invalid file type uploaded: {file.filename}")
        raise HTTPException(
            status_code=400,
            detail="Only CSV files are accepted"
        )

    # Read and parse CSV
    try:
        content = await file.read()
        rows = parse_csv(content)
    except CSVValidationError as e:
        logger.warning(f"CSV validation failed: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to read CSV file: {e}")
        raise HTTPException(
            status_code=400,
            detail="Failed to parse CSV file"
        )

    # Create job entry
    batch_id, job = create_job(rows)
    logger.info(f"Job {batch_id} created, scheduling background processing")

    # Schedule background processing
    background_tasks.add_task(process_bulk, rows, batch_id)

    return BulkUploadResponse(
        batch_id=batch_id,
        status="processing",
        message="Bulk job accepted"
    )


@router.get(
    "/jobs/{batch_id}",
    response_model=JobStatus,
    responses={
        200: {"description": "Job status retrieved"},
        404: {"description": "Job not found"}
    }
)
def get_job(batch_id: str) -> JobStatus:
    """
    Get status of a specific bulk job.
    """
    if batch_id not in jobs:
        logger.warning(f"Job not found: {batch_id}")
        raise HTTPException(
            status_code=404,
            detail=f"Job {batch_id} not found"
        )

    job = jobs[batch_id]
    logger.debug(f"Retrieved job status for {batch_id}: {job.status}")
    return job


@router.get(
    "/jobs",
    response_model=List[JobStatus],
    responses={
        200: {"description": "List of all jobs"}
    }
)
def list_jobs() -> List[JobStatus]:
    """
    Get all jobs.
    """
    job_list = list(jobs.values())
    logger.debug(f"Listing {len(job_list)} jobs")
    return job_list