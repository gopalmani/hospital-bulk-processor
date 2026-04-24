import logging
import uuid
from typing import List

from fastapi import APIRouter, BackgroundTasks, File, HTTPException, UploadFile, status

from .csv_service import CSVValidationError, parse_csv
from .models import BulkUploadResponse, JobStatus
from .processor import process_bulk_job
from .store import create_job, get_job, list_jobs

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post(
    "/hospitals/bulk",
    response_model=BulkUploadResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def bulk_create(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
) -> BulkUploadResponse:
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please upload a CSV file",
        )

    try:
        rows = parse_csv(await file.read())
    except CSVValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc

    batch_id = str(uuid.uuid4())
    await create_job(batch_id=batch_id, total_hospitals=len(rows))
    background_tasks.add_task(process_bulk_job, batch_id, rows)

    logger.info(
        "job_accepted",
        extra={"batch_id": batch_id, "total_hospitals": len(rows)},
    )
    return BulkUploadResponse(
        batch_id=batch_id,
        status="processing",
        message="Bulk job accepted",
    )


@router.get("/jobs/{batch_id}", response_model=JobStatus)
async def get_job_status(batch_id: str) -> JobStatus:
    job = await get_job(batch_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        )
    return job


@router.get("/jobs", response_model=List[JobStatus])
async def get_jobs() -> List[JobStatus]:
    return await list_jobs()
