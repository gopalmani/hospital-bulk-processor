import asyncio
from typing import Dict, List, Optional

from .models import HospitalResult, JobStatus, JobStatusEnum

jobs: Dict[str, JobStatus] = {}
_lock = asyncio.Lock()


def _copy_job(job: JobStatus) -> JobStatus:
    snapshot = job.model_copy(deep=True)
    snapshot.hospitals = sorted(snapshot.hospitals, key=lambda item: item.row)
    return snapshot


async def create_job(batch_id: str, total_hospitals: int) -> JobStatus:
    async with _lock:
        job = JobStatus(
            batch_id=batch_id,
            status=JobStatusEnum.PROCESSING,
            total_hospitals=total_hospitals,
        )
        jobs[batch_id] = job
        return _copy_job(job)


async def get_job(batch_id: str) -> Optional[JobStatus]:
    async with _lock:
        job = jobs.get(batch_id)
        return _copy_job(job) if job else None


async def list_jobs() -> List[JobStatus]:
    async with _lock:
        return [_copy_job(job) for job in jobs.values()]


async def add_hospital_result(batch_id: str, result: HospitalResult) -> None:
    async with _lock:
        job = jobs[batch_id]
        job.hospitals.append(result)
        job.processed_hospitals += 1
        if result.status == "failed":
            job.failed_hospitals += 1


async def mark_batch_activated(batch_id: str) -> None:
    async with _lock:
        job = jobs[batch_id]
        job.batch_activated = True
        for hospital in job.hospitals:
            if hospital.status == "created":
                hospital.status = "created_and_activated"


async def update_job_status(
    batch_id: str,
    status: JobStatusEnum,
    processing_time_seconds: Optional[float] = None,
) -> None:
    async with _lock:
        job = jobs[batch_id]
        job.status = status
        if processing_time_seconds is not None:
            job.processing_time_seconds = processing_time_seconds


async def clear_jobs() -> None:
    async with _lock:
        jobs.clear()
