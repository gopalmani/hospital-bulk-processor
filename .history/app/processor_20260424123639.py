import asyncio
import logging
import time
from typing import List

import httpx

from .config import CONCURRENT_REQUESTS
from .hospital_client import activate_batch, create_hospital, delete_batch
from .models import HospitalCSVRow, HospitalResult, JobStatusEnum
from .store import (
    add_hospital_result,
    get_job,
    mark_batch_activated,
    update_job_status,
)

logger = logging.getLogger(__name__)


async def process_bulk_job(batch_id: str, rows: List[HospitalCSVRow]) -> None:
    start_time = time.monotonic()
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

    logger.info(
        "bulk_job_started",
        extra={"batch_id": batch_id, "total_hospitals": len(rows)},
    )

    async with httpx.AsyncClient() as client:
        await asyncio.gather(
            *(
                _create_hospital_row(client, semaphore, batch_id, row_number, row)
                for row_number, row in enumerate(rows, start=1)
            )
        )

        job = await get_job(batch_id)
        if job is None:
            logger.error("job_missing_during_processing", extra={"batch_id": batch_id})
            return

        if job.failed_hospitals > 0:
            rollback_status = await _rollback_batch(client, batch_id)
            await _finish_job(batch_id, rollback_status, start_time)
            return

        try:
            await activate_batch(client, batch_id)
            await mark_batch_activated(batch_id)
            logger.info("batch_activation_success", extra={"batch_id": batch_id})
            await _finish_job(batch_id, JobStatusEnum.COMPLETED, start_time)
        except Exception:
            logger.exception("batch_activation_failed", extra={"batch_id": batch_id})
            rollback_status = await _rollback_batch(client, batch_id)
            await _finish_job(batch_id, rollback_status, start_time)


async def _create_hospital_row(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    batch_id: str,
    row_number: int,
    row: HospitalCSVRow,
) -> None:
    async with semaphore:
        try:
            created = await create_hospital(client, row, batch_id)
            await add_hospital_result(
                batch_id,
                HospitalResult(
                    row=row_number,
                    hospital_id=created.get("id"),
                    name=row.name,
                    status="created",
                ),
            )
            logger.info(
                "bulk_row_success",
                extra={
                    "batch_id": batch_id,
                    "row": row_number,
                    "hospital_name": row.name,
                    "hospital_id": created.get("id"),
                },
            )
        except Exception as exc:
            await add_hospital_result(
                batch_id,
                HospitalResult(
                    row=row_number,
                    name=row.name,
                    status="failed",
                    error=str(exc),
                ),
            )
            logger.warning(
                "bulk_row_failure",
                extra={
                    "batch_id": batch_id,
                    "row": row_number,
                    "hospital_name": row.name,
                    "error": str(exc),
                },
            )


async def _rollback_batch(client: httpx.AsyncClient, batch_id: str) -> JobStatusEnum:
    try:
        await delete_batch(client, batch_id)
        logger.info("rollback_executed", extra={"batch_id": batch_id})
        return JobStatusEnum.ROLLED_BACK
    except Exception:
        logger.exception("rollback_failed", extra={"batch_id": batch_id})
        return JobStatusEnum.FAILED


async def _finish_job(
    batch_id: str,
    status: JobStatusEnum,
    start_time: float,
) -> None:
    elapsed_seconds = round(time.monotonic() - start_time, 2)
    await update_job_status(batch_id, status, elapsed_seconds)
    logger.info(
        "bulk_job_finished",
        extra={
            "batch_id": batch_id,
            "status": status.value,
            "processing_time_seconds": elapsed_seconds,
        },
    )
