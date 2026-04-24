import asyncio
import uuid
import time
import httpx

from .store import jobs
from .models import JobStatus, HospitalResult
from .hospital_client import create_hospital, activate_batch, delete_batch
from .config import CONCURRENT_REQUESTS

async def process_bulk(rows):
    batch_id = str(uuid.uuid4())

    job = JobStatus(
        batch_id=batch_id,
        status="processing",
        total_hospitals=len(rows),
        processed_hospitals=0,
        failed_hospitals=0,
        hospitals=[]
    )

    jobs[batch_id] = job

    sem = asyncio.Semaphore(CONCURRENT_REQUESTS)
    start = time.time()

    async with httpx.AsyncClient() as client:

        async def worker(index, row):
            async with sem:
                try:
                    data = await create_hospital(client, row, batch_id)

                    result = HospitalResult(
                        row=index + 1,
                        hospital_id=data["id"],
                        name=row.name,
                        status="created"
                    )

                    job.hospitals.append(result)
                    job.processed_hospitals += 1

                except Exception as e:
                    job.failed_hospitals += 1
                    job.hospitals.append(
                        HospitalResult(
                            row=index + 1,
                            name=row.name,
                            status="failed",
                            error=str(e)
                        )
                    )

        await asyncio.gather(
            *(worker(i, row) for i, row in enumerate(rows))
        )

        if job.failed_hospitals == 0:
            await activate_batch(client, batch_id)
            job.batch_activated = True

            for h in job.hospitals:
                h.status = "created_and_activated"

            job.status = "completed"

        else:
            await delete_batch(client, batch_id)
            job.status = "rolled_back"

    elapsed = round(time.time() - start, 2)
    job.processing_time_seconds = elapsed

    return job