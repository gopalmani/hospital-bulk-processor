from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks
from .csv_service import parse_csv
from .processor import process_bulk
from .store import jobs

router = APIRouter()

@router.post("/hospitals/bulk")
async def bulk_create(file: UploadFile = File(...)):
    if not file.filename.endswith(".csv"):
        raise HTTPException(400, "Upload CSV file")

    content = await file.read()

    try:
        rows = parse_csv(content)
    except Exception as e:
        raise HTTPException(400, str(e))

    result = await process_bulk(rows)
    return result


@router.get("/jobs/{batch_id}")
def get_job(batch_id: str):
    if batch_id not in jobs:
        raise HTTPException(404, "Job not found")

    return jobs[batch_id]


@router.get("/jobs")
def list_jobs():
    return list(jobs.values())