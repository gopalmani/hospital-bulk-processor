from pydantic import BaseModel
from typing import Optional, List

class HospitalCSVRow(BaseModel):
    name: str
    address: str
    phone: Optional[str] = None

class HospitalResult(BaseModel):
    row: int
    hospital_id: int | None = None
    name: str
    status: str
    error: str | None = None

class JobStatus(BaseModel):
    batch_id: str
    status: str
    total_hospitals: int
    processed_hospitals: int
    failed_hospitals: int
    batch_activated: bool = False
    hospitals: List[HospitalResult] = []