from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field


class JobStatusEnum(str, Enum):
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


class HospitalCSVRow(BaseModel):
    name: str
    address: str
    phone: Optional[str] = None


class HospitalResult(BaseModel):
    row: int
    hospital_id: Optional[int] = None
    name: str
    status: str
    error: Optional[str] = None


class JobStatus(BaseModel):
    batch_id: str
    status: JobStatusEnum = JobStatusEnum.PROCESSING
    total_hospitals: int = 0
    processed_hospitals: int = 0
    failed_hospitals: int = 0
    batch_activated: bool = False
    processing_time_seconds: Optional[float] = None
    hospitals: List[HospitalResult] = Field(default_factory=list)


class BulkUploadResponse(BaseModel):
    batch_id: str
    status: str
    message: str


class ErrorResponse(BaseModel):
    detail: str
