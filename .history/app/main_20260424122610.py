from fastapi import FastAPI
from fastapi import BackgroundTasks
from .routes import router
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

app = FastAPI(
    title="Hospital Bulk Processing API",
    version="1.0.0",
    description="Async background job system for bulk hospital creation"
)

app.include_router(router)