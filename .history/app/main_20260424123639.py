import logging

from fastapi import FastAPI

from .routes import router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

app = FastAPI(
    title="Hospital Bulk Processing API",
    version="1.0.0",
    description="Async background job system for bulk hospital creation",
)

app.include_router(router)
