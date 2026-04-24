import asyncio
import logging
from typing import Awaitable, Callable, TypeVar

import httpx

from .config import BASE_URL, REQUEST_TIMEOUT

logger = logging.getLogger(__name__)

T = TypeVar("T")
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = [1, 2, 4]
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def _is_retryable(exception: Exception) -> bool:
    if isinstance(exception, (httpx.TimeoutException, httpx.ConnectError)):
        return True
    if isinstance(exception, httpx.HTTPStatusError):
        return exception.response.status_code in RETRYABLE_STATUS_CODES
    return False


async def _with_retries(
    operation_name: str,
    request: Callable[[], Awaitable[T]],
) -> T:
    for attempt in range(MAX_RETRIES + 1):
        try:
            return await request()
        except Exception as exc:
            if attempt >= MAX_RETRIES or not _is_retryable(exc):
                logger.error(
                    "external_api_call_failed",
                    extra={
                        "operation": operation_name,
                        "attempt": attempt + 1,
                        "max_attempts": MAX_RETRIES + 1,
                        "error": str(exc),
                    },
                )
                raise

            backoff = RETRY_BACKOFF_SECONDS[attempt]
            logger.warning(
                "external_api_call_retrying",
                extra={
                    "operation": operation_name,
                    "attempt": attempt + 1,
                    "next_attempt": attempt + 2,
                    "backoff_seconds": backoff,
                    "error": str(exc),
                },
            )
            await asyncio.sleep(backoff)

    raise RuntimeError("Retry loop exited unexpectedly")


async def create_hospital(client: httpx.AsyncClient, hospital, batch_id: str) -> dict:
    payload = {
        "name": hospital.name,
        "address": hospital.address,
        "phone": hospital.phone,
        "creation_batch_id": batch_id,
    }

    async def request() -> dict:
        response = await client.post(
            f"{BASE_URL}/hospitals/",
            json=payload,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        return response.json()

    return await _with_retries("create_hospital", request)


async def activate_batch(client: httpx.AsyncClient, batch_id: str) -> dict:
    async def request() -> dict:
        response = await client.patch(
            f"{BASE_URL}/hospitals/batch/{batch_id}/activate",
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        return response.json()

    return await _with_retries("activate_batch", request)


async def delete_batch(client: httpx.AsyncClient, batch_id: str) -> None:
    async def request() -> None:
        response = await client.delete(
            f"{BASE_URL}/hospitals/batch/{batch_id}",
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()

    await _with_retries("delete_batch", request)
