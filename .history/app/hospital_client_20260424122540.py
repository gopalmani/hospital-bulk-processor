import httpx
import asyncio
import logging
from typing import Optional

from .config import BASE_URL, REQUEST_TIMEOUT

logger = logging.getLogger(__name__)

# Retry configuration
MAX_RETRIES = 3
RETRY_BACKOFF = [1, 2, 4]  # seconds for exponential backoff

# HTTP status codes that should trigger a retry
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def should_retry(exception: Exception, attempt: int) -> bool:
    """Determine if an exception is retryable."""
    if attempt >= MAX_RETRIES:
        return False

    if isinstance(exception, httpx.TimeoutException):
        logger.warning(f"Timeout occurred, retry attempt {attempt + 1}")
        return True

    if isinstance(exception, httpx.ConnectError):
        logger.warning(f"Connection error, retry attempt {attempt + 1}")
        return True

    if isinstance(exception, httpx.HTTPStatusError):
        if exception.response.status_code in RETRYABLE_STATUS_CODES:
            logger.warning(
                f"HTTP {exception.response.status_code}, retry attempt {attempt + 1}"
            )
            return True

    return False


async def create_hospital(
    client: httpx.AsyncClient,
    hospital,
    batch_id: str,
    attempt: int = 0
) -> dict:
    """
    Create a hospital with retry logic and exponential backoff.
    """
    payload = {
        "name": hospital.name,
        "address": hospital.address,
        "phone": hospital.phone,
        "creation_batch_id": batch_id
    }

    try:
        logger.info(f"Creating hospital '{hospital.name}' (attempt {attempt + 1})")
        r = await client.post(
            f"{BASE_URL}/hospitals/",
            json=payload,
            timeout=REQUEST_TIMEOUT
        )
        r.raise_for_status()
        result = r.json()
        logger.info(f"Successfully created hospital '{hospital.name}' with ID {result.get('id')}")
        return result

    except Exception as e:
        if should_retry(e, attempt):
            backoff = RETRY_BACKOFF[attempt] if attempt < len(RETRY_BACKOFF) else RETRY_BACKOFF[-1]
            logger.info(f"Retrying after {backoff}s (attempt {attempt + 2}/{MAX_RETRIES})")
            await asyncio.sleep(backoff)
            return await create_hospital(client, hospital, batch_id, attempt + 1)
        logger.error(f"Failed to create hospital '{hospital.name}' after {MAX_RETRIES} attempts: {e}")
        raise


async def activate_batch(client: httpx.AsyncClient, batch_id: str) -> dict:
    """
    Activate a batch of hospitals.
    """
    logger.info(f"Activating batch {batch_id}")
    r = await client.patch(
        f"{BASE_URL}/hospitals/batch/{batch_id}/activate",
        timeout=REQUEST_TIMEOUT
    )
    r.raise_for_status()
    result = r.json()
    logger.info(f"Successfully activated batch {batch_id}")
    return result


async def delete_batch(client: httpx.AsyncClient, batch_id: str) -> None:
    """
    Delete a batch of hospitals (rollback).
    """
    logger.info(f"Rolling back batch {batch_id}")
    r = await client.delete(
        f"{BASE_URL}/hospitals/batch/{batch_id}",
        timeout=REQUEST_TIMEOUT
    )
    r.raise_for_status()
    logger.info(f"Successfully rolled back batch {batch_id}")