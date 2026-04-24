import httpx
from .config import BASE_URL, REQUEST_TIMEOUT

async def create_hospital(client, hospital, batch_id):
    payload = {
        "name": hospital.name,
        "address": hospital.address,
        "phone": hospital.phone,
        "creation_batch_id": batch_id
    }

    r = await client.post(
        f"{BASE_URL}/hospitals/",
        json=payload,
        timeout=REQUEST_TIMEOUT
    )
    r.raise_for_status()
    return r.json()

async def activate_batch(client, batch_id):
    r = await client.patch(
        f"{BASE_URL}/hospitals/batch/{batch_id}/activate"
    )
    r.raise_for_status()
    return r.json()

async def delete_batch(client, batch_id):
    await client.delete(
        f"{BASE_URL}/hospitals/batch/{batch_id}"
    )