import pytest
from fastapi.testclient import TestClient
import httpx

from app import hospital_client
from app.main import app
from app.store import jobs


@pytest.fixture(autouse=True)
def clear_job_store():
    jobs.clear()
    yield
    jobs.clear()


@pytest.fixture
def client():
    return TestClient(app)


def _csv_file(content: str):
    return {"file": ("hospitals.csv", content.encode("utf-8"), "text/csv")}


def test_valid_csv_upload_returns_202(client, monkeypatch):
    async def noop_process_bulk_job(batch_id, rows):
        return None

    monkeypatch.setattr("app.routes.process_bulk_job", noop_process_bulk_job)

    response = client.post(
        "/hospitals/bulk",
        files=_csv_file("name,address,phone\nABC Hospital,Main Street,123\n"),
    )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "processing"
    assert body["message"] == "Bulk job accepted"
    assert body["batch_id"]


def test_invalid_csv_rejected(client):
    response = client.post(
        "/hospitals/bulk",
        files=_csv_file("name,phone\nABC Hospital,123\n"),
    )

    assert response.status_code == 400
    assert "address" in response.json()["detail"]


def test_get_unknown_batch_returns_404(client):
    response = client.get("/jobs/not-a-real-batch")

    assert response.status_code == 404
    assert response.json()["detail"] == "Job not found"


def test_success_job_flow(client, monkeypatch):
    created_names = []
    activated_batches = []

    async def fake_create_hospital(http_client, row, batch_id):
        created_names.append(row.name)
        return {"id": len(created_names)}

    async def fake_activate_batch(http_client, batch_id):
        activated_batches.append(batch_id)
        return {"ok": True}

    async def fake_delete_batch(http_client, batch_id):
        raise AssertionError("rollback should not be called on success")

    monkeypatch.setattr("app.processor.create_hospital", fake_create_hospital)
    monkeypatch.setattr("app.processor.activate_batch", fake_activate_batch)
    monkeypatch.setattr("app.processor.delete_batch", fake_delete_batch)

    response = client.post(
        "/hospitals/bulk",
        files=_csv_file(
            "name,address,phone\n"
            "ABC Hospital,Main Street,123\n"
            "XYZ Hospital,Second Street,\n"
        ),
    )

    assert response.status_code == 202
    batch_id = response.json()["batch_id"]

    job_response = client.get(f"/jobs/{batch_id}")
    assert job_response.status_code == 200

    job = job_response.json()
    assert job["status"] == "completed"
    assert job["total_hospitals"] == 2
    assert job["processed_hospitals"] == 2
    assert job["failed_hospitals"] == 0
    assert job["batch_activated"] is True
    assert job["processing_time_seconds"] is not None
    assert activated_batches == [batch_id]
    assert [hospital["status"] for hospital in job["hospitals"]] == [
        "created_and_activated",
        "created_and_activated",
    ]


def test_rollback_on_failed_row(client, monkeypatch):
    rolled_back_batches = []

    async def fake_create_hospital(http_client, row, batch_id):
        if row.name == "Bad Hospital":
            raise TimeoutError("timeout")
        return {"id": 101}

    async def fake_activate_batch(http_client, batch_id):
        raise AssertionError("activation should not be called after row failure")

    async def fake_delete_batch(http_client, batch_id):
        rolled_back_batches.append(batch_id)

    monkeypatch.setattr("app.processor.create_hospital", fake_create_hospital)
    monkeypatch.setattr("app.processor.activate_batch", fake_activate_batch)
    monkeypatch.setattr("app.processor.delete_batch", fake_delete_batch)

    response = client.post(
        "/hospitals/bulk",
        files=_csv_file(
            "name,address\n"
            "Good Hospital,Main Street\n"
            "Bad Hospital,Second Street\n"
        ),
    )

    assert response.status_code == 202
    batch_id = response.json()["batch_id"]

    job_response = client.get(f"/jobs/{batch_id}")
    assert job_response.status_code == 200

    job = job_response.json()
    assert job["status"] == "rolled_back"
    assert job["processed_hospitals"] == 2
    assert job["failed_hospitals"] == 1
    assert job["batch_activated"] is False
    assert rolled_back_batches == [batch_id]
    assert job["hospitals"][1]["status"] == "failed"
    assert "timeout" in job["hospitals"][1]["error"]


@pytest.mark.asyncio
async def test_retry_logic_retries_timeout(monkeypatch):
    attempts = 0
    sleep_calls = []

    async def flaky_request():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise httpx.TimeoutException("timeout")
        return {"ok": True}

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr(hospital_client.asyncio, "sleep", fake_sleep)

    result = await hospital_client._with_retries("test_operation", flaky_request)

    assert result == {"ok": True}
    assert attempts == 3
    assert sleep_calls == [1, 2]


@pytest.mark.asyncio
async def test_retry_logic_does_not_retry_400(monkeypatch):
    attempts = 0
    sleep_calls = []

    async def bad_request():
        nonlocal attempts
        attempts += 1
        request = httpx.Request("POST", "https://example.test/hospitals/")
        response = httpx.Response(400, request=request)
        raise httpx.HTTPStatusError("bad request", request=request, response=response)

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr(hospital_client.asyncio, "sleep", fake_sleep)

    with pytest.raises(httpx.HTTPStatusError):
        await hospital_client._with_retries("test_operation", bad_request)

    assert attempts == 1
    assert sleep_calls == []
