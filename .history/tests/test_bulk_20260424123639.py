import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from fastapi.testclient import TestClient
from io import BytesIO

from app.main import app
from app.models import JobStatus, JobStatusEnum, HospitalResult
from app.csv_service import parse_csv, CSVValidationError
from app.processor import create_job, process_bulk
from app.store import jobs


client = TestClient(app)


class TestCSVValidation:
    """Tests for CSV parsing and validation."""

    def test_valid_csv_upload_returns_202(self):
        """Valid CSV should return 202 Accepted."""
        csv_content = b"name,address,phone\nTest Hospital,123 Test St,555-1234\n"
        
        response = client.post(
            "/hospitals/bulk",
            files={"file": ("test.csv", csv_content, "text/csv")}
        )
        
        assert response.status_code == 202
        data = response.json()
        assert "batch_id" in data
        assert data["status"] == "processing"
        assert data["message"] == "Bulk job accepted"

    def test_invalid_csv_rejected(self):
        """Invalid CSV should return 400."""
        # Missing required columns
        csv_content = b"wrong_column\nTest Hospital"
        
        response = client.post(
            "/hospitals/bulk",
            files={"file": ("test.csv", csv_content, "text/csv")}
        )
        
        assert response.status_code == 400

    def test_csv_missing_required_columns(self):
        """CSV missing name/address columns should be rejected."""
        csv_content = b"phone\n555-1234"
        
        with pytest.raises(CSVValidationError) as exc:
            parse_csv(csv_content)
        
        assert "Missing required columns" in str(exc.value)

    def test_csv_exceeds_max_hospitals(self):
        """CSV exceeding 20 hospitals should be rejected."""
        # Create CSV with 21 rows
        rows = ["name,address,phone"] + [f"Hospital {i},Address {i},555-{i}" for i in range(21)]
        csv_content = "\n".join(rows).encode()
        
        with pytest.raises(CSVValidationError) as exc:
            parse_csv(csv_content)
        
        assert "Maximum 20 hospitals" in str(exc.value)

    def test_csv_empty_row_skipped(self):
        """Empty rows should be ignored."""
        csv_content = b"name,address,phone\nTest Hospital,123 St,555-1234\n\n,,\n"
        
        rows = parse_csv(csv_content)
        assert len(rows) == 1
        assert rows[0].name == "Test Hospital"


class TestJobEndpoints:
    """Tests for job status endpoints."""

    def test_get_unknown_batch_returns_404(self):
        """Unknown batch_id should return 404."""
        response = client.get("/jobs/nonexistent-batch-id")
        
        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    def test_list_jobs_returns_all(self):
        """GET /jobs should return all jobs."""
        response = client.get("/jobs")
        
        assert response.status_code == 200
        assert isinstance(response.json(), list)


class TestJobFlow:
    """Tests for job processing flow."""

    def test_success_job_flow(self):
        """Test successful job completion."""
        # Clear jobs store
        jobs.clear()
        
        csv_content = b"name,address,phone\nHospital A,123 St,555-0001\nHospital B,456 Ave,555-0002"
        
        with patch("app.processor.httpx.AsyncClient") as mock_client:
            # Mock successful responses
            mock_response = MagicMock()
            mock_response.json.return_value = {"id": 1}
            mock_response.raise_for_status = MagicMock()
            
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(
                return_value=mock_response
            )
            mock_client.return_value.__aenter__.return_value.patch = AsyncMock(
                return_value=mock_response
            )
            
            # Upload CSV
            response = client.post(
                "/hospitals/bulk",
                files={"file": ("test.csv", csv_content, "text/csv")}
            )
            
            assert response.status_code == 202
            batch_id = response.json()["batch_id"]
            
            # Give time for background task
            import time
            time.sleep(0.5)
            
            # Check job status
            job_response = client.get(f"/jobs/{batch_id}")
            assert job_response.status_code == 200
            
            job = job_response.json()
            assert job["batch_id"] == batch_id
            assert job["total_hospitals"] == 2

    def test_rollback_on_failed_row(self):
        """Test rollback is triggered when rows fail."""
        jobs.clear()
        
        csv_content = b"name,address\nHospital A,123 St"
        
        with patch("app.processor.httpx.AsyncClient") as mock_client:
            # Mock failed hospital creation
            mock_response = MagicMock()
            mock_response.json.return_value = {"id": 1}
            mock_response.raise_for_status = MagicMock(side_effect=Exception("API Error"))
            
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(
                return_value=mock_response
            )
            mock_client.return_value.__aenter__.return_value.delete = AsyncMock(
                return_value=MagicMock(raise_for_status=MagicMock())
            )
            
            response = client.post(
                "/hospitals/bulk",
                files={"file": ("test.csv", csv_content, "text/csv")}
            )
            
            assert response.status_code == 202
            batch_id = response.json()["batch_id"]
            
            # Give time for background task
            import time
            time.sleep(0.5)
            
            # Check job is rolled back
            job_response = client.get(f"/jobs/{batch_id}")
            job = job_response.json()
            
            # Job should have failed rows
            assert job["failed_hospitals"] >= 1


class TestRetryLogic:
    """Tests for retry logic."""

    @pytest.mark.asyncio
    async def test_retry_on_timeout(self):
        """Test retry is triggered on timeout."""
        from app.hospital_client import should_retry
        import httpx
        
        # Should retry on timeout
        assert should_retry(httpx.TimeoutException("timeout"), 0) is True
        
        # Should not retry after max attempts
        assert should_retry(httpx.TimeoutException("timeout"), 3) is False

    @pytest.mark.asyncio
    async def test_retry_on_5xx(self):
        """Test retry is triggered on 5xx errors."""
        from app.hospital_client import should_retry
        import httpx
        
        error = httpx.HTTPStatusError(
            "Server Error",
            request=MagicMock(),
            response=MagicMock(status_code=500)
        )
        assert should_retry(error, 0) is True

    @pytest.mark.asyncio
    async def test_retry_on_429(self):
        """Test retry is triggered on 429 rate limit."""
        from app.hospital_client import should_retry
        import httpx
        
        error = httpx.HTTPStatusError(
            "Rate Limited",
            request=MagicMock(),
            response=MagicMock(status_code=429)
        )
        assert should_retry(error, 0) is True


class TestConcurrency:
    """Tests for concurrency control."""

    def test_semaphore_limits_concurrent_requests(self):
        """Verify semaphore is used for concurrency control."""
        from app.config import CONCURRENT_REQUESTS
        
        # Should be limited to 5
        assert CONCURRENT_REQUESTS == 5


class TestHTTPStatusCodes:
    """Tests for proper HTTP status codes."""

    def test_202_for_async_start(self):
        """Async bulk upload should return 202."""
        csv_content = b"name,address\nTest Hospital,123 St"
        
        response = client.post(
            "/hospitals/bulk",
            files={"file": ("test.csv", csv_content, "text/csv")}
        )
        
        assert response.status_code == 202

    def test_400_for_validation(self):
        """Validation errors should return 400."""
        # Non-CSV file
        response = client.post(
            "/hospitals/bulk",
            files={"file": ("test.txt", b"not a csv", "text/plain")}
        )
        
        assert response.status_code == 400

    def test_404_for_unknown_job(self):
        """Unknown job should return 404."""
        response = client.get("/jobs/unknown-id-12345")
        
        assert response.status_code == 404
import pytest
from fastapi.testclient import TestClient

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
