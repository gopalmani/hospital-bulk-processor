import logging
import uuid
from typing import List

from fastapi import APIRouter, BackgroundTasks, File, HTTPException, UploadFile, status
from fastapi.responses import HTMLResponse

from .csv_service import CSVValidationError, parse_csv
from .models import BulkUploadResponse, JobStatus
from .processor import process_bulk_job
from .store import create_job, get_job, list_jobs

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def root() -> str:
    return """
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Hospital Bulk Processing API</title>
        <style>
          :root {
            --bg: #07130f;
            --panel: rgba(255, 255, 255, 0.08);
            --panel-strong: rgba(255, 255, 255, 0.14);
            --text: #f4f1e8;
            --muted: #b7c4b9;
            --gold: #d6b15f;
            --mint: #7bdcb5;
            --line: rgba(255, 255, 255, 0.16);
          }

          * {
            box-sizing: border-box;
          }

          body {
            margin: 0;
            min-height: 100vh;
            font-family: Georgia, "Times New Roman", serif;
            color: var(--text);
            background:
              radial-gradient(circle at 20% 10%, rgba(123, 220, 181, 0.22), transparent 28rem),
              radial-gradient(circle at 90% 20%, rgba(214, 177, 95, 0.2), transparent 24rem),
              linear-gradient(135deg, #07130f 0%, #10251e 48%, #08120f 100%);
          }

          .grain {
            min-height: 100vh;
            padding: 48px 22px;
            background-image:
              linear-gradient(rgba(255, 255, 255, 0.025) 1px, transparent 1px),
              linear-gradient(90deg, rgba(255, 255, 255, 0.025) 1px, transparent 1px);
            background-size: 44px 44px;
          }

          main {
            width: min(1120px, 100%);
            margin: 0 auto;
          }

          .hero {
            display: grid;
            grid-template-columns: 1.25fr 0.75fr;
            gap: 28px;
            align-items: stretch;
          }

          .card {
            border: 1px solid var(--line);
            background: var(--panel);
            border-radius: 28px;
            box-shadow: 0 24px 80px rgba(0, 0, 0, 0.32);
            backdrop-filter: blur(18px);
          }

          .intro {
            padding: 46px;
          }

          .eyebrow {
            display: inline-flex;
            align-items: center;
            gap: 10px;
            margin-bottom: 22px;
            padding: 8px 12px;
            border: 1px solid rgba(123, 220, 181, 0.35);
            border-radius: 999px;
            color: var(--mint);
            font: 700 12px/1.2 ui-sans-serif, system-ui, sans-serif;
            letter-spacing: 0.12em;
            text-transform: uppercase;
          }

          .dot {
            width: 8px;
            height: 8px;
            border-radius: 999px;
            background: var(--mint);
            box-shadow: 0 0 18px var(--mint);
          }

          h1 {
            margin: 0;
            max-width: 780px;
            font-size: clamp(42px, 7vw, 82px);
            line-height: 0.94;
            letter-spacing: -0.055em;
          }

          .lead {
            max-width: 720px;
            margin: 24px 0 0;
            color: var(--muted);
            font: 400 18px/1.7 ui-sans-serif, system-ui, sans-serif;
          }

          .actions {
            display: flex;
            flex-wrap: wrap;
            gap: 14px;
            margin-top: 34px;
          }

          .button {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            min-height: 48px;
            padding: 0 20px;
            border-radius: 999px;
            text-decoration: none;
            font: 800 14px/1 ui-sans-serif, system-ui, sans-serif;
            transition: transform 160ms ease, background 160ms ease;
          }

          .button:hover {
            transform: translateY(-2px);
          }

          .primary {
            color: #08120f;
            background: linear-gradient(135deg, var(--gold), #f4dfa4);
          }

          .secondary {
            color: var(--text);
            border: 1px solid var(--line);
            background: rgba(255, 255, 255, 0.06);
          }

          .status {
            padding: 28px;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
          }

          .metric {
            padding: 20px;
            border: 1px solid var(--line);
            border-radius: 22px;
            background: rgba(0, 0, 0, 0.18);
          }

          .metric + .metric {
            margin-top: 14px;
          }

          .metric span {
            display: block;
            color: var(--muted);
            font: 700 12px/1 ui-sans-serif, system-ui, sans-serif;
            letter-spacing: 0.1em;
            text-transform: uppercase;
          }

          .metric strong {
            display: block;
            margin-top: 10px;
            font-size: 28px;
            color: var(--text);
          }

          .section {
            margin-top: 28px;
            padding: 32px;
          }

          .section h2 {
            margin: 0 0 18px;
            font-size: 30px;
            letter-spacing: -0.035em;
          }

          .grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 16px;
          }

          .feature {
            padding: 20px;
            border: 1px solid var(--line);
            border-radius: 20px;
            background: rgba(255, 255, 255, 0.055);
          }

          .feature h3 {
            margin: 0 0 8px;
            font: 800 16px/1.3 ui-sans-serif, system-ui, sans-serif;
          }

          .feature p {
            margin: 0;
            color: var(--muted);
            font: 400 14px/1.55 ui-sans-serif, system-ui, sans-serif;
          }

          code, pre {
            font-family: "SFMono-Regular", Consolas, monospace;
          }

          pre {
            overflow-x: auto;
            margin: 0;
            padding: 22px;
            border: 1px solid var(--line);
            border-radius: 20px;
            color: #eaf7ef;
            background: rgba(0, 0, 0, 0.42);
            font-size: 13px;
            line-height: 1.6;
          }

          .endpoints {
            display: grid;
            gap: 12px;
          }

          .endpoint {
            display: flex;
            gap: 12px;
            align-items: center;
            padding: 14px;
            border: 1px solid var(--line);
            border-radius: 16px;
            background: rgba(0, 0, 0, 0.18);
            font: 600 14px/1.2 ui-sans-serif, system-ui, sans-serif;
          }

          .method {
            min-width: 64px;
            padding: 8px 10px;
            border-radius: 999px;
            text-align: center;
            color: #07130f;
            background: var(--mint);
            font-weight: 900;
          }

          footer {
            margin: 28px 0 0;
            color: var(--muted);
            text-align: center;
            font: 500 13px/1.5 ui-sans-serif, system-ui, sans-serif;
          }

          @media (max-width: 840px) {
            .hero, .grid {
              grid-template-columns: 1fr;
            }

            .intro {
              padding: 30px;
            }
          }
        </style>
      </head>
      <body>
        <div class="grain">
          <main>
            <section class="hero">
              <div class="card intro">
                <div class="eyebrow"><span class="dot"></span> Live API</div>
                <h1>Hospital Bulk Processing System</h1>
                <p class="lead">
                  A production-style FastAPI service for CSV hospital uploads, async background processing,
                  progress polling, retry-safe external API integration, and rollback-safe batch activation.
                </p>
                <div class="actions">
                  <a class="button primary" href="/docs">Open Swagger Docs</a>
                  <a class="button secondary" href="/jobs">View Jobs</a>
                  <a class="button secondary" href="https://github.com/gopalmani/hospital-bulk-processor">GitHub Repo</a>
                </div>
              </div>

              <aside class="card status" aria-label="Service highlights">
                <div class="metric">
                  <span>Status</span>
                  <strong>Healthy</strong>
                </div>
                <div class="metric">
                  <span>Concurrency</span>
                  <strong>5 at a time</strong>
                </div>
                <div class="metric">
                  <span>CSV Limit</span>
                  <strong>20 rows</strong>
                </div>
                <div class="metric">
                  <span>Retry Backoff</span>
                  <strong>1s / 2s / 4s</strong>
                </div>
              </aside>
            </section>

            <section class="card section">
              <h2>What This API Does</h2>
              <div class="grid">
                <div class="feature">
                  <h3>Async Uploads</h3>
                  <p>CSV uploads return 202 Accepted immediately while processing continues in the background.</p>
                </div>
                <div class="feature">
                  <h3>Progress Tracking</h3>
                  <p>Poll a job by batch ID to see processed rows, failures, activation state, and timing.</p>
                </div>
                <div class="feature">
                  <h3>Rollback Safety</h3>
                  <p>If any row fails permanently, the system deletes the batch so partial data does not remain.</p>
                </div>
              </div>
            </section>

            <section class="card section">
              <h2>Endpoints</h2>
              <div class="endpoints">
                <div class="endpoint"><span class="method">POST</span><code>/hospitals/bulk</code><span>Upload CSV and start a background job</span></div>
                <div class="endpoint"><span class="method">GET</span><code>/jobs/{batch_id}</code><span>Check one job's progress and result</span></div>
                <div class="endpoint"><span class="method">GET</span><code>/jobs</code><span>List all known in-memory jobs</span></div>
                <div class="endpoint"><span class="method">GET</span><code>/docs</code><span>Interactive Swagger documentation</span></div>
              </div>
            </section>

            <section class="card section">
              <h2>Quick Start</h2>
              <pre>curl -X POST https://hospital-bulk-processor-qkd0.onrender.com/hospitals/bulk \\
  -F "file=@hospitals.csv"

curl https://hospital-bulk-processor-qkd0.onrender.com/jobs/{batch_id}</pre>
            </section>

            <section class="card section">
              <h2>CSV Format</h2>
              <pre>name,address,phone
ABC Hospital,123 Main Street,555-1001
City Care Clinic,45 Park Avenue,555-1002
Sunrise Medical Center,88 Lake Road,</pre>
            </section>

            <footer>
              Built with FastAPI, httpx, asyncio, pytest, Docker, and Render.
              Free Render instances may take a moment to wake after inactivity.
            </footer>
          </main>
        </div>
      </body>
    </html>
    """


@router.post(
    "/hospitals/bulk",
    response_model=BulkUploadResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def bulk_create(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
) -> BulkUploadResponse:
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please upload a CSV file",
        )

    try:
        rows = parse_csv(await file.read())
    except CSVValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc

    batch_id = str(uuid.uuid4())
    await create_job(batch_id=batch_id, total_hospitals=len(rows))
    background_tasks.add_task(process_bulk_job, batch_id, rows)

    logger.info(
        "job_accepted",
        extra={"batch_id": batch_id, "total_hospitals": len(rows)},
    )
    return BulkUploadResponse(
        batch_id=batch_id,
        status="processing",
        message="Bulk job accepted",
    )


@router.get("/jobs/{batch_id}", response_model=JobStatus)
async def get_job_status(batch_id: str) -> JobStatus:
    job = await get_job(batch_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        )
    return job


@router.get("/jobs", response_model=List[JobStatus])
async def get_jobs() -> List[JobStatus]:
    return await list_jobs()
