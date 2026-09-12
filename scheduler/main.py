from fastapi import FastAPI
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles

from scheduler.models import JobCreate
from scheduler.database import (
    init_db,
    create_job,
    get_jobs,
    get_job,
    cancel_job,
    mark_dead_workers,
    enqueue_due_jobs,
    JobTable
)
from scheduler.queue import enqueue_jobs
from scheduler.metrics import (
    jobs_submitted,
    jobs_cancelled
)

from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from datetime import datetime, timezone
import threading
import time
import uuid


app = FastAPI(
    title="Distributed Job Scheduler",
    version="1.0"
)

init_db()


def scheduler_loop():
    while True:
        try:
            job_ids = enqueue_due_jobs()
            enqueue_jobs(job_ids)
            mark_dead_workers()
        except Exception as e:
            print("Scheduler error:", e)

        time.sleep(1)


threading.Thread(
    target=scheduler_loop,
    daemon=True
).start()


@app.get("/")
def home():
    return {
        "message": "Distributed Job Scheduler is running"
    }


@app.get("/health")
def health():
    return {
        "status": "healthy"
    }


@app.post("/jobs")
def submit_job(job: JobCreate):

    job_id = str(uuid.uuid4())

    scheduled_at = (
        job.scheduled_at
        if job.scheduled_at
        else datetime.now(timezone.utc)
    )

    new_job = JobTable(
        id=job_id,
        name=job.name,
        command=job.command,
        priority=job.priority,
        timeout=job.timeout,
        max_retries=job.max_retries,
        retries=0,
        status="QUEUED",
        scheduled_at=scheduled_at,
        worker_id=None,
        enqueued=False
    )

    create_job(new_job)

    jobs_submitted.inc()

    return {
        "id": job_id,
        "name": job.name,
        "command": job.command,
        "priority": job.priority,
        "timeout": job.timeout,
        "max_retries": job.max_retries,
        "status": "QUEUED",
        "scheduled_at": scheduled_at
    }


@app.get("/jobs")
def list_jobs():

    jobs = get_jobs()

    return [
        {
            "id": job.id,
            "name": job.name,
            "command": job.command,
            "priority": job.priority,
            "timeout": job.timeout,
            "max_retries": job.max_retries,
            "retries": job.retries,
            "status": job.status,
            "scheduled_at": job.scheduled_at,
            "worker_id": job.worker_id,
            "error": job.error
        }
        for job in jobs
    ]


@app.get("/jobs/{job_id}")
def job_details(job_id: str):

    job = get_job(job_id)

    if not job:
        return {
            "error": "Job not found"
        }

    return {
        "id": job.id,
        "name": job.name,
        "command": job.command,
        "priority": job.priority,
        "timeout": job.timeout,
        "max_retries": job.max_retries,
        "retries": job.retries,
        "status": job.status,
        "scheduled_at": job.scheduled_at,
        "worker_id": job.worker_id,
        "error": job.error
    }


@app.delete("/jobs/{job_id}")
def cancel(job_id: str):

    result = cancel_job(job_id)

    if result is None:
        return {
            "error": "Job not found"
        }

    if result is False:
        return {
            "error": "Job cannot be cancelled"
        }

    jobs_cancelled.inc()

    return {
        "message": "Job cancelled",
        "job_id": job_id
    }


@app.get("/workers")
def workers():

    from scheduler.database import get_workers

    worker_list = get_workers()

    return [
        {
            "id": worker.id,
            "status": worker.status,
            "capacity": worker.capacity,
            "active_jobs": worker.active_jobs,
            "last_heartbeat": worker.last_heartbeat
        }
        for worker in worker_list
    ]


@app.get("/metrics")
def metrics():

    return Response(
        generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )


app.mount(
    "/dashboard",
    StaticFiles(
        directory="dashboard",
        html=True
    ),
    name="dashboard"
)