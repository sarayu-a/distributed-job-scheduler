import os
import time
import uuid
import threading
import subprocess

from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI
import uvicorn

from scheduler.queue import wait_for_signal

from scheduler.database import (
    register_worker,
    heartbeat,
    claim_job,
    finish_job,
    retry_job,
    get_job_status,
)

from scheduler.metrics import (
    jobs_completed,
    jobs_failed,
    jobs_retried,
    jobs_timed_out,
    job_duration,
    running_jobs,
)

from scheduler.config import (
    RETRY_BASE_DELAY,
    RETRY_MAX_DELAY,
)


WORKER_ID = str(uuid.uuid4())

CAPACITY = int(
    os.getenv("WORKER_CAPACITY", "2")
)


app = FastAPI()


@app.get("/")
def home():
    return {
        "status": "worker running",
        "worker_id": WORKER_ID
    }


@app.get("/health")
def health():
    return {
        "status": "healthy",
        "worker_id": WORKER_ID
    }


def heartbeat_loop():

    while True:

        try:
            heartbeat(WORKER_ID)

        except Exception as e:
            print("Heartbeat error:", e)

        time.sleep(5)


def execute_job(job):

    start = time.time()

    print(
        f"[{WORKER_ID}] Running "
        f"{job.name} ({job.id})"
    )

    running_jobs.inc()

    process = None

    try:

        process = subprocess.Popen(
            job.command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        timeout = job.timeout
        elapsed = 0

        while process.poll() is None:

            time.sleep(0.5)
            elapsed += 0.5

            status = get_job_status(job.id)

            if status == "CANCELLED":

                print(
                    f"[{WORKER_ID}] "
                    f"Cancelling {job.id}"
                )

                process.terminate()

                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()

                return

            if (
                timeout is not None
                and elapsed >= timeout
            ):

                print(
                    f"[{WORKER_ID}] "
                    f"Timeout {job.id}"
                )

                process.kill()

                finish_job(
                    job.id,
                    WORKER_ID,
                    "TIMEOUT",
                    "Job exceeded timeout",
                )

                jobs_timed_out.inc()

                return

        stdout, stderr = process.communicate()

        duration = time.time() - start

        job_duration.observe(duration)

        if stdout:
            print(stdout.strip())

        if stderr:
            print(stderr.strip())

        if get_job_status(job.id) == "CANCELLED":

            print(
                f"[{WORKER_ID}] "
                f"Job cancelled: {job.id}"
            )

            return

        if process.returncode == 0:

            finish_job(
                job.id,
                WORKER_ID,
                "COMPLETED",
            )

            jobs_completed.inc()

            print(
                f"[{WORKER_ID}] "
                f"Completed {job.id}"
            )

        else:

            if job.retries < job.max_retries:

                delay = min(
                    RETRY_BASE_DELAY
                    * (2 ** job.retries),
                    RETRY_MAX_DELAY,
                )

                retry_job(
                    job.id,
                    WORKER_ID,
                    delay,
                )

                jobs_retried.inc()

                print(
                    f"[{WORKER_ID}] "
                    f"Retry {job.id} "
                    f"in {delay}s"
                )

            else:

                finish_job(
                    job.id,
                    WORKER_ID,
                    "FAILED",
                    "Command returned "
                    "non-zero exit code",
                )

                jobs_failed.inc()

                print(
                    f"[{WORKER_ID}] "
                    f"Failed {job.id}"
                )

    except Exception as e:

        print(
            f"[{WORKER_ID}] Error:",
            e,
        )

        try:
            finish_job(
                job.id,
                WORKER_ID,
                "FAILED",
                str(e),
            )

            jobs_failed.inc()

        except Exception as db_error:
            print(
                "Database error:",
                db_error
            )

    finally:

        running_jobs.dec()


def run_worker():

    try:

        register_worker(
            WORKER_ID,
            CAPACITY,
        )

        print(
            f"Worker started: {WORKER_ID}"
        )

        print(
            f"Capacity: {CAPACITY}"
        )

        threading.Thread(
            target=heartbeat_loop,
            daemon=True,
        ).start()

        with ThreadPoolExecutor(
            max_workers=CAPACITY
        ) as executor:

            while True:

                wait_for_signal(
                    timeout=2
                )

                while True:

                    job = claim_job(
                        WORKER_ID
                    )

                    if not job:
                        break

                    executor.submit(
                        execute_job,
                        job,
                    )

    except Exception as e:

        print(
            f"Worker startup error: {e}"
        )


if __name__ == "__main__":

    worker_thread = threading.Thread(
        target=run_worker,
        daemon=True,
    )

    worker_thread.start()

    port = int(
        os.getenv("PORT", "10000")
    )

    print(
        f"Starting worker HTTP server on port {port}"
    )

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
    )