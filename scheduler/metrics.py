from prometheus_client import Counter, Histogram, Gauge


jobs_submitted = Counter(
    "scheduler_jobs_submitted_total",
    "Total jobs submitted"
)

jobs_completed = Counter(
    "scheduler_jobs_completed_total",
    "Total jobs completed"
)

jobs_failed = Counter(
    "scheduler_jobs_failed_total",
    "Total jobs permanently failed"
)

jobs_retried = Counter(
    "scheduler_jobs_retried_total",
    "Total job retries"
)

jobs_cancelled = Counter(
    "scheduler_jobs_cancelled_total",
    "Total jobs cancelled"
)

jobs_timed_out = Counter(
    "scheduler_jobs_timed_out_total",
    "Total jobs that timed out"
)

job_duration = Histogram(
    "scheduler_job_duration_seconds",
    "Job execution duration"
)

active_workers = Gauge(
    "scheduler_active_workers",
    "Number of active workers"
)

running_jobs = Gauge(
    "scheduler_running_jobs",
    "Number of running jobs"
)