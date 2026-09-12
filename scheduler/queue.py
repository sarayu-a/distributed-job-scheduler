import redis

from scheduler.config import REDIS_URL


QUEUE_NAME = "scheduler:jobs"

redis_client = redis.Redis.from_url(
    REDIS_URL,
    decode_responses=True
)


def enqueue_job(job_id):
    redis_client.rpush(
        QUEUE_NAME,
        job_id
    )


def enqueue_jobs(job_ids):
    if not job_ids:
        return

    pipe = redis_client.pipeline()

    for job_id in job_ids:
        pipe.rpush(QUEUE_NAME, job_id)

    pipe.execute()


def wait_for_signal(timeout=2):
    result = redis_client.blpop(
        QUEUE_NAME,
        timeout=timeout
    )

    if not result:
        return None

    return result[1]