import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/scheduler_db"
)

REDIS_URL = os.getenv(
    "REDIS_URL",
    "redis://localhost:6379/0"
)

WORKER_HEARTBEAT_TIMEOUT = 15
SCHEDULER_INTERVAL = 1
RETRY_BASE_DELAY = 2
RETRY_MAX_DELAY = 60