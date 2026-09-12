from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class JobCreate(BaseModel):
    name: str
    command: str
    priority: int = 1
    timeout: Optional[int] = 60
    max_retries: int = 3
    scheduled_at: Optional[datetime] = None


class Job(BaseModel):
    id: str
    name: str
    command: str
    priority: int
    timeout: Optional[int]
    max_retries: int
    status: str
    retries: int
    scheduled_at: Optional[datetime]
    worker_id: Optional[str]