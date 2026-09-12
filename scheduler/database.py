from sqlalchemy import (
    create_engine,
    Column,
    String,
    Integer,
    DateTime,
    Text,
    Boolean,
    select,
)
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime, timezone, timedelta

from scheduler.config import (
    DATABASE_URL,
    WORKER_HEARTBEAT_TIMEOUT,
)


engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)

Base = declarative_base()


class JobTable(Base):
    __tablename__ = "jobs"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    command = Column(Text, nullable=False)
    priority = Column(Integer, default=1)
    timeout = Column(Integer, nullable=True)
    max_retries = Column(Integer, default=3)
    retries = Column(Integer, default=0)
    status = Column(String, default="QUEUED")

    scheduled_at = Column(
        DateTime(timezone=True),
        nullable=False,
    )

    worker_id = Column(
        String,
        nullable=True,
    )

    enqueued = Column(
        Boolean,
        default=False,
    )

    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )

    started_at = Column(
        DateTime(timezone=True),
        nullable=True,
    )

    finished_at = Column(
        DateTime(timezone=True),
        nullable=True,
    )

    error = Column(
        Text,
        nullable=True,
    )


class WorkerTable(Base):
    __tablename__ = "workers"

    id = Column(String, primary_key=True)

    status = Column(
        String,
        default="ACTIVE",
    )

    capacity = Column(
        Integer,
        default=1,
    )

    active_jobs = Column(
        Integer,
        default=0,
    )

    last_heartbeat = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )


def init_db():
    Base.metadata.create_all(engine)


def create_job(job):
    db = SessionLocal()

    try:
        db.add(job)
        db.commit()
    finally:
        db.close()


def get_jobs():
    db = SessionLocal()

    try:
        return db.execute(
            select(JobTable).order_by(
                JobTable.created_at.desc()
            )
        ).scalars().all()
    finally:
        db.close()


def get_job(job_id):
    db = SessionLocal()

    try:
        return db.get(JobTable, job_id)
    finally:
        db.close()


def get_job_status(job_id):
    db = SessionLocal()

    try:
        job = db.get(JobTable, job_id)

        if not job:
            return None

        return job.status

    finally:
        db.close()


def cancel_job(job_id):
    db = SessionLocal()

    try:
        job = db.get(JobTable, job_id)

        if not job:
            return None

        if job.status in [
            "COMPLETED",
            "FAILED",
            "TIMEOUT",
            "CANCELLED",
        ]:
            return False

        job.status = "CANCELLED"
        job.enqueued = False
        job.worker_id = None
        job.finished_at = datetime.now(timezone.utc)

        db.commit()

        return True

    finally:
        db.close()


def claim_job(worker_id):
    db = SessionLocal()

    try:
        now = datetime.now(timezone.utc)

        worker = db.get(
            WorkerTable,
            worker_id,
        )

        if not worker:
            return None

        if worker.status != "ACTIVE":
            return None

        if worker.active_jobs >= worker.capacity:
            return None

        job = db.execute(
            select(JobTable)
            .where(
                JobTable.status == "QUEUED",
                JobTable.scheduled_at <= now,
                JobTable.enqueued == True,
            )
            .order_by(
                JobTable.priority.desc(),
                JobTable.scheduled_at.asc(),
                JobTable.created_at.asc(),
            )
            .with_for_update(
                skip_locked=True
            )
            .limit(1)
        ).scalar_one_or_none()

        if not job:
            return None

        job.status = "RUNNING"
        job.worker_id = worker_id
        job.enqueued = False
        job.started_at = now

        worker.active_jobs += 1

        db.commit()

        return job

    finally:
        db.close()


def finish_job(
    job_id,
    worker_id,
    status,
    error=None,
):
    db = SessionLocal()

    try:
        job = db.get(
            JobTable,
            job_id,
        )

        worker = db.get(
            WorkerTable,
            worker_id,
        )

        # Never overwrite CANCELLED.
        if job and job.status == "CANCELLED":
            if worker and worker.active_jobs > 0:
                worker.active_jobs -= 1

            db.commit()
            return False

        if job and job.status == "RUNNING":
            job.status = status
            job.finished_at = datetime.now(
                timezone.utc
            )
            job.error = error
            job.worker_id = None

        if worker and worker.active_jobs > 0:
            worker.active_jobs -= 1

        db.commit()

        return True

    finally:
        db.close()


def retry_job(
    job_id,
    worker_id,
    delay,
):
    db = SessionLocal()

    try:
        job = db.get(
            JobTable,
            job_id,
        )

        worker = db.get(
            WorkerTable,
            worker_id,
        )

        if not job:
            return

        # A cancelled job must never be retried.
        if job.status == "CANCELLED":
            if worker and worker.active_jobs > 0:
                worker.active_jobs -= 1

            db.commit()
            return

        job.retries += 1
        job.status = "QUEUED"
        job.worker_id = None
        job.enqueued = False

        job.scheduled_at = (
            datetime.now(timezone.utc)
            + timedelta(seconds=delay)
        )

        if worker and worker.active_jobs > 0:
            worker.active_jobs -= 1

        db.commit()

    finally:
        db.close()


def register_worker(
    worker_id,
    capacity,
):
    db = SessionLocal()

    try:
        worker = db.get(
            WorkerTable,
            worker_id,
        )

        if not worker:
            worker = WorkerTable(
                id=worker_id,
                capacity=capacity,
                active_jobs=0,
                status="ACTIVE",
                last_heartbeat=datetime.now(
                    timezone.utc
                ),
            )

            db.add(worker)

        else:
            worker.status = "ACTIVE"
            worker.capacity = capacity
            worker.last_heartbeat = datetime.now(
                timezone.utc
            )

        db.commit()

    finally:
        db.close()


def heartbeat(worker_id):
    db = SessionLocal()

    try:
        worker = db.get(
            WorkerTable,
            worker_id,
        )

        if worker:
            worker.last_heartbeat = datetime.now(
                timezone.utc
            )

            worker.status = "ACTIVE"

            db.commit()

    finally:
        db.close()


def get_workers():
    db = SessionLocal()

    try:
        return db.execute(
            select(WorkerTable).order_by(
                WorkerTable.id
            )
        ).scalars().all()

    finally:
        db.close()


def mark_dead_workers():
    db = SessionLocal()

    try:
        cutoff = (
            datetime.now(timezone.utc)
            - timedelta(
                seconds=WORKER_HEARTBEAT_TIMEOUT
            )
        )

        workers = db.execute(
            select(WorkerTable).where(
                WorkerTable.last_heartbeat < cutoff,
                WorkerTable.status == "ACTIVE",
            )
        ).scalars().all()

        for worker in workers:

            worker.status = "DEAD"

            running_jobs = db.execute(
                select(JobTable).where(
                    JobTable.worker_id == worker.id,
                    JobTable.status == "RUNNING",
                )
            ).scalars().all()

            for job in running_jobs:

                job.status = "QUEUED"
                job.worker_id = None
                job.enqueued = False
                job.scheduled_at = datetime.now(
                    timezone.utc
                )

            worker.active_jobs = 0

        db.commit()

    finally:
        db.close()


def enqueue_due_jobs():
    db = SessionLocal()

    try:
        now = datetime.now(timezone.utc)

        jobs = db.execute(
            select(JobTable)
            .where(
                JobTable.status == "QUEUED",
                JobTable.scheduled_at <= now,
                JobTable.enqueued == False,
            )
            .order_by(
                JobTable.priority.desc(),
                JobTable.created_at.asc(),
            )
            .limit(100)
        ).scalars().all()

        job_ids = []

        for job in jobs:
            job.enqueued = True
            job_ids.append(job.id)

        db.commit()

        return job_ids

    finally:
        db.close()