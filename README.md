# Distributed Job Scheduler

A distributed job scheduling system built with Python, FastAPI, PostgreSQL and Redis.

## Features

- Job submission through REST API
- Priority-based scheduling
- Redis-backed job queue
- PostgreSQL persistent job storage
- Concurrent job execution
- Multiple distributed workers
- Worker capacity management
- Load balancing
- Job cancellation
- Scheduled jobs
- Job timeout handling
- Automatic retries
- Exponential retry backoff
- Worker heartbeat monitoring
- Dead-worker recovery
- Prometheus metrics
- Real-time monitoring dashboard
- Automated API tests

## Architecture

```text
                    ┌─────────────────┐
                    │     FastAPI     │
                    │   REST API      │
                    └────────┬────────┘
                             │
                ┌────────────┴────────────┐
                │                         │
                ▼                         ▼
        ┌───────────────┐        ┌───────────────┐
        │  PostgreSQL   │        │     Redis     │
        │ Job State     │        │ Queue/Signal  │
        └───────────────┘        └───────┬───────┘
                                        │
                         ┌──────────────┼──────────────┐
                         ▼              ▼              ▼
                    ┌─────────┐   ┌─────────┐   ┌─────────┐
                    │ Worker 1│   │ Worker 2│   │ Worker N│
                    └─────────┘   └─────────┘   └─────────┘
                         │              │              │
                         └──────────────┴──────────────┘
                                        │
                                        ▼
                              ┌─────────────────┐
                              │    Dashboard    │
                              │    + Metrics    │
                              └─────────────────┘