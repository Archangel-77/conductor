# Conductor Development Plan

**A lightweight, production-ready async task queue for Python teams that don't need Redis.**

This document outlines the complete development roadmap for Conductor v0.1 through v0.3, including architecture decisions, module structure, database schema, implementation priorities, and testing strategy.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture & Design](#architecture--design)
3. [Core Module Structure](#core-module-structure)
4. [Phase 1: v0.1 MVP](#phase-1-v01-mvp)
5. [Phase 2: v0.2 Advanced Features](#phase-2-v02-advanced-features)
6. [Phase 3: v0.3+ Future](#phase-3-v03-future)
7. [Database Schema](#database-schema)
8. [Testing Strategy](#testing-strategy)
9. [Deployment & Packaging](#deployment--packaging)
10. [Performance & Optimization](#performance--optimization)

---

## Project Overview

### Vision

Conductor fills the gap between simple task queues (RQ) and over-engineered solutions (Celery). It targets Python teams that:
- Already use PostgreSQL in production
- Don't want external message broker dependencies
- Need reliability guarantees (exactly-once semantics)
- Value observability from day one

### Key Differentiators

| Aspect | Value |
|--------|-------|
| **No external dependencies** | PostgreSQL only |
| **Exactly-once semantics** | Guaranteed task execution |
| **Built-in observability** | Structured logs, Prometheus metrics, health endpoints |
| **Production-ready** | Exponential backoff, circuit breakers, graceful shutdown, DLQ |
| **Simple API** | One-liner task submission, two-liner worker registration |
| **Async-native** | 100% asyncio, no threads |

### Success Criteria

- [ ] Pass all unit tests (85%+ coverage)
- [ ] Benchmark: 400+ tasks/sec/worker throughput
- [ ] Latency: Task submission <2ms, polling <500ms
- [ ] Zero data loss on worker crash (exactly-once)
- [ ] Deployable without external services
- [ ] Observable via structured logs + Prometheus
- [ ] Handle graceful shutdown cleanly
- [ ] Documentation with 5+ real-world examples

---

## Architecture & Design

### High-Level Design

```
┌─────────────────────────────────────────────────────────┐
│                   Your Application                      │
│                  queue.submit(task)                     │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│                  PostgreSQL Database                    │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────┐ │
│  │  Tasks   │  │ Workers  │  │ Retries  │  │  DLQ   │ │
│  └──────────┘  └──────────┘  └──────────┘  └────────┘ │
│  + Indexes, constraints, and triggers for consistency  │
└──────────┬──────────────────────────────────┬───────────┘
           │                                  │
      Polls every │                          │
      500ms       ▼                          │
┌──────────────────────────────────────────────────────────┐
│                   Worker Processes                      │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐              │
│  │ Worker-1 │  │ Worker-2 │  │ Worker-N │              │
│  │ async    │  │ async    │  │ async    │              │
│  └──────────┘  └──────────┘  └──────────┘              │
│  (Multiple machines or containers)                      │
└──────────────────────────────────────────────────────────┘
```

### Key Design Decisions

#### 1. PostgreSQL as Source of Truth
- **Why**: No separate message broker dependency; all state in one place
- **Trade-off**: Polling-based dispatch instead of pub/sub (adds ~500ms latency)
- **Mitigation**: Configurable poll interval; acceptable for most use cases

#### 2. Polling-Based Task Dispatch
- **Why**: Simpler implementation, no complex subscription logic
- **Workers**: Poll `conductor_tasks` table every `poll_interval` seconds
- **Deduplication**: Database constraints ensure no duplicate execution
- **Optimization**: Index on `(status, created_at)` for efficient polling

#### 3. Idempotent Task Processing
- **Core Concept**: Every task has a unique `task_id`
- **Mechanism**: Task marked as "processing" before execution; marked "completed" after
- **Guarantee**: If worker crashes, task ID prevents re-execution or marks as failed
- **Application Level**: Task handlers must be idempotent (e.g., use idempotency keys in APIs)

#### 4. Async-First Architecture
- **Why**: Modern Python, non-blocking I/O, single-threaded per worker
- **Stack**: asyncio + aiohttp for HTTP, async drivers for DB (asyncpg planned)
- **No Threads**: Avoid GIL contention, simpler concurrency model

#### 5. Observable by Default
- **Structured Logging**: Every task transition logged with correlation IDs
- **Prometheus Metrics**: Task counters, latencies, worker health
- **Health Endpoints**: `/health` endpoint for orchestration (Kubernetes, Docker Compose, etc.)

---

## Core Module Structure

```
conductor/
├── __init__.py                 # Package entry point, public API
├── core/
│   ├── __init__.py
│   ├── queue.py               # TaskQueue class (submit, list, inspect)
│   ├── worker.py              # Worker class (run, task decorator, shutdown)
│   └── models.py              # Data classes (Task, TaskStatus, etc.)
├── db/
│   ├── __init__.py
│   ├── connection.py          # PostgreSQL connection pooling
│   ├── schema.py              # Schema creation & migrations
│   └── queries.py             # SQL query builders (type-safe)
├── retry/
│   ├── __init__.py
│   ├── policies.py            # RetryPolicy, backoff strategies
│   └── backoff.py             # Exponential, linear backoff
├── dlq/
│   ├── __init__.py
│   └── dead_letter_queue.py   # DeadLetterQueue class
├── observability/
│   ├── __init__.py
│   ├── logging.py             # Structured logging setup
│   ├── metrics.py             # Prometheus metrics exporter
│   └── health.py              # Health check endpoint
├── exceptions.py              # Custom exceptions
└── utils.py                   # Utilities (id generation, serialization)
```

### Module Responsibilities

#### `core/queue.py` – TaskQueue
- Submit tasks: `queue.submit(task_type, payload, retry_policy, ...)`
- List tasks: `queue.list_pending_tasks()`, `queue.list_completed_tasks()`
- Inspect: `queue.get_task(task_id)`
- Schedule: `queue.schedule_recurring(task_type, cron_expression)`

#### `core/worker.py` – Worker
- Register task handler: `@worker.task("task_type")`
- Run event loop: `worker.run()`
- Graceful shutdown: `worker.shutdown()`
- Health status: `worker.get_status()`

#### `core/models.py` – Data Classes
```python
class TaskStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"

class Task:
    id: str
    type: str
    payload: dict
    status: TaskStatus
    created_at: datetime
    scheduled_for: Optional[datetime]
    retry_policy: dict
    attempts: int
    last_error: Optional[str]
    worker_id: Optional[str]
    completed_at: Optional[datetime]

class RetryPolicy:
    max_retries: int
    backoff: str  # "exponential" | "linear" | "fixed"
    initial_delay: int
    max_delay: int
```

#### `db/connection.py` – Connection Pooling
- AsyncPG or psycopg async connection pool
- Configure pool size, idle timeout, max lifetime
- Health check queries on acquire
- Automatic reconnection on failure

#### `db/schema.py` – Migrations
- Create tables on first run
- Auto-migrations for schema changes
- Version tracking (conductor_version table)
- Indexes for performance (see Database Schema section)

#### `retry/policies.py` – Backoff Strategies
```python
class BackoffStrategy:
    def calculate_delay(attempt: int) -> int:
        pass

class ExponentialBackoff(BackoffStrategy):
    # 1, 2, 4, 8, 16, ... (capped at max_delay)

class LinearBackoff(BackoffStrategy):
    # 5, 10, 15, 20, ... (incremented by initial_delay)

class FixedBackoff(BackoffStrategy):
    # 5, 5, 5, 5, ... (same delay each retry)
```

#### `dlq/dead_letter_queue.py` – Dead Letter Queue
- List failed tasks: `dlq.list_tasks(limit=10)`
- Inspect: `dlq.get_task(task_id)`
- Retry manually: `dlq.retry_task(task_id)`
- Discard: `dlq.discard_task(task_id)`

#### `observability/logging.py` – Structured Logging
```python
# Logs include:
# - task_id: Unique identifier
# - task_type: Task category
# - worker_id: Which worker
# - duration_ms: Execution time
# - status: success | failed | retried
# - error: Exception message (if failed)
# - level: DEBUG | INFO | WARNING | ERROR

logger.info(
    "task_completed",
    extra={
        "task_id": task_id,
        "task_type": task_type,
        "duration_ms": 245,
        "status": "success"
    }
)
```

#### `observability/metrics.py` – Prometheus Metrics
- `conductor_tasks_submitted_total` (counter)
- `conductor_tasks_completed_total` (counter)
- `conductor_tasks_failed_total` (counter)
- `conductor_tasks_retried_total` (counter)
- `conductor_task_duration_seconds` (histogram)
- `conductor_workers_active` (gauge)
- `conductor_dlq_size` (gauge)

#### `observability/health.py` – Health Endpoint
```
GET /health
{
  "status": "healthy" | "degraded" | "unhealthy",
  "database": "connected" | "disconnected",
  "pending_tasks": 42,
  "dead_letter_queue": 3,
  "workers_active": 5,
  "uptime_seconds": 3600,
  "last_check": "2025-01-15T10:30:45Z"
}
```

---

## Phase 1: v0.1 MVP

### Goals
- ✅ Core task queue functionality
- ✅ Exactly-once semantics
- ✅ Retry logic with exponential backoff
- ✅ Dead letter queue
- ✅ Worker pool (concurrent task processing)
- ✅ Structured logging
- ✅ Prometheus metrics
- ✅ Health checks
- ✅ Graceful shutdown
- ✅ Comprehensive documentation

### Deliverables

#### Sprint 1: Database & Core Models
**Duration**: 1 week

**Tasks**:
1. Set up project structure (package, setup.py, pyproject.toml)
2. Define database schema (see Database Schema section)
3. Create PostgreSQL connection pool (asyncpg or psycopg3 async)
4. Implement schema creation & migrations
5. Create data models (Task, TaskStatus, RetryPolicy, etc.)
6. Add database query builders (insert, update, select, delete)

**Files**:
- `conductor/__init__.py`
- `conductor/core/models.py`
- `conductor/db/connection.py`
- `conductor/db/schema.py`
- `conductor/db/queries.py`
- `conductor/exceptions.py`
- `conductor/utils.py`

**Acceptance Criteria**:
- [ ] Database tables created without errors
- [ ] Connection pool handles 10+ concurrent connections
- [ ] Migrations can be run idempotently
- [ ] All models have type hints
- [ ] Database module has 80%+ test coverage

---

#### Sprint 2: TaskQueue Implementation
**Duration**: 1 week

**Tasks**:
1. Implement `TaskQueue.submit()` – Insert task into database
2. Implement `TaskQueue.list_pending_tasks()` – Query pending tasks
3. Implement `TaskQueue.list_completed_tasks()` – Query completed tasks
4. Implement `TaskQueue.get_task(task_id)` – Inspect single task
5. Add task ID generation (UUID v4 or nanoid)
6. Add payload serialization (JSON)
7. Add retry policy validation
8. Add idempotency keys for deduplication (optional: v0.2)

**Files**:
- `conductor/core/queue.py`

**Acceptance Criteria**:
- [ ] Tasks submitted successfully
- [ ] Task IDs are unique and persistent
- [ ] Retry policies validated before submission
- [ ] List operations return correct task status
- [ ] TaskQueue has 85%+ test coverage

---

#### Sprint 3: Worker Implementation
**Duration**: 1.5 weeks

**Tasks**:
1. Implement `Worker` class with asyncio event loop
2. Implement `@worker.task(task_type)` decorator
3. Implement task polling from database
4. Implement task execution (run registered handler)
5. Implement task status updates (pending → processing → completed/failed)
6. Implement concurrency control (max concurrent tasks)
7. Implement graceful shutdown (SIGTERM/SIGINT handling)
8. Add worker heartbeat (periodic database update)

**Files**:
- `conductor/core/worker.py`

**Acceptance Criteria**:
- [ ] Worker starts and polls for tasks
- [ ] Tasks execute with correct handler
- [ ] Status transitions are correct
- [ ] Concurrency limit respected
- [ ] Graceful shutdown completes in-flight tasks
- [ ] Worker has 85%+ test coverage

---

#### Sprint 4: Retry Logic & Dead Letter Queue
**Duration**: 1 week

**Tasks**:
1. Implement retry policies (max_retries, backoff strategy)
2. Implement exponential backoff calculation
3. Implement linear backoff calculation
4. Implement failed task handling (move to retries table)
5. Implement retry scheduling (attempt again after delay)
6. Implement dead letter queue (tasks that fail all retries)
7. Implement `DeadLetterQueue` class (list, inspect, retry, discard)

**Files**:
- `conductor/retry/policies.py`
- `conductor/retry/backoff.py`
- `conductor/dlq/dead_letter_queue.py`

**Acceptance Criteria**:
- [ ] Retries execute after correct delays
- [ ] Failed tasks move to DLQ after max retries
- [ ] DLQ tasks can be manually retried
- [ ] Backoff strategies calculate correctly
- [ ] Retry logic has 85%+ test coverage

---

#### Sprint 5: Observability
**Duration**: 1 week

**Tasks**:
1. Set up structured logging (JSON format with context)
2. Create Prometheus metrics exporter (expose on HTTP endpoint)
3. Create health check endpoint (GET /health)
4. Add correlation IDs to logs
5. Add task duration tracking
6. Add worker status tracking
7. Create example Grafana dashboard (documentation)

**Files**:
- `conductor/observability/logging.py`
- `conductor/observability/metrics.py`
- `conductor/observability/health.py`

**Acceptance Criteria**:
- [ ] Logs include task_id, task_type, worker_id, duration_ms
- [ ] Prometheus metrics exportable via HTTP
- [ ] Health endpoint returns correct status
- [ ] Observability has 80%+ test coverage
- [ ] Example Grafana dashboard provided

---

#### Sprint 6: Integration & Documentation
**Duration**: 1.5 weeks

**Tasks**:
1. Write end-to-end integration tests (full queue → worker → completion)
2. Write performance benchmarks (throughput, latency)
3. Write README with quick start
4. Write 5+ real-world examples (email, data processing, scheduled tasks, etc.)
5. Write API reference documentation
6. Write deployment guide (Docker, Docker Compose, Kubernetes)
7. Write troubleshooting guide
8. Create example Docker Compose setup

**Files**:
- `README.md` (update existing)
- `docs/examples/` (5+ examples)
- `docs/deployment.md`
- `docs/api-reference.md`
- `examples/docker-compose.yml`
- `tests/integration/` (integration tests)
- `tests/benchmarks/` (performance tests)

**Acceptance Criteria**:
- [ ] All examples run without errors
- [ ] Benchmarks show 400+ tasks/sec/worker
- [ ] Documentation complete and accurate
- [ ] Integration tests pass
- [ ] Performance tests pass
- [ ] Docker Compose example works

---

### Phase 1 Dependencies

```
Dependency Graph:
1. Database & Models (Sprint 1)
   └─ required by all subsequent sprints
2. TaskQueue (Sprint 2)
   └─ depends on Sprint 1
3. Worker (Sprint 3)
   └─ depends on Sprint 1, 2
4. Retry & DLQ (Sprint 4)
   └─ depends on Sprint 1, 2, 3
5. Observability (Sprint 5)
   └─ depends on all previous sprints
6. Integration & Docs (Sprint 6)
   └─ depends on all previous sprints
```

---

## Phase 2: v0.2 Advanced Features

### Goals
- 🔲 Task routing (multiple queues/worker pools)
- 🔲 Priority queues
- 🔲 Scheduled/recurring tasks (cron)
- 🔲 Web dashboard (task monitoring UI)
- 🔲 gRPC API (for polyglot workers)
- 🔲 Circuit breaker pattern
- 🔲 Task dependencies/chaining

### Key Features

#### Feature 1: Task Routing
**What**: Route tasks to specific worker pools

**Use Case**:
```python
# Submit to "critical" queue
queue.submit(
    task_type="send_sms",
    payload={"phone": "+1234567890"},
    route="critical"
)

# Only "critical" workers process this
worker = Worker(routes=["critical"])
```

**Implementation**:
- Add `route` column to tasks table
- Workers subscribe to specific routes
- Polling query filters by route
- Allow multiple routes per worker

**Effort**: 1 week

---

#### Feature 2: Priority Queues
**What**: Execute high-priority tasks before low-priority ones

**Use Case**:
```python
queue.submit(
    task_type="send_sms",
    payload={"phone": "+1234567890"},
    priority=10  # Higher number = execute first
)
```

**Implementation**:
- Add `priority` column to tasks table
- Polling query orders by priority DESC
- Default priority = 0
- Range: -100 to 100

**Effort**: 3 days

---

#### Feature 3: Scheduled & Recurring Tasks
**What**: Defer task execution to specific time; repeat on schedule

**Use Case**:
```python
from datetime import datetime, timedelta

# Schedule for 1 hour from now
queue.submit(
    task_type="send_reminder",
    payload={"user_id": 123},
    scheduled_for=datetime.utcnow() + timedelta(hours=1)
)

# Cron scheduling
queue.schedule_recurring(
    task_type="cleanup_old_sessions",
    payload={},
    cron_expression="0 2 * * *"  # 2 AM daily
)
```

**Implementation**:
- Add `scheduled_for` column to tasks table
- Create `conductor_recurring_tasks` table (cron definitions)
- Polling query filters `scheduled_for <= NOW()`
- Cron parser (use croniter library)
- Scheduler daemon to create recurring task instances

**Effort**: 2 weeks

---

#### Feature 4: Web Dashboard
**What**: Visual monitoring UI for tasks, workers, metrics

**Stack**: React + Vite (frontend), FastAPI endpoints (backend)

**Screens**:
1. Tasks overview (pending, completed, failed)
2. Task details (payload, retries, logs)
3. Workers status (active, idle, uptime)
4. Metrics (throughput, latency, error rate)
5. DLQ inspector (failed tasks)

**Implementation**:
- FastAPI endpoints for task/worker/metrics queries
- React UI with real-time polling (WebSocket optional)
- Authentication (API key based)
- Embed in conductor package or standalone

**Effort**: 3 weeks

---

#### Feature 5: gRPC API
**What**: Polyglot workers (not just Python)

**Why**: Support workers in Go, Rust, Node.js, etc.

**Proto Definition**:
```protobuf
service ConductorWorker {
  rpc ProcessTask(TaskRequest) returns (TaskResponse);
  rpc RegisterHandler(RegisterRequest) returns (RegisterResponse);
}

message TaskRequest {
  string task_id = 1;
  string task_type = 2;
  bytes payload = 3;
}

message TaskResponse {
  string task_id = 1;
  bool success = 2;
  bytes result = 3;
  string error = 4;
}
```

**Implementation**:
- Add gRPC server to worker (alongside asyncio)
- Protocol Buffers for serialization
- Client library for other languages (Go, Rust, etc.)

**Effort**: 2 weeks

---

#### Feature 6: Circuit Breaker
**What**: Stop submitting tasks if external service is down

**Use Case**:
```python
queue.submit(
    task_type="call_external_api",
    payload={"url": "https://api.down.com/data"},
    circuit_breaker={
        "threshold": 5,        # Fail 5 times
        "timeout": 60,         # Then stop for 60s
        "half_open_attempts": 2
    }
)
```

**Implementation**:
- Add circuit breaker state tracking (open, closed, half-open)
- Track failures per task_type
- Transition to "open" after N failures
- Attempt recovery after timeout (half-open)
- Reject new submissions when open

**Effort**: 1 week

---

#### Feature 7: Task Dependencies
**What**: Chain tasks; execute task B only if task A succeeds

**Use Case**:
```python
# Submit task A
task_a_id = queue.submit(
    task_type="download_file",
    payload={"url": "https://..."}
)

# Task B depends on A
queue.submit(
    task_type="process_file",
    payload={"file_id": "123"},
    depends_on=[task_a_id]  # Only run if A succeeds
)
```

**Implementation**:
- Add `depends_on` (array of task IDs) to tasks table
- Polling query filters tasks with no unmet dependencies
- Mark task as "blocked" if dependency fails
- Transitive dependencies (A → B → C)

**Effort**: 1.5 weeks

---

### Phase 2 Timeline

| Feature | Effort | Priority | Start |
|---------|--------|----------|-------|
| Task Routing | 1 week | HIGH | Week 1 |
| Priority Queues | 3 days | HIGH | Week 2 |
| Scheduled Tasks | 2 weeks | HIGH | Week 2 |
| Circuit Breaker | 1 week | MEDIUM | Week 4 |
| Task Dependencies | 1.5 weeks | MEDIUM | Week 4 |
| gRPC API | 2 weeks | MEDIUM | Week 6 |
| Web Dashboard | 3 weeks | LOW | Week 6 |

**Total Duration**: ~8 weeks

---

## Phase 3: v0.3+ Future

### Goals
- 🔲 MySQL/MariaDB backend support
- 🔲 SQLite backend (embedded, single-server)
- 🔲 Distributed tracing (OpenTelemetry)
- 🔲 Managed SaaS offering (Conductor Cloud)
- 🔲 Advanced workflow orchestration
- 🔲 Multi-database replication

### Possible Features

#### 1. Multi-Database Backend
**What**: Support MySQL, SQLite, MongoDB

**Why**: Flexibility for different deployment scenarios

**Effort**: 2-3 weeks per database

---

#### 2. Distributed Tracing
**What**: OpenTelemetry integration for tracing task execution across services

**Why**: Observability at scale

**Components**:
- Trace context propagation (W3C Trace Context)
- Span creation for task submission, execution, retry
- Export to Jaeger, Datadog, New Relic

**Effort**: 1 week

---

#### 3. Conductor Cloud (SaaS)
**What**: Managed Conductor hosting

**How**: Deploy Conductor backend, expose API

**Why**: Lower barrier to entry for small teams

**MVP**: Single-tenant, multi-workspace

**Effort**: 4+ weeks

---

#### 4. Advanced Workflows
**What**: DAG-based task workflows (like Airflow)

**Use Case**:
```python
# Define workflow
dag = Workflow("data_pipeline")
dag.add_task("extract", extract_data)
dag.add_task("transform", transform_data, depends_on=["extract"])
dag.add_task("load", load_data, depends_on=["transform"])

# Submit workflow
conductor.submit_workflow(dag, inputs={...})
```

**Effort**: 4+ weeks

---

#### 5. Task Versioning & Rollback
**What**: Version task definitions; rollback to older versions

**Why**: Safely deploy task handler changes

**Effort**: 1 week

---

---

## Database Schema

### Tables

#### 1. `conductor_tasks`
Main task queue table.

```sql
CREATE TABLE conductor_tasks (
    id TEXT PRIMARY KEY,                        -- UUID/nanoid
    type TEXT NOT NULL,                         -- Task type
    payload JSONB NOT NULL,                     -- Task data
    status TEXT NOT NULL,                       -- pending|processing|completed|failed|retrying
    
    -- Scheduling
    scheduled_for TIMESTAMP,                    -- When to execute
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    
    -- Retry policy
    max_retries INT NOT NULL DEFAULT 3,
    retry_backoff TEXT DEFAULT 'exponential',   -- exponential|linear|fixed
    retry_initial_delay INT DEFAULT 1,
    retry_max_delay INT DEFAULT 3600,
    attempts INT NOT NULL DEFAULT 0,
    
    -- Routing & priority
    route TEXT DEFAULT 'default',
    priority INT DEFAULT 0,
    
    -- Execution tracking
    worker_id TEXT,
    last_error TEXT,
    result JSONB,
    
    -- Indexes
    INDEX idx_status_created (status, created_at),
    INDEX idx_scheduled_for (scheduled_for),
    INDEX idx_worker_id (worker_id),
    INDEX idx_route_status (route, status),
    INDEX idx_type_status (type, status)
);
```

#### 2. `conductor_retries`
Retry history (audit trail).

```sql
CREATE TABLE conductor_retries (
    id SERIAL PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES conductor_tasks(id),
    attempt_number INT NOT NULL,
    error_message TEXT,
    next_retry_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    INDEX idx_task_id (task_id),
    INDEX idx_next_retry_at (next_retry_at)
);
```

#### 3. `conductor_dead_letter`
Failed tasks (exhausted retries).

```sql
CREATE TABLE conductor_dead_letter (
    id SERIAL PRIMARY KEY,
    task_id TEXT NOT NULL UNIQUE REFERENCES conductor_tasks(id),
    type TEXT NOT NULL,
    payload JSONB NOT NULL,
    error_message TEXT NOT NULL,
    last_error JSONB,
    attempts INT NOT NULL,
    reason TEXT,                       -- Manual discard reason
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    discarded_at TIMESTAMP,
    
    INDEX idx_created_at (created_at),
    INDEX idx_type (type)
);
```

#### 4. `conductor_workers`
Worker heartbeats & status.

```sql
CREATE TABLE conductor_workers (
    id TEXT PRIMARY KEY,                        -- worker_id
    hostname TEXT,
    pid INT,
    routes TEXT[],                              -- Array of routes
    concurrency INT,
    
    -- Status
    status TEXT DEFAULT 'idle',                 -- idle|processing|unhealthy
    current_task_id TEXT REFERENCES conductor_tasks(id),
    
    -- Metrics
    tasks_processed_total INT DEFAULT 0,
    tasks_failed_total INT DEFAULT 0,
    uptime_seconds INT,
    
    -- Heartbeat
    last_heartbeat TIMESTAMP NOT NULL DEFAULT NOW(),
    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    INDEX idx_last_heartbeat (last_heartbeat)
);
```

#### 5. `conductor_recurring_tasks`
Recurring task definitions (cron).

```sql
CREATE TABLE conductor_recurring_tasks (
    id SERIAL PRIMARY KEY,
    task_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    cron_expression TEXT NOT NULL,              -- "0 2 * * *" (2 AM daily)
    
    -- State
    active BOOLEAN DEFAULT TRUE,
    last_run_at TIMESTAMP,
    next_run_at TIMESTAMP NOT NULL,
    
    -- Metadata
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by TEXT,
    
    INDEX idx_next_run_at (next_run_at),
    INDEX idx_active (active)
);
```

#### 6. `conductor_version`
Schema version tracking.

```sql
CREATE TABLE conductor_version (
    version INT PRIMARY KEY,
    applied_at TIMESTAMP NOT NULL DEFAULT NOW()
);
```

### Indexes & Constraints

**Performance Indexes**:
- `idx_status_created` on `conductor_tasks(status, created_at)` – Fast polling
- `idx_scheduled_for` on `conductor_tasks(scheduled_for)` – Scheduled task dispatch
- `idx_worker_id` on `conductor_tasks(worker_id)` – Worker task tracking
- `idx_route_status` on `conductor_tasks(route, status)` – Route-specific polling
- `idx_type_status` on `conductor_tasks(type, status)` – Task-type monitoring

**Constraints**:
- Task ID is unique (`PRIMARY KEY`)
- Worker ID references valid `conductor_workers.id`
- DLQ task_id is unique (one entry per failed task)
- Status must be one of enum values (trigger or CHECK constraint)

**Triggers**:
- Auto-update `conductor_workers.last_heartbeat` on INSERT
- Auto-move completed tasks to archive (optional, v0.2+)

---

## Testing Strategy

### Test Coverage Goals
- **Unit Tests**: 85%+ line coverage
- **Integration Tests**: Cover all user workflows
- **Performance Tests**: Benchmark throughput, latency
- **End-to-End Tests**: Full queue → worker → completion

### Test Pyramid

```
        ▲
       /│\
      / │ \     E2E Tests (5-10%)
     /  │  \    - Full workflow tests
    /   │   \   - Docker Compose integration
   /────┼────\
  /     │     \ Integration Tests (20-30%)
 /      │      \ - Database operations
/       │       \- Worker startup/shutdown
────────┼────────\ - Multi-worker scenarios
        │       Unit Tests (60-70%)
        │       - Models
        │       - Backoff logic
        │       - Query builders
        │       - Retry policies
```

### Unit Tests

**Coverage**:
- `core/models.py` – Data class validation
- `retry/backoff.py` – Backoff calculation (exponential, linear)
- `retry/policies.py` – Retry policy validation
- `db/queries.py` – SQL query builders
- `utils.py` – ID generation, serialization

**Example**:
```python
# tests/unit/test_backoff.py

def test_exponential_backoff():
    backoff = ExponentialBackoff(initial_delay=1, max_delay=300)
    assert backoff.calculate_delay(0) == 1
    assert backoff.calculate_delay(1) == 2
    assert backoff.calculate_delay(2) == 4
    assert backoff.calculate_delay(10) == 300  # Capped at max_delay

def test_linear_backoff():
    backoff = LinearBackoff(initial_delay=5, max_delay=300)
    assert backoff.calculate_delay(0) == 5
    assert backoff.calculate_delay(1) == 10
    assert backoff.calculate_delay(2) == 15
    assert backoff.calculate_delay(60) == 300  # Capped
```

### Integration Tests

**Coverage**:
- Database operations (create, read, update, delete)
- Task submission and retrieval
- Worker task polling and execution
- Retry logic (task failure and resubmission)
- Dead letter queue operations
- Worker health checks

**Example**:
```python
# tests/integration/test_task_queue.py

@pytest.mark.asyncio
async def test_submit_and_execute_task():
    # Setup
    queue = TaskQueue(database_url=TEST_DB_URL)
    worker = Worker(database_url=TEST_DB_URL)
    
    executed_tasks = []
    
    @worker.task("test_task")
    async def handle_task(payload):
        executed_tasks.append(payload)
        return {"status": "done"}
    
    # Submit task
    task_id = queue.submit(
        task_type="test_task",
        payload={"message": "hello"}
    )
    
    # Run worker (1 iteration)
    await worker.run_once()
    
    # Verify
    assert len(executed_tasks) == 1
    assert executed_tasks[0]["message"] == "hello"
    
    # Verify task status
    task = queue.get_task(task_id)
    assert task.status == TaskStatus.COMPLETED
```

### End-to-End Tests

**Coverage**:
- Full workflow: Submit → Poll → Execute → Retry → Complete
- Multi-worker scenarios (multiple tasks, multiple workers)
- Graceful shutdown
- Worker crash recovery
- Docker Compose environment

**Example**:
```python
# tests/e2e/test_full_workflow.py

@pytest.mark.asyncio
async def test_full_workflow_with_retry():
    """
    1. Submit task that fails 2 times
    2. Task retries automatically
    3. Task succeeds on 3rd attempt
    4. Task marked as completed
    """
    queue = TaskQueue(database_url=TEST_DB_URL)
    worker = Worker(database_url=TEST_DB_URL)
    
    attempt_count = 0
    
    @worker.task("flaky_task")
    async def flaky_handler(payload):
        nonlocal attempt_count
        attempt_count += 1
        if attempt_count < 3:
            raise Exception("Simulated failure")
        return {"attempt": attempt_count}
    
    # Submit with retry policy
    task_id = queue.submit(
        task_type="flaky_task",
        payload={},
        retry_policy={
            "max_retries": 3,
            "backoff": "exponential",
            "initial_delay": 0.1
        }
    )
    
    # Poll until complete (with timeout)
    timeout = time.time() + 30
    while time.time() < timeout:
        task = queue.get_task(task_id)
        if task.status == TaskStatus.COMPLETED:
            break
        await worker.run_once()
        await asyncio.sleep(0.5)
    
    # Verify
    assert task.status == TaskStatus.COMPLETED
    assert task.attempts == 3
```

### Performance Tests

**Goals**:
- Task submission: <2ms per task
- Task polling latency: <500ms
- Task processing (empty task): <10ms
- Throughput: 400+ tasks/sec per worker
- Memory per worker: ~50MB base + payload

**Tools**:
- pytest + pytest-benchmark
- Apache JMeter (load testing)
- Custom scripts (Docker Compose + monitoring)

**Example**:
```python
# tests/perf/test_throughput.py

@pytest.mark.asyncio
async def test_submission_throughput(benchmark):
    """Measure task submission throughput."""
    queue = TaskQueue(database_url=TEST_DB_URL)
    
    async def submit_tasks():
        for _ in range(1000):
            await queue.submit_async(
                task_type="perf_test",
                payload={}
            )
    
    result = benchmark(asyncio.run, submit_tasks())
    # Expected: ~1000 tasks in <5 seconds (~2ms per task)
```

### Test Infrastructure

**Test Database**:
- Use Docker for isolated test PostgreSQL instance
- Auto-cleanup before/after each test
- pytest fixtures for setup/teardown

**Fixtures**:
```python
# tests/conftest.py

@pytest.fixture
async def test_queue():
    queue = TaskQueue(database_url=TEST_DB_URL)
    await queue.init()  # Create tables
    yield queue
    await queue.cleanup()  # Drop tables

@pytest.fixture
async def test_worker(test_queue):
    worker = Worker(database_url=TEST_DB_URL)
    yield worker
    await worker.shutdown()
```

---

## Deployment & Packaging

### Package Distribution

#### PyPI Package
```
Package: conductor-task-queue
Version: 0.1.0
Python: >=3.11
```

**Setup.py**:
```python
setup(
    name="conductor-task-queue",
    version="0.1.0",
    description="Lightweight async task queue for Python",
    author="Panagiotis Panageas",
    packages=find_packages(),
    install_requires=[
        "asyncpg>=0.28.0",          # PostgreSQL async driver
        "aiohttp>=3.9.0",           # Async HTTP client
        "pydantic>=2.0",            # Data validation
        "prometheus-client>=0.18",  # Metrics
        "python-dotenv>=1.0",       # Env vars
        "croniter>=1.4",            # Cron parser (v0.2+)
    ],
    extras_require={
        "dev": [
            "pytest>=7.0",
            "pytest-asyncio>=0.21",
            "pytest-cov>=4.0",
            "black>=23.0",
            "mypy>=1.0",
            "flake8>=6.0",
        ]
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ]
)
```

### Deployment Options

#### 1. Docker Container
```dockerfile
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y postgresql-client
RUN pip install conductor-task-queue

COPY .env .
COPY worker_config.py .

CMD ["python", "-m", "conductor.worker"]
```

#### 2. Docker Compose (Development)
```yaml
version: '3.8'

services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: conductor
      POSTGRES_PASSWORD: password
    ports:
      - "5432:5432"
    volumes:
      - conductor_data:/var/lib/postgresql/data

  conductor_worker:
    build: .
    depends_on:
      - postgres
    environment:
      DATABASE_URL: postgresql://postgres:password@postgres:5432/conductor
    command: python -m conductor.worker

volumes:
  conductor_data:
```

#### 3. Kubernetes Deployment
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: conductor-worker
spec:
  replicas: 3
  selector:
    matchLabels:
      app: conductor-worker
  template:
    metadata:
      labels:
        app: conductor-worker
    spec:
      containers:
      - name: conductor
        image: myregistry/conductor:0.1.0
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: conductor-secrets
              key: database_url
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 10
          periodSeconds: 10
        resources:
          requests:
            memory: "128Mi"
            cpu: "250m"
          limits:
            memory: "512Mi"
            cpu: "500m"
```

#### 4. Systemd Service
```ini
[Unit]
Description=Conductor Worker
After=network.target postgresql.service

[Service]
Type=simple
User=conductor
WorkingDirectory=/opt/conductor
Environment="DATABASE_URL=postgresql://..."
ExecStart=/usr/local/bin/python -m conductor.worker
Restart=on-failure
RestartSec=10s
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

### Environment Configuration

**.env.example**:
```env
# Database
DATABASE_URL=postgresql://postgres:password@localhost:5432/conductor

# Worker
WORKER_ID=worker-1
CONCURRENCY=10
POLL_INTERVAL=0.5
ROUTES=default

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json

# Metrics
METRICS_PORT=8000
METRICS_ENABLED=true

# Health
HEALTH_PORT=8000
HEALTH_ENABLED=true

# Application
TASK_TIMEOUT=300
GRACEFUL_SHUTDOWN_TIMEOUT=30
```

### CI/CD Pipeline (GitHub Actions)

**.github/workflows/test.yml**:
```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    
    services:
      postgres:
        image: postgres:15
        env:
          POSTGRES_PASSWORD: password
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
        ports:
          - 5432:5432
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.11'
    
    - name: Install dependencies
      run: |
        pip install -e ".[dev]"
    
    - name: Lint
      run: |
        black --check .
        flake8 .
        mypy src/
    
    - name: Run tests
      env:
        DATABASE_URL: postgresql://postgres:password@localhost:5432/postgres
      run: |
        pytest tests/ --cov=conductor --cov-report=xml
    
    - name: Upload coverage
      uses: codecov/codecov-action@v3
```

**.github/workflows/release.yml**:
```yaml
name: Release

on:
  push:
    tags:
      - 'v*'

jobs:
  release:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.11'
    
    - name: Build package
      run: |
        pip install build
        python -m build
    
    - name: Publish to PyPI
      uses: pypa/gh-action-pypi-publish@release/v1
      with:
        password: ${{ secrets.PYPI_API_TOKEN }}
```

---

## Performance & Optimization

### Benchmarks & Goals

| Metric | Target | Current | Status |
|--------|--------|---------|--------|
| Task submission | <2ms | TBD | 🔄 |
| Task polling latency | <500ms | 500ms (polling interval) | ✅ |
| Task processing (empty) | <10ms | TBD | 🔄 |
| Throughput per worker | 400+ tasks/sec | TBD | 🔄 |
| Memory per worker | ~50MB | TBD | 🔄 |
| Database CPU (100 tasks/sec) | <20% | TBD | 🔄 |

### Optimization Strategies

#### 1. Database Query Optimization
- **Index Strategy**: All filtered columns must be indexed
- **Batch Operations**: Poll multiple tasks in one query
- **Connection Pooling**: Reuse connections (asyncpg pool)
- **Query Plans**: Analyze slow queries with EXPLAIN

**Implementation**:
```python
# Instead of polling one task at a time
async def poll_tasks_batch(self, limit: int = 10) -> List[Task]:
    query = """
        SELECT * FROM conductor_tasks
        WHERE status = 'pending'
        AND scheduled_for <= NOW()
        ORDER BY priority DESC, created_at ASC
        LIMIT %s
        FOR UPDATE SKIP LOCKED
    """
    rows = await self.db.fetch(query, limit)
    return [Task(**row) for row in rows]
```

#### 2. Async I/O Optimization
- **asyncpg**: Faster than psycopg2 for async operations
- **Connection Reuse**: Maintain pool, don't create connections per query
- **Batch Commits**: Group writes into transactions
- **No Blocking Calls**: All I/O must be async (aiohttp, asyncpg)

#### 3. Worker Concurrency
- **Concurrency Limit**: Configurable max concurrent tasks per worker
- **Asyncio Task Scheduling**: Let asyncio schedule I/O-bound tasks
- **No Threads**: Avoid thread pool overhead

**Implementation**:
```python
class Worker:
    def __init__(self, concurrency: int = 10):
        self.concurrency = concurrency
        self._semaphore = asyncio.Semaphore(concurrency)
    
    async def execute_task(self, task: Task):
        async with self._semaphore:
            # Only N tasks execute concurrently
            await self._run_handler(task)
```

#### 4. Polling Interval Tuning
- **Trade-off**: Lower interval = lower latency, higher DB load
- **Recommended**: 0.5 seconds (balance latency and load)
- **Configurable**: Allow users to adjust based on their needs

#### 5. Batch Retries
- **Move multiple retries to queue in one transaction**
- **Avoid per-task database round-trips**

---

## Summary: Development Checklist

### Phase 1 (v0.1) – MVP
- [ ] Database & models (Sprint 1)
- [ ] TaskQueue API (Sprint 2)
- [ ] Worker implementation (Sprint 3)
- [ ] Retry logic & DLQ (Sprint 4)
- [ ] Observability (Sprint 5)
- [ ] Integration & documentation (Sprint 6)
- [ ] Publish to PyPI

### Phase 2 (v0.2) – Advanced Features
- [ ] Task routing
- [ ] Priority queues
- [ ] Scheduled & recurring tasks
- [ ] Web dashboard
- [ ] gRPC API
- [ ] Circuit breaker
- [ ] Task dependencies

### Phase 3 (v0.3+) – Enterprise
- [ ] Multi-database support (MySQL, SQLite)
- [ ] Distributed tracing (OpenTelemetry)
- [ ] Conductor Cloud (SaaS)
- [ ] Advanced workflows (DAGs)
- [ ] Task versioning

---

## Conclusion

Conductor is a green-field opportunity to build a modern, reliable task queue that fills the gap between simple (RQ) and complex (Celery/dramatiq) solutions. By focusing on PostgreSQL as the single source of truth and async-native Python, we can deliver a product that's:

- **Simple**: One-liner API for users
- **Reliable**: Exactly-once semantics, no data loss
- **Observable**: Structured logs, metrics, health checks
- **Deployable**: No external dependencies

The roadmap prioritizes MVP features first (Phase 1), then advanced features (Phase 2), and finally enterprise/scaling concerns (Phase 3). Each phase builds on the previous one, allowing for iterative delivery and user feedback.

**Success is measured by**:
1. Passing all tests (85%+ coverage)
2. Meeting performance targets (400+ tasks/sec)
3. Zero data loss on crashes
4. Productive developer experience
5. Clear, comprehensive documentation

Good luck, and happy building! 🚀
