# Conductor Development TODO

**Project**: Lightweight async task queue for Python (PostgreSQL-backed, no Redis)  
**Version**: 0.1.0 MVP  
**Timeline**: ~6 weeks (6 sprints)  
**Last Updated**: 2025-01-15

---

## Table of Contents

- [Phase 1: MVP (v0.1)](#phase-1-mvp-v01)
- [Phase 2: Advanced Features (v0.2)](#phase-2-advanced-features-v02)
- [Phase 3: Future (v0.3+)](#phase-3-future-v03)
- [Backlog & Non-Priorities](#backlog--non-priorities)

---

## Phase 1: MVP (v0.1)

### Sprint 1: Database & Core Models (Week 1)

#### Setup & Project Structure
- [ ] Create Python package structure
  - [ ] `conductor/__init__.py` (package exports)
  - [ ] `conductor/core/` (models, queue, worker)
  - [ ] `conductor/db/` (database operations)
  - [ ] `conductor/retry/` (retry logic)
  - [ ] `conductor/dlq/` (dead letter queue)
  - [ ] `conductor/observability/` (logging, metrics, health)
  - [ ] `tests/` (unit, integration, e2e, perf)
  - [ ] `examples/` (real-world usage examples)
  - [ ] `docs/` (documentation)

- [ ] Create `setup.py` and `pyproject.toml`
  - [ ] Define package metadata
  - [ ] Add dependencies: asyncpg, aiohttp, pydantic, prometheus-client, python-dotenv
  - [ ] Add dev dependencies: pytest, pytest-asyncio, pytest-cov, black, mypy, flake8

- [ ] Create `.env.example`
  - [ ] DATABASE_URL
  - [ ] WORKER_ID
  - [ ] CONCURRENCY
  - [ ] POLL_INTERVAL
  - [ ] LOG_LEVEL
  - [ ] METRICS_PORT

- [ ] Create `README.md` (placeholder, expand in Sprint 6)

- [ ] Initialize Git repository (if not done)
  - [ ] Create `.gitignore`
  - [ ] Create `.npmignore` (for PyPI)
  - [ ] Add LICENSE (MIT)

#### Database Connection & Schema
- [ ] Implement `conductor/db/connection.py`
  - [ ] Create PostgreSQL connection pool using asyncpg
  - [ ] Implement health check query
  - [ ] Handle connection retries with backoff
  - [ ] Support connection timeout configuration
  - [ ] Add connection pooling (min_size, max_size, timeout)

- [ ] Implement `conductor/db/schema.py`
  - [ ] Define schema version management (`conductor_version` table)
  - [ ] Create migration function for v0 → v1
  - [ ] Implement auto-migration on startup
  - [ ] Create `conductor_tasks` table with all columns and indexes
  - [ ] Create `conductor_workers` table
  - [ ] Create `conductor_retries` table
  - [ ] Create `conductor_dead_letter` table
  - [ ] Create `conductor_recurring_tasks` table (for v0.2)
  - [ ] Add all indexes for performance
  - [ ] Add constraints and checks
  - [ ] Write migration rollback logic

- [ ] Implement `conductor/db/queries.py`
  - [ ] Build type-safe query builders (using raw SQL + asyncpg)
  - [ ] Insert task query
  - [ ] Select pending tasks query
  - [ ] Update task status query
  - [ ] Select completed tasks query
  - [ ] Select failed tasks query
  - [ ] Insert retry history query
  - [ ] Move task to DLQ query
  - [ ] Select DLQ tasks query
  - [ ] Worker heartbeat insert/update
  - [ ] Worker select by ID
  - [ ] Add query parameter validation
  - [ ] Add error handling

#### Data Models
- [ ] Implement `conductor/core/models.py`
  - [ ] Create `TaskStatus` enum (pending, processing, completed, failed, retrying)
  - [ ] Create `Task` dataclass with full schema
  - [ ] Create `RetryPolicy` dataclass
  - [ ] Create `Worker` dataclass
  - [ ] Create `RetryRecord` dataclass
  - [ ] Create `DLQTask` dataclass
  - [ ] Add Pydantic validation for all models
  - [ ] Add JSON serialization/deserialization methods
  - [ ] Add type hints for all fields

#### Utilities & Exceptions
- [ ] Implement `conductor/exceptions.py`
  - [ ] Define `ConductorException` (base)
  - [ ] Define `DatabaseError`
  - [ ] Define `WorkerError`
  - [ ] Define `TaskError`
  - [ ] Define `RetryPolicyError`
  - [ ] Define `ConnectionError`

- [ ] Implement `conductor/utils.py`
  - [ ] Task ID generation (UUID v4)
  - [ ] Timestamp utilities (UTC handling)
  - [ ] Payload serialization (JSON)
  - [ ] Payload deserialization (with error handling)
  - [ ] Correlation ID generation
  - [ ] Hostname/PID utilities for worker identification

#### Unit Tests (Sprint 1)
- [ ] Create test fixtures
  - [ ] `tests/conftest.py` with database setup/teardown
  - [ ] Test database URL configuration
  - [ ] Database initialization fixture
  - [ ] Auto-cleanup after each test

- [ ] Write database tests (`tests/unit/test_db_connection.py`)
  - [ ] Test connection pool creation
  - [ ] Test health check query
  - [ ] Test connection retry on failure
  - [ ] Test connection timeout
  - [ ] Test concurrent connections

- [ ] Write schema tests (`tests/unit/test_db_schema.py`)
  - [ ] Test table creation
  - [ ] Test index creation
  - [ ] Test constraint enforcement
  - [ ] Test idempotent migrations
  - [ ] Test schema version tracking

- [ ] Write model tests (`tests/unit/test_models.py`)
  - [ ] Test Task creation and validation
  - [ ] Test RetryPolicy validation
  - [ ] Test JSON serialization
  - [ ] Test enum values
  - [ ] Test default values

- [ ] Write utility tests (`tests/unit/test_utils.py`)
  - [ ] Test ID generation (uniqueness)
  - [ ] Test serialization/deserialization
  - [ ] Test timestamp handling

**Acceptance Criteria**:
- [ ] Database tables created without errors
- [ ] Connection pool handles 10+ concurrent connections
- [ ] Migrations run idempotently
- [ ] All models have type hints and validation
- [ ] Database module has 80%+ test coverage

---

### Sprint 2: TaskQueue Implementation (Week 2)

#### Core TaskQueue Class
- [ ] Implement `conductor/core/queue.py`
  - [ ] Create `TaskQueue` class
  - [ ] Implement `__init__` (database_url, timeout, max_task_age, log_level)
  - [ ] Implement async context manager (`async with` support)

#### Task Submission
- [ ] Implement `TaskQueue.submit()` method
  - [ ] Accept task_type, payload, retry_policy
  - [ ] Generate unique task ID
  - [ ] Validate retry policy
  - [ ] Insert into `conductor_tasks` table
  - [ ] Return task ID
  - [ ] Handle database errors gracefully
  - [ ] Support optional parameters: scheduled_for, route, priority (v0.2)

- [ ] Implement `TaskQueue.submit_async()` method
  - [ ] Async version for non-blocking submission
  - [ ] Return awaitable task ID

#### Task Queries
- [ ] Implement `TaskQueue.list_pending_tasks(limit=10)`
  - [ ] Query pending tasks from database
  - [ ] Return list of Task objects
  - [ ] Support pagination (limit, offset)

- [ ] Implement `TaskQueue.list_completed_tasks(limit=10)`
  - [ ] Query completed tasks from database
  - [ ] Return list of Task objects

- [ ] Implement `TaskQueue.list_failed_tasks(limit=10)`
  - [ ] Query failed tasks from database
  - [ ] Return list of Task objects

- [ ] Implement `TaskQueue.get_task(task_id)`
  - [ ] Query single task by ID
  - [ ] Return Task object or None
  - [ ] Raise TaskError if not found (option)

#### Advanced Features (v0.2 prep, skip for v0.1)
- [ ] Implement `TaskQueue.schedule_recurring()` [SKIP FOR V0.1]
  - Accept cron expression
  - Store in `conductor_recurring_tasks` table

#### Retry Policy Handling
- [ ] Implement retry policy validation
  - [ ] Check max_retries >= 0
  - [ ] Check backoff strategy is valid (exponential, linear, fixed)
  - [ ] Check delays are positive
  - [ ] Set defaults (max_retries=3, exponential, initial_delay=1, max_delay=3600)

#### Configuration
- [ ] Support configuration via environment variables
  - [ ] DATABASE_URL (required)
  - [ ] TASK_TIMEOUT (default: 300)
  - [ ] MAX_TASK_AGE (default: 86400)

#### Integration Tests (Sprint 2)
- [ ] Write integration tests (`tests/integration/test_queue.py`)
  - [ ] Test task submission
  - [ ] Test task retrieval
  - [ ] Test task listing (pending, completed, failed)
  - [ ] Test retry policy validation
  - [ ] Test task uniqueness
  - [ ] Test concurrent submissions
  - [ ] Test database error handling

**Acceptance Criteria**:
- [ ] Tasks submitted successfully
- [ ] Task IDs are unique and persistent
- [ ] Retry policies validated before submission
- [ ] List operations return correct task status
- [ ] TaskQueue has 85%+ test coverage

---

### Sprint 3: Worker Implementation (Week 3-4)

#### Core Worker Class
- [ ] Implement `conductor/core/worker.py`
  - [ ] Create `Worker` class
  - [ ] Implement `__init__` (database_url, worker_id, concurrency, poll_interval, routes, log_level)
  - [ ] Implement async context manager support

#### Task Handler Registration
- [ ] Implement `@worker.task(task_type)` decorator
  - [ ] Register task handler function
  - [ ] Support async functions
  - [ ] Validate handler signature
  - [ ] Store handlers in registry dict

#### Worker Event Loop
- [ ] Implement `worker.run()` method
  - [ ] Start asyncio event loop
  - [ ] Begin polling for tasks
  - [ ] Run indefinitely until shutdown signal
  - [ ] Handle SIGTERM and SIGINT gracefully

- [ ] Implement `worker.run_once()` method (for testing)
  - [ ] Single iteration of poll + execute
  - [ ] Useful for testing and debugging

#### Task Polling
- [ ] Implement `_poll_tasks()` method
  - [ ] Query database for pending tasks
  - [ ] Filter by route (if specified)
  - [ ] Order by priority DESC, created_at ASC (v0.2)
  - [ ] Use FOR UPDATE SKIP LOCKED for atomicity
  - [ ] Respect poll_interval (default 500ms)
  - [ ] Handle empty results gracefully
  - [ ] Limit batch size (e.g., 10 tasks per poll)

#### Task Execution
- [ ] Implement `_execute_task()` method
  - [ ] Update task status to "processing"
  - [ ] Record worker_id and started_at
  - [ ] Find registered handler for task_type
  - [ ] Call handler with task payload
  - [ ] Handle handler errors and exceptions
  - [ ] Update task status to "completed"
  - [ ] Store result in database
  - [ ] Record completed_at timestamp

#### Concurrency Control
- [ ] Implement concurrency limiting with asyncio.Semaphore
  - [ ] Respect max concurrent tasks (default: 10)
  - [ ] Queue tasks internally when at limit
  - [ ] Release semaphore on completion

#### Worker Heartbeat
- [ ] Implement `_heartbeat()` coroutine
  - [ ] Update worker record in database every N seconds
  - [ ] Record current_task_id (if processing)
  - [ ] Record status (idle, processing, unhealthy)
  - [ ] Record uptime
  - [ ] Track tasks_processed_total, tasks_failed_total
  - [ ] Run as background task (concurrent with polling)

#### Graceful Shutdown
- [ ] Implement `shutdown()` method
  - [ ] Set shutdown flag
  - [ ] Stop accepting new tasks
  - [ ] Wait for in-flight tasks to complete (with timeout)
  - [ ] Close database connection
  - [ ] Exit cleanly

- [ ] Implement signal handlers
  - [ ] Handle SIGTERM
  - [ ] Handle SIGINT (Ctrl+C)
  - [ ] Trigger graceful shutdown

#### Worker Status
- [ ] Implement `worker.get_status()` method
  - [ ] Return worker health info
  - [ ] Include uptime, tasks processed, errors
  - [ ] Include current task (if any)

#### Configuration
- [ ] Support configuration via environment variables
  - [ ] WORKER_ID (default: hostname-pid)
  - [ ] CONCURRENCY (default: 10)
  - [ ] POLL_INTERVAL (default: 0.5)
  - [ ] ROUTES (default: ["default"])
  - [ ] GRACEFUL_SHUTDOWN_TIMEOUT (default: 30)

#### Integration Tests (Sprint 3)
- [ ] Write integration tests (`tests/integration/test_worker.py`)
  - [ ] Test worker startup
  - [ ] Test task handler registration
  - [ ] Test task polling
  - [ ] Test task execution (success)
  - [ ] Test task status transitions
  - [ ] Test concurrency limiting
  - [ ] Test graceful shutdown (in-flight task completion)
  - [ ] Test SIGTERM/SIGINT handling
  - [ ] Test heartbeat updates
  - [ ] Test multiple workers
  - [ ] Test worker crash recovery

#### End-to-End Tests (Sprint 3)
- [ ] Write E2E tests (`tests/e2e/test_submit_and_execute.py`)
  - [ ] Submit task → Worker polls → Executes → Completes
  - [ ] Verify task status transitions
  - [ ] Verify result storage

**Acceptance Criteria**:
- [ ] Worker starts and polls for tasks
- [ ] Tasks execute with correct handler
- [ ] Status transitions are correct
- [ ] Concurrency limit respected
- [ ] Graceful shutdown completes in-flight tasks
- [ ] Worker has 85%+ test coverage

---

### Sprint 4: Retry Logic & Dead Letter Queue (Week 4-5)

#### Retry Policies
- [ ] Implement `conductor/retry/policies.py`
  - [ ] Create `RetryPolicy` class
  - [ ] Implement `RetryPolicy.get_backoff_strategy()`
  - [ ] Support exponential, linear, fixed strategies
  - [ ] Implement policy validation
  - [ ] Default policy: max_retries=3, exponential, initial_delay=1, max_delay=3600

#### Backoff Strategies
- [ ] Implement `conductor/retry/backoff.py`
  - [ ] Create `BackoffStrategy` base class
  - [ ] Implement `ExponentialBackoff`
    - [ ] Formula: initial_delay * (2 ^ attempt), capped at max_delay
    - [ ] Implement `calculate_delay(attempt_number)`
  - [ ] Implement `LinearBackoff`
    - [ ] Formula: initial_delay + (initial_delay * attempt), capped at max_delay
    - [ ] Implement `calculate_delay(attempt_number)`
  - [ ] Implement `FixedBackoff`
    - [ ] Always return initial_delay
    - [ ] Implement `calculate_delay(attempt_number)`

#### Failed Task Handling
- [ ] Implement `_handle_task_failure()` in Worker
  - [ ] Record error message
  - [ ] Insert into `conductor_retries` table
  - [ ] Check if max_retries exceeded
  - [ ] Calculate next retry time (with backoff)
  - [ ] Schedule retry (update task status to "retrying")
  - [ ] Move to DLQ if max retries exceeded

#### Retry Scheduling
- [ ] Implement retry task re-submission
  - [ ] Calculate delay using backoff strategy
  - [ ] Set `scheduled_for` to current_time + delay
  - [ ] Update task status to "retrying"
  - [ ] Increment attempt counter
  - [ ] Keep task in `conductor_tasks` (don't delete)

#### Dead Letter Queue
- [ ] Implement `conductor/dlq/dead_letter_queue.py`
  - [ ] Create `DeadLetterQueue` class
  - [ ] Implement `__init__` (database_url)

- [ ] Implement `dlq.list_tasks(limit=10, offset=0)`
  - [ ] Query `conductor_dead_letter` table
  - [ ] Return list of DLQTask objects
  - [ ] Support pagination

- [ ] Implement `dlq.get_task(task_id)`
  - [ ] Query single DLQ task
  - [ ] Return DLQTask object or None

- [ ] Implement `dlq.retry_task(task_id)`
  - [ ] Remove from DLQ
  - [ ] Reset task status to "pending"
  - [ ] Reset attempts to 0
  - [ ] Re-add to main queue
  - [ ] Preserve original payload

- [ ] Implement `dlq.discard_task(task_id, reason)`
  - [ ] Mark as permanently discarded
  - [ ] Store discard reason
  - [ ] Record timestamp

#### Unit Tests (Sprint 4)
- [ ] Write backoff strategy tests (`tests/unit/test_backoff.py`)
  - [ ] Test exponential backoff (1, 2, 4, 8, 16, ...)
  - [ ] Test linear backoff (5, 10, 15, 20, ...)
  - [ ] Test fixed backoff (5, 5, 5, 5, ...)
  - [ ] Test max_delay capping

- [ ] Write retry policy tests (`tests/unit/test_retry_policies.py`)
  - [ ] Test policy validation
  - [ ] Test default values
  - [ ] Test strategy selection

#### Integration Tests (Sprint 4)
- [ ] Write DLQ tests (`tests/integration/test_dlq.py`)
  - [ ] Test task move to DLQ
  - [ ] Test list DLQ tasks
  - [ ] Test retry from DLQ
  - [ ] Test discard from DLQ

- [ ] Write retry tests (`tests/integration/test_retry.py`)
  - [ ] Test task failure and retry
  - [ ] Test backoff delays
  - [ ] Test max retries limit
  - [ ] Test DLQ move on exhausted retries

#### End-to-End Tests (Sprint 4)
- [ ] Write retry E2E tests (`tests/e2e/test_retry_workflow.py`)
  - [ ] Task fails 2x, succeeds on 3rd attempt
  - [ ] Verify retry delays respect backoff
  - [ ] Verify move to DLQ on exhausted retries
  - [ ] Verify task can be retried from DLQ

**Acceptance Criteria**:
- [ ] Retries execute after correct delays
- [ ] Failed tasks move to DLQ after max retries
- [ ] DLQ tasks can be manually retried
- [ ] Backoff strategies calculate correctly
- [ ] Retry logic has 85%+ test coverage

---

### Sprint 5: Observability (Logging, Metrics, Health) (Week 5)

#### Structured Logging
- [ ] Implement `conductor/observability/logging.py`
  - [ ] Set up JSON logging formatter
  - [ ] Create structured logger with context
  - [ ] Add correlation ID tracking
  - [ ] Log all task state transitions:
    - [ ] task_submitted (submitted by user)
    - [ ] task_polling (picked up by worker)
    - [ ] task_started (execution began)
    - [ ] task_completed (execution succeeded)
    - [ ] task_failed (execution failed)
    - [ ] task_retrying (scheduled for retry)
    - [ ] task_dlq (moved to dead letter queue)
  - [ ] Include in all logs:
    - [ ] timestamp
    - [ ] level (DEBUG, INFO, WARNING, ERROR)
    - [ ] task_id
    - [ ] task_type
    - [ ] worker_id (if applicable)
    - [ ] duration_ms
    - [ ] status
    - [ ] error_message (if failed)
  - [ ] Support log_level configuration (DEBUG, INFO, WARNING, ERROR)
  - [ ] Use Python's logging module

#### Prometheus Metrics
- [ ] Implement `conductor/observability/metrics.py`
  - [ ] Create `MetricsExporter` class
  - [ ] Implement HTTP endpoint (FastAPI or aiohttp)
  - [ ] Listen on configurable port (default: 8000)
  - [ ] Export metrics in Prometheus text format

- [ ] Implement metrics (counters)
  - [ ] `conductor_tasks_submitted_total` (counter)
  - [ ] `conductor_tasks_completed_total` (counter)
  - [ ] `conductor_tasks_failed_total` (counter)
  - [ ] `conductor_tasks_retried_total` (counter)

- [ ] Implement metrics (histograms)
  - [ ] `conductor_task_duration_seconds` (histogram with buckets)

- [ ] Implement metrics (gauges)
  - [ ] `conductor_workers_active` (gauge)
  - [ ] `conductor_dlq_size` (gauge)
  - [ ] `conductor_pending_tasks` (gauge)

- [ ] Implement metrics collection
  - [ ] Record metric on task submit
  - [ ] Record metric on task completion
  - [ ] Record metric on task failure
  - [ ] Record metric on task retry
  - [ ] Update gauge on worker heartbeat
  - [ ] Update gauge periodically (DLQ size, pending tasks)

#### Health Checks
- [ ] Implement `conductor/observability/health.py`
  - [ ] Create `HealthChecker` class
  - [ ] Implement HTTP endpoint (FastAPI or aiohttp)
  - [ ] Listen on configurable port (default: 8000, can share with metrics)

- [ ] Implement health endpoint GET /health
  - [ ] Response structure:
    ```json
    {
      "status": "healthy|degraded|unhealthy",
      "database": "connected|disconnected",
      "pending_tasks": 42,
      "dead_letter_queue": 3,
      "workers_active": 5,
      "uptime_seconds": 3600,
      "last_check": "2025-01-15T10:30:45Z"
    }
    ```
  - [ ] Check database connectivity
  - [ ] Count pending tasks
  - [ ] Count DLQ size
  - [ ] Count active workers (heartbeat within 30s)
  - [ ] Return "unhealthy" if database down
  - [ ] Return "degraded" if DLQ size > threshold
  - [ ] Return "healthy" otherwise

#### Configuration
- [ ] Support logging configuration via environment variables
  - [ ] LOG_LEVEL (default: INFO)
  - [ ] LOG_FORMAT (default: json, alternative: text)

- [ ] Support metrics configuration
  - [ ] METRICS_ENABLED (default: true)
  - [ ] METRICS_PORT (default: 8000)

- [ ] Support health check configuration
  - [ ] HEALTH_ENABLED (default: true)
  - [ ] HEALTH_PORT (default: 8000, shared with metrics)

#### Integration Tests (Sprint 5)
- [ ] Write logging tests (`tests/integration/test_logging.py`)
  - [ ] Test JSON log format
  - [ ] Test correlation ID inclusion
  - [ ] Test all event types logged

- [ ] Write metrics tests (`tests/integration/test_metrics.py`)
  - [ ] Test metrics endpoint returns 200
  - [ ] Test counter increments on events
  - [ ] Test histogram records durations
  - [ ] Test gauge updates

- [ ] Write health check tests (`tests/integration/test_health.py`)
  - [ ] Test health endpoint returns 200
  - [ ] Test healthy status
  - [ ] Test degraded status (high DLQ)
  - [ ] Test unhealthy status (database down)

#### Documentation (Sprint 5)
- [ ] Create example Grafana dashboard JSON
  - [ ] Task throughput graph
  - [ ] Task latency graph
  - [ ] Error rate graph
  - [ ] Active workers gauge

**Acceptance Criteria**:
- [ ] Logs include task_id, task_type, worker_id, duration_ms
- [ ] Prometheus metrics exportable via HTTP
- [ ] Health endpoint returns correct status
- [ ] Observability has 80%+ test coverage
- [ ] Example Grafana dashboard provided

---

### Sprint 6: Integration, Documentation, & Release (Week 6-7)

#### Full Integration Testing
- [ ] Create comprehensive E2E test suite
  - [ ] Test file: `tests/e2e/test_full_workflow.py`
  - [ ] Test: Submit → Poll → Execute → Complete
  - [ ] Test: Submit → Fail → Retry → Complete
  - [ ] Test: Submit → Fail all retries → DLQ
  - [ ] Test: Multiple workers processing tasks
  - [ ] Test: Graceful shutdown
  - [ ] Test: Worker crash recovery
  - [ ] Test: Concurrent task execution
  - [ ] Test: Task observability (logs + metrics)

#### Performance Benchmarking
- [ ] Create performance test suite (`tests/perf/`)
  - [ ] Benchmark task submission throughput
    - [ ] Target: <2ms per task
    - [ ] Measure: 1000 submissions
  - [ ] Benchmark task polling latency
    - [ ] Target: <500ms (polling interval)
    - [ ] Measure: Time from submit to poll detection
  - [ ] Benchmark task execution (empty task)
    - [ ] Target: <10ms
    - [ ] Measure: Execution + status update time
  - [ ] Benchmark overall throughput
    - [ ] Target: 400+ tasks/sec per worker
    - [ ] Measure: Tasks submitted and completed per second
  - [ ] Memory usage per worker
    - [ ] Target: ~50MB base
    - [ ] Measure: psutil memory tracking

- [ ] Use pytest-benchmark for consistent results

#### Docker & Deployment
- [ ] Create `Dockerfile`
  - [ ] Base: python:3.11-slim
  - [ ] Install dependencies
  - [ ] Copy conductor package
  - [ ] Expose metrics/health ports (8000)
  - [ ] Entry point: worker run command

- [ ] Create `docker-compose.yml` (development example)
  - [ ] PostgreSQL 15 service
  - [ ] Conductor worker service
  - [ ] Volume for data persistence
  - [ ] Network configuration
  - [ ] Environment variables

- [ ] Create `docker-compose.prod.yml` (production example)
  - [ ] Multiple worker replicas
  - [ ] PostgreSQL with backup
  - [ ] Health checks
  - [ ] Resource limits
  - [ ] Logging configuration

- [ ] Create `examples/kubernetes.yaml`
  - [ ] Deployment manifest
  - [ ] ConfigMap for configuration
  - [ ] Secret for database URL
  - [ ] Service for metrics/health
  - [ ] Liveness/readiness probes

#### Documentation
- [ ] Update `README.md`
  - [ ] Add badges (Python version, PostgreSQL, MIT, PyPI)
  - [ ] Add quick start (installation, basic example)
  - [ ] Add feature highlights
  - [ ] Add comparison table (Celery, RQ, dramatiq)
  - [ ] Add links to docs

- [ ] Create comprehensive documentation (`docs/`)
  - [ ] `docs/installation.md`
    - [ ] pip install
    - [ ] Database setup
    - [ ] Worker startup
  - [ ] `docs/api-reference.md`
    - [ ] TaskQueue API
    - [ ] Worker API
    - [ ] DeadLetterQueue API
  - [ ] `docs/deployment.md`
    - [ ] Docker
    - [ ] Docker Compose
    - [ ] Kubernetes
    - [ ] Systemd
  - [ ] `docs/troubleshooting.md`
    - [ ] Common issues
    - [ ] Debug tips
    - [ ] Logging configuration
  - [ ] `docs/configuration.md`
    - [ ] All environment variables
    - [ ] All configuration options

#### Real-World Examples
- [ ] Create 5+ example scripts (`examples/`)
  - [ ] `examples/1_basic_queue.py`
    - [ ] Simple submit and execute
  - [ ] `examples/2_email_notifications.py`
    - [ ] Send emails with retry
    - [ ] Use aiohttp for SendGrid API
  - [ ] `examples/3_data_processing.py`
    - [ ] Multi-step pipeline
    - [ ] Task chaining (v0.2 feature, show pattern)
  - [ ] `examples/4_scheduled_cleanup.py`
    - [ ] Scheduled task pattern (manual scheduling)
    - [ ] Cron simulation
  - [ ] `examples/5_error_handling.py`
    - [ ] Error handling patterns
    - [ ] Custom exception handling
    - [ ] Idempotency patterns

- [ ] Each example should include:
  - [ ] Clear problem statement
  - [ ] Complete, runnable code
  - [ ] Comments explaining logic
  - [ ] Expected output
  - [ ] README with instructions

#### CI/CD Pipeline
- [ ] Create `.github/workflows/test.yml`
  - [ ] Trigger: push, pull_request
  - [ ] Python 3.11, 3.12 matrix
  - [ ] PostgreSQL service
  - [ ] Run linting (black, flake8)
  - [ ] Run type checking (mypy)
  - [ ] Run tests (pytest with coverage)
  - [ ] Upload coverage to codecov

- [ ] Create `.github/workflows/release.yml`
  - [ ] Trigger: tag push (v*)
  - [ ] Build package
  - [ ] Publish to PyPI

#### Package Publishing
- [ ] Update version in `setup.py` to 0.1.0
- [ ] Update CHANGELOG.md
  - [ ] Document all features
  - [ ] Document all bugfixes
  - [ ] Document breaking changes (none for v0.1)
- [ ] Create GitHub Release
  - [ ] Tag: v0.1.0
  - [ ] Release notes: markdown summary
  - [ ] Upload PyPI package
- [ ] Publish to PyPI via GitHub Actions
- [ ] Update `README.md` with PyPI badge

#### Final Testing & QA
- [ ] Run full test suite locally
  - [ ] Unit tests (85%+ coverage)
  - [ ] Integration tests
  - [ ] E2E tests
  - [ ] Performance tests
- [ ] Manual testing
  - [ ] Start worker from fresh install
  - [ ] Submit tasks programmatically
  - [ ] Verify execution and logs
  - [ ] Verify metrics endpoint
  - [ ] Verify health endpoint
- [ ] Documentation review
  - [ ] Check all links work
  - [ ] Verify code examples run
  - [ ] Grammar and spelling check
- [ ] Test Docker deployment
  - [ ] Build Docker image
  - [ ] Run docker-compose example
  - [ ] Verify worker starts
  - [ ] Verify tasks execute

#### Project Cleanup
- [ ] Update `.gitignore` (no secrets, venv, etc.)
- [ ] Create CONTRIBUTING.md
  - [ ] Development setup
  - [ ] Running tests
  - [ ] Code style
  - [ ] Pull request process
- [ ] Create CODE_OF_CONDUCT.md
- [ ] Create SECURITY.md
  - [ ] Reporting vulnerabilities
  - [ ] Security policies

**Acceptance Criteria**:
- [ ] All examples run without errors
- [ ] Benchmarks show 400+ tasks/sec/worker
- [ ] Documentation complete and accurate
- [ ] Integration tests pass
- [ ] Performance tests pass
- [ ] Docker Compose example works
- [ ] Published to PyPI
- [ ] GitHub Release created

---

## Phase 2: Advanced Features (v0.2)

### Sprint 1: Task Routing & Priority Queues (Week 1-2)

#### Task Routing
- [ ] Add `route` column to `conductor_tasks` table
- [ ] Update task submission to accept `route` parameter
- [ ] Update polling query to filter by route
- [ ] Update Worker to accept `routes` parameter
- [ ] Update worker configuration (ROUTES env var)
- [ ] Write tests for routing
- [ ] Update documentation with routing examples

#### Priority Queues
- [ ] Add `priority` column to `conductor_tasks` table
- [ ] Update task submission to accept `priority` parameter
- [ ] Update polling query to order by priority
- [ ] Set default priority (0)
- [ ] Document priority range (-100 to 100)
- [ ] Write tests for priority ordering
- [ ] Add priority to examples

---

### Sprint 2: Scheduled & Recurring Tasks (Week 2-4)

#### Scheduled Tasks
- [ ] Add `scheduled_for` column to `conductor_tasks` table (already in schema)
- [ ] Update task submission to accept `scheduled_for` parameter
- [ ] Update polling query to filter `scheduled_for <= NOW()`
- [ ] Write tests for scheduled execution
- [ ] Update documentation with scheduled task examples

#### Recurring Tasks
- [ ] Create `conductor_recurring_tasks` table (already in schema)
- [ ] Create recurring task scheduler daemon
  - [ ] Query tasks with `next_run_at <= NOW()`
  - [ ] Create task instance for each recurring definition
  - [ ] Calculate next_run_at using cron expression
  - [ ] Run as background process
- [ ] Implement `queue.schedule_recurring(task_type, cron_expression, payload)`
- [ ] Use croniter library for cron parsing
- [ ] Write tests for recurring task execution
- [ ] Update documentation with recurring examples

---

### Sprint 3: gRPC API (Week 4-5)

#### Protocol Buffers
- [ ] Create `proto/conductor.proto`
  - [ ] Define ConductorWorker service
  - [ ] Define TaskRequest message
  - [ ] Define TaskResponse message
  - [ ] Compile proto files

#### gRPC Server
- [ ] Add gRPC server to Worker
- [ ] Implement ProcessTask RPC
- [ ] Implement RegisterHandler RPC
- [ ] Handle task execution via gRPC
- [ ] Configuration for gRPC port

#### Tests & Documentation
- [ ] Write gRPC integration tests
- [ ] Create examples in other languages (Go, Rust, Node.js - stubs)
- [ ] Document gRPC API

---

### Sprint 4: Web Dashboard (Week 5-8)

#### Backend API (FastAPI)
- [ ] Create `conductor/api/` module
- [ ] GET /api/tasks (list all tasks)
- [ ] GET /api/tasks/{id} (get task details)
- [ ] GET /api/workers (list active workers)
- [ ] GET /api/metrics (expose Prometheus metrics as JSON)
- [ ] GET /api/dlq (list DLQ tasks)
- [ ] POST /api/dlq/{id}/retry (retry DLQ task)
- [ ] POST /api/tasks/{id}/cancel (cancel pending task)

#### Frontend (React + Vite)
- [ ] Create `conductor/web/` frontend directory
- [ ] Tasks overview page
  - [ ] Filter by status
  - [ ] Search by task_id or type
  - [ ] Pagination
- [ ] Task details page
  - [ ] Payload display
  - [ ] Retry history
  - [ ] Logs
  - [ ] Result
- [ ] Workers status page
  - [ ] List active workers
  - [ ] Worker uptime
  - [ ] Tasks processed
- [ ] Metrics page
  - [ ] Task throughput graph
  - [ ] Latency graph
  - [ ] Error rate graph
  - [ ] Worker count gauge
- [ ] DLQ page
  - [ ] List failed tasks
  - [ ] Retry failed task
  - [ ] Discard task

#### Testing & Deployment
- [ ] Integration tests for API endpoints
- [ ] Build and serve frontend
- [ ] Docker image includes frontend assets

---

### Sprint 5: Circuit Breaker (Week 8-9)

#### Circuit Breaker Pattern
- [ ] Add circuit breaker state tracking
- [ ] Track failures per task_type
- [ ] Transition states: Closed → Open → Half-Open → Closed
- [ ] Configuration: threshold, timeout, half_open_attempts
- [ ] Reject new submissions when Open
- [ ] Update worker to respect circuit state
- [ ] Write comprehensive tests
- [ ] Update documentation with circuit breaker examples

---

### Sprint 6: Task Dependencies (Week 9-10)

#### Dependency Tracking
- [ ] Add `depends_on` array column to `conductor_tasks`
- [ ] Update task submission to accept `depends_on` parameter
- [ ] Create dependency resolution logic
- [ ] Mark task as "blocked" if dependency fails
- [ ] Update polling query to filter unmet dependencies
- [ ] Handle transitive dependencies
- [ ] Write comprehensive tests
- [ ] Update documentation with chaining examples

---

## Phase 3: Future (v0.3+)

### Multi-Database Support

- [ ] MySQL/MariaDB backend
  - [ ] Rewrite queries for MySQL syntax
  - [ ] Test thoroughly
  - [ ] Document

- [ ] SQLite backend
  - [ ] Implement for embedded deployments
  - [ ] Single-server only (no distributed polling)
  - [ ] Test thoroughly

### Distributed Tracing (OpenTelemetry)

- [ ] Add OpenTelemetry instrumentation
- [ ] Span creation for task submit/execute/retry
- [ ] Export to Jaeger, Datadog, etc.
- [ ] Update documentation

### Conductor Cloud (SaaS)

- [ ] Design multi-tenant architecture
- [ ] Implement workspace isolation
- [ ] API authentication
- [ ] Billing system
- [ ] Deployment infrastructure

### Advanced Workflows

- [ ] DAG-based workflows
- [ ] Workflow definition language
- [ ] Conditional execution
- [ ] Parallel execution
- [ ] Workflow versioning

---

## Backlog & Non-Priorities

### Explicitly NOT Planned for v0.1

- [ ] Web dashboard (v0.2)
- [ ] gRPC API (v0.2)
- [ ] Circuit breaker (v0.2)
- [ ] Task dependencies (v0.2)
- [ ] Scheduled/recurring tasks (v0.2)
- [ ] Multi-database support (v0.3)
- [ ] Distributed tracing (v0.3)
- [ ] SaaS offering (v0.3)
- [ ] Workflow orchestration (v0.3)

### Nice-to-Have (Consider for Future)

- [ ] Task prioritization with weighted fair queuing
- [ ] Task rate limiting (per task_type)
- [ ] Cost tracking (per task, per worker)
- [ ] A/B testing framework (route % to new workers)
- [ ] Task versioning & rollback
- [ ] Built-in authentication (API keys)
- [ ] RBAC (role-based access control)
- [ ] Webhook notifications on task completion
- [ ] Email alerts for DLQ
- [ ] Task result caching
- [ ] Bulk task submission API
- [ ] Task pause/resume
- [ ] Timeout enforcement at task level

---

## Progress Tracking

### Phase 1 Milestone Checklist

- **Sprint 1 (Week 1)**: Database & Models
  - [ ] Submitted for review
  - [ ] Code review passed
  - [ ] Tests pass (80%+ coverage)
  - [ ] Ready to merge

- **Sprint 2 (Week 2)**: TaskQueue
  - [ ] Submitted for review
  - [ ] Code review passed
  - [ ] Integration tests pass
  - [ ] Ready to merge

- **Sprint 3 (Week 3-4)**: Worker
  - [ ] Submitted for review
  - [ ] Code review passed
  - [ ] E2E tests pass
  - [ ] Graceful shutdown working
  - [ ] Ready to merge

- **Sprint 4 (Week 4-5)**: Retry & DLQ
  - [ ] Submitted for review
  - [ ] Code review passed
  - [ ] All retry scenarios tested
  - [ ] DLQ operations tested
  - [ ] Ready to merge

- **Sprint 5 (Week 5)**: Observability
  - [ ] Submitted for review
  - [ ] Code review passed
  - [ ] Logging working end-to-end
  - [ ] Metrics exportable
  - [ ] Health checks passing
  - [ ] Ready to merge

- **Sprint 6 (Week 6-7)**: Integration & Release
  - [ ] All E2E tests pass
  - [ ] Performance benchmarks meet targets
  - [ ] Documentation complete
  - [ ] Examples working
  - [ ] Docker image builds successfully
  - [ ] Published to PyPI
  - [ ] GitHub Release created

### Version Release Checklist

**For v0.1.0**:
- [ ] All Phase 1 tasks completed
- [ ] Tests: 85%+ coverage, all passing
- [ ] Documentation: Complete and accurate
- [ ] Examples: 5+, all working
- [ ] Performance: Benchmarks met
- [ ] PyPI: Published
- [ ] GitHub: Release created
- [ ] License: MIT, included
- [ ] Code of Conduct: Added
- [ ] Contributing guide: Added

---

## Notes & Reminders

### Code Quality Standards

- **Type Hints**: All functions must have type hints (checked by mypy)
- **Test Coverage**: Minimum 85% for core modules
- **Linting**: All code must pass black and flake8
- **Documentation**: Every public API must have docstrings
- **Async**: All I/O must be async (no blocking calls)

### Performance Targets (v0.1)

- Task submission: <2ms per task
- Polling latency: <500ms (dictated by poll_interval)
- Task execution (empty): <10ms
- Throughput: 400+ tasks/sec per worker
- Memory per worker: ~50MB base

### Database Schema Considerations

- All filtered columns must be indexed
- Polling query must use FOR UPDATE SKIP LOCKED (atomicity)
- Batch operations for performance
- Constraints for data integrity
- Version tracking for migrations

### Testing Reminders

- Test database setup/teardown in fixtures
- Mock external services (email, APIs)
- Use pytest-asyncio for async tests
- Include both happy-path and error-path tests
- Performance tests for throughput and latency

---

## Questions & Decisions to Make

1. **Task ID Generation**: UUID v4 or nanoid? (Recommend: UUID v4 for simplicity)
2. **Payload Serialization**: JSON only, or support other formats? (Recommend: JSON only for v0.1)
3. **Connection Pool Size**: Default 10, configurable? (Recommend: Yes, configurable)
4. **Polling Batch Size**: Fetch 1 or N tasks per poll? (Recommend: N=10 for efficiency)
5. **Worker Heartbeat Interval**: Every N seconds? (Recommend: 10 seconds)
6. **Health Check Database Timeout**: How long to wait? (Recommend: 5 seconds)
7. **Metrics Endpoint**: Separate port or same as health? (Recommend: Same port, /metrics and /health)
8. **Default Log Level**: DEBUG or INFO? (Recommend: INFO for production, configurable)

---

**Last Updated**: 2025-01-15  
**Next Review**: After Sprint 1 completion
