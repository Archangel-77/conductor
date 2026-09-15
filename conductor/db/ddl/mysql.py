"""
MySQL/MariaDB DDL and migrations.

Type mapping from PostgreSQL:

==================  ==================================================
PostgreSQL          MySQL/MariaDB
==================  ==================================================
``TEXT`` (id)       ``VARCHAR(64)`` – ``TEXT`` cannot be indexed/PK-ed
``TIMESTAMPTZ``     ``DATETIME(6)`` – UTC by convention
``JSONB``           ``JSON``
``TEXT[]``          ``JSON`` – queried with ``JSON_CONTAINS``
``BOOLEAN``         ``TINYINT(1)`` – coerced back to ``bool`` on read
``REAL``            ``DOUBLE``
==================  ==================================================

Two structural differences from the other backends:

* **Indexes live inside the ``CREATE TABLE`` statements** (``KEY`` clauses).
  MySQL has no ``CREATE INDEX IF NOT EXISTS``, and embedding the indexes makes
  the whole v0 → v1 step idempotent — re-running it is a no-op because
  ``CREATE TABLE IF NOT EXISTS`` skips an existing table entirely
  (``index_statements`` is therefore empty).
* **No index on ``depends_on``**: MySQL cannot index a ``JSON`` column directly,
  and ``JSON_CONTAINS`` predicates cannot use one anyway.

MySQL ships in Conductor after schema v6, so the v0 → v1 step creates the latest
shape directly and the historic steps are no-ops that still record a version row.

**Requires MySQL 8.0.16+** (``CHECK`` constraints) — and 8.0.19+ to run the
``DROP CHECK``-based status migrations.
"""

from __future__ import annotations

from conductor.db.ddl import SchemaDDL

_TABLE_SUFFIX = " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
"""Storage engine and charset applied to every table."""

CREATE_VERSION_TABLE = f"""
CREATE TABLE IF NOT EXISTS conductor_version (
    version     INTEGER NOT NULL,
    applied_at  DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (version)
){_TABLE_SUFFIX};
"""

CREATE_TASKS_TABLE = f"""
CREATE TABLE IF NOT EXISTS conductor_tasks (
    task_id         VARCHAR(64)  NOT NULL,
    task_type       VARCHAR(255) NOT NULL,
    payload         JSON         NOT NULL,
    status          VARCHAR(32)  NOT NULL DEFAULT 'pending',
    priority        INTEGER      NOT NULL DEFAULT 0,
    route           VARCHAR(255) NOT NULL DEFAULT 'default',
    attempt         INTEGER      NOT NULL DEFAULT 0,
    max_retries     INTEGER      NOT NULL DEFAULT 3,
    retry_policy    JSON         NOT NULL,
    depends_on      JSON         NOT NULL,
    scheduled_for   DATETIME(6),
    worker_id       VARCHAR(255),
    result          JSON,
    error_message   TEXT,
    created_at      DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    started_at      DATETIME(6),
    completed_at    DATETIME(6),
    traceparent     VARCHAR(255),

    CONSTRAINT pk_tasks PRIMARY KEY (task_id),
    CONSTRAINT chk_task_status CHECK (
        status IN (
            'pending', 'processing', 'completed', 'failed', 'retrying',
            'cancelled', 'blocked'
        )
    ),
    CONSTRAINT chk_task_priority CHECK (priority >= -100 AND priority <= 100),
    CONSTRAINT chk_task_attempt CHECK (attempt >= 0),
    CONSTRAINT chk_task_max_retries CHECK (max_retries >= 0),

    KEY idx_tasks_status (status),
    KEY idx_tasks_task_type (task_type),
    KEY idx_tasks_route (route),
    KEY idx_tasks_priority (priority DESC),
    KEY idx_tasks_created_at (created_at),
    KEY idx_tasks_scheduled_for (scheduled_for),
    KEY idx_tasks_worker_id (worker_id),
    KEY idx_tasks_polling (status, scheduled_for, priority DESC, created_at)
){_TABLE_SUFFIX};
"""

CREATE_WORKERS_TABLE = f"""
CREATE TABLE IF NOT EXISTS conductor_workers (
    worker_id             VARCHAR(64)  NOT NULL,
    status                VARCHAR(32)  NOT NULL DEFAULT 'idle',
    current_task_id       VARCHAR(64),
    hostname              VARCHAR(255) NOT NULL DEFAULT '',
    pid                   INTEGER      NOT NULL DEFAULT 0,
    uptime_seconds        DOUBLE       NOT NULL DEFAULT 0.0,
    tasks_processed_total INTEGER      NOT NULL DEFAULT 0,
    tasks_failed_total    INTEGER      NOT NULL DEFAULT 0,
    last_heartbeat        DATETIME(6),
    started_at            DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

    CONSTRAINT pk_workers PRIMARY KEY (worker_id),
    CONSTRAINT chk_worker_status CHECK (
        status IN ('idle', 'processing', 'unhealthy')
    ),

    KEY idx_workers_status (status),
    KEY idx_workers_last_heartbeat (last_heartbeat)
){_TABLE_SUFFIX};
"""

CREATE_RETRIES_TABLE = f"""
CREATE TABLE IF NOT EXISTS conductor_retries (
    id              VARCHAR(64) NOT NULL,
    task_id         VARCHAR(64) NOT NULL,
    attempt         INTEGER     NOT NULL,
    error_message   TEXT,
    scheduled_at    DATETIME(6) NOT NULL,
    created_at      DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

    CONSTRAINT pk_retries PRIMARY KEY (id),
    CONSTRAINT fk_retries_task
        FOREIGN KEY (task_id)
        REFERENCES conductor_tasks (task_id)
        ON DELETE CASCADE,

    KEY idx_retries_task_id (task_id),
    KEY idx_retries_scheduled_at (scheduled_at)
){_TABLE_SUFFIX};
"""

CREATE_DEAD_LETTER_TABLE = f"""
CREATE TABLE IF NOT EXISTS conductor_dead_letter (
    task_id         VARCHAR(64)  NOT NULL,
    task_type       VARCHAR(255) NOT NULL,
    payload         JSON         NOT NULL,
    error_message   TEXT,
    attempts        INTEGER      NOT NULL DEFAULT 0,
    retry_policy    JSON         NOT NULL,
    route           VARCHAR(255) NOT NULL DEFAULT 'default',
    priority        INTEGER      NOT NULL DEFAULT 0,
    depends_on      JSON         NOT NULL,
    moved_at        DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    discarded       TINYINT(1)   NOT NULL DEFAULT 0,
    discard_reason  TEXT,
    discarded_at    DATETIME(6),
    traceparent     VARCHAR(255),

    CONSTRAINT pk_dead_letter PRIMARY KEY (task_id),

    KEY idx_dead_letter_discarded (discarded),
    KEY idx_dead_letter_moved_at (moved_at)
){_TABLE_SUFFIX};
"""

CREATE_RECURRING_TASKS_TABLE = f"""
CREATE TABLE IF NOT EXISTS conductor_recurring_tasks (
    id              VARCHAR(64)  NOT NULL,
    task_type       VARCHAR(255) NOT NULL,
    payload         JSON         NOT NULL,
    cron_expression VARCHAR(255) NOT NULL,
    route           VARCHAR(255) NOT NULL DEFAULT 'default',
    priority        INTEGER      NOT NULL DEFAULT 0,
    retry_policy    JSON         NOT NULL,
    enabled         TINYINT(1)   NOT NULL DEFAULT 1,
    next_run_at     DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    last_run_at     DATETIME(6),
    created_at      DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

    CONSTRAINT pk_recurring_tasks PRIMARY KEY (id),

    KEY idx_recurring_next_run (next_run_at),
    KEY idx_recurring_enabled (enabled),
    KEY idx_recurring_polling (enabled, next_run_at)
){_TABLE_SUFFIX};
"""

CREATE_TABLE_STATEMENTS = [
    CREATE_TASKS_TABLE,
    CREATE_WORKERS_TABLE,
    CREATE_RETRIES_TABLE,
    CREATE_DEAD_LETTER_TABLE,
    CREATE_RECURRING_TASKS_TABLE,
]

INDEX_STATEMENTS: list[str] = []
"""Indexes are declared inside the ``CREATE TABLE`` statements (see the module docstring)."""

ROLLBACK_SQL = [
    "DROP TABLE IF EXISTS conductor_recurring_tasks;",
    "DROP TABLE IF EXISTS conductor_dead_letter;",
    "DROP TABLE IF EXISTS conductor_retries;",
    "DROP TABLE IF EXISTS conductor_workers;",
    "DROP TABLE IF EXISTS conductor_tasks;",
    "DROP TABLE IF EXISTS conductor_version;",
]


DDL = SchemaDDL(
    backend="mysql",
    version_table=CREATE_VERSION_TABLE,
    create_statements=CREATE_TABLE_STATEMENTS,
    index_statements=INDEX_STATEMENTS,
    # MySQL ships with the latest shape: every historic step is a no-op.
    migrations={version: [] for version in range(1, 7)},
    rollback_statements=ROLLBACK_SQL,
)
"""The MySQL/MariaDB DDL plan."""
