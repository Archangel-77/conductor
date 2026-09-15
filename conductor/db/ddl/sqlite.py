"""
SQLite DDL and migrations.

Type mapping from PostgreSQL:

=================  ==========================================
PostgreSQL         SQLite
=================  ==========================================
``TIMESTAMPTZ``    ``TEXT`` – ``YYYY-MM-DD HH:MM:SS.ffffff`` UTC
``JSONB``          ``TEXT`` – JSON document
``TEXT[]``         ``TEXT`` – JSON array (JSON1 functions)
``BOOLEAN``        ``INTEGER`` – 0/1 (coerced back to ``bool`` on read)
``REAL``           ``REAL``
=================  ==========================================

SQLite shipped in Conductor *after* schema v5, so the v0 → v1 step creates the
latest shape directly and the historic steps are no-ops (they still record a
version row, so the migration ledger matches PostgreSQL).

The PostgreSQL GIN index on ``depends_on`` has no SQLite equivalent — the JSON1
``json_each`` predicates used by the polling query cannot be indexed — so
dependency checks scan the (single-process, single-writer) database.
"""

from __future__ import annotations

from conductor.db.ddl import SchemaDDL

NOW = "DEFAULT (strftime('%Y-%m-%d %H:%M:%f000', 'now'))"
"""SQLite expression for "current UTC timestamp in the storage format".

SQLite's ``%f`` is ``SS.SSS`` (seconds with milliseconds) rather than Python's
six-digit microseconds, so ``000`` pads it to the exact six-digit shape used by
``conductor.db.backends.sqlite.TIMESTAMP_FORMAT`` – which keeps lexicographic
and chronological ordering identical.
"""

CREATE_VERSION_TABLE = f"""
CREATE TABLE IF NOT EXISTS conductor_version (
    version     INTEGER NOT NULL,
    applied_at  TEXT NOT NULL {NOW},
    PRIMARY KEY (version)
);
"""

CREATE_TASKS_TABLE = f"""
CREATE TABLE IF NOT EXISTS conductor_tasks (
    task_id         TEXT    NOT NULL,
    task_type       TEXT    NOT NULL,
    payload         TEXT    NOT NULL DEFAULT '{{}}',
    status          TEXT    NOT NULL DEFAULT 'pending',
    priority        INTEGER NOT NULL DEFAULT 0,
    route           TEXT    NOT NULL DEFAULT 'default',
    attempt         INTEGER NOT NULL DEFAULT 0,
    max_retries     INTEGER NOT NULL DEFAULT 3,
    retry_policy    TEXT    NOT NULL DEFAULT '{{}}',
    depends_on      TEXT    NOT NULL DEFAULT '[]',
    scheduled_for   TEXT,
    worker_id       TEXT,
    result          TEXT,
    error_message   TEXT,
    created_at      TEXT    NOT NULL {NOW},
    started_at      TEXT,
    completed_at    TEXT,
    traceparent     TEXT,

    CONSTRAINT pk_tasks PRIMARY KEY (task_id),
    CONSTRAINT chk_task_status CHECK (
        status IN (
            'pending', 'processing', 'completed', 'failed', 'retrying',
            'cancelled', 'blocked'
        )
    ),
    CONSTRAINT chk_task_priority CHECK (priority >= -100 AND priority <= 100),
    CONSTRAINT chk_task_attempt CHECK (attempt >= 0),
    CONSTRAINT chk_task_max_retries CHECK (max_retries >= 0)
);
"""

CREATE_WORKERS_TABLE = f"""
CREATE TABLE IF NOT EXISTS conductor_workers (
    worker_id             TEXT    NOT NULL,
    status                TEXT    NOT NULL DEFAULT 'idle',
    current_task_id       TEXT,
    hostname              TEXT    NOT NULL DEFAULT '',
    pid                   INTEGER NOT NULL DEFAULT 0,
    uptime_seconds        REAL    NOT NULL DEFAULT 0.0,
    tasks_processed_total INTEGER NOT NULL DEFAULT 0,
    tasks_failed_total    INTEGER NOT NULL DEFAULT 0,
    last_heartbeat        TEXT,
    started_at            TEXT    NOT NULL {NOW},

    CONSTRAINT pk_workers PRIMARY KEY (worker_id),
    CONSTRAINT chk_worker_status CHECK (
        status IN ('idle', 'processing', 'unhealthy')
    )
);
"""

CREATE_RETRIES_TABLE = f"""
CREATE TABLE IF NOT EXISTS conductor_retries (
    id              TEXT    NOT NULL,
    task_id         TEXT    NOT NULL,
    attempt         INTEGER NOT NULL,
    error_message   TEXT,
    scheduled_at    TEXT    NOT NULL,
    created_at      TEXT    NOT NULL {NOW},

    CONSTRAINT pk_retries PRIMARY KEY (id),
    CONSTRAINT fk_retries_task
        FOREIGN KEY (task_id)
        REFERENCES conductor_tasks (task_id)
        ON DELETE CASCADE
);
"""

CREATE_DEAD_LETTER_TABLE = f"""
CREATE TABLE IF NOT EXISTS conductor_dead_letter (
    task_id         TEXT    NOT NULL,
    task_type       TEXT    NOT NULL,
    payload         TEXT    NOT NULL DEFAULT '{{}}',
    error_message   TEXT,
    attempts        INTEGER NOT NULL DEFAULT 0,
    retry_policy    TEXT    NOT NULL DEFAULT '{{}}',
    route           TEXT    NOT NULL DEFAULT 'default',
    priority        INTEGER NOT NULL DEFAULT 0,
    depends_on      TEXT    NOT NULL DEFAULT '[]',
    moved_at        TEXT    NOT NULL {NOW},
    discarded       INTEGER NOT NULL DEFAULT 0,
    discard_reason  TEXT,
    discarded_at    TEXT,
    traceparent     TEXT,

    CONSTRAINT pk_dead_letter PRIMARY KEY (task_id)
);
"""

CREATE_RECURRING_TASKS_TABLE = f"""
CREATE TABLE IF NOT EXISTS conductor_recurring_tasks (
    id              TEXT    NOT NULL,
    task_type       TEXT    NOT NULL,
    payload         TEXT    NOT NULL DEFAULT '{{}}',
    cron_expression TEXT    NOT NULL,
    route           TEXT    NOT NULL DEFAULT 'default',
    priority        INTEGER NOT NULL DEFAULT 0,
    retry_policy    TEXT    NOT NULL DEFAULT '{{}}',
    enabled         INTEGER NOT NULL DEFAULT 1,
    next_run_at     TEXT    NOT NULL {NOW},
    last_run_at     TEXT,
    created_at      TEXT    NOT NULL {NOW},

    CONSTRAINT pk_recurring_tasks PRIMARY KEY (id)
);
"""

CREATE_TABLE_STATEMENTS = [
    CREATE_TASKS_TABLE,
    CREATE_WORKERS_TABLE,
    CREATE_RETRIES_TABLE,
    CREATE_DEAD_LETTER_TABLE,
    CREATE_RECURRING_TASKS_TABLE,
]

TASK_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_tasks_status ON conductor_tasks (status);",
    "CREATE INDEX IF NOT EXISTS idx_tasks_task_type ON conductor_tasks (task_type);",
    "CREATE INDEX IF NOT EXISTS idx_tasks_route ON conductor_tasks (route);",
    "CREATE INDEX IF NOT EXISTS idx_tasks_priority ON conductor_tasks (priority DESC);",
    "CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON conductor_tasks (created_at);",
    "CREATE INDEX IF NOT EXISTS idx_tasks_scheduled_for ON conductor_tasks (scheduled_for);",
    "CREATE INDEX IF NOT EXISTS idx_tasks_worker_id ON conductor_tasks (worker_id);",
    (
        "CREATE INDEX IF NOT EXISTS idx_tasks_polling"
        " ON conductor_tasks (status, scheduled_for, priority DESC, created_at);"
    ),
]

WORKER_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_workers_status ON conductor_workers (status);",
    (
        "CREATE INDEX IF NOT EXISTS idx_workers_last_heartbeat"
        " ON conductor_workers (last_heartbeat);"
    ),
]

RETRIES_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_retries_task_id ON conductor_retries (task_id);",
    "CREATE INDEX IF NOT EXISTS idx_retries_scheduled_at ON conductor_retries (scheduled_at);",
]

DEAD_LETTER_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_dead_letter_discarded ON conductor_dead_letter (discarded);",
    "CREATE INDEX IF NOT EXISTS idx_dead_letter_moved_at ON conductor_dead_letter (moved_at);",
]

RECURRING_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_recurring_next_run ON conductor_recurring_tasks (next_run_at);",
    "CREATE INDEX IF NOT EXISTS idx_recurring_enabled ON conductor_recurring_tasks (enabled);",
    (
        "CREATE INDEX IF NOT EXISTS idx_recurring_polling"
        " ON conductor_recurring_tasks (enabled, next_run_at);"
    ),
]

INDEX_STATEMENTS = [
    *TASK_INDEXES,
    *WORKER_INDEXES,
    *RETRIES_INDEXES,
    *DEAD_LETTER_INDEXES,
    *RECURRING_INDEXES,
]

ROLLBACK_SQL = [
    "DROP TABLE IF EXISTS conductor_recurring_tasks;",
    "DROP TABLE IF EXISTS conductor_dead_letter;",
    "DROP TABLE IF EXISTS conductor_retries;",
    "DROP TABLE IF EXISTS conductor_workers;",
    "DROP TABLE IF EXISTS conductor_tasks;",
    "DROP TABLE IF EXISTS conductor_version;",
]


DDL = SchemaDDL(
    backend="sqlite",
    version_table=CREATE_VERSION_TABLE,
    create_statements=CREATE_TABLE_STATEMENTS,
    index_statements=INDEX_STATEMENTS,
    # SQLite ships with the latest shape: every historic step is a no-op.
    migrations={version: [] for version in range(1, 7)},
    rollback_statements=ROLLBACK_SQL,
)
"""The SQLite DDL plan."""
