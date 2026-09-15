"""
PostgreSQL DDL and migrations.

The statement text here is the authoritative PostgreSQL schema.
"""

from __future__ import annotations

from conductor.db.ddl import SchemaDDL

CREATE_VERSION_TABLE = """
CREATE TABLE IF NOT EXISTS conductor_version (
    version     INTEGER NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (version)
);
"""

# ---------------------------------------------------------------------------
# v0 → v1 migration
# ---------------------------------------------------------------------------

CREATE_TASKS_TABLE = """
CREATE TABLE IF NOT EXISTS conductor_tasks (
    task_id         TEXT        NOT NULL,
    task_type       TEXT        NOT NULL,
    payload         JSONB       NOT NULL DEFAULT '{}',
    status          TEXT        NOT NULL DEFAULT 'pending',
    priority        INTEGER     NOT NULL DEFAULT 0,
    route           TEXT        NOT NULL DEFAULT 'default',
    attempt         INTEGER     NOT NULL DEFAULT 0,
    max_retries     INTEGER     NOT NULL DEFAULT 3,
    retry_policy    JSONB       NOT NULL DEFAULT '{}',
    depends_on      TEXT[]      NOT NULL DEFAULT '{}',
    scheduled_for   TIMESTAMPTZ,
    worker_id       TEXT,
    result          JSONB,
    error_message   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
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

CREATE_WORKERS_TABLE = """
CREATE TABLE IF NOT EXISTS conductor_workers (
    worker_id           TEXT        NOT NULL,
    status              TEXT        NOT NULL DEFAULT 'idle',
    current_task_id     TEXT,
    hostname            TEXT        NOT NULL DEFAULT '',
    pid                 INTEGER     NOT NULL DEFAULT 0,
    uptime_seconds      REAL        NOT NULL DEFAULT 0.0,
    tasks_processed_total INTEGER   NOT NULL DEFAULT 0,
    tasks_failed_total  INTEGER     NOT NULL DEFAULT 0,
    last_heartbeat      TIMESTAMPTZ,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_workers PRIMARY KEY (worker_id),
    CONSTRAINT chk_worker_status CHECK (
        status IN ('idle', 'processing', 'unhealthy')
    )
);
"""

CREATE_RETRIES_TABLE = """
CREATE TABLE IF NOT EXISTS conductor_retries (
    id              TEXT        NOT NULL,
    task_id         TEXT        NOT NULL,
    attempt         INTEGER     NOT NULL,
    error_message   TEXT,
    scheduled_at    TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_retries PRIMARY KEY (id),
    CONSTRAINT fk_retries_task
        FOREIGN KEY (task_id)
        REFERENCES conductor_tasks (task_id)
        ON DELETE CASCADE
);
"""

CREATE_DEAD_LETTER_TABLE = """
CREATE TABLE IF NOT EXISTS conductor_dead_letter (
    task_id         TEXT        NOT NULL,
    task_type       TEXT        NOT NULL,
    payload         JSONB       NOT NULL DEFAULT '{}',
    error_message   TEXT,
    attempts        INTEGER     NOT NULL DEFAULT 0,
    retry_policy    JSONB       NOT NULL DEFAULT '{}',
    route           TEXT        NOT NULL DEFAULT 'default',
    priority        INTEGER     NOT NULL DEFAULT 0,
    depends_on      TEXT[]      NOT NULL DEFAULT '{}',
    moved_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    discarded       BOOLEAN     NOT NULL DEFAULT FALSE,
    discard_reason  TEXT,
    discarded_at    TIMESTAMPTZ,
    traceparent     TEXT,

    CONSTRAINT pk_dead_letter PRIMARY KEY (task_id)
);
"""

CREATE_RECURRING_TASKS_TABLE = """
CREATE TABLE IF NOT EXISTS conductor_recurring_tasks (
    id              TEXT        NOT NULL,
    task_type       TEXT        NOT NULL,
    payload         JSONB       NOT NULL DEFAULT '{}',
    cron_expression TEXT        NOT NULL,
    route           TEXT        NOT NULL DEFAULT 'default',
    priority        INTEGER     NOT NULL DEFAULT 0,
    retry_policy    JSONB       NOT NULL DEFAULT '{}',
    enabled         BOOLEAN     NOT NULL DEFAULT TRUE,
    next_run_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_run_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

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

# ---------------------------------------------------------------------------
# v1 → v2 migration
# ---------------------------------------------------------------------------

# Statements to add ``route``/``priority`` to the dead-letter table.
# Idempotent (``IF NOT EXISTS``), so it is safe to re-run on an
# already-migrated schema.  The columns let routed/prioritized tasks keep
# their routing metadata when retried from the DLQ.
MIGRATE_V1_TO_V2_SQL = [
    "ALTER TABLE conductor_dead_letter "
    "ADD COLUMN IF NOT EXISTS route TEXT NOT NULL DEFAULT 'default';",
    "ALTER TABLE conductor_dead_letter "
    "ADD COLUMN IF NOT EXISTS priority INTEGER NOT NULL DEFAULT 0;",
]

# ---------------------------------------------------------------------------
# v2 → v3 migration
# ---------------------------------------------------------------------------

# Composite index for the recurring scheduler's hot query
# (``WHERE enabled AND next_run_at <= $1``).  Idempotent (``IF NOT EXISTS``).
MIGRATE_V2_TO_V3_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_recurring_polling"
    " ON conductor_recurring_tasks (enabled, next_run_at);",
]

# ---------------------------------------------------------------------------
# v3 → v4 migration
# ---------------------------------------------------------------------------

# Allow ``cancelled`` in the task status CHECK constraint.  ``DROP CONSTRAINT``
# is idempotent, and re-adding the constraint with the extended status list
# keeps the CHECK up to date on existing v3 databases.
MIGRATE_V3_TO_V4_SQL = [
    "ALTER TABLE conductor_tasks DROP CONSTRAINT chk_task_status;",
    "ALTER TABLE conductor_tasks ADD CONSTRAINT chk_task_status CHECK ("
    " status IN ('pending', 'processing', 'completed', 'failed', 'retrying',"
    " 'cancelled')"
    ");",
]

# ---------------------------------------------------------------------------
# v4 → v5 migration
# ---------------------------------------------------------------------------

# Task dependencies: ``depends_on`` array column on tasks + dead-letter
# (preserved across DLQ retries, like route/priority in v2), a GIN index for
# dependency lookups, and the new ``blocked`` task status.  ``DROP CONSTRAINT``
# is idempotent; re-adding it with ``blocked`` keeps the CHECK up to date.
MIGRATE_V4_TO_V5_SQL = [
    "ALTER TABLE conductor_tasks "
    "ADD COLUMN IF NOT EXISTS depends_on TEXT[] NOT NULL DEFAULT '{}';",
    "CREATE INDEX IF NOT EXISTS idx_tasks_depends_on" " ON conductor_tasks USING GIN (depends_on);",
    "ALTER TABLE conductor_dead_letter "
    "ADD COLUMN IF NOT EXISTS depends_on TEXT[] NOT NULL DEFAULT '{}';",
    "ALTER TABLE conductor_tasks DROP CONSTRAINT chk_task_status;",
    "ALTER TABLE conductor_tasks ADD CONSTRAINT chk_task_status CHECK ("
    " status IN ('pending', 'processing', 'completed', 'failed', 'retrying',"
    " 'cancelled', 'blocked')"
    ");",
]

# ---------------------------------------------------------------------------
# v5 → v6 migration
# ---------------------------------------------------------------------------

# Distributed tracing: the W3C ``traceparent`` of the span that submitted the
# task, so a worker in another process can continue the trace.  The column is
# carried on the dead-letter table too, which keeps the link alive across DLQ
# retries (the same reasoning as ``route``/``priority`` in v2).
MIGRATE_V5_TO_V6_SQL = [
    "ALTER TABLE conductor_tasks " "ADD COLUMN IF NOT EXISTS traceparent TEXT;",
    "ALTER TABLE conductor_dead_letter " "ADD COLUMN IF NOT EXISTS traceparent TEXT;",
]

# ---------------------------------------------------------------------------
# Indexes
# ---------------------------------------------------------------------------

TASK_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_tasks_status" " ON conductor_tasks (status);",
    "CREATE INDEX IF NOT EXISTS idx_tasks_task_type" " ON conductor_tasks (task_type);",
    "CREATE INDEX IF NOT EXISTS idx_tasks_route" " ON conductor_tasks (route);",
    "CREATE INDEX IF NOT EXISTS idx_tasks_priority" " ON conductor_tasks (priority DESC);",
    "CREATE INDEX IF NOT EXISTS idx_tasks_created_at" " ON conductor_tasks (created_at);",
    "CREATE INDEX IF NOT EXISTS idx_tasks_scheduled_for" " ON conductor_tasks (scheduled_for);",
    "CREATE INDEX IF NOT EXISTS idx_tasks_worker_id" " ON conductor_tasks (worker_id);",
    # GIN index for dependency containment lookups (``depends_on @> ARRAY[...]``)
    (
        "CREATE INDEX IF NOT EXISTS idx_tasks_depends_on"
        " ON conductor_tasks USING GIN (depends_on);"
    ),
    # Composite index used by the polling query
    (
        "CREATE INDEX IF NOT EXISTS idx_tasks_polling"
        " ON conductor_tasks"
        " (status, scheduled_for, priority DESC, created_at);"
    ),
]

WORKER_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_workers_status" " ON conductor_workers (status);",
    "CREATE INDEX IF NOT EXISTS idx_workers_last_heartbeat"
    " ON conductor_workers (last_heartbeat);",
]

RETRIES_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_retries_task_id" " ON conductor_retries (task_id);",
    "CREATE INDEX IF NOT EXISTS idx_retries_scheduled_at" " ON conductor_retries (scheduled_at);",
]

DEAD_LETTER_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_dead_letter_discarded" " ON conductor_dead_letter (discarded);",
    "CREATE INDEX IF NOT EXISTS idx_dead_letter_moved_at" " ON conductor_dead_letter (moved_at);",
]

RECURRING_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_recurring_next_run"
    " ON conductor_recurring_tasks (next_run_at);",
    "CREATE INDEX IF NOT EXISTS idx_recurring_enabled" " ON conductor_recurring_tasks (enabled);",
    "CREATE INDEX IF NOT EXISTS idx_recurring_polling"
    " ON conductor_recurring_tasks (enabled, next_run_at);",
]

INDEX_STATEMENTS = [
    *TASK_INDEXES,
    *WORKER_INDEXES,
    *RETRIES_INDEXES,
    *DEAD_LETTER_INDEXES,
    *RECURRING_INDEXES,
]

# ---------------------------------------------------------------------------
# Rollback (v1 → v0)
# ---------------------------------------------------------------------------

ROLLBACK_SQL = [
    "DROP TABLE IF EXISTS conductor_recurring_tasks CASCADE;",
    "DROP TABLE IF EXISTS conductor_dead_letter CASCADE;",
    "DROP TABLE IF EXISTS conductor_retries CASCADE;",
    "DROP TABLE IF EXISTS conductor_workers CASCADE;",
    "DROP TABLE IF EXISTS conductor_tasks CASCADE;",
    "DROP TABLE IF EXISTS conductor_version CASCADE;",
]


DDL = SchemaDDL(
    backend="postgresql",
    version_table=CREATE_VERSION_TABLE,
    create_statements=CREATE_TABLE_STATEMENTS,
    index_statements=INDEX_STATEMENTS,
    migrations={
        1: [],
        2: MIGRATE_V1_TO_V2_SQL,
        3: MIGRATE_V2_TO_V3_SQL,
        4: MIGRATE_V3_TO_V4_SQL,
        5: MIGRATE_V4_TO_V5_SQL,
        6: MIGRATE_V5_TO_V6_SQL,
    },
    rollback_statements=ROLLBACK_SQL,
)
"""The PostgreSQL DDL plan."""
