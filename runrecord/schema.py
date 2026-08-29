"""
Schema for the run-records SQLite database.

One row in ``run`` per independent script start. Script-specific counters live in
the associated ``run_metric`` table (so the fixed ``run`` columns never grow),
and the active log-file paths live in ``run_log`` (a run may write INFO, WARNING
and optionally DEBUG files at once, so this is a set, not a column).

Deliberately NOT stored: ``exit_code`` (a script cannot reliably read its own),
free-text ``summary`` (it duplicates the structured fields + metrics), and
``duration_ms`` (derivable from ``finished_at - started_at``).

``init`` is idempotent and doubles as the migration entry point: it bumps
``PRAGMA user_version`` and applies any future migration steps in order.
"""

__all__ = ['SCHEMA_VERSION', 'init']

SCHEMA_VERSION = 1

_DDL_V1 = """
CREATE TABLE IF NOT EXISTS run (
    run_id       TEXT PRIMARY KEY,
    script_name  TEXT NOT NULL,
    host         TEXT NOT NULL,
    code_version TEXT,
    started_at   TEXT NOT NULL,
    finished_at  TEXT,
    status       TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_run_script_started ON run (script_name, started_at);
CREATE INDEX IF NOT EXISTS idx_run_status ON run (status);

CREATE TABLE IF NOT EXISTS run_metric (
    run_id TEXT NOT NULL REFERENCES run (run_id),
    scope  TEXT NOT NULL,
    name   TEXT NOT NULL,
    value  REAL NOT NULL,
    unit   TEXT,
    PRIMARY KEY (run_id, scope, name)
);

CREATE TABLE IF NOT EXISTS run_log (
    run_id TEXT NOT NULL REFERENCES run (run_id),
    level  TEXT NOT NULL,
    path   TEXT NOT NULL,
    PRIMARY KEY (run_id, level, path)
);
"""


def init(conn):
    """Create/upgrade the schema on ``conn``. Idempotent."""
    current = conn.execute('PRAGMA user_version').fetchone()[0]

    if current < 1:
        conn.executescript(_DDL_V1)

    # future: `if current < 2: conn.executescript(_DDL_V2)` ...

    if current != SCHEMA_VERSION:
        # PRAGMA does not accept bound parameters
        conn.execute(f'PRAGMA user_version = {SCHEMA_VERSION}')
    conn.commit()
