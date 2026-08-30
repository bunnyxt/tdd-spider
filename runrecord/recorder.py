"""
RunRecorder -- one structured, persisted row per script start.

Usage (see 51_hourly-video-record-add.py):

    recorder = RunRecorder.start('51_hourly-video-record-add')
    recorder.attach_log_locations()
    try:
        ... do the work ...
        recorder.add_job_stat_metrics('record-fetch', fetch_stat)
        recorder.finish('succeeded')
    except Exception:
        recorder.finish('failed')
        raise

Every method is best-effort: any failure is logged at WARNING and swallowed, and
`RunRecorder.start` returns a disabled (no-op) recorder if the database cannot be
opened. Recording is an index over the logs, never a thing that can break a run.
"""

import logging
import os
import socket
import subprocess
import uuid
from datetime import datetime, timezone
from typing import Optional

from ._sqlite import sqlite3
from . import schema

__all__ = ['RunRecorder', 'display_status', 'RUNNING', 'SUCCEEDED', 'FAILED']

logger = logging.getLogger('runrecord')

_BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DEFAULT_DB_PATH = os.path.join(_BASE_DIR, 'data', 'run-records.sqlite3')

RUNNING = 'running'
SUCCEEDED = 'succeeded'
FAILED = 'failed'
_TERMINAL = (SUCCEEDED, FAILED)

# a run still 'running' this long after it started is shown as 'stale' by the
# query side (process killed / power loss / interpreter could not finish).
DEFAULT_STALE_AFTER_S = 3 * 60 * 60


def _utc_now_iso() -> str:
    # e.g. '2026-08-29T22:45:00.123456+00:00' -- ISO 8601 UTC with a +00:00 offset
    return datetime.now(timezone.utc).isoformat()


def _parse_iso(value: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def _code_version() -> Optional[str]:
    try:
        out = subprocess.run(
            ['git', 'rev-parse', '--short', 'HEAD'],
            cwd=_BASE_DIR, capture_output=True, text=True, timeout=5)
        return out.stdout.strip() or None
    except Exception:
        return None


def _resolve_db_path(db_path: Optional[str]) -> str:
    return db_path or os.environ.get('TDD_RUN_RECORD_DB') or DEFAULT_DB_PATH


class RunRecorder:
    def __init__(self, conn=None, run_id: Optional[str] = None):
        self._conn = conn
        self.run_id = run_id
        self.enabled = conn is not None

    @classmethod
    def start(cls, script_name: str, db_path: Optional[str] = None) -> 'RunRecorder':
        try:
            if sqlite3 is None:
                raise RuntimeError('no SQLite driver available '
                                   '(stdlib sqlite3 missing, pysqlite3 not installed)')
            path = _resolve_db_path(db_path)
            parent = os.path.dirname(path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            conn = sqlite3.connect(path, timeout=30)
            schema.init(conn)
            run_id = uuid.uuid4().hex
            conn.execute(
                'INSERT INTO run (run_id, script_name, host, code_version, '
                'started_at, finished_at, status) '
                'VALUES (?, ?, ?, ?, ?, NULL, ?)',
                (run_id, script_name, socket.gethostname(), _code_version(),
                 _utc_now_iso(), RUNNING))
            conn.commit()
            logger.info(f'run record started: run_id={run_id}, db={path}')
            return cls(conn, run_id)
        except Exception as e:
            logger.warning(f'run record disabled: could not start ({e!r})')
            return cls(None, None)

    def attach_log_locations(self, source_logger: Optional[logging.Logger] = None) -> None:
        """Record the baseFilename of every active FileHandler, keyed by level."""
        if not self.enabled:
            return
        try:
            root = source_logger if source_logger is not None else logging.getLogger()
            rows = [
                (self.run_id, logging.getLevelName(h.level), h.baseFilename)
                for h in root.handlers
                if isinstance(h, logging.FileHandler)
            ]
            if rows:
                self._conn.executemany(
                    'INSERT OR IGNORE INTO run_log (run_id, level, path) VALUES (?, ?, ?)',
                    rows)
                self._conn.commit()
        except Exception as e:
            logger.warning(f'run record: failed to attach log locations ({e!r})')

    def add_metric(self, scope: str, name: str, value: float,
                   unit: Optional[str] = None) -> None:
        if not self.enabled:
            return
        try:
            self._conn.execute(
                'INSERT OR REPLACE INTO run_metric (run_id, scope, name, value, unit) '
                'VALUES (?, ?, ?, ?, ?)',
                (self.run_id, scope, name, float(value), unit))
            self._conn.commit()
        except Exception as e:
            logger.warning(f'run record: failed to add metric {scope}/{name} ({e!r})')

    def add_job_stat_metrics(self, scope: str, stat) -> None:
        """
        Persist the key counts of one JobStat under `scope`: total_count plus
        every condition counter (the `*_ms` stage-timing accumulators are left
        out -- they are durations, not counts, and not part of this contract).
        """
        if not self.enabled or stat is None:
            return
        try:
            rows = [(self.run_id, scope, 'total_count', float(stat.total_count), 'count')]
            for name, value in stat.condition.items():
                if name.endswith('_ms'):
                    continue
                rows.append((self.run_id, scope, name, float(value), 'count'))
            self._conn.executemany(
                'INSERT OR REPLACE INTO run_metric (run_id, scope, name, value, unit) '
                'VALUES (?, ?, ?, ?, ?)', rows)
            self._conn.commit()
        except Exception as e:
            logger.warning(f'run record: failed to add metrics for scope {scope!r} ({e!r})')

    def finish(self, status: str) -> None:
        if not self.enabled:
            return
        if status not in _TERMINAL:
            logger.warning(f'run record: ignoring non-terminal finish status {status!r}')
            return
        try:
            self._conn.execute(
                'UPDATE run SET finished_at = ?, status = ? WHERE run_id = ?',
                (_utc_now_iso(), status, self.run_id))
            self._conn.commit()
            logger.info(f'run record finished: run_id={self.run_id}, status={status}')
        except Exception as e:
            logger.warning(f'run record: failed to finish ({e!r})')
        finally:
            self._close()

    def _close(self) -> None:
        try:
            if self._conn is not None:
                self._conn.close()
        except Exception:
            pass
        self._conn = None
        self.enabled = False


def display_status(status: str, started_at: str, finished_at: Optional[str] = None,
                   now: Optional[datetime] = None,
                   stale_after_s: int = DEFAULT_STALE_AFTER_S) -> str:
    """
    Query-side display status. Persisted status is only ever running / succeeded
    / failed; a run stuck in 'running' past `stale_after_s` is surfaced as
    'stale'. Read-time only -- nothing writes 'stale'.
    """
    if status != RUNNING:
        return status
    started = _parse_iso(started_at)
    if started is None:
        return RUNNING
    now_dt = now if now is not None else datetime.now(timezone.utc)
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=timezone.utc)
    if (now_dt - started).total_seconds() > stale_after_s:
        return 'stale'
    return RUNNING
