"""
Tests for the run-record maintenance commands (snapshot + retention).

Run from the repo root:

    python tests/test_run_record_maintain.py
    # or
    python -m unittest discover -s tests

Stdlib only (unittest + the driver shim, which resolves to stdlib sqlite3 on a
dev machine).
"""

import contextlib
import gzip
import hashlib
import io
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from runrecord import schema  # noqa: E402
from runrecord import maintain  # noqa: E402
from runrecord.query import QueryError  # noqa: E402
from runrecord._sqlite import sqlite3  # noqa: E402

NOW = datetime(2026, 10, 10, 12, 0, 0, tzinfo=timezone.utc)


def _make_db(path, runs=()):
    """``runs`` is a sequence of (run_id, started_at datetime)."""
    conn = sqlite3.connect(path)
    schema.init(conn)
    for run_id, started_at in runs:
        conn.execute(
            'INSERT INTO run (run_id, script_name, host, code_version, started_at, '
            'finished_at, status) VALUES (?, ?, ?, ?, ?, ?, ?)',
            (run_id, '51_hourly-video-record-add', 'H', 'abcdef0',
             started_at.isoformat(), started_at.isoformat(), 'succeeded'))
        conn.execute(
            'INSERT INTO run_metric (run_id, scope, name, value, unit) '
            'VALUES (?, ?, ?, ?, ?)', (run_id, 'record-fetch', 'total_count', 1, 'count'))
        conn.execute(
            'INSERT INTO run_log (run_id, level, path) VALUES (?, ?, ?)',
            (run_id, 'INFO', f'/log/{run_id}.log'))
    conn.commit()
    conn.close()


def _rows(path):
    conn = sqlite3.connect(path)
    try:
        return {
            table: sorted(conn.execute(f'SELECT * FROM {table}').fetchall())
            for table in ('run', 'run_metric', 'run_log')
        }
    finally:
        conn.close()


class BackupTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = self._tmp.name
        self.db = os.path.join(self.dir, 'run-records.sqlite3')
        self.out = os.path.join(self.dir, 'snapshots')
        _make_db(self.db, [('a', NOW - timedelta(days=1)), ('b', NOW)])

    def tearDown(self):
        self._tmp.cleanup()

    def test_snapshot_restores_to_identical_rows(self):
        result = maintain.backup(self.db, self.out, now=NOW)

        self.assertEqual(
            os.path.basename(result['path']), 'run-records_20261010T120000Z.sqlite3.gz')
        self.assertEqual(result['runs'], 2)
        self.assertEqual(result['schema_version'], schema.SCHEMA_VERSION)
        self.assertEqual(result['latest_started_at'], NOW.isoformat())

        restored = os.path.join(self.dir, 'restored.sqlite3')
        with gzip.open(result['path'], 'rb') as packed, open(restored, 'wb') as raw:
            raw.write(packed.read())
        self.assertEqual(_rows(restored), _rows(self.db))

    def test_sha256_file_matches_in_sha256sum_format(self):
        result = maintain.backup(self.db, self.out, now=NOW)

        with open(result['path'], 'rb') as f:
            expected = hashlib.sha256(f.read()).hexdigest()
        with open(result['path'] + '.sha256') as f:
            line = f.read()
        self.assertEqual(line, f'{expected}  {os.path.basename(result["path"])}\n')
        self.assertEqual(result['sha256'], expected)

    def test_only_published_files_remain(self):
        maintain.backup(self.db, self.out, now=NOW)
        self.assertEqual(sorted(os.listdir(self.out)), [
            'run-records_20261010T120000Z.sqlite3.gz',
            'run-records_20261010T120000Z.sqlite3.gz.sha256',
        ])

    def test_existing_snapshot_is_not_overwritten(self):
        first = maintain.backup(self.db, self.out, now=NOW)
        with self.assertRaises(QueryError):
            maintain.backup(self.db, self.out, now=NOW)
        self.assertEqual(maintain._sha256(first['path']), first['sha256'])

    def test_failed_integrity_check_publishes_nothing(self):
        with mock.patch.object(maintain, '_integrity_errors',
                               return_value=['page 2 is never used']):
            with self.assertRaises(QueryError) as ctx:
                maintain.backup(self.db, self.out, now=NOW)
        self.assertIn('integrity_check', str(ctx.exception))
        self.assertEqual(os.listdir(self.out), [])

    def test_missing_database_is_refused_and_not_created(self):
        missing = os.path.join(self.dir, 'nope.sqlite3')
        with self.assertRaises(QueryError) as ctx:
            maintain.backup(missing, self.out, now=NOW)
        self.assertEqual(ctx.exception.exit_code, 5)
        self.assertFalse(os.path.exists(missing))

    def test_newer_schema_is_refused(self):
        conn = sqlite3.connect(self.db)
        conn.execute(f'PRAGMA user_version = {schema.SCHEMA_VERSION + 1}')
        conn.commit()
        conn.close()
        with self.assertRaises(QueryError) as ctx:
            maintain.backup(self.db, self.out, now=NOW)
        self.assertEqual(ctx.exception.exit_code, 3)


class PruneTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self._tmp.name, 'run-records.sqlite3')
        _make_db(self.db, [
            ('old', NOW - timedelta(days=31)),
            ('edge', NOW - timedelta(days=30)),
            ('new', NOW - timedelta(days=1)),
        ])

    def tearDown(self):
        self._tmp.cleanup()

    def _run_ids(self):
        return [row[0] for row in _rows(self.db)['run']]

    def test_deletes_only_older_runs_with_their_metrics_and_logs(self):
        result = maintain.prune(self.db, keep_days=30, now=NOW)

        self.assertEqual((result['runs'], result['metrics'], result['logs']), (1, 1, 1))
        rows = _rows(self.db)
        self.assertEqual([r[0] for r in rows['run']], ['edge', 'new'])
        self.assertEqual(sorted({r[0] for r in rows['run_metric']}), ['edge', 'new'])
        self.assertEqual(sorted({r[0] for r in rows['run_log']}), ['edge', 'new'])

    def test_dry_run_deletes_nothing(self):
        before = _rows(self.db)
        result = maintain.prune(self.db, keep_days=30, dry_run=True, now=NOW)
        self.assertEqual(result['runs'], 1)
        self.assertTrue(result['dry_run'])
        self.assertEqual(_rows(self.db), before)

    def test_default_keeps_recent_history(self):
        result = maintain.prune(self.db, now=NOW)
        self.assertEqual(result['runs'], 0)
        self.assertEqual(self._run_ids(), ['edge', 'new', 'old'])

    def test_rejects_non_positive_keep_days(self):
        with self.assertRaises(ValueError):
            maintain.prune(self.db, keep_days=0, now=NOW)
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            maintain.main(['--db', self.db, 'prune', '--keep-days', '0'])
        self.assertEqual(len(self._run_ids()), 3)

    def test_missing_database_is_refused_and_not_created(self):
        missing = os.path.join(self._tmp.name, 'nope.sqlite3')
        with self.assertRaises(QueryError) as ctx:
            maintain.prune(missing, keep_days=30, now=NOW)
        self.assertEqual(ctx.exception.exit_code, 5)
        self.assertFalse(os.path.exists(missing))

    def test_newer_schema_is_refused_without_deleting(self):
        conn = sqlite3.connect(self.db)
        conn.execute(f'PRAGMA user_version = {schema.SCHEMA_VERSION + 1}')
        conn.commit()
        conn.close()
        with self.assertRaises(QueryError) as ctx:
            maintain.prune(self.db, keep_days=30, now=NOW)
        self.assertEqual(ctx.exception.exit_code, 3)
        self.assertEqual(len(self._run_ids()), 3)


class MainTest(unittest.TestCase):
    def test_backup_then_prune_via_cli(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, 'run-records.sqlite3')
            out = os.path.join(tmp, 'snapshots')
            _make_db(db, [('a', datetime.now(timezone.utc))])
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                self.assertEqual(maintain.main(['--db', db, 'backup', '--out-dir', out]), 0)
                self.assertEqual(maintain.main(['--db', db, 'prune', '--dry-run']), 0)
            output = stdout.getvalue()
            self.assertIn('backup ok:', output)
            self.assertIn('runs=1', output)
            self.assertIn('prune ok:', output)
            self.assertIn('would delete runs=0', output)

    def test_missing_database_exit_code(self):
        with tempfile.TemporaryDirectory() as tmp:
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                code = maintain.main(['--db', os.path.join(tmp, 'x.sqlite3'), 'backup',
                                      '--out-dir', tmp])
            self.assertEqual(code, 5)
            self.assertIn('backup error:', stderr.getvalue())


if __name__ == '__main__':
    unittest.main()
