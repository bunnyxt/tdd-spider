"""
Tests / reproducible verification for the run-record store (BL-0001).

Run from the repo root:

    python tests/test_run_record.py
    # or
    python -m unittest discover -s tests

Uses only the stdlib (unittest + stdlib sqlite3). On a dev machine the driver
shim in runrecord/_sqlite.py resolves to the stdlib sqlite3; the production
venv-3.11 uses the pysqlite3-binary wheel instead (verified separately on the
Ubuntu 16.04 host).
"""

import logging
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from runrecord import RunRecorder, display_status, schema  # noqa: E402
from runrecord._sqlite import sqlite3  # noqa: E402


class FakeStat:
    """Duck-types job.JobStat for add_job_stat_metrics (total_count + condition)."""

    def __init__(self, total_count, condition):
        self.total_count = total_count
        self.condition = condition


def _connect(path):
    return sqlite3.connect(path)


def _rows(path, query, params=()):
    conn = _connect(path)
    try:
        return conn.execute(query, params).fetchall()
    finally:
        conn.close()


class SchemaTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, 'db.sqlite3')

    def test_init_is_idempotent_and_versioned(self):
        conn = _connect(self.path)
        schema.init(conn)
        schema.init(conn)  # second call must be a no-op
        version = conn.execute('PRAGMA user_version').fetchone()[0]
        self.assertEqual(version, schema.SCHEMA_VERSION)
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertLessEqual({'run', 'run_metric', 'run_log'}, tables)
        conn.close()

    def test_run_table_omits_forbidden_columns(self):
        conn = _connect(self.path)
        schema.init(conn)
        cols = {r[1] for r in conn.execute('PRAGMA table_info(run)')}
        self.assertEqual(cols, {
            'run_id', 'script_name', 'host', 'code_version',
            'started_at', 'finished_at', 'status'})
        for forbidden in ('exit_code', 'summary', 'duration_ms'):
            self.assertNotIn(forbidden, cols)
        conn.close()

    def test_newer_database_is_not_downgraded(self):
        conn = _connect(self.path)
        schema.init(conn)
        # simulate a future schema version written by newer code
        future = schema.SCHEMA_VERSION + 5
        conn.execute(f'PRAGMA user_version = {future}')
        conn.commit()

        with self.assertRaises(schema.SchemaTooNewError):
            schema.init(conn)
        # version must be left exactly as it was, never rewritten backwards
        self.assertEqual(conn.execute('PRAGMA user_version').fetchone()[0], future)
        conn.close()

    def test_start_on_a_newer_database_yields_disabled_recorder(self):
        conn = _connect(self.path)
        schema.init(conn)
        future = schema.SCHEMA_VERSION + 5
        conn.execute(f'PRAGMA user_version = {future}')
        conn.commit()
        conn.close()

        rec = RunRecorder.start('s', db_path=self.path)
        self.assertFalse(rec.enabled)
        # the database on disk is untouched
        self.assertEqual(
            _rows(self.path, 'PRAGMA user_version')[0][0], future)
        self.assertEqual(_rows(self.path, 'SELECT count(*) FROM run')[0][0], 0)

    def test_metric_and_log_semantics(self):
        conn = _connect(self.path)
        schema.init(conn)
        metric_cols = [r[1] for r in conn.execute('PRAGMA table_info(run_metric)')]
        self.assertEqual(metric_cols, ['run_id', 'scope', 'name', 'value', 'unit'])
        log_cols = [r[1] for r in conn.execute('PRAGMA table_info(run_log)')]
        self.assertEqual(log_cols, ['run_id', 'level', 'path'])
        conn.close()


class FreshEnvTest(unittest.TestCase):
    def test_start_creates_nested_db_and_schema(self):
        d = tempfile.mkdtemp()
        path = os.path.join(d, 'data', 'nested', 'run-records.sqlite3')
        self.assertFalse(os.path.exists(path))
        rec = RunRecorder.start('51_hourly-video-record-add', db_path=path)
        self.addCleanup(rec.finish, 'succeeded')
        self.assertTrue(rec.enabled)
        self.assertTrue(os.path.exists(path))
        version = _rows(path, 'PRAGMA user_version')[0][0]
        self.assertEqual(version, schema.SCHEMA_VERSION)


class RunLifecycleTest(unittest.TestCase):
    def setUp(self):
        self.path = os.path.join(tempfile.mkdtemp(), 'db.sqlite3')

    def _core(self):
        return _rows(
            self.path,
            'SELECT run_id, script_name, host, code_version, started_at, '
            'finished_at, status FROM run')

    def test_start_creates_unique_running_row_with_core_fields(self):
        rec = RunRecorder.start('51_hourly-video-record-add', db_path=self.path)
        rows = self._core()
        self.assertEqual(len(rows), 1)
        run_id, script_name, host, code_version, started_at, finished_at, status = rows[0]
        self.assertEqual(run_id, rec.run_id)
        self.assertTrue(run_id)
        self.assertEqual(script_name, '51_hourly-video-record-add')
        self.assertTrue(host)
        # code_version is git-derived; may be None off a checkout but the column exists
        self.assertIn(type(code_version), (str, type(None)))
        self.assertTrue(started_at)
        self.assertIsNone(finished_at)
        self.assertEqual(status, 'running')

        rec2 = RunRecorder.start('51_hourly-video-record-add', db_path=self.path)
        self.assertNotEqual(rec.run_id, rec2.run_id)
        self.assertEqual(len(self._core()), 2)
        rec.finish('succeeded')
        rec2.finish('succeeded')

    def test_timestamps_are_iso8601_utc_with_offset(self):
        rec = RunRecorder.start('s', db_path=self.path)
        rec.finish('succeeded')
        started_at, finished_at = _rows(
            self.path, 'SELECT started_at, finished_at FROM run')[0]
        for value in (started_at, finished_at):
            self.assertTrue(value.endswith('+00:00'), value)
            parsed = datetime.fromisoformat(value)
            self.assertEqual(parsed.utcoffset(), timedelta(0))
        self.assertLessEqual(
            datetime.fromisoformat(started_at), datetime.fromisoformat(finished_at))

    def test_status_lifecycle_succeeded(self):
        rec = RunRecorder.start('s', db_path=self.path)
        self.assertEqual(self._core()[0][6], 'running')
        rec.finish('succeeded')
        self.assertEqual(self._core()[0][6], 'succeeded')

    def test_status_lifecycle_failed(self):
        rec = RunRecorder.start('s', db_path=self.path)
        rec.finish('failed')
        self.assertEqual(self._core()[0][6], 'failed')

    def test_non_terminal_finish_status_ignored(self):
        rec = RunRecorder.start('s', db_path=self.path)
        rec.finish('running')  # not allowed
        self.assertEqual(self._core()[0][6], 'running')
        self.assertIsNone(self._core()[0][5])
        rec.finish('succeeded')


class DisplayStatusTest(unittest.TestCase):
    def test_terminal_status_passes_through(self):
        now = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)
        old = (now - timedelta(hours=99)).isoformat()
        self.assertEqual(display_status('succeeded', old, now=now), 'succeeded')
        self.assertEqual(display_status('failed', old, now=now), 'failed')

    def test_running_within_budget_stays_running(self):
        now = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)
        started = (now - timedelta(minutes=20)).isoformat()
        self.assertEqual(display_status('running', started, now=now), 'running')

    def test_running_past_budget_is_stale(self):
        now = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)
        started = (now - timedelta(hours=5)).isoformat()
        self.assertEqual(display_status('running', started, now=now), 'stale')

    def test_custom_budget(self):
        now = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)
        started = (now - timedelta(minutes=50)).isoformat()
        self.assertEqual(
            display_status('running', started, now=now, stale_after_s=2400), 'stale')


class MetricsTest(unittest.TestCase):
    def setUp(self):
        self.path = os.path.join(tempfile.mkdtemp(), 'db.sqlite3')

    def test_three_job_stat_scopes_recorded(self):
        rec = RunRecorder.start('51_hourly-video-record-add', db_path=self.path)
        stats = {
            'record-fetch': FakeStat(997715, {
                'success': 995500, 'code_error': 2215, 'other_exception': 0,
                'record_dropped_queue_full': 0, 'duration_limit_reached': 0,
                'http_ms': 431900}),  # *_ms must be excluded
            'record-db-writer': FakeStat(995500, {
                'batch_insert': 996, 'batch_insert_fail': 0}),
            'record-video-update': FakeStat(2215, {
                'update_exception': 0, 'duration_limit_reached': 0}),
        }
        for scope, stat in stats.items():
            rec.add_job_stat_metrics(scope, stat)
        rec.finish('succeeded')

        rows = _rows(
            self.path,
            'SELECT run_id, scope, name, value, unit FROM run_metric ORDER BY scope, name')
        by_scope = {}
        for run_id, scope, name, value, unit in rows:
            self.assertEqual(run_id, rec.run_id)
            self.assertEqual(unit, 'count')
            self.assertIsInstance(value, float)
            by_scope.setdefault(scope, {})[name] = value

        self.assertEqual(set(by_scope), set(stats))
        self.assertEqual(by_scope['record-fetch']['total_count'], 997715.0)
        self.assertEqual(by_scope['record-fetch']['success'], 995500.0)
        self.assertEqual(by_scope['record-fetch']['code_error'], 2215.0)
        self.assertNotIn('http_ms', by_scope['record-fetch'])
        self.assertEqual(by_scope['record-db-writer']['batch_insert_fail'], 0.0)
        self.assertEqual(by_scope['record-video-update']['total_count'], 2215.0)

    def test_metric_values_are_numeric_only(self):
        rec = RunRecorder.start('s', db_path=self.path)
        rec.add_metric('scope', 'name', 5, unit='count')
        rec.finish('succeeded')
        (value,) = _rows(self.path, 'SELECT value FROM run_metric')[0]
        self.assertIsInstance(value, float)


class LogLocationTest(unittest.TestCase):
    def test_paths_come_from_active_filehandler_basefilenames(self):
        d = tempfile.mkdtemp()
        db_path = os.path.join(d, 'db.sqlite3')
        info_path = os.path.join(d, '51_202608291200_INFO.log')
        warn_path = os.path.join(d, '51_202608291200_WARNING.log')

        probe = logging.getLogger('runrecord_test_probe')
        probe.handlers = []
        for path, level in ((info_path, logging.INFO), (warn_path, logging.WARNING)):
            h = logging.FileHandler(path, encoding='utf-8')
            h.setLevel(level)
            probe.addHandler(h)
        # a non-file handler must be ignored
        probe.addHandler(logging.StreamHandler())

        rec = RunRecorder.start('s', db_path=db_path)
        rec.attach_log_locations(source_logger=probe)
        rec.finish('succeeded')

        for h in probe.handlers:
            h.close()

        rows = _rows(db_path, 'SELECT run_id, level, path FROM run_log ORDER BY level')
        self.assertEqual(len(rows), 2)
        got = {(level, path) for _, level, path in rows}
        self.assertEqual(got, {
            ('INFO', info_path),
            ('WARNING', warn_path),
        })
        for run_id, _, _ in rows:
            self.assertEqual(run_id, rec.run_id)


class DebugLogLocationTest(unittest.TestCase):
    def test_debug_filehandler_is_captured_when_present(self):
        d = tempfile.mkdtemp()
        db_path = os.path.join(d, 'db.sqlite3')
        debug_path = os.path.join(d, '51_202608291200_DEBUG.log')
        info_path = os.path.join(d, '51_202608291200_INFO.log')

        probe = logging.getLogger('runrecord_test_debug_probe')
        probe.handlers = []
        for path, level in ((debug_path, logging.DEBUG), (info_path, logging.INFO)):
            h = logging.FileHandler(path, encoding='utf-8')
            h.setLevel(level)
            probe.addHandler(h)

        rec = RunRecorder.start('s', db_path=db_path)
        rec.attach_log_locations(source_logger=probe)
        rec.finish('succeeded')
        for h in probe.handlers:
            h.close()

        got = {(lvl, p) for _, lvl, p in
               _rows(db_path, 'SELECT run_id, level, path FROM run_log')}
        self.assertEqual(got, {('DEBUG', debug_path), ('INFO', info_path)})


class SecretHygieneTest(unittest.TestCase):
    def test_nothing_but_the_fixed_structured_fields_is_stored(self):
        d = tempfile.mkdtemp()
        db_path = os.path.join(d, 'db.sqlite3')
        rec = RunRecorder.start('51_hourly-video-record-add', db_path=db_path)
        rec.attach_log_locations()
        rec.add_job_stat_metrics('record-fetch', FakeStat(10, {'success': 10}))
        # a failed finish must persist only status/finished_at -- no message
        rec.finish('failed')

        conn = _connect(db_path)
        # no column anywhere is a free-text message / error / summary sink
        all_cols = set()
        for (table,) in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"):
            all_cols |= {r[1] for r in conn.execute(f'PRAGMA table_info({table})')}
        for banned in ('message', 'error', 'summary', 'traceback', 'stacktrace',
                       'desp', 'sckey', 'exit_code', 'duration_ms'):
            self.assertNotIn(banned, all_cols)

        # dump every stored string value; assert it is only the values we passed
        strings = []
        for table in ('run', 'run_metric', 'run_log'):
            for row in conn.execute(f'SELECT * FROM {table}'):
                strings += [c for c in row if isinstance(c, str)]
        conn.close()
        for s in strings:
            self.assertNotRegex(s, r'(?i)sckey|token|password|secret|ftqq')


class PersistFailureTest(unittest.TestCase):
    def test_unopenable_db_yields_disabled_noop_recorder(self):
        # parent path is a regular file, so makedirs / connect underneath fails
        f = tempfile.NamedTemporaryFile(delete=False)
        f.close()
        bad_path = os.path.join(f.name, 'sub', 'db.sqlite3')

        rec = RunRecorder.start('51_hourly-video-record-add', db_path=bad_path)
        self.assertFalse(rec.enabled)
        self.assertIsNone(rec.run_id)
        # every method must be a silent no-op
        rec.attach_log_locations()
        rec.add_metric('s', 'n', 1)
        rec.add_job_stat_metrics('s', FakeStat(1, {'x': 1}))
        rec.finish('failed')

    def test_start_never_raises_even_with_absurd_path(self):
        rec = RunRecorder.start('s', db_path='/proc/nonexistent/definitely/nope.sqlite3')
        self.assertFalse(rec.enabled)


class RealJobStatTest(unittest.TestCase):
    """If job.JobStat is importable standalone, exercise the real type once."""

    def test_with_real_jobstat(self):
        try:
            from job import JobStat
        except Exception as e:  # import chain pulls service/db on some setups
            self.skipTest(f'job.JobStat not importable standalone: {e!r}')
        path = os.path.join(tempfile.mkdtemp(), 'db.sqlite3')
        stat = JobStat()
        stat.total_count = 10
        stat.condition['success'] = 9
        stat.condition['code_error'] = 1
        stat.condition['http_ms'] = 1234
        rec = RunRecorder.start('s', db_path=path)
        rec.add_job_stat_metrics('record-fetch', stat)
        rec.finish('succeeded')
        names = {r[0] for r in _rows(path, 'SELECT name FROM run_metric')}
        self.assertEqual(names, {'total_count', 'success', 'code_error'})


if __name__ == '__main__':
    unittest.main(verbosity=2)
