"""
Tests for the read-only run-record query CLI (BL-0005).

Run from the repo root:

    python tests/test_run_record_query.py
    # or
    python -m unittest discover -s tests

Stdlib only (unittest + the driver shim, which resolves to stdlib sqlite3 on a
dev machine). The CLI is exercised through ``query.main(argv)`` so exit codes and
rendered output are both asserted.
"""

import contextlib
import io
import json
import os
import sys
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from runrecord import RunRecorder, schema  # noqa: E402
from runrecord import query  # noqa: E402
from runrecord._sqlite import sqlite3  # noqa: E402


def _iso(dt):
    return dt.astimezone(timezone.utc).isoformat()


def _make_db(path):
    conn = sqlite3.connect(path)
    schema.init(conn)
    conn.close()


def _insert_run(path, run_id, script, started_at, finished_at=None,
                status='running', host='H', code_version='abcdef0'):
    conn = sqlite3.connect(path)
    schema.init(conn)
    conn.execute(
        'INSERT INTO run (run_id, script_name, host, code_version, started_at, '
        'finished_at, status) VALUES (?, ?, ?, ?, ?, ?, ?)',
        (run_id, script, host, code_version,
         _iso(started_at) if isinstance(started_at, datetime) else started_at,
         _iso(finished_at) if isinstance(finished_at, datetime) else finished_at,
         status))
    conn.commit()
    conn.close()


def _insert_metric(path, run_id, scope, name, value, unit='count'):
    conn = sqlite3.connect(path)
    conn.execute(
        'INSERT INTO run_metric (run_id, scope, name, value, unit) '
        'VALUES (?, ?, ?, ?, ?)', (run_id, scope, name, float(value), unit))
    conn.commit()
    conn.close()


def _insert_log(path, run_id, level, log_path):
    conn = sqlite3.connect(path)
    conn.execute('INSERT INTO run_log (run_id, level, path) VALUES (?, ?, ?)',
                 (run_id, level, log_path))
    conn.commit()
    conn.close()


def run_cli(argv):
    """Invoke the CLI; return (exit_code, stdout, stderr)."""
    out, err = io.StringIO(), io.StringIO()
    try:
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = query.main(argv)
    except SystemExit as e:  # argparse
        code = e.code if isinstance(e.code, int) else (0 if e.code is None else 1)
    return code, out.getvalue(), err.getvalue()


class _DbCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, 'run-records.sqlite3')
        # seed relative to the real clock so --since/--until (which use the real
        # clock) and the stale derivation line up
        self.now = datetime.now(timezone.utc)

    def seed_basic(self):
        _make_db(self.path)
        # three runs, staggered in time
        _insert_run(self.path, 'aaaa1111' + '0' * 24, '51_hourly-video-record-add',
                    self.now - timedelta(hours=3), self.now - timedelta(hours=2, minutes=52),
                    status='succeeded')
        _insert_run(self.path, 'bbbb2222' + '0' * 24, '12_add-latest-video-with-tid-30',
                    self.now - timedelta(hours=2), self.now - timedelta(hours=2),
                    status='failed')
        _insert_run(self.path, 'cccc3333' + '0' * 24, '15_update-video-info',
                    self.now - timedelta(minutes=30), None, status='running')


class ListTest(_DbCase):
    def test_lists_recent_first(self):
        self.seed_basic()
        code, out, _ = run_cli(['list', '--db', self.path])
        self.assertEqual(code, 0)
        order = [out.index(s) for s in ('15_update-video-info',
                                       '12_add-latest-video-with-tid-30',
                                       '51_hourly-video-record-add')]
        self.assertEqual(order, sorted(order))

    def test_default_command_is_list(self):
        self.seed_basic()
        code, out, _ = run_cli(['--db', self.path])
        self.assertEqual(code, 0)
        self.assertIn('51_hourly-video-record-add', out)

    def test_filter_by_script(self):
        self.seed_basic()
        code, out, _ = run_cli(['list', '--db', self.path,
                                '--script', '51_hourly-video-record-add'])
        self.assertEqual(code, 0)
        self.assertIn('51_hourly-video-record-add', out)
        self.assertNotIn('12_add-latest-video-with-tid-30', out)

    def test_filter_by_persisted_status(self):
        self.seed_basic()
        code, out, _ = run_cli(['list', '--db', self.path, '--status', 'failed'])
        self.assertEqual(code, 0)
        self.assertIn('12_add-latest-video-with-tid-30', out)
        self.assertNotIn('51_hourly-video-record-add', out)

    def test_limit(self):
        self.seed_basic()
        code, out, _ = run_cli(['list', '--db', self.path, '--limit', '1', '--json'])
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out)['count'], 1)

    def test_time_range(self):
        self.seed_basic()
        # only the run started 30 min ago falls in the last hour
        code, out, _ = run_cli(['list', '--db', self.path, '--since', '1h', '--json'])
        self.assertEqual(code, 0)
        runs = json.loads(out)['runs']
        self.assertEqual([r['script_name'] for r in runs], ['15_update-video-info'])

    def test_time_range_iso_until(self):
        self.seed_basic()
        cutoff = _iso(self.now - timedelta(hours=1))
        code, out, _ = run_cli(['list', '--db', self.path, '--until', cutoff, '--json'])
        self.assertEqual(code, 0)
        names = {r['script_name'] for r in json.loads(out)['runs']}
        self.assertEqual(names, {'51_hourly-video-record-add',
                                 '12_add-latest-video-with-tid-30'})

    def test_no_match_exit_code(self):
        self.seed_basic()
        code, out, _ = run_cli(['list', '--db', self.path, '--script', 'nope'])
        self.assertEqual(code, query.EXIT_NO_MATCH)


class StaleDerivationTest(_DbCase):
    def test_running_past_budget_shows_stale(self):
        _make_db(self.path)
        _insert_run(self.path, 'd' * 32, '17_add-member-follower-record',
                    datetime.now(timezone.utc) - timedelta(hours=6), None,
                    status='running')
        code, out, _ = run_cli(['list', '--db', self.path, '--json'])
        self.assertEqual(code, 0)
        run = json.loads(out)['runs'][0]
        self.assertEqual(run['status'], 'running')       # persisted, untouched
        self.assertEqual(run['display_status'], 'stale')  # derived
        self.assertTrue(run['stale'])

    def test_filter_by_stale(self):
        _make_db(self.path)
        _insert_run(self.path, 'a' * 32, 's', datetime.now(timezone.utc) - timedelta(hours=6))
        _insert_run(self.path, 'b' * 32, 's', datetime.now(timezone.utc) - timedelta(minutes=5))
        code, out, _ = run_cli(['list', '--db', self.path, '--status', 'stale', '--json'])
        self.assertEqual(code, 0)
        runs = json.loads(out)['runs']
        self.assertEqual([r['run_id'] for r in runs], ['a' * 32])

    def test_running_filter_excludes_stale(self):
        _make_db(self.path)
        _insert_run(self.path, 'a' * 32, 's', datetime.now(timezone.utc) - timedelta(hours=6))
        _insert_run(self.path, 'b' * 32, 's', datetime.now(timezone.utc) - timedelta(minutes=5))
        code, out, _ = run_cli(['list', '--db', self.path, '--status', 'running', '--json'])
        self.assertEqual(code, 0)
        self.assertEqual([r['run_id'] for r in json.loads(out)['runs']], ['b' * 32])

    def test_custom_stale_after(self):
        _make_db(self.path)
        _insert_run(self.path, 'a' * 32, 's', datetime.now(timezone.utc) - timedelta(minutes=40))
        code, out, _ = run_cli(['list', '--db', self.path, '--stale-after', '1800', '--json'])
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out)['runs'][0]['display_status'], 'stale')


class ShowTest(_DbCase):
    def test_show_by_full_id(self):
        self.seed_basic()
        code, out, _ = run_cli(['show', '--db', self.path, 'aaaa1111' + '0' * 24])
        self.assertEqual(code, 0)
        self.assertIn('51_hourly-video-record-add', out)

    def test_show_by_unique_prefix(self):
        self.seed_basic()
        code, out, _ = run_cli(['show', '--db', self.path, 'aaaa'])
        self.assertEqual(code, 0)
        self.assertIn('51_hourly-video-record-add', out)

    def test_ambiguous_prefix_is_usage_error(self):
        _make_db(self.path)
        _insert_run(self.path, 'ab' + '0' * 30, 's', self.now)
        _insert_run(self.path, 'ac' + '1' * 30, 's', self.now)
        code, _, err = run_cli(['show', '--db', self.path, 'a'])
        self.assertEqual(code, query.EXIT_USAGE)
        self.assertIn('ambiguous', err)

    def test_unknown_id_is_no_match(self):
        self.seed_basic()
        code, _, err = run_cli(['show', '--db', self.path, 'ffff'])
        self.assertEqual(code, query.EXIT_NO_MATCH)

    def test_latest_selects_most_recent(self):
        self.seed_basic()
        code, out, _ = run_cli(['show', '--db', self.path, '--latest'])
        self.assertEqual(code, 0)
        self.assertIn('15_update-video-info', out)

    def test_latest_with_script_filter(self):
        self.seed_basic()
        code, out, _ = run_cli(['show', '--db', self.path, '--latest',
                                '--script', '51_hourly-video-record-add'])
        self.assertEqual(code, 0)
        self.assertIn('51_hourly-video-record-add', out)

    def test_latest_no_match(self):
        self.seed_basic()
        code, _, _ = run_cli(['show', '--db', self.path, '--latest', '--script', 'nope'])
        self.assertEqual(code, query.EXIT_NO_MATCH)

    def test_id_and_latest_together_is_usage_error(self):
        self.seed_basic()
        code, _, err = run_cli(['show', '--db', self.path, 'aaaa', '--latest'])
        self.assertEqual(code, query.EXIT_USAGE)

    def test_neither_id_nor_latest_is_usage_error(self):
        self.seed_basic()
        code, _, err = run_cli(['show', '--db', self.path])
        self.assertEqual(code, query.EXIT_USAGE)

    def test_show_renders_grouped_metrics_and_logs(self):
        _make_db(self.path)
        rid = 'e' * 32
        _insert_run(self.path, rid, '51_hourly-video-record-add',
                    self.now - timedelta(minutes=10), self.now, status='succeeded')
        _insert_metric(self.path, rid, 'record-fetch', 'total_count', 170287)
        _insert_metric(self.path, rid, 'record-fetch', 'other_exception', 877)
        _insert_metric(self.path, rid, 'record-db-writer', 'batch_insert', 170)
        _insert_log(self.path, rid, 'INFO', '/home/ubuntu/tdd-spider/log/51_x_INFO.log')
        _insert_log(self.path, rid, 'WARNING', '/home/ubuntu/tdd-spider/log/51_x_WARNING.log')

        code, out, _ = run_cli(['show', '--db', self.path, rid])
        self.assertEqual(code, 0)
        self.assertIn('record-fetch', out)
        self.assertIn('record-db-writer', out)
        self.assertIn('total_count', out)
        self.assertIn('/home/ubuntu/tdd-spider/log/51_x_INFO.log', out)

        code, out, _ = run_cli(['show', '--db', self.path, rid, '--json'])
        payload = json.loads(out)
        run = payload['runs'][0]
        self.assertEqual(set(run['metrics']), {'record-fetch', 'record-db-writer'})
        self.assertEqual(run['metrics']['record-fetch'][0],
                         {'name': 'other_exception', 'value': 877.0, 'unit': 'count'})
        self.assertEqual(
            [l['level'] for l in run['logs']], ['INFO', 'WARNING'])
        self.assertAlmostEqual(run['duration_s'], 600.0)


class JsonShapeTest(_DbCase):
    def test_list_json_shape(self):
        self.seed_basic()
        code, out, _ = run_cli(['list', '--db', self.path, '--json',
                                '--script', '51_hourly-video-record-add'])
        self.assertEqual(code, 0)
        payload = json.loads(out)
        self.assertEqual(set(payload), {
            'schema_version', 'db_path', 'generated_at', 'query', 'count', 'runs'})
        self.assertEqual(payload['schema_version'], schema.SCHEMA_VERSION)
        self.assertEqual(payload['query']['command'], 'list')
        self.assertEqual(payload['query']['script'], '51_hourly-video-record-add')
        run = payload['runs'][0]
        self.assertEqual(set(run), {
            'run_id', 'script_name', 'host', 'code_version', 'started_at',
            'finished_at', 'status', 'display_status', 'stale', 'duration_s'})
        # timestamps stay ISO-8601 UTC, never reformatted for a terminal
        self.assertTrue(run['started_at'].endswith('+00:00'))
        self.assertNotIn('  ', json.dumps(run))  # no padded table cells leaked in

    def test_json_is_stable_sorted(self):
        self.seed_basic()
        _, out1, _ = run_cli(['list', '--db', self.path, '--json'])
        _, out2, _ = run_cli(['list', '--db', self.path, '--json'])
        d1, d2 = json.loads(out1), json.loads(out2)
        d1.pop('generated_at'), d2.pop('generated_at')
        self.assertEqual(d1, d2)
        # output is the canonical sorted-key rendering of its own content
        canonical = json.dumps(json.loads(out1), indent=2,
                               ensure_ascii=False, sort_keys=True)
        self.assertEqual(out1.strip(), canonical)


class ExitCodeTest(_DbCase):
    def test_success(self):
        self.seed_basic()
        self.assertEqual(run_cli(['list', '--db', self.path])[0], query.EXIT_OK)

    def test_no_match(self):
        self.seed_basic()
        self.assertEqual(
            run_cli(['list', '--db', self.path, '--script', 'x'])[0],
            query.EXIT_NO_MATCH)

    def test_invalid_arguments(self):
        self.assertEqual(
            run_cli(['list', '--db', self.path, '--status', 'bogus'])[0],
            query.EXIT_USAGE)
        self.assertEqual(
            run_cli(['list', '--db', self.path, '--since', 'yesterday'])[0],
            query.EXIT_USAGE)
        self.assertEqual(
            run_cli(['list', '--db', self.path, '--limit', '-2'])[0],
            query.EXIT_USAGE)

    def test_incompatible_schema(self):
        self.seed_basic()
        conn = sqlite3.connect(self.path)
        conn.execute(f'PRAGMA user_version = {schema.SCHEMA_VERSION + 7}')
        conn.commit()
        conn.close()
        code, _, err = run_cli(['list', '--db', self.path])
        self.assertEqual(code, query.EXIT_SCHEMA)
        self.assertIn('schema', err)

    def test_database_error_on_garbage_file(self):
        with open(self.path, 'wb') as f:
            f.write(b'this is definitely not a sqlite database' * 4)
        code, _, err = run_cli(['list', '--db', self.path])
        self.assertEqual(code, query.EXIT_DB_ERROR)

    def test_database_error_on_uninitialised_file(self):
        open(self.path, 'wb').close()  # 0-byte file == empty db, no tables
        code, _, err = run_cli(['list', '--db', self.path])
        self.assertEqual(code, query.EXIT_DB_ERROR)

    def test_missing_database_file(self):
        code, _, err = run_cli(['list', '--db', os.path.join(self.dir, 'absent.sqlite3')])
        self.assertEqual(code, query.EXIT_NO_DB)
        self.assertIn('not found', err)

    def _partial_v1_db(self, drop_table):
        """A v1 database with a valid run + row but one associated table gone."""
        _make_db(self.path)
        _insert_run(self.path, 'a' * 32, '51_hourly-video-record-add',
                    self.now - timedelta(minutes=5), self.now, status='succeeded')
        conn = sqlite3.connect(self.path)
        conn.execute(f'DROP TABLE {drop_table}')
        conn.commit()
        conn.close()

    def test_missing_run_metric_table_is_db_error(self):
        self._partial_v1_db('run_metric')
        for argv in (['show', '--db', self.path, 'a' * 32],
                     ['show', '--db', self.path, 'a' * 32, '--json'],
                     ['show', '--db', self.path, '--latest'],
                     ['list', '--db', self.path],
                     ['list', '--db', self.path, '--json']):
            code, out, err = run_cli(argv)
            self.assertEqual(code, query.EXIT_DB_ERROR, argv)
            self.assertIn('run_metric', err)
            self.assertNotIn('Traceback', err + out)
            self.assertFalse(out.strip(), argv)  # nothing half-rendered on stdout

    def test_missing_run_log_table_is_db_error(self):
        self._partial_v1_db('run_log')
        for argv in (['show', '--db', self.path, 'a' * 32],
                     ['show', '--db', self.path, 'a' * 32, '--json'],
                     ['list', '--db', self.path, '--json']):
            code, out, err = run_cli(argv)
            self.assertEqual(code, query.EXIT_DB_ERROR, argv)
            self.assertIn('run_log', err)
            self.assertNotIn('Traceback', err + out)

    def test_detail_query_error_is_translated_even_if_probe_skipped(self):
        # exercise the defensive wrapper on the detail helpers directly
        self._partial_v1_db('run_metric')
        conn = sqlite3.connect(f'file:{self.path}?mode=ro', uri=True)
        try:
            with self.assertRaises(query.QueryError) as cm:
                query._grouped_metrics(conn, 'a' * 32)
            self.assertEqual(cm.exception.exit_code, query.EXIT_DB_ERROR)
        finally:
            conn.close()


class ReadOnlyTest(_DbCase):
    def test_queries_do_not_touch_the_database(self):
        self.seed_basic()
        before = os.stat(self.path)
        before_rows = _count_rows(self.path)
        time.sleep(0.01)
        for argv in (['list', '--db', self.path],
                     ['list', '--db', self.path, '--json'],
                     ['show', '--db', self.path, '--latest'],
                     ['show', '--db', self.path, 'aaaa']):
            run_cli(argv)
        after = os.stat(self.path)
        self.assertEqual(before.st_mtime, after.st_mtime)
        self.assertEqual(before.st_size, after.st_size)
        self.assertEqual(before_rows, _count_rows(self.path))
        # no side-car files created by a write attempt
        self.assertFalse(os.path.exists(self.path + '-wal'))
        self.assertFalse(os.path.exists(self.path + '-journal'))

    def test_missing_db_is_not_created(self):
        absent = os.path.join(self.dir, 'absent.sqlite3')
        run_cli(['list', '--db', absent])
        self.assertFalse(os.path.exists(absent))


class RecorderIntegrationTest(_DbCase):
    """End-to-end against data written by the real RunRecorder."""

    class _Stat:
        def __init__(self, total_count, condition):
            self.total_count = total_count
            self.condition = condition

    def test_query_reads_real_recorder_output(self):
        rec = RunRecorder.start('51_hourly-video-record-add', db_path=self.path)
        rec.add_job_stat_metrics('record-fetch', self._Stat(
            170287, {'success': 169410, 'other_exception': 877, 'http_ms': 999}))
        rec.finish('succeeded')

        code, out, _ = run_cli(['show', '--db', self.path, '--latest', '--json'])
        self.assertEqual(code, 0)
        run = json.loads(out)['runs'][0]
        self.assertEqual(run['run_id'], rec.run_id)
        self.assertEqual(run['display_status'], 'succeeded')
        self.assertEqual(run['status'], 'succeeded')
        self.assertIn('record-fetch', run['metrics'])
        names = {m['name'] for m in run['metrics']['record-fetch']}
        self.assertEqual(names, {'total_count', 'success', 'other_exception'})
        self.assertNotIn('http_ms', names)  # *_ms excluded upstream


class TimeParseTest(unittest.TestCase):
    def test_relative_spans(self):
        now = datetime(2026, 8, 30, 12, 0, tzinfo=timezone.utc)
        self.assertEqual(query._parse_time('90m', now), now - timedelta(minutes=90))
        self.assertEqual(query._parse_time('2h', now), now - timedelta(hours=2))
        self.assertEqual(query._parse_time('7d', now), now - timedelta(days=7))
        self.assertEqual(query._parse_time('1w', now), now - timedelta(weeks=1))

    def test_iso_values_are_utc(self):
        got = query._parse_time('2026-08-30')
        self.assertEqual(got, datetime(2026, 8, 30, tzinfo=timezone.utc))
        got = query._parse_time('2026-08-30T06:30:00+08:00')
        self.assertEqual(got.utcoffset(), timedelta(0))

    def test_bad_value_raises_usage_error(self):
        with self.assertRaises(query.QueryError) as cm:
            query._parse_time('later')
        self.assertEqual(cm.exception.exit_code, query.EXIT_USAGE)


def _count_rows(path):
    conn = sqlite3.connect(path)
    try:
        return {t: conn.execute(f'SELECT count(*) FROM {t}').fetchone()[0]
                for t in ('run', 'run_metric', 'run_log')}
    finally:
        conn.close()


if __name__ == '__main__':
    unittest.main(verbosity=2)
