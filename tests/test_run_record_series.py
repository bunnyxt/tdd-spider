"""
Contract tests for the Phase 1.5 reading layer: the key-metric convention
(``runrecord.keymetric``), the schema-v2 ``run_metric.is_key`` migration
(``runrecord.schema``) and the aligned per-run time-series query
(``runrecord.series``).

Run from the repo root:

    python -m unittest discover -s tests

Stdlib only (unittest + the driver shim, which resolves to stdlib sqlite3 on a
dev machine).
"""

import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from runrecord import schema, series  # noqa: E402
from runrecord.keymetric import is_key_metric  # noqa: E402
from runrecord.recorder import RunRecorder  # noqa: E402
from runrecord import query  # noqa: E402
from runrecord._sqlite import sqlite3  # noqa: E402

BASE = datetime(2026, 8, 1, 12, 0, 0, tzinfo=timezone.utc)


def _iso(dt):
    return dt.astimezone(timezone.utc).isoformat()


class _DbCase(unittest.TestCase):
    def setUp(self):
        self.path = os.path.join(tempfile.mkdtemp(), 'run-records.sqlite3')

    def _conn(self):
        conn = sqlite3.connect(self.path)
        self.addCleanup(conn.close)
        return conn

    def add_run(self, run_id, script, started, finished=None, status='running',
                host='H', code_version='abc1234'):
        conn = sqlite3.connect(self.path)
        schema.init(conn)
        conn.execute(
            'INSERT INTO run (run_id, script_name, host, code_version, '
            'started_at, finished_at, status) VALUES (?, ?, ?, ?, ?, ?, ?)',
            (run_id, script, host, code_version, _iso(started),
             _iso(finished) if finished else None, status))
        conn.commit()
        conn.close()

    def add_metric(self, run_id, scope, name, value, unit='count', is_key=None):
        conn = sqlite3.connect(self.path)
        conn.execute(
            'INSERT OR REPLACE INTO run_metric '
            '(run_id, scope, name, value, unit, is_key) VALUES (?, ?, ?, ?, ?, ?)',
            (run_id, scope, name, float(value), unit, is_key))
        conn.commit()
        conn.close()


# --------------------------------------------------------------------------- #
# key convention
# --------------------------------------------------------------------------- #

class KeyConventionTest(unittest.TestCase):
    def test_name_convention(self):
        for key_name in ('total_count', 'exception', 'other_exception',
                         'code_error', 'batch_insert_fail',
                         'record_dropped_queue_full', 'MillionException'):
            self.assertTrue(is_key_metric(key_name), key_name)
        for plain_name in ('success', 'new_video', 'records_added', '0_update'):
            self.assertFalse(is_key_metric(plain_name), plain_name)

    def test_explicit_flag_overrides_convention(self):
        self.assertTrue(is_key_metric('records_added', True))    # promote
        self.assertFalse(is_key_metric('total_count', False))    # suppress
        self.assertFalse(is_key_metric('other_exception', 0))


# --------------------------------------------------------------------------- #
# schema v2 migration
# --------------------------------------------------------------------------- #

class SchemaMigrationTest(_DbCase):
    def _cols(self):
        return [r[1] for r in self._conn().execute(
            'PRAGMA table_info(run_metric)')]

    def _version(self):
        return self._conn().execute('PRAGMA user_version').fetchone()[0]

    def test_fresh_database_is_v2_with_is_key(self):
        conn = sqlite3.connect(self.path)
        schema.init(conn)
        conn.close()
        self.assertEqual(schema.SCHEMA_VERSION, 2)
        self.assertEqual(self._version(), 2)
        self.assertIn('is_key', self._cols())

    def test_v1_database_migrates_and_keeps_history(self):
        conn = sqlite3.connect(self.path)
        conn.executescript(schema._DDL_V1)
        conn.execute('PRAGMA user_version = 1')
        conn.execute(
            "INSERT INTO run (run_id, script_name, host, code_version, "
            "started_at, finished_at, status) "
            "VALUES ('r1', 's', 'H', 'v0', ?, ?, 'succeeded')",
            (_iso(BASE), _iso(BASE + timedelta(minutes=5))))
        conn.execute("INSERT INTO run_metric (run_id, scope, name, value, unit) "
                     "VALUES ('r1', 'x', 'total_count', 42, 'count')")
        conn.commit()
        conn.close()

        conn = sqlite3.connect(self.path)
        schema.init(conn)
        conn.close()

        self.assertEqual(self._version(), 2)
        self.assertIn('is_key', self._cols())
        row = self._conn().execute(
            'SELECT value, unit, is_key FROM run_metric WHERE run_id = ?',
            ('r1',)).fetchone()
        self.assertEqual(row, (42.0, 'count', None))

    def test_migration_is_idempotent(self):
        conn = sqlite3.connect(self.path)
        schema.init(conn)
        schema.init(conn)
        conn.close()
        self.assertEqual(self._version(), 2)
        self.assertEqual(self._cols().count('is_key'), 1)

    def test_migration_recovers_if_column_already_present(self):
        # column added but version still 1 (process died mid-migration)
        conn = sqlite3.connect(self.path)
        conn.executescript(schema._DDL_V1)
        conn.execute('ALTER TABLE run_metric ADD COLUMN is_key INTEGER')
        conn.execute('PRAGMA user_version = 1')
        conn.commit()
        schema.init(conn)
        conn.close()
        self.assertEqual(self._version(), 2)
        self.assertEqual(self._cols().count('is_key'), 1)

    def test_recorder_writes_the_explicit_flag(self):
        rec = RunRecorder.start('s', db_path=self.path)
        rec.add_metric('x', 'plain', 1)
        rec.add_metric('x', 'declared', 2, key=True)
        rec.add_metric('x', 'suppressed', 3, key=False)
        rec.finish('succeeded')
        got = dict(self._conn().execute('SELECT name, is_key FROM run_metric'))
        self.assertEqual((got['plain'], got['declared'], got['suppressed']),
                         (None, 1, 0))


# --------------------------------------------------------------------------- #
# available_metrics
# --------------------------------------------------------------------------- #

class AvailableMetricsTest(_DbCase):
    def seed(self):
        self.add_run('r1', 's', BASE, BASE + timedelta(minutes=5), 'succeeded')
        self.add_metric('r1', 'fetch', 'total_count', 100)
        self.add_metric('r1', 'fetch', 'other_exception', 3)
        self.add_metric('r1', 'fetch', 'success', 97)
        self.add_metric('r1', 'db', 'total_count', 97)          # same name, other scope
        self.add_metric('r1', 'fetch', 'records_added', 97, is_key=1)

    def test_identities_carry_scope_name_unit_and_key(self):
        self.seed()
        rows = {(r['scope'], r['name']): r
                for r in series.available_metrics(self._conn(), 's')}
        self.assertEqual(set(rows), {
            ('fetch', 'total_count'), ('fetch', 'other_exception'),
            ('fetch', 'success'), ('fetch', 'records_added'),
            ('db', 'total_count')})
        self.assertTrue(rows[('fetch', 'total_count')]['key'])
        self.assertTrue(rows[('db', 'total_count')]['key'])       # not merged with fetch
        self.assertTrue(rows[('fetch', 'other_exception')]['key'])
        self.assertFalse(rows[('fetch', 'success')]['key'])
        self.assertTrue(rows[('fetch', 'records_added')]['key'])  # explicit is_key=1
        self.assertEqual(rows[('fetch', 'total_count')]['unit'], 'count')

    def test_default_key_metrics_filters(self):
        self.seed()
        keys = {(r['scope'], r['name'])
                for r in series.default_key_metrics(self._conn(), 's')}
        self.assertEqual(keys, {
            ('fetch', 'total_count'), ('db', 'total_count'),
            ('fetch', 'other_exception'), ('fetch', 'records_added')})

    def test_unknown_script_is_empty(self):
        self.seed()
        self.assertEqual(series.available_metrics(self._conn(), 'nope'), [])


# --------------------------------------------------------------------------- #
# fetch_series
# --------------------------------------------------------------------------- #

class FetchSeriesTest(_DbCase):
    def seed(self):
        self.add_run('r1', 's', BASE, BASE + timedelta(minutes=5), 'succeeded')
        self.add_run('r2', 's', BASE + timedelta(hours=1),
                     BASE + timedelta(hours=1, minutes=10), 'succeeded')
        self.add_run('r3', 's', BASE + timedelta(hours=2),
                     BASE + timedelta(hours=2, minutes=3), 'failed')
        self.add_run('r4', 's', BASE + timedelta(hours=3), None, 'running')
        self.add_metric('r1', 'fetch', 'total_count', 100)
        self.add_metric('r1', 'fetch', 'other_exception', 0)      # recorded zero
        self.add_metric('r2', 'fetch', 'total_count', 200)
        self.add_metric('r2', 'fetch', 'other_exception', 5)
        self.add_metric('r3', 'fetch', 'total_count', 50)         # no exception metric

    def test_points_are_aligned_and_carry_run_context(self):
        self.seed()
        res = series.fetch_series(
            self._conn(), 's', order='ASC',
            metrics=[('fetch', 'total_count'), ('fetch', 'other_exception')],
            now=BASE + timedelta(hours=7))
        self.assertEqual(res['script_name'], 's')
        self.assertEqual([s['scope'] + '.' + s['name'] for s in res['series']],
                         ['fetch.total_count', 'fetch.other_exception'])
        p = {pt['run_id']: pt for pt in res['points']}
        self.assertEqual(set(p['r1']), {
            'run_id', 'started_at', 'finished_at', 'duration_s', 'status',
            'display_status', 'stale', 'host', 'code_version', 'values'})
        self.assertEqual(p['r1']['duration_s'], 300.0)
        self.assertEqual(p['r1']['values'],
                         {'fetch': {'total_count': 100.0, 'other_exception': 0.0}})
        self.assertEqual(p['r2']['values'],
                         {'fetch': {'total_count': 200.0, 'other_exception': 5.0}})

    def test_missing_is_distinct_from_zero(self):
        self.seed()
        res = series.fetch_series(self._conn(), 's',
                                  metrics=[('fetch', 'other_exception')],
                                  now=BASE + timedelta(hours=7))
        p = {pt['run_id']: pt for pt in res['points']}
        self.assertEqual(p['r1']['values'], {'fetch': {'other_exception': 0.0}})
        self.assertEqual(p['r3']['values'], {})       # never recorded -> absent
        self.assertEqual(p['r4']['values'], {})

    def test_same_name_under_two_scopes_stays_separate(self):
        self.add_run('r1', 's', BASE, BASE + timedelta(minutes=5), 'succeeded')
        self.add_metric('r1', 'fetch', 'total_count', 100, unit='count')
        self.add_metric('r1', 'db', 'total_count', 98, unit='rows')
        res = series.fetch_series(
            self._conn(), 's',
            metrics=[('fetch', 'total_count'), ('db', 'total_count')],
            now=BASE + timedelta(hours=1))
        self.assertEqual(res['series'], [
            {'scope': 'fetch', 'name': 'total_count', 'unit': 'count', 'key': True},
            {'scope': 'db', 'name': 'total_count', 'unit': 'rows', 'key': True}])
        self.assertEqual(res['points'][0]['values'],
                         {'fetch': {'total_count': 100.0},
                          'db': {'total_count': 98.0}})

    def test_metrics_none_selects_key_metrics(self):
        self.seed()
        res = series.fetch_series(self._conn(), 's', now=BASE + timedelta(hours=7))
        self.assertEqual({(s['scope'], s['name']) for s in res['series']},
                         {('fetch', 'total_count'), ('fetch', 'other_exception')})
        self.assertTrue(all(s['key'] for s in res['series']))

    def test_duration_is_a_builtin_series(self):
        self.seed()
        res = series.fetch_series(
            self._conn(), 's',
            metrics=[('fetch', 'total_count'), series.DURATION_SERIES],
            now=BASE + timedelta(hours=7))
        self.assertEqual(res['series'][-1], {'scope': 'run', 'name': 'duration_s',
                                             'unit': 'seconds', 'key': False})
        p = {pt['run_id']: pt for pt in res['points']}
        self.assertEqual(p['r2']['duration_s'], 600.0)                  # point field
        self.assertEqual(p['r2']['values']['run']['duration_s'], 600.0)  # mirrored
        self.assertNotIn('run', p['r4']['values'])                      # unfinished

    def test_duration_not_mirrored_unless_selected(self):
        self.seed()
        res = series.fetch_series(self._conn(), 's',
                                  metrics=[('fetch', 'total_count')],
                                  now=BASE + timedelta(hours=7))
        p = {pt['run_id']: pt for pt in res['points']}
        self.assertNotIn('run', p['r1']['values'])
        self.assertEqual(p['r1']['duration_s'], 300.0)

    def test_since_until_order_limit(self):
        self.seed()
        asc = series.fetch_series(self._conn(), 's', metrics=[], order='ASC',
                                  now=BASE + timedelta(hours=7))
        self.assertEqual([p['run_id'] for p in asc['points']],
                         ['r1', 'r2', 'r3', 'r4'])
        desc2 = series.fetch_series(self._conn(), 's', metrics=[], order='DESC',
                                    limit=2, now=BASE + timedelta(hours=7))
        self.assertEqual([p['run_id'] for p in desc2['points']], ['r4', 'r3'])
        window = series.fetch_series(
            self._conn(), 's', metrics=[],
            since=BASE + timedelta(minutes=30),
            until=BASE + timedelta(hours=2, minutes=30),
            now=BASE + timedelta(hours=7))
        self.assertEqual([p['run_id'] for p in window['points']], ['r3', 'r2'])

    def test_lifecycle_points_are_returned_and_labelled(self):
        self.seed()
        res = series.fetch_series(self._conn(), 's', metrics=[],
                                  now=BASE + timedelta(hours=7))
        p = {pt['run_id']: pt for pt in res['points']}
        self.assertEqual(p['r3']['display_status'], 'failed')
        self.assertEqual(p['r4']['display_status'], 'stale')   # running > 3h
        self.assertTrue(p['r4']['stale'])
        self.assertIsNone(p['r4']['duration_s'])

    def test_null_unit_coexists_with_one_declared_unit(self):
        self.add_run('r1', 's', BASE, BASE + timedelta(minutes=5), 'succeeded')
        self.add_run('r2', 's', BASE + timedelta(hours=1),
                     BASE + timedelta(hours=1, minutes=5), 'succeeded')
        self.add_metric('r1', 'x', 'n', 1, unit=None)
        self.add_metric('r2', 'x', 'n', 2, unit='count')
        res = series.fetch_series(self._conn(), 's', metrics=[('x', 'n')],
                                  order='ASC', now=BASE + timedelta(hours=7))
        self.assertEqual(res['series'][0]['unit'], 'count')
        p = {pt['run_id']: pt for pt in res['points']}
        self.assertEqual(p['r1']['values'], {'x': {'n': 1.0}})
        self.assertEqual(p['r2']['values'], {'x': {'n': 2.0}})

    def test_inconsistent_units_raise_query_error(self):
        self.add_run('r1', 's', BASE, BASE + timedelta(minutes=5), 'succeeded')
        self.add_run('r2', 's', BASE + timedelta(hours=1),
                     BASE + timedelta(hours=1, minutes=5), 'succeeded')
        self.add_metric('r1', 'x', 'window', 60, unit='count')
        self.add_metric('r2', 'x', 'window', 3600, unit='seconds')
        with self.assertRaises(query.QueryError) as ctx:
            series.fetch_series(self._conn(), 's', metrics=[('x', 'window')],
                                now=BASE + timedelta(hours=7))
        msg = str(ctx.exception)
        self.assertIn('x/window', msg)
        self.assertIn('count', msg)
        self.assertIn('seconds', msg)


# --------------------------------------------------------------------------- #
# pre-v2 read-only + read-only invariants
# --------------------------------------------------------------------------- #

class PreV2ReadOnlyTest(_DbCase):
    def test_v1_database_is_queryable_read_only(self):
        conn = sqlite3.connect(self.path)
        conn.executescript(schema._DDL_V1)          # no is_key column
        conn.execute('PRAGMA user_version = 1')
        conn.execute(
            "INSERT INTO run (run_id, script_name, host, code_version, "
            "started_at, finished_at, status) "
            "VALUES ('r1', 's', 'H', 'v0', ?, ?, 'succeeded')",
            (_iso(BASE), _iso(BASE + timedelta(minutes=5))))
        conn.executescript(
            "INSERT INTO run_metric (run_id, scope, name, value, unit) VALUES "
            "('r1', 'fetch', 'total_count', 100, 'count'),"
            "('r1', 'fetch', 'success', 98, 'count');")
        conn.commit()
        conn.close()

        ro = query._connect_ro(self.path)
        self.addCleanup(ro.close)
        rows = {(r['scope'], r['name']): r
                for r in series.available_metrics(ro, 's')}
        self.assertTrue(rows[('fetch', 'total_count')]['key'])
        self.assertFalse(rows[('fetch', 'success')]['key'])
        res = series.fetch_series(ro, 's', now=BASE + timedelta(hours=1))
        self.assertEqual([s['name'] for s in res['series']], ['total_count'])
        self.assertEqual(res['points'][0]['values']['fetch']['total_count'], 100.0)


class ReadOnlyInvariantTest(_DbCase):
    def test_queries_never_write_the_database(self):
        self.add_run('r1', 's', BASE, BASE + timedelta(minutes=5), 'succeeded')
        self.add_metric('r1', 'fetch', 'total_count', 100)
        before = os.stat(self.path)
        ro = query._connect_ro(self.path)
        try:
            series.available_metrics(ro, 's')
            series.fetch_series(ro, 's')
            series.fetch_series(ro, 's', metrics=[('fetch', 'total_count')],
                                order='ASC', limit=5)
        finally:
            ro.close()
        after = os.stat(self.path)
        self.assertEqual((before.st_size, before.st_mtime),
                         (after.st_size, after.st_mtime))
        for sidecar in ('-wal', '-journal', '-shm'):
            self.assertFalse(os.path.exists(self.path + sidecar))


if __name__ == '__main__':
    unittest.main()
