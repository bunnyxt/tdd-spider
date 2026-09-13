"""
Full-scan snapshots and the weekly-growth activity update of 51.

Two layers:

* ``core.full_scan`` -- pure functions: snapshot write / read / prune and the
  pairing rule of ``compute_activity``. No DB, always runs.
* ``51_hourly-video-record-add.py`` -- ``RecordsSaveToFileRunner`` writing the
  04:00 snapshot and ``RecentActivityFreqUpdateRunner`` writing activity back
  through a fake session, plus the run-record metrics. The script imports
  ``db`` (SQLAlchemy); where that is absent these tests skip.
"""

import datetime
import gzip
import importlib.util
import logging
import os
import sys
import tempfile
import threading
import types
import unittest
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import RecordNew  # noqa: E402
from core.full_scan import (  # noqa: E402
    compute_activity, load_snapshot_views, prune_snapshots, record_views,
    snapshot_path, write_snapshot)
from runrecord._sqlite import sqlite3  # noqa: E402


def _rec(aid, view, added=1789243200):
    return RecordNew(added=added, aid=aid, bvid=f'BV{aid}', view=view, danmaku=1, reply=2,
                     favorite=3, coin=4, share=5, like=6, dislike=0, now_rank=0,
                     his_rank=0, vt=None, vv=None)


class ComputeActivityTest(unittest.TestCase):
    def test_thresholds_on_weekly_growth(self):
        last = {1: 100, 2: 100, 3: 100, 4: 100, 5: 100}
        this = {1: 1099, 2: 1100, 3: 5099, 4: 5100, 5: 100}
        result = compute_activity(this, last)
        self.assertEqual(result.activity, {1: 0, 2: 1, 3: 1, 4: 2, 5: 0})
        self.assertEqual((result.hot, result.active, result.paired), (1, 2, 5))

    def test_only_videos_in_both_scans_are_classified(self):
        # 3 is new this week (no last value): it is NOT treated as growth of
        # its whole view count, it is simply left out
        result = compute_activity({1: 9000, 2: 50, 3: 1_000_000}, {1: 0, 2: 0, 4: 7})
        self.assertEqual(result.activity, {1: 2, 2: 0})
        self.assertEqual((result.this_count, result.last_count, result.paired), (3, 3, 2))
        self.assertAlmostEqual(result.pair_ratio, 2 / 3)

    def test_empty_scan_has_zero_pair_ratio(self):
        self.assertEqual(compute_activity({}, {1: 1}).pair_ratio, 0.0)


class SnapshotTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def test_round_trip_and_views(self):
        day = datetime.date(2026, 9, 13)
        path = write_snapshot([_rec(1, 10), _rec(2, 20)], self.dir, day)
        self.assertEqual(path, snapshot_path(self.dir, day))
        self.assertTrue(path.endswith('2026-09-13.csv.gz'))
        self.assertEqual(load_snapshot_views(path), {1: 10, 2: 20})
        with gzip.open(path, 'rt') as f:
            self.assertEqual(f.readline().strip(),
                             'added,aid,bvid,view,danmaku,reply,favorite,coin,share,like')
        self.assertEqual(os.listdir(self.dir), ['2026-09-13.csv.gz'])

    def test_missing_snapshot_is_none(self):
        self.assertIsNone(load_snapshot_views(os.path.join(self.dir, 'nope.csv.gz')))

    def test_invalid_view_is_missing_and_duplicates_widen_growth(self):
        day = datetime.date(2026, 9, 6)
        path = write_snapshot([_rec(1, -1), _rec(2, 30), _rec(2, 20)], self.dir, day)
        self.assertEqual(load_snapshot_views(path), {2: 20})  # min on last week's side
        self.assertEqual(record_views([_rec(1, -1), _rec(2, 30), _rec(2, 20)]), {2: 30})  # max

    def test_failed_write_keeps_previous_snapshot(self):
        day = datetime.date(2026, 9, 13)
        write_snapshot([_rec(1, 10)], self.dir, day)

        def broken():
            yield _rec(1, 99)
            raise RuntimeError('boom')

        with self.assertRaises(RuntimeError):
            write_snapshot(broken(), self.dir, day)
        self.assertEqual(load_snapshot_views(snapshot_path(self.dir, day)), {1: 10})

    def test_prune_keeps_retention_window_and_ignores_other_files(self):
        today = datetime.date(2026, 9, 13)
        names = ['2026-08-13.csv.gz',      # exactly 31 days old -> removed
                 '2026-08-14.csv.gz',      # exactly 30 days old -> kept
                 '2026-09-12.csv.gz',
                 '2025-01-01.csv.gz.tmp',  # not a snapshot name
                 '2026-13-01.csv.gz',      # not a real date
                 'notes.txt']
        for name in names:
            open(os.path.join(self.dir, name), 'w').close()
        removed = prune_snapshots(self.dir, today)
        self.assertEqual([os.path.basename(p) for p in removed], ['2026-08-13.csv.gz'])
        self.assertEqual(sorted(os.listdir(self.dir)), sorted(names[1:]))

    def test_prune_missing_folder(self):
        self.assertEqual(prune_snapshots(os.path.join(self.dir, 'absent'), datetime.date.today()), [])


# ---- 51 integration ------------------------------------------------------------

def _install_stub_conf():
    # conf/conf.ini is git-ignored; `import db` builds an engine at import time
    if 'conf' in sys.modules and hasattr(sys.modules['conf'], 'get_db_args'):
        try:
            sys.modules['conf'].get_db_args()
            return
        except Exception:
            pass
    stub = types.ModuleType('conf')
    stub.CONFIG_PATH = ''
    stub.CONFIG = None
    stub.get_db_args = lambda: {'user': 'stub', 'password': 'stub', 'host': '127.0.0.1',
                                'port': '3306', 'dbname': 'stub'}
    stub.get_sckey = lambda: 'stub'
    stub.__all__ = ['CONFIG_PATH', 'CONFIG', 'get_db_args', 'get_sckey']
    sys.modules['conf'] = stub
    sys.modules['conf.conf'] = stub


try:
    _install_stub_conf()
    _path = os.path.join(ROOT, '51_hourly-video-record-add.py')
    _spec = importlib.util.spec_from_file_location('scriptmod_51_activity', _path)
    s51 = importlib.util.module_from_spec(_spec)
    sys.modules['scriptmod_51_activity'] = s51
    _spec.loader.exec_module(s51)
    _DEPS = True
except Exception as _e:  # pragma: no cover - depends on the environment
    _DEPS = False
    _DEPS_ERR = repr(_e)


class FakeSession:
    def __init__(self, current=None, fail_on=None):
        self.current = current or {}  # aid -> non-zero activity in tdd_video
        self.fail_on = fail_on
        self.statements = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def execute(self, sql):
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError('db down')
        self.statements.append(sql)
        if sql.startswith('select aid, activity from tdd_video'):
            return list(self.current.items())
        return None

    def updates(self, prefix='update tdd_video set activity'):
        return [s for s in self.statements if s.startswith(prefix)]

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


@unittest.skipUnless(_DEPS, 'db dependencies unavailable')
class ActivityRunnerTest(unittest.TestCase):
    TASK = '2026-09-13 04:00'

    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def _last_week(self, records):
        write_snapshot(records, self.dir, datetime.date(2026, 9, 6))

    def _runner(self, records, task=TASK):
        return s51.RecentActivityFreqUpdateRunner(task, records, snapshot_folder=self.dir)

    def _values(self, runner):
        return {name: value for name, (value, _key) in runner.metrics.items()}

    def test_missing_last_week_scan_warns_and_writes_nothing(self):
        # a snapshot from 6 days ago does not stand in for 7
        write_snapshot([_rec(1, 0)], self.dir, datetime.date(2026, 9, 7))
        runner, session = self._runner([_rec(1, 99999)]), FakeSession(current={2: 2})
        with self.assertLogs('RecentActivityFreqUpdateRunner', level='WARNING') as logs:
            runner._update_activity(session)
        self.assertIn('not found', '\n'.join(logs.output))
        self.assertEqual(session.statements, [])
        self.assertEqual(session.commits, 0)
        self.assertEqual(self._values(runner),
                         {'activity_update_fail': 0, 'activity_skipped_no_last_scan': 1})
        self.assertTrue(runner.metrics['activity_skipped_no_last_scan'][1])  # key metric

    def test_partial_last_week_scan_updates_paired_videos_only(self):
        # last week's scan only reached aids 1 and 2
        self._last_week([_rec(1, 100), _rec(2, 100)])
        this = [_rec(1, 150), _rec(2, 6100), _rec(3, 900_000), _rec(4, 10)]
        # 1 was hot, 3 was hot (unpaired, must stay), 2 was 0
        session = FakeSession(current={1: 2, 3: 2})
        runner = self._runner(this)
        with self.assertLogs('RecentActivityFreqUpdateRunner', level='WARNING') as logs:
            runner._update_activity(session)
        self.assertIn('paired with last week scan', '\n'.join(logs.output))
        self.assertEqual(session.updates(), [
            'update tdd_video set activity = 0 where aid in (1)',
            'update tdd_video set activity = 2 where aid in (2)',
        ])
        self.assertFalse(any('activity = 0' in s and 'where aid in' not in s
                             for s in session.statements), 'no table-wide reset')
        self.assertEqual(session.commits, 1)
        self.assertEqual(self._values(runner), {
            'activity_skipped_no_last_scan': 0, 'activity_this_scan': 4,
            'activity_last_scan': 2, 'activity_paired': 2, 'activity_hot': 1,
            'activity_active': 0, 'activity_low_pair_ratio': 1,
            'activity_changed': 2, 'activity_update_fail': 0})

    def test_unchanged_activity_writes_nothing_and_chunks_large_changes(self):
        self._last_week([_rec(aid, 0) for aid in range(1, 8)])
        this = [_rec(1, 2000)] + [_rec(aid, 9000) for aid in range(2, 8)]
        session = FakeSession(current={1: 1})
        runner = self._runner(this)
        runner.UPDATE_CHUNK_SIZE = 4
        runner._update_activity(session)
        self.assertEqual(session.updates(), [
            'update tdd_video set activity = 2 where aid in (2,3,4,5)',
            'update tdd_video set activity = 2 where aid in (6,7)',
        ])
        self.assertEqual(self._values(runner)['activity_low_pair_ratio'], 0)
        self.assertEqual(self._values(runner)['activity_changed'], 6)

    def test_db_failure_rolls_back_and_is_recorded(self):
        self._last_week([_rec(1, 0)])
        session = FakeSession(fail_on='update tdd_video set activity')
        runner = self._runner([_rec(1, 7000)])
        with self.assertLogs('RecentActivityFreqUpdateRunner', level='ERROR'):
            runner._update_activity(session)
        self.assertEqual((session.commits, session.rollbacks), (0, 1))
        self.assertEqual(self._values(runner)['activity_update_fail'], 1)

    def test_recent_and_freq_failures_are_errors(self):
        runner = self._runner([], task='2026-09-13 05:00')
        session = FakeSession(fail_on='update tdd_video set')
        with self.assertLogs('RecentActivityFreqUpdateRunner', level='ERROR') as logs:
            runner._update_recent(session)
            runner._update_freq(session)
        self.assertEqual(len(logs.records), 2)
        self.assertEqual(self._values(runner), {'recent_update_fail': 1, 'freq_update_fail': 1})

    def test_activity_only_runs_at_0400(self):
        session = FakeSession()
        with mock.patch.object(s51, 'Session', return_value=session):
            runner = self._runner([_rec(1, 1)], task='2026-09-13 05:00')
            runner.run()
        self.assertNotIn('activity_update_fail', runner.metrics)
        self.assertEqual(session.updates(), [])
        self.assertTrue(session.closed)


@unittest.skipUnless(_DEPS, 'db dependencies unavailable')
class SnapshotRunnerTest(unittest.TestCase):
    def setUp(self):
        self.data = tempfile.mkdtemp()
        self.snap = os.path.join(self.data, '0400')
        logging.disable(logging.CRITICAL)
        self.addCleanup(logging.disable, logging.NOTSET)

    def _run(self, task, records):
        s51.RecordsSaveToFileRunner(records, task, data_folder=self.data,
                                    snapshot_folder=self.snap).run()

    def test_0400_run_writes_snapshot_and_prunes(self):
        os.makedirs(self.snap)
        open(os.path.join(self.snap, '2026-08-01.csv.gz'), 'w').close()
        self._run('2026-09-13 04:00', [_rec(1, 10), _rec(2, -1)])
        self.assertEqual(sorted(os.listdir(self.snap)), ['2026-09-13.csv.gz'])
        self.assertEqual(load_snapshot_views(os.path.join(self.snap, '2026-09-13.csv.gz')), {1: 10})
        self.assertTrue(os.path.isfile(os.path.join(self.data, '2026-09-13 04:00.csv')))

    def test_other_hours_write_no_snapshot(self):
        self._run('2026-09-13 05:00', [_rec(1, 10)])
        self.assertFalse(os.path.exists(self.snap))


@unittest.skipUnless(_DEPS, 'db dependencies unavailable')
class ActivityRunRecordMetricsTest(unittest.TestCase):
    def setUp(self):
        self.db_path = os.path.join(tempfile.mkdtemp(), 'run-records.sqlite3')
        logging.disable(logging.CRITICAL)
        self.addCleanup(logging.disable, logging.NOTSET)

    def test_update_metrics_land_in_run_record(self):
        class FakeAcquisition:
            api_stats = None

            def __init__(self, *args):
                pass

            def start(self):
                pass

            def join(self):
                pass

            def stats(self):
                return {}

        class NoopRunner(threading.Thread):
            def __init__(self, *args, **kwargs):
                super().__init__()

        recorder = s51.RunRecorder.start('51_hourly-video-record-add', db_path=self.db_path)
        with mock.patch.object(s51, 'VideoRecordAcquisitionJob', FakeAcquisition), \
                mock.patch.object(s51, 'RecordsSaveToFileRunner', NoopRunner), \
                mock.patch.object(s51, 'RecentRecordsAnalystRunner', NoopRunner), \
                mock.patch.object(s51, 'Session', return_value=FakeSession(fail_on='set freq')):
            s51.run_hourly_video_record_add('2026-09-13 05:00', recorder)
        run_id = recorder.run_id
        recorder.finish('succeeded')

        conn = sqlite3.connect(self.db_path)
        try:
            rows = conn.execute('SELECT scope, name, value FROM run_metric WHERE run_id = ?',
                                (run_id,)).fetchall()
        finally:
            conn.close()
        self.assertEqual(sorted(rows), [
            ('activity-freq-update', 'freq_update_fail', 1.0),
            ('activity-freq-update', 'recent_update_fail', 0.0),
        ])


if __name__ == '__main__':
    unittest.main()
