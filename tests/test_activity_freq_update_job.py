"""
51's daily full-scan snapshots and ActivityFreqUpdateJob.

* snapshot helpers -- write (atomic, ascending aid), streaming read, prune;
* ActivityFreqUpdateJob -- weekly view growth merged against the snapshot from
  7 days earlier and written back through a fake session: missing snapshot,
  partial scan, unchanged rows, chunking, ordering guards, DB failures;
* RecordsSaveToFileRunner writing the 04:00 snapshot, and the job's JobStat
  landing in the run record.

The script imports ``db`` (SQLAlchemy); where that is absent the module skips.
"""

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
from runrecord._sqlite import sqlite3  # noqa: E402


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

TASK = '2026-09-13 04:00'


def _rec(aid, view):
    return RecordNew(added=1789243200, aid=aid, bvid=f'BV{aid}', view=view, danmaku=1, reply=2,
                     favorite=3, coin=4, share=5, like=6, dislike=0, now_rank=0,
                     his_rank=0, vt=None, vv=None)


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

    def activity_updates(self):
        return [s for s in self.statements if s.startswith('update tdd_video set activity')]

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


@unittest.skipUnless(_DEPS, 'db dependencies unavailable')
class SnapshotTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def test_round_trip_streams_views_in_file_order(self):
        path = s51.write_full_scan_snapshot([_rec(1, 10), _rec(2, -1), _rec(5, 20)],
                                            s51.full_scan_snapshot_path(self.dir, TASK))
        self.assertEqual(path, os.path.join(self.dir, '2026-09-13 04:00.csv.gz'))
        self.assertEqual(list(s51.iter_full_scan_snapshot_views(path)), [(1, 10), (2, -1), (5, 20)])
        with gzip.open(path, 'rt') as f:
            self.assertEqual(f.readline(), s51.RECORD_CSV_HEADER)
        self.assertEqual(os.listdir(self.dir), ['2026-09-13 04:00.csv.gz'])

    def test_unsorted_snapshot_raises(self):
        path = s51.write_full_scan_snapshot([_rec(5, 1), _rec(3, 1)], s51.full_scan_snapshot_path(self.dir, TASK))
        with self.assertRaises(ValueError):
            list(s51.iter_full_scan_snapshot_views(path))

    def test_failed_write_keeps_previous_snapshot(self):
        path = s51.full_scan_snapshot_path(self.dir, TASK)
        s51.write_full_scan_snapshot([_rec(1, 10)], path)

        def broken():
            yield _rec(1, 99)
            raise RuntimeError('boom')

        with self.assertRaises(RuntimeError):
            s51.write_full_scan_snapshot(broken(), path)
        self.assertEqual(list(s51.iter_full_scan_snapshot_views(path)), [(1, 10)])

    def test_prune_keeps_retention_window_and_ignores_other_files(self):
        names = ['2026-08-13 04:00.csv.gz',       # 31 days old -> removed
                 '2026-08-14 04:00.csv.gz',       # 30 days old -> kept
                 '2026-09-12 04:00.csv.gz',
                 '2025-01-01 04:00.csv.gz.tmp',   # not a snapshot name
                 '2025-01-01.csv.gz',             # not a snapshot name
                 'notes.txt']
        for name in names:
            open(os.path.join(self.dir, name), 'w').close()
        removed = s51.prune_full_scan_snapshots(self.dir, TASK)
        self.assertEqual([os.path.basename(p) for p in removed], ['2026-08-13 04:00.csv.gz'])
        self.assertEqual(sorted(os.listdir(self.dir)), sorted(names[1:]))
        self.assertEqual(s51.prune_full_scan_snapshots(os.path.join(self.dir, 'absent'), TASK), [])


@unittest.skipUnless(_DEPS, 'db dependencies unavailable')
class ActivityFreqUpdateJobTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def _last_week(self, records):
        s51.write_full_scan_snapshot(records, s51.full_scan_snapshot_path(self.dir, '2026-09-06 04:00'))

    def _job(self, records, session, task=TASK):
        with mock.patch.object(s51, 'Session', return_value=session):
            return s51.ActivityFreqUpdateJob(task, records, snapshot_folder=self.dir)

    def test_thresholds_on_weekly_growth(self):
        self._last_week([_rec(aid, 100) for aid in range(1, 6)])
        session = FakeSession()
        job = self._job([_rec(1, 1099), _rec(2, 1100), _rec(3, 5099), _rec(4, 5100), _rec(5, 100)],
                        session)
        job._update_activity()
        self.assertEqual(session.activity_updates(), [
            'update tdd_video set activity = 1 where aid in (2,3)',
            'update tdd_video set activity = 2 where aid in (4)',
        ])
        self.assertEqual(job.stat.total_count, 5)
        self.assertEqual((job.stat.condition['activity_hot'], job.stat.condition['activity_active']), (1, 2))

    def test_missing_last_week_scan_warns_and_writes_nothing(self):
        # a snapshot from 6 days ago does not stand in for 7
        s51.write_full_scan_snapshot([_rec(1, 0)], s51.full_scan_snapshot_path(self.dir, '2026-09-07 04:00'))
        session = FakeSession(current={2: 2})
        job = self._job([_rec(1, 99999)], session)
        with self.assertLogs('ActivityFreqUpdateJob', level='WARNING') as logs:
            job._update_activity()
        self.assertIn('not found', '\n'.join(logs.output))
        self.assertEqual((session.statements, session.commits), ([], 0))
        self.assertEqual(dict(job.stat.condition),
                         {'activity_skipped_no_last_scan': 1, 'activity_update_fail': 0})

    def test_partial_last_week_scan_updates_paired_videos_only(self):
        # last week's scan only reached aids 1 and 3 (and 5 with an invalid view)
        self._last_week([_rec(1, 100), _rec(3, 100), _rec(5, -1)])
        this = [_rec(1, 150), _rec(2, 900_000), _rec(3, 6100), _rec(4, 10), _rec(5, 9000), _rec(6, -1)]
        # 1 was hot, 2 was hot (unpaired, must stay), 3 was 0
        session = FakeSession(current={1: 2, 2: 2})
        job = self._job(this, session)
        with self.assertLogs('ActivityFreqUpdateJob', level='WARNING') as logs:
            job._update_activity()
        self.assertIn('paired with last week scan', '\n'.join(logs.output))
        self.assertEqual(session.activity_updates(), [
            'update tdd_video set activity = 0 where aid in (1)',
            'update tdd_video set activity = 2 where aid in (3)',
        ])
        self.assertEqual(session.commits, 1)
        self.assertEqual(job.stat.total_count, 2)
        self.assertEqual(dict(job.stat.condition), {
            'activity_skipped_no_last_scan': 0, 'activity_hot': 1, 'activity_active': 0,
            'activity_changed': 2, 'activity_low_pair_ratio': 1, 'activity_update_fail': 0})

    def test_unchanged_activity_writes_nothing_and_large_changes_are_chunked(self):
        self._last_week([_rec(aid, 0) for aid in range(1, 8)])
        session = FakeSession(current={1: 1})
        job = self._job([_rec(1, 2000)] + [_rec(aid, 9000) for aid in range(2, 8)], session)
        job.UPDATE_CHUNK_SIZE = 4
        job._update_activity()
        self.assertEqual(session.activity_updates(), [
            'update tdd_video set activity = 2 where aid in (2,3,4,5)',
            'update tdd_video set activity = 2 where aid in (6,7)',
        ])
        self.assertEqual(job.stat.condition['activity_low_pair_ratio'], 0)
        self.assertEqual(job.stat.condition['activity_changed'], 6)

    def test_unsorted_input_fails_without_writing(self):
        self._last_week([_rec(1, 0), _rec(2, 0)])
        for records, snapshot in (([_rec(2, 9000), _rec(1, 9000)], None),
                                  ([_rec(1, 9000), _rec(2, 9000)], [_rec(2, 0), _rec(1, 0)])):
            if snapshot is not None:
                self._last_week(snapshot)
            session = FakeSession()
            job = self._job(records, session)
            with self.assertLogs('ActivityFreqUpdateJob', level='ERROR'):
                job._update_activity()
            self.assertEqual((session.activity_updates(), session.commits, session.rollbacks), ([], 0, 1))
            self.assertEqual(job.stat.condition['activity_update_fail'], 1)

    def test_db_failure_rolls_back_and_is_counted(self):
        self._last_week([_rec(1, 0)])
        session = FakeSession(fail_on='update tdd_video set activity')
        job = self._job([_rec(1, 7000)], session)
        with self.assertLogs('ActivityFreqUpdateJob', level='ERROR'):
            job._update_activity()
        self.assertEqual((session.commits, session.rollbacks), (0, 1))
        self.assertEqual(job.stat.condition['activity_update_fail'], 1)

    def test_recent_and_freq_failures_are_errors(self):
        session = FakeSession(fail_on='update tdd_video set')
        job = self._job([], session, task='2026-09-13 05:00')
        with self.assertLogs('ActivityFreqUpdateJob', level='ERROR') as logs:
            job._update_recent()
            job._update_freq()
        self.assertEqual(len(logs.records), 2)
        self.assertEqual(dict(job.stat.condition), {'recent_update_fail': 1, 'freq_update_fail': 1})

    def test_run_skips_activity_outside_0400_and_closes_session(self):
        session = FakeSession()
        job = self._job([_rec(1, 1)], session, task='2026-09-13 05:00')
        with self.assertLogs('ActivityFreqUpdateJob', level='INFO'):
            job.run()
        self.assertNotIn('activity_update_fail', job.stat.condition)
        self.assertEqual(session.activity_updates(), [])
        self.assertTrue(session.closed)


@unittest.skipUnless(_DEPS, 'db dependencies unavailable')
class PipelineTest(unittest.TestCase):
    def setUp(self):
        self.data = tempfile.mkdtemp()
        self.snap = os.path.join(self.data, '0400')
        logging.disable(logging.CRITICAL)
        self.addCleanup(logging.disable, logging.NOTSET)

    def test_0400_run_writes_snapshot_and_prunes(self):
        os.makedirs(self.snap)
        open(os.path.join(self.snap, '2026-08-01 04:00.csv.gz'), 'w').close()
        s51.RecordsSaveToFileRunner([_rec(1, 10), _rec(2, 20)], '2026-09-13 04:00',
                                    data_folder=self.data, snapshot_folder=self.snap).run()
        self.assertEqual(os.listdir(self.snap), ['2026-09-13 04:00.csv.gz'])
        self.assertTrue(os.path.isfile(os.path.join(self.data, '2026-09-13 04:00.csv')))
        s51.RecordsSaveToFileRunner([_rec(1, 10)], '2026-09-13 05:00',
                                    data_folder=self.data, snapshot_folder=self.snap).run()
        self.assertEqual(os.listdir(self.snap), ['2026-09-13 04:00.csv.gz'])

    def test_records_sorted_and_job_stat_lands_in_run_record(self):
        class FakeAcquisition:
            api_stats = None

            def __init__(self, time_task, record_queue):
                for aid in (3, 1, 2):
                    record_queue.put(_rec(aid, 1))

            def start(self):
                pass

            def join(self):
                pass

            def stats(self):
                return {}

        seen = {}

        class SpyRunner(threading.Thread):
            def __init__(self, records, *args, **kwargs):
                super().__init__()
                seen['aids'] = [r.aid for r in records]

        db_path = os.path.join(tempfile.mkdtemp(), 'run-records.sqlite3')
        recorder = s51.RunRecorder.start('51_hourly-video-record-add', db_path=db_path)
        with mock.patch.object(s51, 'VideoRecordAcquisitionJob', FakeAcquisition), \
                mock.patch.object(s51, 'RecordsSaveToFileRunner', SpyRunner), \
                mock.patch.object(s51, 'RecentRecordsAnalystRunner', SpyRunner), \
                mock.patch.object(s51, 'Session', return_value=FakeSession(fail_on='set freq')):
            s51.run_hourly_video_record_add('2026-09-13 05:00', recorder)
        run_id = recorder.run_id
        recorder.finish('succeeded')

        self.assertEqual(seen['aids'], [1, 2, 3])
        conn = sqlite3.connect(db_path)
        try:
            rows = conn.execute('SELECT scope, name, value FROM run_metric WHERE run_id = ?',
                                (run_id,)).fetchall()
        finally:
            conn.close()
        self.assertEqual(sorted(rows), [
            ('activity-freq-update', 'freq_update_fail', 1.0),
            ('activity-freq-update', 'recent_update_fail', 0.0),
            ('activity-freq-update', 'total_count', 0.0),
        ])


if __name__ == '__main__':
    unittest.main()
