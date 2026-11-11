"""51's RecentActivityFreqUpdateRunner and the 04:00 full-scan snapshot it reads a week later."""

import gzip
import os
import tempfile
import threading
import unittest
from unittest import mock

from core import RecordNew
from runrecord._sqlite import sqlite3

from test_remaining_script_run_records import _DEPS, _load

s51 = _load('51_hourly-video-record-add.py') if _DEPS else None


def _rec(aid, view):
    return RecordNew(1789243200, aid, f'BV{aid}', view, 1, 2, 3, 4, 5, 6, 0, 0, 0, None, None)


class FakeSession:
    def __init__(self, current=None, fail=False):
        self.current, self.fail = current or {}, fail  # current: aid -> non-zero activity
        self.statements, self.commits, self.rollbacks = [], 0, 0

    def execute(self, sql):
        if self.fail and sql.startswith('update'):
            raise RuntimeError('db down')
        self.statements.append(sql)
        return list(self.current.items()) if sql.startswith('select') else None

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        pass


@unittest.skipUnless(_DEPS, 'db dependencies unavailable')
class RecentActivityFreqUpdateRunnerTest(unittest.TestCase):
    def setUp(self):
        cwd = os.getcwd()
        os.chdir(tempfile.mkdtemp())  # data/0400 is relative to cwd, as under cron
        self.addCleanup(os.chdir, cwd)
        os.makedirs(s51.FULL_SCAN_SNAPSHOT_DIR)

    def _last_week(self, rows):
        with gzip.open(s51.full_scan_snapshot_path('2026-09-06 04:00'), 'wt') as f:
            f.write('added,aid,bvid,view,danmaku,reply,favorite,coin,share,like\n')
            f.writelines(f'1,{aid},BV{aid},{view},0,0,0,0,0,0\n' for aid, view in rows)

    def _update_activity(self, records, session):
        runner = s51.RecentActivityFreqUpdateRunner('2026-09-13 04:00', records)
        runner._update_activity(session)
        return runner, [s for s in session.statements if s.startswith('update')]

    def test_thresholds(self):
        self._last_week([(aid, 100) for aid in range(1, 6)])
        records = [_rec(1, 1099), _rec(2, 1100), _rec(3, 5099), _rec(4, 5100), _rec(5, 100)]
        runner, updates = self._update_activity(records, FakeSession())
        self.assertEqual(updates, ['update tdd_video set activity = 1 where aid in (2,3)',
                                   'update tdd_video set activity = 2 where aid in (4)'])
        self.assertEqual(runner.metrics,
                         {'activity_paired': 5, 'activity_hot': 1, 'activity_active': 2, 'activity_changed': 3})

    def test_missing_snapshot_skips(self):
        session = FakeSession(current={1: 2})
        with self.assertLogs('RecentActivityFreqUpdateRunner', level='WARNING'):
            runner, _ = self._update_activity([_rec(1, 99999)], session)
        self.assertEqual((session.statements, runner.metrics), ([], {}))

    def test_only_videos_in_both_scans_change(self):
        # unsorted rows, a repeated row, an invalid view, videos missing on either side
        self._last_week([(3, 100), (1, 100), (5, -1), (7, 0), (1, 100)])
        records = [_rec(2, 900_000), _rec(1, 150), _rec(5, 9000), _rec(3, 6100), _rec(6, -1)]
        runner, updates = self._update_activity(records, FakeSession(current={1: 2, 2: 2, 7: 2}))
        self.assertEqual(updates, ['update tdd_video set activity = 0 where aid in (1)',
                                   'update tdd_video set activity = 2 where aid in (3)'])
        self.assertEqual(runner.metrics['activity_paired'], 2)

    def test_only_changed_rows_are_written_in_chunks(self):
        self._last_week([(aid, 0) for aid in range(1, 1003)])
        _, updates = self._update_activity([_rec(aid, 9000) for aid in range(1, 1003)],
                                           FakeSession(current={1: 2}))
        self.assertEqual([u.count(',') + 1 for u in updates], [1000, 1])

    def test_db_failure_rolls_back(self):
        self._last_week([(1, 0)])
        session = FakeSession(fail=True)
        with self.assertLogs('RecentActivityFreqUpdateRunner', level='ERROR'):
            runner, _ = self._update_activity([_rec(1, 7000)], session)
        self.assertEqual((session.commits, session.rollbacks, runner.metrics), (0, 1, {}))

    def test_0400_csv_is_kept_as_snapshot_and_old_ones_pruned(self):
        stale = s51.full_scan_snapshot_path('2026-08-13 04:00')  # 31 days old
        kept = s51.full_scan_snapshot_path('2026-08-14 04:00')   # 30 days old
        for path in (stale, kept):
            open(path, 'w').close()
        s51.RecordsSaveToFileRunner([_rec(1, 10)], '2026-09-13 04:00').run()
        snapshot = s51.full_scan_snapshot_path('2026-09-13 04:00')
        with open('data/2026-09-13 04:00.csv', 'rb') as f, gzip.open(snapshot, 'rb') as g:
            self.assertEqual(f.read(), g.read())
        self.assertEqual(sorted(os.listdir(s51.FULL_SCAN_SNAPSHOT_DIR)),
                         sorted(os.path.basename(p) for p in (kept, snapshot)))

    def test_metrics_land_in_run_record(self):
        class FakeAcquisition:
            api_stats = None

            def __init__(self, time_task, record_queue):
                record_queue.put(_rec(1, 9000))

            def start(self):
                pass

            join = start

            def stats(self):
                return {}

        class NoopRunner(threading.Thread):
            def __init__(self, *args):
                super().__init__()

        self._last_week([(1, 0)])
        recorder = s51.RunRecorder.start('51_hourly-video-record-add', db_path='run-records.sqlite3')
        with mock.patch.object(s51, 'VideoRecordAcquisitionJob', FakeAcquisition), \
                mock.patch.object(s51, 'RecordsSaveToFileRunner', NoopRunner), \
                mock.patch.object(s51, 'RecentRecordsAnalystRunner', NoopRunner), \
                mock.patch.object(s51, 'Session', return_value=FakeSession()):
            s51.run_hourly_video_record_add('2026-09-13 04:00', recorder)
        run_id = recorder.run_id
        recorder.finish('succeeded')
        conn = sqlite3.connect('run-records.sqlite3')
        self.addCleanup(conn.close)
        rows = conn.execute('SELECT scope, name, value FROM run_metric WHERE run_id = ?', (run_id,)).fetchall()
        self.assertEqual(sorted(rows), [('recent-activity-freq-update', 'activity_active', 0.0),
                                        ('recent-activity-freq-update', 'activity_changed', 1.0),
                                        ('recent-activity-freq-update', 'activity_hot', 1.0),
                                        ('recent-activity-freq-update', 'activity_paired', 1.0)])


if __name__ == '__main__':
    unittest.main()
