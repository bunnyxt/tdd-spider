"""51's RecentActivityFreqUpdateRunner and the 04:00 full-scan snapshot it reads a week later."""

import gzip
import os
import tempfile
import threading
import unittest
from contextlib import contextmanager, nullcontext
from unittest import mock
from uuid import uuid4

from psycopg.pq import TransactionStatus

from core import VideoRecord
from runrecord._sqlite import sqlite3

from test_remaining_script_run_records import _DEPS, _load

s51 = _load('51_hourly-video-record-add.py') if _DEPS else None


def _rec(aid, view):
    return VideoRecord(1789243200, aid, f'BV{aid}', view, 1, 2, 3, 4, 5, 6)


class FakeConnection:
    def __init__(self, current=None, fail=False, control=('formal', None)):
        self.current, self.fail = current or {}, fail
        self.statements, self.commits, self.rollbacks = [], 0, 0
        self.control, self.events = control, []
        self.info = mock.Mock(transaction_status=TransactionStatus.IDLE)

    def execute(self, sql, params=None):
        self.events.append(sql)
        if sql.startswith(('select aid', 'update tdd_video')):
            if self.fail and sql.startswith('update'):
                raise RuntimeError('db down')
            self.statements.append((sql, params))
            return mock.Mock(fetchall=mock.Mock(return_value=list(self.current.items())))
        row = (self.control if sql.startswith('SELECT mode,active_run_id')
               else ('read committed',) if sql == 'SHOW transaction_isolation' else (None,))
        return mock.Mock(fetchone=mock.Mock(return_value=row))

    @contextmanager
    def transaction(self):
        self.info.transaction_status = TransactionStatus.INTRANS
        try:
            yield self
        except Exception:
            self.rollbacks += 1
            raise
        else:
            self.commit()
        finally:
            self.info.transaction_status = TransactionStatus.IDLE

    def commit(self):
        self.commits += 1


def use_connection(connection):
    return mock.patch('db.observation.engine.connection', side_effect=lambda: nullcontext(connection))


@unittest.skipUnless(_DEPS, 'db dependencies unavailable')
class RecentActivityFreqUpdateRunnerTest(unittest.TestCase):
    def setUp(self):
        environment = mock.patch.dict(os.environ, {'TDD_TRIAL_RUN_ID': '',
                                                   'TDD_ALLOW_UNMANAGED_INGEST': ''})
        environment.start()
        self.addCleanup(environment.stop)
        cwd = os.getcwd()
        os.chdir(tempfile.mkdtemp())  # data/0400 is relative to cwd, as under cron
        self.addCleanup(os.chdir, cwd)
        os.makedirs(s51.FULL_SCAN_SNAPSHOT_DIR)

    def _last_week(self, rows):
        with gzip.open(s51.full_scan_snapshot_path('2026-09-06 04:00'), 'wt') as f:
            f.write('added,aid,bvid,view,danmaku,reply,favorite,coin,share,like\n')
            f.writelines(f'1788638400,{aid},BV{aid},{view},0,0,0,0,0,0\n' for aid, view in rows)

    def _update_activity(self, records, session):
        runner = s51.RecentActivityFreqUpdateRunner('2026-09-13 04:00', records)
        with use_connection(session):
            runner._update_activity()
        return runner, [params for sql, params in session.statements if sql.startswith('update')]

    def test_thresholds(self):
        self._last_week([(aid, 100) for aid in range(1, 6)])
        records = [_rec(1, 1099), _rec(2, 1100), _rec(3, 5099), _rec(4, 5100), _rec(5, 100)]
        runner, updates = self._update_activity(records, FakeConnection())
        self.assertEqual(updates, [(1, [2, 3]), (2, [4])])
        self.assertEqual(runner.metrics,
                         {'activity_paired': 5, 'activity_hot': 1, 'activity_active': 2, 'activity_changed': 3})

    def test_each_derived_transaction_rejects_stopped_and_obsolete_runs(self):
        self._last_week([(1, 0)])
        for method in ('_update_recent', '_update_activity', '_update_freq'):
            for control in (('stopped', None), ('trial', uuid4())):
                for records in ([], [_rec(1, 9000)]):
                    with self.subTest(method=method, control=control, empty=not records), \
                            mock.patch.dict(os.environ, {'TDD_TRIAL_RUN_ID':
                                str(uuid4()) if control[0] == 'trial' else ''}):
                        session = FakeConnection(control=control)
                        runner = s51.RecentActivityFreqUpdateRunner('2026-09-13 04:00', records)
                        with self.assertLogs('RecentActivityFreqUpdateRunner', level='ERROR'):
                            with self.assertRaisesRegex(RuntimeError, 'does not permit'):
                                with use_connection(session):
                                    getattr(runner, method)()
                        self.assertEqual(session.statements, [])
                        self.assertEqual((session.commits, session.rollbacks), (0, 1))

    def test_formal_and_matching_trial_lock_before_business_reads_or_writes(self):
        self._last_week([(1, 0)])
        run_id = uuid4()
        for control, configured_run in ((('formal', None), ''), (('trial', run_id), str(run_id))):
            with self.subTest(control=control), mock.patch.dict(
                    os.environ, {'TDD_TRIAL_RUN_ID': configured_run}):
                runner = s51.RecentActivityFreqUpdateRunner('2026-09-13 04:00', [_rec(1, 9000)])
                session = FakeConnection(control=control)
                for method in ('_update_recent', '_update_activity', '_update_freq'):
                    session.events.clear()
                    with use_connection(session):
                        getattr(runner, method)()
                    lock = session.events.index('SELECT pg_advisory_xact_lock_shared(141376009,169)')
                    check = session.events.index('SELECT mode,active_run_id FROM tdd_ingest.control WHERE singleton')
                    business = next(i for i, sql in enumerate(session.events)
                                    if sql.startswith(('select aid', 'update tdd_video')))
                    self.assertLess(lock, check)
                    self.assertLess(check, business)
                self.assertEqual((session.commits, session.rollbacks), (3, 0))

    def test_run_control_is_rechecked_after_an_earlier_phase_commits(self):
        session = FakeConnection()
        def stop_after_commit():
            session.commits += 1
            session.control = ('stopped', None)
        session.commit = stop_after_commit
        runner = s51.RecentActivityFreqUpdateRunner('2026-09-13 05:00', [])
        with use_connection(session), \
                self.assertLogs('RecentActivityFreqUpdateRunner', level='ERROR'):
            with self.assertRaisesRegex(RuntimeError, 'does not permit'):
                runner.process()
        self.assertEqual(len(session.statements), 3)  # recent committed; freq was never written.
        self.assertTrue(all('set recent' in sql for sql, _ in session.statements))
        self.assertEqual((session.commits, session.rollbacks), (1, 1))

    def test_missing_snapshot_fails(self):
        session = FakeConnection(current={1: 2})
        with self.assertLogs('RecentActivityFreqUpdateRunner', level='ERROR'):
            with self.assertRaises(FileNotFoundError):
                self._update_activity([_rec(1, 99999)], session)
        self.assertEqual(session.statements, [])

    def test_snapshot_header_date_and_corruption_fail(self):
        path = s51.full_scan_snapshot_path('2026-09-06 04:00')
        for content in ('aid,added,view\n',
                        'added,aid,bvid,view,danmaku,reply,favorite,coin,share,like\n1,1,BV1,100,0,0,0,0,0,0\n',
                        'added,aid,bvid,view,danmaku,reply,favorite,coin,share,like\n1788638400,1,BV1,100,0\n'):
            with self.subTest(content=content):
                with gzip.open(path, 'wt') as stream:
                    stream.write(content)
                with self.assertLogs('RecentActivityFreqUpdateRunner', level='ERROR'):
                    with self.assertRaises(ValueError):
                        self._update_activity([_rec(1, 99999)], FakeConnection())
        with open(path, 'wb') as stream:
            stream.write(b'not gzip')
        with self.assertLogs('RecentActivityFreqUpdateRunner', level='ERROR'):
            with self.assertRaises(gzip.BadGzipFile):
                self._update_activity([_rec(1, 99999)], FakeConnection())

    def test_valid_snapshot_with_no_changed_rows_succeeds(self):
        self._last_week([(1, 100)])
        runner, updates = self._update_activity([_rec(1, 100)], FakeConnection())
        self.assertEqual(updates, [])
        self.assertEqual(runner.metrics['activity_paired'], 1)
        self.assertEqual(runner.metrics['activity_changed'], 0)

    def test_only_videos_in_both_scans_change(self):
        # unsorted rows, a repeated row, an invalid view, videos missing on either side
        self._last_week([(3, 100), (1, 100), (5, -1), (7, 0), (1, 100)])
        records = [_rec(2, 900_000), _rec(1, 150), _rec(5, 9000), _rec(3, 6100), _rec(6, -1)]
        runner, updates = self._update_activity(records, FakeConnection(current={1: 2, 2: 2, 7: 2}))
        self.assertEqual(updates, [(0, [1]), (2, [3])])
        self.assertEqual(runner.metrics['activity_paired'], 2)

    def test_only_changed_rows_are_written_in_chunks(self):
        self._last_week([(aid, 0) for aid in range(1, 1003)])
        _, updates = self._update_activity([_rec(aid, 9000) for aid in range(1, 1003)],
                                           FakeConnection(current={1: 2}))
        self.assertEqual([len(aids) for _, aids in updates], [1000, 1])

    def test_db_failure_rolls_back(self):
        self._last_week([(1, 0)])
        session = FakeConnection(fail=True)
        with self.assertLogs('RecentActivityFreqUpdateRunner', level='ERROR'):
            with self.assertRaisesRegex(RuntimeError, 'db down'):
                self._update_activity([_rec(1, 7000)], session)
        runner = s51.RecentActivityFreqUpdateRunner('2026-09-13 04:00', [])
        self.assertEqual((session.commits, session.rollbacks, runner.metrics), (0, 1, {}))

    def test_unconfirmed_writer_prevents_analysis(self):
        from job import JobStat
        fetch, writer, updater = JobStat(), JobStat(), JobStat()
        fetch.condition['success'] = 3
        writer.total_count = 3
        writer.condition.update(persisted=2, batch_insert_fail=1)
        runner = s51.VideoRecordAcquisitionJob('2026-09-13 04:00', s51.Queue())
        with mock.patch.object(s51, 'get_need_insert_aid_list', return_value=[1, 2, 3]), \
                mock.patch.object(s51, 'fetch_and_batch_insert_records', return_value=(fetch, writer, updater, None)):
            runner.run()
        self.assertIsInstance(runner.error, RuntimeError)
        with mock.patch.object(s51, 'VideoRecordAcquisitionJob', return_value=runner), \
                mock.patch.object(runner, 'start'), mock.patch.object(runner, 'join'), \
                mock.patch.object(s51, 'RecordsSaveToFileRunner') as save:
            with self.assertRaisesRegex(RuntimeError, 'downstream analysis was not started'):
                s51.run_hourly_video_record_add('2026-09-13 04:00')
        save.assert_not_called()

    def test_thread_failure_is_visible_to_caller(self):
        runner = s51.RecentActivityFreqUpdateRunner('2026-09-13 05:00', [])
        with use_connection(FakeConnection(fail=True)):
            runner.start()
            runner.join()
        self.assertIsInstance(runner.error, RuntimeError)

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
                use_connection(FakeConnection()):
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
