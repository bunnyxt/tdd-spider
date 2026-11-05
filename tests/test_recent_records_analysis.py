"""Scoped metadata reads and visible failures in the hourly analysis stage."""

import os
import tempfile
import threading
import unittest
from unittest import mock

from test_remaining_script_run_records import _DEPS, _load

if _DEPS:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from core import RecordNew
    from runrecord._sqlite import sqlite3
    s51 = _load('51_hourly-video-record-add.py')


def record(aid, added, view):
    return RecordNew(added, aid, 'BV%s' % aid, view, 1, 1, 1, 1, 1, 1,
                     0, 0, 0, None, None)


class AnalysisSession:
    def __init__(self, fail_commit=False):
        self.closed = False
        self.rollbacks = self.commits = 0
        self.added = []
        self.fail_commit = fail_commit

    def add(self, obj):
        self.added.append(obj)

    def commit(self):
        if self.fail_commit:
            raise RuntimeError('write unavailable')
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


@unittest.skipUnless(_DEPS, 'db dependencies unavailable')
class PubdateQueryTest(unittest.TestCase):
    def setUp(self):
        engine = create_engine('sqlite:///:memory:')
        self.addCleanup(engine.dispose)
        self.session = sessionmaker(bind=engine)()
        self.addCleanup(self.session.close)
        self.session.execute('CREATE TABLE tdd_video (aid BIGINT PRIMARY KEY, pubdate BIGINT)')
        self.session.execute('INSERT INTO tdd_video VALUES (:aid, :pubdate)',
                             [{'aid': aid, 'pubdate': 100} for aid in range(1, 1002)] +
                             [{'aid': 2**40, 'pubdate': None}])

    def test_bound_chunked_query_returns_only_requested_rows(self):
        with mock.patch.object(self.session, 'execute', wraps=self.session.execute) as execute:
            result = s51.DBOperation.query_video_pubdates_for_aids(
                list(range(1, 1002)) + [1, 2**40, 2**41], self.session)
        self.assertEqual(len(result), 1002)
        self.assertEqual(result[1001], 100)
        self.assertIsNone(result[2**40])
        self.assertNotIn(2**41, result)
        self.assertEqual([len(call.args[1]) for call in execute.call_args_list], [1000, 3])
        self.assertNotIn(str(2**40), execute.call_args_list[-1].args[0])

    def test_empty_does_not_query(self):
        session = mock.Mock()
        self.assertEqual(s51.DBOperation.query_video_pubdates_for_aids([], session), {})
        session.execute.assert_not_called()

    def test_query_failure_is_not_silenced(self):
        session = mock.Mock()
        session.execute.side_effect = RuntimeError('read unavailable')
        with self.assertRaisesRegex(RuntimeError, 'read unavailable'):
            s51.DBOperation.query_video_pubdates_for_aids([1], session)

    def test_invalid_identifiers_rejected_before_query(self):
        for aids in ([0], [-1], [True], ['1 OR 1=1']):
            with self.subTest(aids=aids), self.assertRaises(ValueError):
                s51.DBOperation.query_video_pubdates_for_aids(aids, mock.Mock())


@unittest.skipUnless(_DEPS, 'db dependencies unavailable')
class RecentRecordsAnalysisTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.folder = self.temp.name

    def runner(self, records):
        return s51.RecentRecordsAnalystRunner(records, '2026-09-13 03:00', self.folder)

    def history(self, aid, views):
        for hour, view in enumerate(views, 1):
            with open(os.path.join(self.folder, '2026-09-13 %02d:00.csv' % hour), 'a') as f:
                if f.tell() == 0:
                    f.write('added,aid,bvid,view,danmaku,reply,favorite,coin,share,like\n')
                f.write(','.join(map(str, record(aid, 10000 + hour * 3600, view)[:10])) + '\n')

    def test_short_histories_and_empty_run_never_open_session(self):
        self.history(1, [100])
        for records in ([], [record(1, 20800, 200), record(2, 20800, 200)]):
            with mock.patch.object(s51, 'Session') as session:
                runner = self.runner(records)
                runner.run()
            self.assertTrue(runner.completed)
            self.assertEqual(runner.metrics['analysis_candidates'], 0)
            session.assert_not_called()

    def test_only_eligible_aids_need_metadata_and_null_or_absent_is_skipped(self):
        for aid in (1, 2, 3):
            self.history(aid, [100, 200])
        session = AnalysisSession()
        runner = self.runner([record(aid, 20800, 300) for aid in (1, 2, 3, 4)])
        with mock.patch.object(s51, 'Session', return_value=session), \
                mock.patch.object(s51.DBOperation, 'query_video_pubdates_for_aids',
                                  return_value={1: 10000, 2: None}) as query, \
                self.assertLogs('RecentRecordsAnalystRunner', level='WARNING'):
            runner.run()
        self.assertEqual(set(query.call_args.args[0]), {1, 2, 3})
        self.assertEqual(runner.metrics['no_valid_pubdate'], 2)
        self.assertEqual(runner.metrics['records_len_less_than_3'], 1)
        self.assertTrue(runner.completed)
        self.assertTrue(session.closed)
        self.assertEqual(session.rollbacks, 0)

    def test_query_failure_rolls_back_closes_and_marks_incomplete(self):
        self.history(1, [100, 200])
        runner = self.runner([record(1, 20800, 300)])
        session = AnalysisSession()
        with mock.patch.object(s51, 'Session', return_value=session), \
                mock.patch.object(s51.DBOperation, 'query_video_pubdates_for_aids',
                                  side_effect=RuntimeError('read unavailable')), \
                self.assertLogs('RecentRecordsAnalystRunner', level='ERROR') as logs:
            runner.run()
        self.assertFalse(runner.completed)
        self.assertIsInstance(runner.error, RuntimeError)
        self.assertEqual(runner.metrics['completed'], 0)
        self.assertEqual(session.rollbacks, 1)
        self.assertTrue(session.closed)
        self.assertIn('analysis incomplete', '\n'.join(logs.output))

    def test_detection_results_keep_increase_drop_and_constant_growth_rules(self):
        for aid, views in ((1, [100, 200]), (2, [1000, 900]), (3, [100, 200])):
            self.history(aid, views)
        runner = self.runner([record(1, 20800, 1000), record(2, 20800, 700),
                              record(3, 20800, 300)])
        session = AnalysisSession()
        with mock.patch.object(s51, 'Session', return_value=session), \
                mock.patch.object(s51.DBOperation, 'query_video_pubdates_for_aids',
                                  return_value={1: 10000, 2: 10000, 3: 10000}):
            runner.run()
        self.assertTrue(runner.completed)
        self.assertEqual([(x.aid, x.attr, x.speed_now, x.speed_last, x.speed_now_incr_rate)
                          for x in session.added],
                         [(1, 'view', 800, 100, 7), (2, 'view', -200, -100, 1)])
        self.assertEqual(runner.metrics['persisted_changes'], 2)
        self.assertEqual(session.commits, 2)
        self.assertTrue(session.closed)

    def test_write_failure_is_visible_and_releases_session(self):
        self.history(1, [100, 200])
        runner = self.runner([record(1, 20800, 1000)])
        session = AnalysisSession(fail_commit=True)
        with mock.patch.object(s51, 'Session', return_value=session), \
                mock.patch.object(s51.DBOperation, 'query_video_pubdates_for_aids',
                                  return_value={1: 10000}), \
                self.assertLogs('RecentRecordsAnalystRunner', level='ERROR'):
            runner.run()
        self.assertFalse(runner.completed)
        self.assertEqual(runner.metrics['persisted_changes'], 0)
        self.assertEqual(runner.metrics['unexpected_increase'], 1)
        self.assertEqual(session.rollbacks, 1)
        self.assertTrue(session.closed)

    def test_failed_analysis_is_joined_then_recorded_as_failed_without_stopping_other_downstream(self):
        class Acquisition:
            api_stats = None

            def __init__(self, time_task, queue):
                for added, view in ((13600, 100), (17200, 200), (20800, 300)):
                    queue.put(record(1, added, view))

            def start(self):
                pass

            join = start

            def stats(self):
                return {}

        finished = []

        class OtherDownstream(threading.Thread):
            def __init__(self, *args):
                super().__init__()
                self.metrics = {}

            def run(self):
                finished.append(self)

        # Exercise the real thread wrapper, main join/summary and SQLite run record.
        runner = self.runner([])
        runner._analyse = mock.Mock(side_effect=RuntimeError('read unavailable'))
        db_path = os.path.join(self.folder, 'runs.sqlite3')
        with mock.patch.dict(os.environ, TDD_RUN_RECORD_DB=db_path), \
                mock.patch.object(s51, 'VideoRecordAcquisitionJob', Acquisition), \
                mock.patch.object(s51, 'RecordsSaveToFileRunner', OtherDownstream), \
                mock.patch.object(s51, 'RecentActivityFreqUpdateRunner', OtherDownstream), \
                mock.patch.object(s51, 'RecentRecordsAnalystRunner', return_value=runner), \
                self.assertLogs(level='INFO') as logs, self.assertRaises(SystemExit) as exit_:
            s51.hourly_video_record_add()
        self.assertEqual(exit_.exception.code, 1)
        self.assertEqual(len(finished), 2)
        self.assertTrue(all(not thread.is_alive() for thread in finished))
        self.assertIn('recent_records_analysis=incomplete', '\n'.join(logs.output))
        self.assertNotIn('Finish downstream data analysis pipelines!', '\n'.join(logs.output))
        with sqlite3.connect(db_path) as conn:
            self.assertEqual(conn.execute('SELECT status FROM run').fetchall(), [('failed',)])
            self.assertEqual(conn.execute("SELECT value FROM run_metric WHERE scope = "
                                          "'recent-records-analysis' AND name = 'completed'").fetchall(),
                             [(0.0,)])


if __name__ == '__main__':
    unittest.main()
