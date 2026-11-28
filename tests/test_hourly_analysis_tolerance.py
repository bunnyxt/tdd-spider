"""Untrusted headers skip their stage; per-video failures leave later work runnable."""
import gzip
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

from core import VideoRecord
from runrecord._sqlite import sqlite3
from test_remaining_script_run_records import _DEPS, _load

s51 = _load('51_hourly-video-record-add.py') if _DEPS else None
HEADER = 'added,aid,bvid,view,danmaku,reply,favorite,coin,share,like\n'


def _record(aid, added=10800, view=100):
    return VideoRecord(added, aid, f'BV{aid}', view, 1, 2, 3, 4, 5, 6)


def _acquisition(records):
    def acquire(time_task, queue):
        for record in records:
            queue.put(record)
        return SimpleNamespace(start=lambda: None, join=lambda: None, stats=lambda: {},
                               api_stats=None, error=None, failure_counts={})
    return acquire


@unittest.skipUnless(_DEPS, 'db dependencies unavailable')
class HourlyAnalysisToleranceTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        previous = os.getcwd()
        os.chdir(directory.name)
        self.addCleanup(os.chdir, previous)
        Path('data/0400').mkdir(parents=True)

    def assert_failed_metric(self, recorder, scope, key):
        with sqlite3.connect('run-records.sqlite3') as connection:
            self.assertEqual(connection.execute('SELECT status FROM run WHERE run_id=?',
                                                (recorder.run_id,)).fetchone()[0], 'failed')
            self.assertEqual(connection.execute(
                'SELECT value FROM run_metric WHERE run_id=? AND scope=? AND name=?',
                (recorder.run_id, scope, key)).fetchone()[0], 1.0)

    def test_bad_snapshot_header_skips_activity_but_runs_freq_and_reports_failed_run(self):
        snapshot = s51.full_scan_snapshot_path('2026-09-06 04:00')
        with gzip.open(snapshot, 'wt') as stream:
            stream.write('view,aid,bvid,added,danmaku,reply,favorite,coin,share,like\n'
                         '1788638400,1,BV1,0,0,0,0,0,0,0\n')
        recorder = s51.RunRecorder.start(s51.script_fullname, db_path='run-records.sqlite3')
        with mock.patch.object(s51.RunRecorder, 'start', return_value=recorder), \
                mock.patch.object(s51, 'get_ts_s_str', return_value='2026-09-13 04:00:00'), \
                mock.patch.object(s51, 'VideoRecordAcquisitionJob', side_effect=_acquisition([_record(1)])), \
                mock.patch.object(s51, 'query_video_pubdate_all', return_value=[(1, 0)]), \
                mock.patch.object(s51, 'update_video_recent') as recent, \
                mock.patch.object(s51, 'update_video_activity') as activity, \
                mock.patch.object(s51, 'update_video_freq') as freq, \
                self.assertLogs(level='ERROR') as logs:
            with self.assertRaises(SystemExit) as stopped:
                s51.hourly_video_record_add()
        self.assertEqual(stopped.exception.code, 1)
        activity.assert_not_called()
        recent.assert_called_once()
        freq.assert_called_once()
        self.assertTrue(any(snapshot in entry for entry in logs.output))
        self.assertTrue(Path('data/2026-09-13 04:00.csv').is_file())
        self.assert_failed_metric(recorder, 'recent-activity-freq-update', 'bad_header')

    def test_bad_recent_header_skips_all_analysis_without_affecting_other_stages(self):
        Path('data/2026-09-13 01:00.csv').write_text(HEADER + '3600,1,BV1,1000,1,2,3,4,5,6\n')
        bad_path = Path('data/2026-09-13 02:00.csv')
        bad_path.write_text('aid,added,bvid,view,danmaku,reply,favorite,coin,share,like\n'
                            '7200,1,BV1,1100,1,2,3,4,5,6\n')
        recorder = s51.RunRecorder.start(s51.script_fullname, db_path='run-records.sqlite3')
        with mock.patch.object(s51.RunRecorder, 'start', return_value=recorder), \
                mock.patch.object(s51, 'get_ts_s_str', return_value='2026-09-13 03:00:00'), \
                mock.patch.object(s51, 'VideoRecordAcquisitionJob', side_effect=_acquisition([_record(1)])), \
                mock.patch.object(s51, 'query_video_pubdate_all') as pubdates, \
                mock.patch.object(s51, 'insert_video_record_abnormal_change') as insert, \
                mock.patch.object(s51, 'update_video_recent') as recent, \
                mock.patch.object(s51, 'update_video_freq') as freq, \
                self.assertLogs(level='ERROR') as logs:
            with self.assertRaises(SystemExit):
                s51.hourly_video_record_add()
        pubdates.assert_not_called()
        insert.assert_not_called()
        recent.assert_called_once()
        freq.assert_called_once()
        self.assertTrue(any(str(bad_path) in entry for entry in logs.output))
        self.assertTrue(Path('data/2026-09-13 03:00.csv').is_file())
        self.assert_failed_metric(recorder, 'recent-records-analysis', 'bad_header')

    def test_empty_pubdate_result_counts_failure_and_skips_analysis(self):
        runner = s51.RecentRecordsAnalystRunner([_record(1)], '2026-09-13 03:00')
        with mock.patch.object(s51, 'query_video_pubdate_all', return_value=[]), \
                mock.patch.object(s51, 'insert_video_record_abnormal_change') as insert, \
                self.assertLogs(runner.logger, level='ERROR'):
            runner.run()
        insert.assert_not_called()
        self.assertIsNone(runner.error)
        self.assertEqual(runner.failure_count, 1)
        self.assertEqual(runner.metrics, {'pubdate_empty': 1})

    def test_per_video_read_failure_skips_only_that_video_and_reports_count(self):
        Path('data/2026-09-13 01:00.csv').write_text(
            HEADER + '3600,1,BV1,0,0,0,0,0,0,0\n3600,2,BV2,1000,1,2,3,4,5,6\n')
        Path('data/2026-09-13 02:00.csv').write_text(
            HEADER + '7200,1,BV1,1100,1,2,3,4,5,6\n7200,2,BV2,1100,1,2,3,4,5,6\n')
        recorder = s51.RunRecorder.start(s51.script_fullname, db_path='run-records.sqlite3')
        with mock.patch.object(s51.RunRecorder, 'start', return_value=recorder), \
                mock.patch.object(s51, 'get_ts_s_str', return_value='2026-09-13 03:00:00'), \
                mock.patch.object(s51, 'VideoRecordAcquisitionJob',
                                  side_effect=_acquisition([_record(1), _record(2)])), \
                mock.patch.object(s51, 'query_video_pubdate_all', return_value=[(1, 0), (2, 0)]), \
                mock.patch.object(s51, 'query_video_records_of_given_aid_added_before_given_ts',
                                  side_effect=RuntimeError('history read timed out')) as history, \
                mock.patch.object(s51, 'insert_video_record_abnormal_change') as insert, \
                mock.patch.object(s51, 'update_video_recent'), \
                mock.patch.object(s51, 'update_video_freq') as freq, \
                self.assertLogs(level='WARNING') as logs:
            with self.assertRaises(SystemExit):
                s51.hourly_video_record_add()
        history.assert_called_once_with(1, 3600)
        self.assertEqual([call.kwargs['aid'] for call in insert.call_args_list], [2])
        freq.assert_called_once()
        self.assertTrue(any('aid 1' in entry and 'history read timed out' in entry
                            for entry in logs.output))
        self.assert_failed_metric(recorder, 'recent-records-analysis', 'per_video_read_failed')


if __name__ == '__main__':
    unittest.main()
