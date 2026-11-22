"""Hourly work continues after local failures and reports them after all stages."""
import gzip
import os
from pathlib import Path
from queue import Queue
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

from core import VideoRecord
from job import BatchInsertVideoRecordJob, JobStat
from runrecord._sqlite import sqlite3
from test_remaining_script_run_records import _DEPS, _load

s51 = _load('51_hourly-video-record-add.py') if _DEPS else None


def _record(aid, view=100, added=1789243200):
    return VideoRecord(added, aid, f'BV{aid}', view, 1, 2, 3, 4, 5, 6)


@unittest.skipUnless(_DEPS, 'db dependencies unavailable')
class HourlyFailureToleranceTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        previous = os.getcwd()
        os.chdir(directory.name)
        self.addCleanup(os.chdir, previous)
        Path('data').mkdir()

    def test_failed_batch_still_saves_confirmed_csv_snapshot_and_runs_analysis_before_failed_status(self):
        fetched = [_record(1), _record(2)]

        def fetch_and_write(aids, downstream, **kwargs):
            queue = Queue()
            for record in fetched:
                queue.put(record)
            queue.put(None)
            writer = BatchInsertVideoRecordJob('partial', queue, downstream, batch_size=1)
            writer.process()
            fetch = JobStat()
            fetch.condition['success'] = 2
            return fetch, writer.stat, JobStat(), None

        recorder = s51.RunRecorder.start(s51.script_fullname, db_path='run-records.sqlite3')
        with mock.patch.object(s51.RunRecorder, 'start', return_value=recorder), \
                mock.patch.object(s51, 'get_ts_s_str', return_value='2026-09-13 04:00:00'), \
                mock.patch.object(s51, 'get_ts_ms', return_value=1789243200000), \
                mock.patch.object(s51, 'get_need_insert_aid_list', return_value=[1, 2]), \
                mock.patch.object(s51, 'fetch_and_batch_insert_records', side_effect=fetch_and_write), \
                mock.patch('job.BatchInsertVideoRecordJob.commit_video_records_batch',
                           side_effect=[RuntimeError('write unconfirmed'), None]) as commit, \
                mock.patch.object(s51, 'query_video_pubdate_all', return_value=[(2, 0)]) as analyze, \
                mock.patch.object(s51, 'update_video_recent') as recent, \
                mock.patch.object(s51, 'update_video_freq') as freq, \
                self.assertLogs(level='WARNING') as logs:
            with self.assertRaises(SystemExit) as stopped:
                s51.hourly_video_record_add()
        self.assertEqual(stopped.exception.code, 1)
        self.assertEqual(commit.call_count, 2)
        analyze.assert_called_once()
        recent.assert_called_once()
        freq.assert_called_once()
        content = Path('data/2026-09-13 04:00.csv').read_text()
        self.assertIn(',2,BV2,', content)
        self.assertNotIn(',1,BV1,', content)
        with gzip.open(s51.full_scan_snapshot_path('2026-09-13 04:00'), 'rt') as stream:
            self.assertEqual(stream.read(), content)
        with sqlite3.connect('run-records.sqlite3') as connection:
            self.assertEqual(connection.execute('SELECT status FROM run WHERE run_id=?',
                                                (recorder.run_id,)).fetchone()[0], 'failed')
            counts = dict(connection.execute(
                "SELECT name,value FROM run_metric WHERE run_id=? AND scope='record-acquisition'",
                (recorder.run_id,)))
        self.assertEqual(counts, {'db_write_failed': 1.0, 'queue_full_dropped': 0.0,
                                  'persisted_success_mismatch': 1.0})
        self.assertEqual([entry.getMessage() for entry in logs.records
                          if entry.getMessage().startswith('Video collection results:')], [
            'Video collection results: fetched=2, persisted=1, db_write_failed=1, queue_full_dropped=0.',
        ])

    def test_snapshot_save_failure_is_counted_and_other_stages_finish_before_failure(self):
        records = [_record(1)]
        save = s51.RecordsSaveToFileRunner(records, '2026-09-13 04:00')
        with mock.patch.object(s51.gzip, 'open', side_effect=OSError('snapshot disk unavailable')), \
                self.assertLogs(save.logger, level='ERROR'):
            save.run()
        self.assertIsNone(save.error)
        self.assertEqual(save.failure_count, 1)
        self.assertEqual(save.metrics, {'snapshot_save_failed': 1})
        self.assertTrue(Path('data/2026-09-13 04:00.csv').is_file())

        def acquisition(time_task, queue):
            queue.put(records[0])
            return SimpleNamespace(start=lambda: None, join=lambda: None, stats=lambda: {},
                                   api_stats=None, error=None, failure_counts={})

        recorder = mock.Mock()
        with mock.patch.object(s51, 'VideoRecordAcquisitionJob', side_effect=acquisition), \
                mock.patch.object(s51, 'RecordsSaveToFileRunner', return_value=save), \
                mock.patch.object(save, 'start'), mock.patch.object(save, 'join'), \
                mock.patch.object(s51, 'RecentRecordsAnalystRunner',
                                  return_value=mock.Mock(error=None, failure_count=0, metrics={})) as analyze, \
                mock.patch.object(s51, 'RecentActivityFreqUpdateRunner',
                                  return_value=mock.Mock(error=None, failure_count=0, metrics={})) as activity:
            with self.assertRaisesRegex(RuntimeError, 'after downstream completed'):
                s51.run_hourly_video_record_add('2026-09-13 04:00', recorder)
        for stage in (analyze, activity):
            stage.return_value.start.assert_called_once()
            stage.return_value.join.assert_called_once()
        recorder.add_metric.assert_any_call('records-save-to-file', 'snapshot_save_failed', 1, unit='count')

    def test_abnormal_insert_failure_warns_for_aid_and_continues_next_video(self):
        for hour, added, view in ((1, 3600, 1000), (2, 7200, 1100)):
            rows = ''.join(f'{added},{aid},BV{aid},{view},1,2,3,4,5,6\n' for aid in (1, 2))
            Path(f'data/2026-09-13 {hour:02d}:00.csv').write_text(
                'added,aid,bvid,view,danmaku,reply,favorite,coin,share,like\n' + rows)
        records = [_record(aid, 100, 10800) for aid in (1, 2)]
        def acquisition(time_task, queue):
            for record in records:
                queue.put(record)
            return SimpleNamespace(start=lambda: None, join=lambda: None, stats=lambda: {},
                                   api_stats=None, error=None, failure_counts={})

        recorder = s51.RunRecorder.start(s51.script_fullname, db_path='run-records.sqlite3')
        with mock.patch.object(s51.RunRecorder, 'start', return_value=recorder), \
                mock.patch.object(s51, 'get_ts_s_str', return_value='2026-09-13 03:00:00'), \
                mock.patch.object(s51, 'VideoRecordAcquisitionJob', side_effect=acquisition), \
                mock.patch.object(s51, 'query_video_pubdate_all', return_value=[(1, 0), (2, 0)]), \
                mock.patch.object(s51, 'insert_video_record_abnormal_change',
                                  side_effect=[RuntimeError('insert failed'), None]) as insert, \
                mock.patch.object(s51, 'update_video_recent'), \
                mock.patch.object(s51, 'update_video_freq') as freq, \
                self.assertLogs(level='WARNING') as logs:
            with self.assertRaises(SystemExit) as stopped:
                s51.hourly_video_record_add()
        self.assertEqual(stopped.exception.code, 1)
        self.assertEqual([call.kwargs['aid'] for call in insert.call_args_list], [1, 2])
        self.assertTrue(any('aid 1' in entry for entry in logs.output))
        freq.assert_called_once()
        self.assertTrue(Path('data/2026-09-13 03:00.csv').is_file())
        with sqlite3.connect('run-records.sqlite3') as connection:
            self.assertEqual(connection.execute('SELECT status FROM run WHERE run_id=?',
                                                (recorder.run_id,)).fetchone()[0], 'failed')
            counts = dict(connection.execute(
                "SELECT name,value FROM run_metric WHERE run_id=? AND scope='recent-records-analysis'",
                (recorder.run_id,)))
        self.assertEqual(counts, {'abnormal_write_failed': 1.0})

    def test_pubdate_read_failure_finishes_its_stage_without_unhandled_error(self):
        runner = s51.RecentRecordsAnalystRunner([_record(1)], '2026-09-13 04:00')
        with mock.patch.object(s51, 'query_video_pubdate_all', side_effect=RuntimeError('read timed out')), \
                mock.patch.object(s51, 'insert_video_record_abnormal_change') as insert, \
                self.assertLogs(runner.logger, level='ERROR'):
            runner.run()
        insert.assert_not_called()
        self.assertIsNone(runner.error)
        self.assertEqual(runner.failure_count, 1)
        self.assertEqual(runner.metrics['pubdate_read_failed'], 1)


if __name__ == '__main__':
    unittest.main()
