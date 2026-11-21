"""Collected records bind directly to SQL and retain their CSV/analysis values."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from queue import Queue
import tempfile
import unittest
from unittest import mock
from uuid import uuid4

from core import VideoRecord
from db.write import VIDEO_RECORD_COLUMNS
from job import BatchInsertVideoRecordJob
from task import commit_video_records_batch, fetch_video_record_via_video_view
from test_ingest_manifest import connection
from test_endpoint_get_video_view import expected_result
from test_remaining_script_run_records import _load


class VideoRecordWriteTest(unittest.TestCase):
    def record(self):
        return VideoRecord(123, 7, '1AbCdEfGhJ', 100, 1, 2, 3, 4, 5, 6)

    def test_batch_binds_fields_and_returns_independent_keys_without_mutating_input(self):
        record = self.record()
        records = [record, record]
        added = datetime.fromtimestamp(123, timezone.utc)
        expected = [(7, added, 10), (7, added, 11)]
        conn, cursors = connection([expected, []])
        run_id = uuid4()
        def run(factory, callback, *, run_id):
            self.assertEqual(str(run_id), os.environ['TDD_TRIAL_RUN_ID'])
            return callback(conn)
        with mock.patch.dict(os.environ, {'TDD_TRIAL_RUN_ID': str(run_id)}), \
                mock.patch('db.ingest.run_transaction', side_effect=run), \
                mock.patch('db.models.TddVideoRecord', side_effect=AssertionError('no mutable record')), \
                mock.patch('db.observation._save_failure') as save:
            result = commit_video_records_batch(records)
        self.assertEqual(result, expected)
        query, rows = cursors[0].executemany.call_args.args
        self.assertNotIn('bvid', query.as_string())
        self.assertEqual(rows, [(added, 7, 100, 1, 2, 3, 4, 5, 6)] * 2)
        self.assertEqual(tuple(VIDEO_RECORD_COLUMNS), VideoRecord._fields[:2] + VideoRecord._fields[3:])
        self.assertEqual([row[4].obj['id'] for row in cursors[1].executemany.call_args.args[1]], [10, 11])
        self.assertIs(records[0], record)
        self.assertEqual(record.bvid, '1AbCdEfGhJ')
        self.assertFalse(hasattr(record, 'id'))
        save.assert_not_called()

    def test_fetch_returns_shared_record_and_keeps_counter_normalization(self):
        view = expected_result()
        view = view._replace(stat=view.stat._replace(view='--'))
        service = mock.Mock()
        service.get_video_view_trimmed.return_value = view
        with mock.patch('task.task.get_ts_s', return_value=123):
            record = fetch_video_record_via_video_view(7, service)
        self.assertIs(type(record), VideoRecord)
        self.assertEqual((record.added, record.aid, record.bvid, record.view),
                         (123, 7, view.bvid.removeprefix('BV'), -1))

    def test_failed_batch_preserves_original_collected_input_for_manual_reconciliation(self):
        record = self.record()
        with tempfile.TemporaryDirectory() as directory, \
                mock.patch.dict(os.environ, {'TDD_FAILURE_DIR': directory, 'TDD_TRIAL_RUN_ID': ''}), \
                mock.patch('db.ingest.run_transaction', side_effect=RuntimeError('write failed')), \
                self.assertLogs('db.observation', level='ERROR'):
            with self.assertRaisesRegex(RuntimeError, 'write failed'):
                commit_video_records_batch([record])
            path, = Path(directory).glob('*.json')
            saved = json.loads(path.read_text())
        self.assertEqual(saved['operation'], 'append_video_records')
        self.assertEqual(saved['payload']['records'][0]['values'], record._asdict())
        self.assertEqual(saved['commit_status'], 'unconfirmed')

    def test_writer_passes_original_records_downstream_only_after_confirmed_commit(self):
        record = self.record()
        downstream = Queue()
        writer = BatchInsertVideoRecordJob('writer', Queue(), downstream)
        with mock.patch('job.BatchInsertVideoRecordJob.commit_video_records_batch') as commit:
            writer._flush([record])
        self.assertIs(commit.call_args.args[0][0], record)
        self.assertIs(downstream.get_nowait(), record)
        with mock.patch('job.BatchInsertVideoRecordJob.commit_video_records_batch',
                        side_effect=RuntimeError('unconfirmed')), self.assertLogs(level='ERROR'):
            writer._flush([record])
        self.assertTrue(downstream.empty())
        self.assertEqual(writer.stat.condition['persisted'], 1)

    def test_csv_history_fresh_and_publication_time_use_the_same_record_type(self):
        script = _load('51_hourly-video-record-add.py')
        record = self.record()._replace(added=10800, view=300)
        with tempfile.TemporaryDirectory() as directory:
            for hour, added, view in ((1, 3600, 100), (2, 7200, 200)):
                Path(directory, f'2026-01-01 {hour:02d}:00.csv').write_text(
                    'added,aid,bvid,view,danmaku,reply,favorite,coin,share,like\n'
                    f'{added},7,1AbCdEfGhJ,{view},1,2,3,4,5,6\n')
            runner = script.RecentRecordsAnalystRunner([record], '2026-01-01 03:00',
                                                       data_folder=directory + '/')
            with mock.patch.object(script, 'Session'), \
                    mock.patch.object(script.DBOperation, 'query_video_pubdate_all', return_value=[(7, 0)]), \
                    mock.patch.object(runner, '_calc_record_speed', wraps=runner._calc_record_speed) as speed:
                runner.process()
        self.assertEqual(speed.call_count, 4)
        for call in speed.call_args_list:
            self.assertTrue(all(type(item) is VideoRecord for item in call.args[:2]))
        self.assertEqual(speed.call_args_list[-1].args[0], VideoRecord(0, 7, script.a2b(7), 0, 0, 0, 0, 0, 0, 0))
        self.assertIs(speed.call_args_list[-1].args[1], record)
