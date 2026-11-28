"""Abnormal changes bind observations directly and retain trial cleanup keys."""
from datetime import datetime, timezone
from contextlib import nullcontext
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock
from uuid import uuid4

from psycopg.pq import TransactionStatus

from core import VideoRecord
from db.write import _insert_video_record_abnormal_change
from test_ingest_manifest import connection
from test_remaining_script_run_records import _load


class AbnormalChangeWriteTest(unittest.TestCase):
    def observations(self):
        return [VideoRecord(3600, 7, '1AbCdEfGhJ', 1000, 1, 2, 3, 4, 5, 6),
                VideoRecord(7200, 7, '1AbCdEfGhJ', 1100, 1, 2, 3, 4, 5, 6),
                VideoRecord(10800, 7, '1AbCdEfGhJ', 3000, 1, 2, 3, 4, 5, 6)]

    def run_analysis(self, conn, records, directory):
        script = _load('51_hourly-video-record-add.py')
        runner = script.RecentRecordsAnalystRunner(records, '2026-01-01 03:00',
                                                    data_folder=directory)
        conn.info.transaction_status = TransactionStatus.IDLE
        conn.transaction.return_value.__enter__.side_effect = lambda: setattr(
            conn.info, 'transaction_status', TransactionStatus.INTRANS)
        with mock.patch.dict(os.environ, {'TDD_TRIAL_RUN_ID': '', 'TDD_FAILURE_DIR': directory}), \
                mock.patch.object(script, 'query_video_pubdate_all', return_value=[(7, 0)]), \
                mock.patch('db.transaction.engine.connection', return_value=nullcontext(conn)), \
                mock.patch('db.migration.before_write', return_value={'run_id': None}), \
                mock.patch.object(runner, '_assemble_record_abnormal_change',
                                  wraps=runner._assemble_record_abnormal_change) as assemble:
            runner.process()
        return assemble.call_args.kwargs

    def test_actual_hourly_analysis_passes_observations_and_description_to_sql(self):
        records = self.observations()
        conn, cursors = connection([[(31,)]])
        with tempfile.TemporaryDirectory() as directory:
            assembled = self.run_analysis(conn, records, directory)
            self.assertEqual(list(Path(directory).iterdir()), [])
        self.assertIs(assembled['this_record'], records[-1])
        self.assertIs(assembled['last_record'], records[-2])
        row, = cursors[0].executemany.call_args.args[1]
        self.assertEqual(row[:9], (datetime.fromtimestamp(10800, timezone.utc), 7, 'view',
                                  1900.0, 100.0, 18.0, 7200, 1000.0, 1000.0))
        self.assertIsNone(row[9])
        self.assertEqual(row[10:18], (datetime.fromtimestamp(10800, timezone.utc),) + tuple(records[-1][3:]))
        self.assertIsNone(row[18])
        self.assertEqual(row[19:27], (datetime.fromtimestamp(7200, timezone.utc),) + tuple(records[-2][3:]))
        self.assertEqual(row[27], assembled['description'])
        self.assertEqual(row[27],
                         'unexpected increase detected, speed now of prop view is 18.00, > 200%')
        self.assertIsNone(row[28])

    def test_actual_hourly_analysis_warns_on_failed_insert_and_preserves_input(self):
        records = self.observations()
        conn, cursors = connection([[]])
        error = RuntimeError('insert failed')
        cursors[0].executemany.side_effect = error
        with tempfile.TemporaryDirectory() as directory, self.assertLogs(level='WARNING') as logs:
            self.run_analysis(conn, records, directory)
            path, = Path(directory).glob('*.json')
            saved = json.loads(path.read_text())
        self.assertTrue(any('Fail to add abnormal change of video aid 7' in entry for entry in logs.output))
        self.assertEqual(saved['operation'], '_insert_video_record_abnormal_change')
        self.assertEqual(saved['payload']['this_record']['values'], records[-1]._asdict())
        self.assertEqual(saved['payload']['last_record']['values'], records[-2]._asdict())
        self.assertIn('unexpected increase detected', saved['payload']['description'])
        self.assertEqual(saved['commit_status'], 'unconfirmed')
        conn.commit.assert_not_called()

    def test_direct_values_preserve_nullable_counters_without_source_record_ids(self):
        current = VideoRecord(123, 7, '1AbCdEfGhJ', 200, 11, None, 13, 14, 15, 16)
        previous = VideoRecord(122, 7, '1AbCdEfGhJ', 100, 21, 22, 23, None, 25, 26)
        conn, cursors = connection([[(31,)], []])
        run_id = uuid4()
        with mock.patch.dict(os.environ, {'TDD_TRIAL_RUN_ID': str(run_id)}):
            result = _insert_video_record_abnormal_change(
                conn, added=124, aid=7, attr='view', speed_now=100, speed_last=10,
                speed_now_incr_rate=10, period_range=24, speed_period=20, speed_overall=5,
                this_record=current, last_record=previous, description='view\x00 changed')
        self.assertIsNone(result)
        query, rows = cursors[0].executemany.call_args.args
        self.assertNotIn('bvid', query.as_string())
        self.assertEqual(rows, [(
            datetime.fromtimestamp(124, timezone.utc), 7, 'view', 100, 10, 10, 24, 20, 5,
            None, datetime.fromtimestamp(123, timezone.utc), 200, 11, None, 13, 14, 15, 16,
            None, datetime.fromtimestamp(122, timezone.utc), 100, 21, 22, 23, None, 25, 26,
            'view changed', None)])
        manifest, = cursors[1].executemany.call_args.args[1]
        self.assertEqual(manifest[2:4], (run_id, 'tdd_video_record_abnormal_change'))
        self.assertEqual(manifest[4].obj, {'id': 31})
        self.assertFalse(hasattr(current, 'id'))
        self.assertFalse(hasattr(previous, 'id'))


if __name__ == '__main__':
    unittest.main()
