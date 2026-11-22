"""Nullable sprint statistics persist without a million-status transition."""
from datetime import datetime, timezone
from contextlib import nullcontext
import json
import os
from pathlib import Path
from queue import Queue
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock
from uuid import uuid4

from psycopg.pq import TransactionStatus

from db import TddSprintVideoRecord
from db.models import STAT_FIELDS
from job import AddSprintVideoRecordJob
from service import Service
from service.endpoints import get_video_view_trimmed
from db.sprint import _insert_sprint_video_record, insert_sprint_video_record
from task import add_sprint_video_record_via_video_view
from test_endpoint_get_video_view_trimmed import valid_response
from test_ingest_manifest import connection


def sprint_stat(**values):
    stat = get_video_view_trimmed.get(lambda *args, **kwargs: valid_response()).stat
    return {field: values.get(field, getattr(stat, field)) for field in STAT_FIELDS}


class SprintNullableRecordTest(unittest.TestCase):
    def test_task_uses_requested_aid_and_persists_trimmed_response_without_mutation(self):
        service = Service(mode='worker', endpoints={})
        self.addCleanup(service._session.close)
        for response_view, stored_view in ((None, None), ('--', -1)):
            with self.subTest(view=response_view):
                response = valid_response()
                # Both response aid values are 7; the requested aid must win.
                response['data']['stat']['view'] = response_view
                stored_row = (
                    31, datetime.fromtimestamp(124, timezone.utc), 10,
                    stored_view, 2, 3, 4, 5, 6, 7)
                conn, cursors = connection([[stored_row]])
                conn.info.transaction_status = TransactionStatus.IDLE
                conn.transaction.return_value.__enter__.side_effect = lambda: setattr(
                    conn.info, 'transaction_status', TransactionStatus.INTRANS)
                with mock.patch.object(service, '_get', return_value=response) as request, \
                        mock.patch('task.task.get_ts_s', return_value=123), \
                        mock.patch.dict(os.environ, {'TDD_TRIAL_RUN_ID': ''}), \
                        mock.patch('db.transaction.engine.connection', return_value=nullcontext(conn)), \
                        mock.patch('db.migration.before_write', return_value={'run_id': None}), \
                        mock.patch('db.transaction._save_failure') as save:
                    record = add_sprint_video_record_via_video_view(10, service)
                request.assert_called_once_with('get_video_view_trimmed', params={'aid': 10},
                                                headers=None, retry=None, timeout=None,
                                                colddown_factor=None)
                self.assertEqual(cursors[0].executemany.call_args.args[1], [(
                    datetime.fromtimestamp(123, timezone.utc), 10,
                    stored_view, 2, 3, 4, 5, 6, 7)])
                self.assertIs(type(record), TddSprintVideoRecord)
                self.assertEqual((record.id, record.added, record.aid, record.view),
                                 (31, 124, 10, stored_view))
                self.assertEqual(response['data']['stat']['view'], response_view)
                save.assert_not_called()

    def test_trimmed_parser_accepts_explicit_null_view(self):
        response = valid_response()
        response['data']['stat']['view'] = None
        parsed = get_video_view_trimmed.get(lambda *args,**kwargs:response)
        self.assertIsNone(parsed.stat.view)

    def test_nullable_record_is_appended_without_finished_update(self):
        stat = sprint_stat(view=None, danmaku=None, reply=None,
                           favorite=None, coin=None, share=None, like=None)
        added = datetime.fromtimestamp(123, timezone.utc)
        conn, cursors = connection([[(31, added, 10) + (None,) * 7], []])
        run_id = uuid4()
        with mock.patch.dict(os.environ, {'TDD_TRIAL_RUN_ID': str(run_id)}):
            record = _insert_sprint_video_record(conn, 10, 123, stat)
        self.assertIs(type(record), TddSprintVideoRecord)
        self.assertEqual((record.id, record.added, record.aid, record.view), (31, 123, 10, None))
        conn.execute.assert_not_called()
        self.assertEqual(cursors[0].executemany.call_args.args[1], [(added, 10) + (None,) * 7])
        manifest, = cursors[1].executemany.call_args.args[1]
        self.assertEqual(manifest[2:4], (run_id, 'tdd_sprint_video_record'))
        self.assertEqual(manifest[4].obj, {'id': 31})
        self.assertIsNone(stat['view'])
        self.assertFalse(hasattr(stat, 'id'))

    def test_normalized_unknown_view_is_stored_without_mutating_input(self):
        stat = sprint_stat(view=-1, danmaku=1, reply=2, favorite=3, coin=4, share=5, like=6)
        added = datetime.fromtimestamp(123, timezone.utc)
        conn, cursors = connection([[(31, added, 10, -1, 1, 2, 3, 4, 5, 6)]])
        with mock.patch.dict(os.environ, {'TDD_TRIAL_RUN_ID': ''}), \
                mock.patch('db.migration.uuid4', side_effect=AssertionError('formal write needs no manifest')):
            record = _insert_sprint_video_record(conn, 10, 123, stat)
        self.assertEqual(cursors[0].executemany.call_args.args[1], [(added, 10, -1, 1, 2, 3, 4, 5, 6)])
        self.assertEqual(record.view, -1)
        self.assertEqual(stat['view'], -1)
        conn.cursor.assert_called_once_with()

    def test_finished_status_and_result_use_returned_database_values(self):
        stat = sprint_stat(view=999999)
        # RETURNING, not mutation of the response, defines the stored snapshot.
        added = datetime.fromtimestamp(124, timezone.utc)
        conn, _ = connection([[(31, added, 10, 1000000, 1, 2, 3, 4, 5, 6)]])
        with mock.patch.dict(os.environ, {'TDD_TRIAL_RUN_ID': ''}):
            record = _insert_sprint_video_record(conn, 10, 123, stat)
        self.assertEqual((record.id, record.added, record.view), (31, 124, 1000000))
        self.assertEqual(conn.execute.call_args_list, [mock.call(
            'UPDATE tdd_sprint_video SET status = %s WHERE aid = %s', ('finished', 10))])
        self.assertEqual(stat['view'], 999999)

    def test_manifest_failure_prevents_returning_a_confirmed_sprint_record(self):
        conn, cursors = connection([[(
            31, datetime.fromtimestamp(123, timezone.utc), 10, 1000000, 1, 2, 3, 4, 5, 6)], []])
        cursors[1].executemany.side_effect = RuntimeError('manifest failed')
        with mock.patch.dict(os.environ, {'TDD_TRIAL_RUN_ID': str(uuid4())}), \
                self.assertRaisesRegex(RuntimeError, 'manifest failed'):
            _insert_sprint_video_record(conn, 10, 123, sprint_stat(view=1000000))
        conn.commit.assert_not_called()

    def test_missing_returning_result_is_not_accepted(self):
        conn, _ = connection([[None]])
        with self.assertRaisesRegex(RuntimeError, 'complete key'):
            _insert_sprint_video_record(conn, 10, 123, sprint_stat())

    def test_failed_insert_keeps_converted_stat_for_manual_reconciliation(self):
        stat = sprint_stat(view=-1)
        with tempfile.TemporaryDirectory() as directory, \
                mock.patch.dict(os.environ, {'TDD_FAILURE_DIR': directory, 'TDD_TRIAL_RUN_ID': ''}), \
                mock.patch('db.transaction.engine.connection', side_effect=RuntimeError('offline')), \
                self.assertLogs('db.transaction', level='ERROR'):
            with self.assertRaisesRegex(RuntimeError, 'offline'):
                insert_sprint_video_record(10, 123, stat)
            path, = Path(directory).glob('*.json')
            saved = json.loads(path.read_text())
        self.assertEqual(saved['operation'], '_insert_sprint_video_record')
        self.assertEqual(saved['payload']['aid'], 10)
        self.assertEqual(saved['payload']['added'], 123)
        self.assertEqual(saved['payload']['stat'], stat)
        self.assertEqual(saved['commit_status'], 'unconfirmed')

    def test_job_counts_confirmed_nullable_record_without_million_status(self):
        queue = Queue()
        queue.put(10)
        job = AddSprintVideoRecordJob('nullable',queue,mock.Mock())
        self.addCleanup(job.cleanup)
        with mock.patch('job.AddSprintVideoRecordJob.fetch_video_record_via_video_view',
                        return_value=SimpleNamespace(aid=10,view=None)), \
                mock.patch('job.AddSprintVideoRecordJob.commit_sprint_video_record',
                           return_value=SimpleNamespace(aid=10,view=None)):
            job.process()
        self.assertEqual(job.stat.condition['persisted'],1)
        self.assertEqual(job.stat.condition['success'],1)
        self.assertEqual(job.stat.condition['million_success'],0)
        self.assertEqual(job.stat.condition['exception'],0)
