"""Successful writes avoid local persistence; failures remain manually inspectable."""
import json
import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest import mock
from uuid import uuid4

from psycopg.pq import TransactionStatus

from core import VideoRecord
from db.transaction import write
from db.write import insert_video_record_abnormal_change
from service import MemberCard


def save_member(conn, card, record, present):
    return card


def unsupported(conn, value):
    return value


def abnormal_input(aid=1):
    return dict(added=123, aid=aid, attr='view', speed_now=100, speed_last=10,
                speed_now_incr_rate=10, period_range=24, speed_period=20, speed_overall=5,
                this_record=VideoRecord(123, aid, '1AbCdEfGhJ', 200, 1, 2, 3, 4, 5, 6),
                last_record=VideoRecord(122, aid, '1AbCdEfGhJ', 100, 1, 2, 3, 4, 5, 6),
                description='view changed')


class TransactionTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name) / 'failed'
        environment = mock.patch.dict(os.environ, {'TDD_FAILURE_DIR': str(self.directory),
                                                   'TDD_TRIAL_RUN_ID': ''})
        environment.start()
        self.addCleanup(environment.stop)
        self.connection = mock.MagicMock()
        self.connection.__enter__.return_value = self.connection
        self.connection.info.transaction_status = TransactionStatus.IDLE
        pool = mock.patch('db.transaction.engine.connection', return_value=self.connection)
        pool.start()
        self.addCleanup(pool.stop)
        guard = mock.patch('db.migration.before_write', return_value={})
        self.guard = guard.start()
        self.addCleanup(guard.stop)

    def test_successful_append_skips_encoding_and_filesystem_work(self):
        with mock.patch('db.ingest.append_rows') as append, \
             mock.patch('db.transaction.encode', side_effect=AssertionError('must not encode')), \
             mock.patch('db.transaction._save_failure') as save:
            result = insert_video_record_abnormal_change(**abnormal_input())
        self.assertIsNone(result)
        append.assert_called_once()
        save.assert_not_called()
        self.assertFalse(self.directory.exists())

    def test_failure_is_saved_after_one_attempt_and_preserves_input_types(self):
        run_id = uuid4()
        payload = {'card': MemberCard(7, 'name', None, '', 'sign'),
                   'record': VideoRecord(123, 1, '1AbCdEfGhJ', 100, 1, 2, 3, 4, 5, 6),
                   'present': frozenset(('name', 'sign'))}
        error = RuntimeError('COMMIT outcome unknown')
        def fail(*args, **kwargs):
            self.assertFalse(self.directory.exists(), 'input must not be persisted before DB attempt')
            raise error
        self.guard.return_value = {'run_id': str(run_id)}
        with mock.patch.object(self.connection.transaction.return_value, '__exit__', side_effect=fail) as runner, \
             self.assertLogs('db.transaction', level='ERROR') as logs:
            with self.assertRaises(RuntimeError) as caught:
                write(save_member, **payload)
        self.assertIs(caught.exception, error)
        runner.assert_called_once()
        path, = self.directory.glob('*.json')
        data = json.loads(path.read_text())
        self.assertEqual((data['operation'], data['run_id'], data['commit_status']),
                         ('save_member', str(run_id), 'unconfirmed'))
        self.assertEqual(data['payload']['card']['values']['face'], '')
        self.assertIsNone(data['payload']['card']['values']['sex'])
        self.assertEqual(data['payload']['record']['response_type'], 'VideoRecord')
        self.assertEqual(data['payload']['record']['values']['view'], 100)
        self.assertEqual(data['payload']['present']['set_values'], ['name', 'sign'])
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(self.directory.stat().st_mode), 0o700)
        self.assertIn('reconcile manually before retrying', logs.output[0])

    def test_failures_create_separate_files_without_overwriting_prior_inputs(self):
        with mock.patch('db.transaction.engine.connection', side_effect=RuntimeError('offline')), \
             self.assertLogs('db.transaction', level='ERROR'):
            for aid in (1, 2):
                with self.assertRaises(RuntimeError):
                    insert_video_record_abnormal_change(**abnormal_input(aid))
        files = list(self.directory.glob('*.json'))
        self.assertEqual(len(files), 2)
        self.assertEqual({json.loads(path.read_text())['payload']['aid']
                          for path in files}, {1, 2})
        for path in files:
            payload = json.loads(path.read_text())['payload']
            self.assertEqual(payload['this_record']['values']['view'], 200)
            self.assertEqual(payload['last_record']['values']['view'], 100)

    def test_archive_failure_does_not_mask_database_exception(self):
        original = RuntimeError('database failed')
        with mock.patch('db.transaction.engine.connection', side_effect=original), \
             mock.patch('db.transaction._save_failure', side_effect=OSError('disk full')), \
             self.assertLogs('db.transaction', level='ERROR') as logs:
            with self.assertRaises(RuntimeError) as caught:
                insert_video_record_abnormal_change(**abnormal_input())
        self.assertIs(caught.exception, original)
        self.assertIn('could not be saved', logs.output[0])

    def test_partial_archive_is_removed_when_input_cannot_be_encoded(self):
        with mock.patch('db.transaction.engine.connection', side_effect=RuntimeError('offline')), \
             self.assertLogs('db.transaction', level='ERROR'):
            with self.assertRaisesRegex(RuntimeError, 'offline'):
                write(unsupported, object())
        self.assertEqual(list(self.directory.iterdir()), [])

    def test_operation_strings_are_rejected_before_database_work(self):
        with mock.patch('db.transaction.engine.connection') as run:
            with self.assertRaisesRegex(TypeError, 'callable'):
                write('append', objects=[])
        run.assert_not_called()

    def test_callable_without_name_preserves_original_exception(self):
        from functools import partial
        original = RuntimeError('database failed')
        with mock.patch('db.transaction.engine.connection', side_effect=original), \
             self.assertLogs('db.transaction', level='ERROR'):
            with self.assertRaises(RuntimeError) as caught:
                write(partial(unsupported, value=123))
        self.assertIs(caught.exception, original)

    def test_uncertain_commit_attempts_the_callback_once(self):
        error = RuntimeError('lost COMMIT acknowledgement')
        self.connection.transaction.return_value.__exit__.side_effect = error
        callback = mock.Mock(return_value=object())
        with self.assertLogs('db.transaction', level='ERROR'):
            with self.assertRaises(RuntimeError) as caught:
                write(callback)
        self.assertIs(caught.exception, error)
        callback.assert_called_once_with(self.connection)
        self.guard.assert_called_once_with(self.connection)

    def test_result_is_returned_only_after_commit_without_encoding(self):
        result = object()
        committed = []
        self.connection.transaction.return_value.__exit__.side_effect = lambda *args: committed.append(True)
        with mock.patch('db.transaction.encode', side_effect=AssertionError('must not encode')):
            self.assertIs(write(lambda conn: result), result)
        self.assertEqual(committed, [True])

    def test_existing_transaction_is_rejected_before_business_work(self):
        self.connection.info.transaction_status = TransactionStatus.INTRANS
        callback = mock.Mock()
        with self.assertLogs('db.transaction', level='ERROR'):
            with self.assertRaisesRegex(RuntimeError, 'idle connection'):
                write(callback)
        callback.assert_not_called()
        self.guard.assert_not_called()
