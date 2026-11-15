"""Successful writes avoid local persistence; failures remain manually inspectable."""
import json
import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest import mock
from uuid import uuid4

from db import TddVideoRecord
from db.observation import observe
from service import MemberCard


class ObservationTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name) / 'failed'
        environment = mock.patch.dict(os.environ, {'TDD_FAILURE_DIR': str(self.directory),
                                                   'TDD_TRIAL_RUN_ID': ''})
        environment.start()
        self.addCleanup(environment.stop)

    def test_success_returns_original_result_without_encoding_or_filesystem_work(self):
        record = TddVideoRecord(added=123, aid=1, view=100)
        def run(factory, callback, *, run_id):
            return callback(mock.Mock())
        with mock.patch('db.ingest.run_transaction', side_effect=run), \
             mock.patch('db.observation._dispatch', return_value=[record]) as dispatch, \
             mock.patch('db.observation.encode', side_effect=AssertionError('must not encode')), \
             mock.patch('db.observation._save_failure') as save:
            result = observe('append', {'objects': [record]})
        self.assertIs(result[0], record)
        dispatch.assert_called_once()
        save.assert_not_called()
        self.assertFalse(self.directory.exists())

    def test_failure_is_saved_after_one_attempt_and_preserves_input_types(self):
        run_id = uuid4()
        payload = {'card': MemberCard(7, 'name', None, '', 'sign'),
                   'record': TddVideoRecord(added=123, aid=1, view=100),
                   'present': frozenset(('name', 'sign'))}
        error = RuntimeError('COMMIT outcome unknown')
        def fail(*args, **kwargs):
            self.assertFalse(self.directory.exists(), 'input must not be persisted before DB attempt')
            raise error
        with mock.patch.dict(os.environ, {'TDD_TRIAL_RUN_ID': str(run_id)}), \
             mock.patch('db.ingest.run_transaction', side_effect=fail) as runner, \
             self.assertLogs('db.observation', level='ERROR') as logs:
            with self.assertRaises(RuntimeError) as caught:
                observe('member-update', payload)
        self.assertIs(caught.exception, error)
        runner.assert_called_once()
        path, = self.directory.glob('*.json')
        data = json.loads(path.read_text())
        self.assertEqual((data['operation'], data['run_id'], data['commit_status']),
                         ('member-update', str(run_id), 'unconfirmed'))
        self.assertEqual(data['payload']['card']['values']['face'], '')
        self.assertIsNone(data['payload']['card']['values']['sex'])
        self.assertEqual(data['payload']['record']['values']['view'], 100)
        self.assertEqual(data['payload']['present']['set_values'], ['name', 'sign'])
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(self.directory.stat().st_mode), 0o700)
        self.assertIn('reconcile manually before retrying', logs.output[0])

    def test_failures_create_separate_files_without_overwriting_prior_inputs(self):
        with mock.patch('db.ingest.run_transaction', side_effect=RuntimeError('offline')), \
             self.assertLogs('db.observation', level='ERROR'):
            for aid in (1, 2):
                with self.assertRaises(RuntimeError):
                    observe('append', {'objects': [TddVideoRecord(added=1, aid=aid)]})
        files = list(self.directory.glob('*.json'))
        self.assertEqual(len(files), 2)
        self.assertEqual({json.loads(path.read_text())['payload']['objects'][0]['values']['aid']
                          for path in files}, {1, 2})

    def test_archive_failure_does_not_mask_database_exception(self):
        original = RuntimeError('database failed')
        with mock.patch('db.ingest.run_transaction', side_effect=original), \
             mock.patch('db.observation._save_failure', side_effect=OSError('disk full')), \
             self.assertLogs('db.observation', level='ERROR') as logs:
            with self.assertRaises(RuntimeError) as caught:
                observe('append', {'objects': []})
        self.assertIs(caught.exception, original)
        self.assertIn('could not be saved', logs.output[0])

    def test_partial_archive_is_removed_when_input_cannot_be_encoded(self):
        with mock.patch('db.ingest.run_transaction', side_effect=RuntimeError('offline')), \
             self.assertLogs('db.observation', level='ERROR'):
            with self.assertRaisesRegex(RuntimeError, 'offline'):
                observe('invalid', {'unsupported': object()})
        self.assertEqual(list(self.directory.iterdir()), [])

    def test_caller_cannot_inject_an_old_receipt_id(self):
        with self.assertRaises(TypeError):
            observe('append', {'objects': []}, batch_id=uuid4())
