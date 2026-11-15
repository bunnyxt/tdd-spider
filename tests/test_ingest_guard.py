"""Run control is checked before receipt replay or any business callback."""
import os
import unittest
from unittest import mock
from uuid import uuid4

import psycopg

from db.ingest import _claim, _digest, run_batch


class IngestControlTest(unittest.TestCase):
    def setUp(self):
        self.batch_id, self.run_id = uuid4(), uuid4()
        self.payload = {'value': 1}
        environment = mock.patch.dict(os.environ, {'TDD_ALLOW_UNMANAGED_INGEST': ''})
        environment.start()
        self.addCleanup(environment.stop)

    def connection(self, control, *, existing_receipt=False):
        conn = mock.MagicMock()
        conn.info.transaction_status = psycopg.pq.TransactionStatus.IDLE
        inserted = None if existing_receipt else (self.batch_id,)
        receipt = ('observation', _digest(self.payload), self.run_id, {'value': 42})
        results = [(None,), control, inserted, receipt]
        conn.execute.side_effect = [mock.Mock(fetchone=mock.Mock(return_value=row))
                                    for row in results]
        return conn

    def claim(self, conn, run_id):
        return _claim(conn, self.batch_id, 'observation', self.payload, run_id)

    def assert_guard_only(self, conn):
        self.assertEqual(conn.execute.call_args_list, [
            mock.call('SELECT pg_advisory_xact_lock_shared(141376009,169)'),
            mock.call('SELECT mode,active_run_id FROM tdd_ingest.control WHERE singleton')])

    def test_old_run_is_rejected_before_an_existing_receipt_is_queried(self):
        conn = self.connection(('trial', uuid4()), existing_receipt=True)
        with self.assertRaisesRegex(RuntimeError, 'does not permit'):
            self.claim(conn, self.run_id)
        self.assert_guard_only(conn)

    def test_trial_rejects_missing_run_before_callback(self):
        conn = self.connection(('trial', self.run_id))
        conn.__enter__.return_value = conn
        callback = mock.Mock()
        with mock.patch('db.ingest._require_transaction'), \
             mock.patch('db.ingest._set_transaction_limits'):
            with self.assertRaisesRegex(RuntimeError, 'does not permit'):
                run_batch(lambda: conn, callback, batch_id=self.batch_id,
                          payload=self.payload)
        callback.assert_not_called()
        self.assert_guard_only(conn)

    def test_formal_rejects_trial_run_including_receipt_replay(self):
        conn = self.connection(('formal', None), existing_receipt=True)
        with self.assertRaisesRegex(RuntimeError, 'does not permit'):
            self.claim(conn, self.run_id)
        self.assert_guard_only(conn)

    def test_missing_control_defaults_to_rejection(self):
        conn = self.connection(None)
        with self.assertRaisesRegex(RuntimeError, 'not configured'):
            self.claim(conn, self.run_id)
        self.assert_guard_only(conn)

    def test_stopped_rejects_an_unlabelled_observation(self):
        conn = self.connection(('stopped', None), existing_receipt=True)
        with self.assertRaisesRegex(RuntimeError, 'does not permit'):
            self.claim(conn, None)
        self.assert_guard_only(conn)

    def test_compatibility_requires_the_exact_opt_in_value(self):
        for value in ('true', 'yes', '0'):
            with mock.patch.dict(os.environ, {'TDD_ALLOW_UNMANAGED_INGEST': value}):
                conn = self.connection(None)
                with self.assertRaisesRegex(RuntimeError, 'not configured'):
                    self.claim(conn, self.run_id)
                self.assert_guard_only(conn)

    def test_disposable_opt_in_allows_empty_control_only(self):
        with mock.patch.dict(os.environ, {'TDD_ALLOW_UNMANAGED_INGEST': '1'}):
            conn = self.connection(None)
            self.assertEqual(self.claim(conn, self.run_id), (True, None))
            for control in (('stopped', None), ('trial', uuid4()), ('formal', None)):
                conn = self.connection(control)
                with self.assertRaisesRegex(RuntimeError, 'does not permit'):
                    self.claim(conn, self.run_id)
                self.assert_guard_only(conn)

    def test_opt_in_does_not_ignore_a_missing_control_table(self):
        conn = self.connection(None)
        conn.execute.side_effect = [mock.Mock(), psycopg.errors.UndefinedTable('missing control')]
        with mock.patch.dict(os.environ, {'TDD_ALLOW_UNMANAGED_INGEST': '1'}):
            with self.assertRaises(psycopg.errors.UndefinedTable):
                self.claim(conn, self.run_id)
        self.assert_guard_only(conn)

    def test_matching_trial_and_unlabelled_formal_can_claim(self):
        for control, run_id in ((('trial', self.run_id), self.run_id), (('formal', None), None)):
            conn = self.connection(control)
            self.assertEqual(self.claim(conn, run_id), (True, None))
            self.assertIn('INSERT INTO tdd_ingest.batch', conn.execute.call_args.args[0])

    def test_matching_trial_can_return_an_existing_receipt(self):
        conn = self.connection(('trial', self.run_id), existing_receipt=True)
        self.assertEqual(self.claim(conn, self.run_id), (False, {'value': 42}))
        self.assertEqual(conn.execute.call_count, 4)


if __name__ == '__main__':
    unittest.main()
