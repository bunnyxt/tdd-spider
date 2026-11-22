"""Migration control gates each transaction before its business callback."""
import os
import unittest
from unittest import mock
from uuid import uuid4

import psycopg

from db.migration import before_write
from db.transaction import write


class IngestControlTest(unittest.TestCase):
    def setUp(self):
        self.run_id = uuid4()
        environment = mock.patch.dict(os.environ, {'TDD_ALLOW_UNMANAGED_INGEST': '',
                                                   'TDD_TRIAL_RUN_ID': ''})
        environment.start()
        self.addCleanup(environment.stop)

    def connection(self, control):
        conn = mock.MagicMock()
        conn.info.transaction_status = psycopg.pq.TransactionStatus.INTRANS
        results = [('read committed',), (None,), control]
        conn.execute.side_effect = [mock.Mock(fetchone=mock.Mock(return_value=row)) for row in results]
        return conn

    def claim(self, conn, run_id):
        with mock.patch.dict(os.environ, {'TDD_TRIAL_RUN_ID': str(run_id) if run_id else ''}):
            return before_write(conn)

    def assert_guard_only(self, conn):
        self.assertEqual(conn.execute.call_args_list, [
            mock.call('SHOW transaction_isolation'),
            mock.call('SELECT pg_advisory_xact_lock_shared(141376009,169)'),
            mock.call('SELECT mode,active_run_id FROM tdd_ingest.control WHERE singleton')])

    def test_old_run_is_rejected_before_business_work(self):
        conn = self.connection(('trial', uuid4()))
        with self.assertRaisesRegex(RuntimeError, 'does not permit'):
            self.claim(conn, self.run_id)
        self.assert_guard_only(conn)

    def test_trial_rejects_missing_run_before_callback(self):
        conn = self.connection(('trial', self.run_id))
        conn.__enter__.return_value = conn
        conn.info.transaction_status = psycopg.pq.TransactionStatus.IDLE
        def begin():
            conn.info.transaction_status = psycopg.pq.TransactionStatus.INTRANS
        conn.transaction.return_value.__enter__.side_effect = begin
        callback = mock.Mock()
        with mock.patch('db.transaction._save_failure'), self.assertLogs('db.transaction', level='ERROR'):
            with self.assertRaisesRegex(RuntimeError, 'does not permit'):
                write(callback, connection_factory=lambda: conn)
        callback.assert_not_called()
        self.assert_guard_only(conn)

    def test_formal_rejects_trial_run(self):
        conn = self.connection(('formal', None))
        with self.assertRaisesRegex(RuntimeError, 'does not permit'):
            self.claim(conn, self.run_id)
        self.assert_guard_only(conn)

    def test_missing_control_defaults_to_rejection(self):
        conn = self.connection(None)
        with self.assertRaisesRegex(RuntimeError, 'not configured'):
            self.claim(conn, self.run_id)
        self.assert_guard_only(conn)

    def test_stopped_rejects_an_unlabelled_write(self):
        conn = self.connection(('stopped', None))
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
            self.assertEqual(self.claim(conn, self.run_id), {'run_id': str(self.run_id)})
            for control in (('stopped', None), ('trial', uuid4()), ('formal', None)):
                conn = self.connection(control)
                with self.assertRaisesRegex(RuntimeError, 'does not permit'):
                    self.claim(conn, self.run_id)
                self.assert_guard_only(conn)

    def test_opt_in_does_not_ignore_a_missing_control_table(self):
        conn = self.connection(None)
        conn.execute.side_effect = [mock.Mock(fetchone=lambda: ('read committed',)),
                                   mock.Mock(), psycopg.errors.UndefinedTable('missing control')]
        with mock.patch.dict(os.environ, {'TDD_ALLOW_UNMANAGED_INGEST': '1'}):
            with self.assertRaises(psycopg.errors.UndefinedTable):
                self.claim(conn, self.run_id)
        self.assert_guard_only(conn)

    def test_matching_trial_and_unlabelled_formal_are_allowed(self):
        for control, run_id in ((('trial', self.run_id), self.run_id), (('formal', None), None)):
            conn = self.connection(control)
            self.assertEqual(self.claim(conn, run_id), {'run_id': str(run_id) if run_id else None})
            self.assert_guard_only(conn)

    def test_guard_requires_an_explicit_read_committed_transaction(self):
        conn = mock.Mock()
        conn.info.transaction_status = psycopg.pq.TransactionStatus.IDLE
        with self.assertRaisesRegex(RuntimeError, 'explicit transaction'):
            before_write(conn)
        conn.execute.assert_not_called()
        conn.info.transaction_status = psycopg.pq.TransactionStatus.INTRANS
        conn.execute.return_value.fetchone.return_value = 'repeatable read',
        with self.assertRaisesRegex(RuntimeError, 'READ COMMITTED'):
            before_write(conn)
        conn.execute.assert_called_once_with('SHOW transaction_isolation')


if __name__ == '__main__':
    unittest.main()
