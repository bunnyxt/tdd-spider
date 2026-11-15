"""Run control gates one transaction before any business callback."""
import os
import unittest
from unittest import mock
from uuid import uuid4

import psycopg

from db.ingest import _check_run_control, guard_write_transaction, run_transaction


class IngestControlTest(unittest.TestCase):
    def setUp(self):
        self.run_id = uuid4()
        environment = mock.patch.dict(os.environ, {'TDD_ALLOW_UNMANAGED_INGEST': ''})
        environment.start()
        self.addCleanup(environment.stop)

    def connection(self, control):
        conn = mock.MagicMock()
        conn.info.transaction_status = psycopg.pq.TransactionStatus.IDLE
        results = [(None,), control]
        conn.execute.side_effect = [mock.Mock(fetchone=mock.Mock(return_value=row))
                                    for row in results]
        return conn

    def claim(self, conn, run_id):
        return _check_run_control(conn, run_id)

    def assert_guard_only(self, conn):
        self.assertEqual(conn.execute.call_args_list, [
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
        callback = mock.Mock()
        with mock.patch('db.ingest._require_transaction'), \
             mock.patch('db.ingest._set_transaction_limits'):
            with self.assertRaisesRegex(RuntimeError, 'does not permit'):
                run_transaction(lambda: conn, callback)
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

    def test_stopped_rejects_an_unlabelled_observation(self):
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
            self.assertEqual(self.claim(conn, self.run_id), None)
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

    def test_matching_trial_and_unlabelled_formal_are_allowed(self):
        for control, run_id in ((('trial', self.run_id), self.run_id), (('formal', None), None)):
            conn = self.connection(control)
            self.assertIsNone(self.claim(conn, run_id))
            self.assert_guard_only(conn)

    def test_uncertain_commit_is_attempted_once(self):
        conn = self.connection(('formal', None))
        conn.__enter__.return_value = conn
        conn.transaction.return_value.__exit__.side_effect = psycopg.OperationalError('lost COMMIT ack')
        callback = mock.Mock(return_value=42)
        factory = mock.Mock(return_value=conn)
        with mock.patch('db.ingest.guard_write_transaction') as guard, \
             mock.patch('db.ingest._require_transaction'):
            with self.assertRaisesRegex(psycopg.OperationalError, 'lost COMMIT ack'):
                run_transaction(factory, callback)
        factory.assert_called_once_with()
        callback.assert_called_once_with(conn)
        guard.assert_called_once_with(conn, None)

    def test_guarded_business_result_is_not_serialized(self):
        conn = self.connection(('formal', None))
        conn.__enter__.return_value = conn
        result = object()
        with mock.patch('db.ingest.guard_write_transaction'), \
             mock.patch('db.ingest._require_transaction'):
            self.assertIs(run_transaction(lambda: conn, lambda connection: result), result)

    def test_derived_guard_requires_an_explicit_read_committed_transaction(self):
        conn = mock.Mock()
        conn.info.transaction_status = psycopg.pq.TransactionStatus.IDLE
        with self.assertRaisesRegex(RuntimeError, 'explicit transaction'):
            guard_write_transaction(conn)
        conn.execute.assert_not_called()
        conn.info.transaction_status = psycopg.pq.TransactionStatus.INTRANS
        conn.execute.return_value.fetchone.return_value = 'repeatable read',
        with self.assertRaisesRegex(RuntimeError, 'READ COMMITTED'):
            guard_write_transaction(conn)
        conn.execute.assert_called_once_with('SHOW transaction_isolation')

    def test_derived_guard_sets_limits_then_locks_and_checks_current_control(self):
        conn = self.connection(('trial', self.run_id))
        conn.info.transaction_status = psycopg.pq.TransactionStatus.INTRANS
        conn.execute.side_effect = [mock.Mock(fetchone=mock.Mock(return_value=row))
                                    for row in [('read committed',), (None,),
                                                (None,), ('trial', self.run_id)]]
        guard_write_transaction(conn, self.run_id)
        statements = [call.args[0] for call in conn.execute.call_args_list]
        self.assertEqual(len(statements), 4)
        self.assertEqual(statements[0], 'SHOW transaction_isolation')
        self.assertIn("set_config('lock_timeout'", statements[1])
        self.assertEqual(statements[2:], [
            'SELECT pg_advisory_xact_lock_shared(141376009,169)',
            'SELECT mode,active_run_id FROM tdd_ingest.control WHERE singleton'])


if __name__ == '__main__':
    unittest.main()
