"""Follower observations bind directly and publish only confirmed batches."""
from datetime import datetime, timezone
from contextlib import nullcontext
import os
from queue import Queue
import unittest
from unittest import mock
from uuid import uuid4

from psycopg.pq import TransactionStatus

from core import MemberFollowerRecord
from job import BatchInsertMemberFollowerRecordJob
from task import commit_member_follower_records_batch
from test_ingest_manifest import connection


class MemberFollowerWriteTest(unittest.TestCase):
    def test_direct_same_second_rows_return_distinct_complete_keys_and_manifest(self):
        record = MemberFollowerRecord(123, 2147483648, 100)
        added = datetime.fromtimestamp(123, timezone.utc)
        expected = [(record.mid, added, 10), (record.mid, added, 11)]
        conn, cursors = connection([expected, []])
        run_id = uuid4()
        conn.info.transaction_status = TransactionStatus.IDLE
        conn.transaction.return_value.__enter__.side_effect = lambda: setattr(
            conn.info, 'transaction_status', TransactionStatus.INTRANS)
        with mock.patch.dict(os.environ, {'TDD_TRIAL_RUN_ID': str(run_id)}), \
             mock.patch('db.transaction.engine.connection', return_value=nullcontext(conn)), \
             mock.patch('db.migration.before_write', return_value={'run_id': str(run_id)}):
            keys = commit_member_follower_records_batch([record, record])
        self.assertEqual(keys, expected)
        query, rows = cursors[0].executemany.call_args.args
        self.assertIn('tdd_member_follower_record', query.as_string())
        self.assertEqual(rows, [(added, record.mid, 100)] * 2)
        manifests = cursors[1].executemany.call_args.args[1]
        self.assertEqual([row[4].obj['id'] for row in manifests], [10, 11])
        self.assertTrue(all(set(row[4].obj) == {'mid', 'added', 'id'} for row in manifests))
        self.assertEqual(record, MemberFollowerRecord(123, 2147483648, 100))

    def test_batch_job_counts_only_confirmed_rows(self):
        record = MemberFollowerRecord(123, 7, 100)
        job = BatchInsertMemberFollowerRecordJob('followers', Queue())
        with mock.patch('job.BatchInsertMemberFollowerRecordJob.commit_member_follower_records_batch') as write:
            job._flush([record])
        self.assertIs(write.call_args.args[0][0], record)
        with mock.patch('job.BatchInsertMemberFollowerRecordJob.commit_member_follower_records_batch',
                        side_effect=RuntimeError('unconfirmed')), self.assertLogs(job.logger, level='ERROR') as logs:
            job._flush([record])
        self.assertEqual(job.stat.condition['persisted'], 1)
        self.assertEqual(job.stat.condition['batch_insert_fail'], 1)
        self.assertEqual([entry.getMessage() for entry in logs.records], [
            'Follower batch write failed: rows=1, error=unconfirmed',
        ])
