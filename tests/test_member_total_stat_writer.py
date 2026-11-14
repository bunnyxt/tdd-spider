"""Batch boundaries and partial completion stay observable after a write fails."""
from contextlib import contextmanager
import logging
import unittest
from unittest import mock
from util.member_total_stat_writer import write_member_stats

LOG = logging.getLogger('test.aggregate.writer')


def records(count):
    return [dict(added=123,mid=2147483647+i,video_count=1,
                 view=2147483648+i,danmaku=0,reply=0,favorite=0,coin=0,share=0,like=0)
            for i in range(count)]


class Session:
    def __init__(self, fail=None):
        self.committed, self.active, self.batches = [], [], []
        self.fail = fail

    @contextmanager
    def transaction(self):
        self.active = []
        try:
            yield self
            self.batches.append(self.active)
            if len(self.batches) == self.fail:
                raise RuntimeError('batch commit outcome unknown')
            self.committed.extend(self.active)
        finally:
            self.active = []


def observe_for(session):
    def observe(operation,payload,**kwargs):
        with session.transaction():
            session.active.extend(payload['objects'])
        return payload['objects']
    return observe


class MemberTotalStatWriterTest(unittest.TestCase):
    def test_bigint_rows_and_tail_are_committed_in_independent_batches(self):
        for count in (0,1,999,1000,1001):
            with self.subTest(count=count):
                session = Session()
                patcher = mock.patch('util.member_total_stat_writer.observe',side_effect=observe_for(session))
                writer = patcher.start()
                self.addCleanup(patcher.stop)
                self.assertEqual(write_member_stats(session,iter(records(count)),LOG),count)
                self.assertEqual(len(session.committed),count)
                self.assertEqual([len(batch) for batch in session.batches], [1000]*(count//1000) + ([count%1000] if count%1000 else []))
                ids = [call.kwargs['batch_id'] for call in writer.call_args_list]
                self.assertEqual(len(ids),len(set(ids)))
                if count:
                    self.assertEqual(session.committed[0].view,2147483648)

    def test_failed_second_batch_keeps_only_confirmed_prior_batch(self):
        session = Session(fail=2)
        with mock.patch('util.member_total_stat_writer.observe',side_effect=observe_for(session)):
            with self.assertLogs(LOG,level='CRITICAL') as logs, self.assertRaisesRegex(RuntimeError,'unknown'):
                write_member_stats(session,records(1001),LOG)
            self.assertEqual(len(session.committed),1000)
            self.assertIn('confirmed_committed_rows=1000',logs.output[0])
            self.assertIn('failed_batch_rows=1',logs.output[0])
            self.assertIn('batch_id=',logs.output[0])

    def test_invalid_batch_size_fails_before_opening_transaction(self):
        for size in (0,-1,True,1.5):
            with self.subTest(size=size), self.assertRaises(ValueError):
                write_member_stats(Session(),[],LOG,size)
