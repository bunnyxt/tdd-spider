"""Observation input survives uncertain commits and can replay without HTTP."""
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock
from uuid import uuid4

from db.models import TddMember
from db.replay import observe,replay,encode,decode
from service import MemberCard


class ReplayTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        patcher = mock.patch.dict(os.environ,{'TDD_REPLAY_DIR':str(self.directory)})
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_input_is_durable_before_commit_and_unknown_result_keeps_same_uuid(self):
        batch,run = uuid4(),uuid4()
        payload = {'member':TddMember(added=123,mid=2147483648,name='name',face='',code=0)}
        path = self.directory/(str(batch)+'.json')
        def lost_ack(*args,**kwargs):
            self.assertTrue(path.exists())
            self.assertEqual(path.stat().st_mode & 0o777,0o600)
            self.assertEqual(kwargs['batch_id'],batch)
            raise RuntimeError('commit acknowledgement lost')
        with mock.patch.dict(os.environ,{'TDD_TRIAL_RUN_ID':str(run)}), mock.patch('db.ingest.run_batch',side_effect=lost_ack):
            with self.assertRaisesRegex(RuntimeError,'acknowledgement'):
                observe('member-add',payload,batch_id=batch)
        self.assertEqual(json.loads(path.read_text())['run_id'],str(run))
        member = TddMember(**vars(payload['member']))
        member.id = 42
        with mock.patch.dict(os.environ,{'TDD_TRIAL_RUN_ID':str(uuid4())}), \
             mock.patch('db.ingest.run_batch',return_value=encode(member)) as runner:
            result = replay(path)
        self.assertEqual((result.id,result.mid),(42,2147483648))
        self.assertEqual(runner.call_args.kwargs['run_id'],run)
        self.assertEqual(runner.call_args.kwargs['batch_id'],batch)
        self.assertFalse(path.exists())

    def test_uuid_cannot_overwrite_a_different_pending_observation(self):
        batch = uuid4()
        with mock.patch('db.ingest.run_batch',side_effect=RuntimeError('offline')):
            with self.assertRaises(RuntimeError):
                observe('member-ensure',{'members':[]},batch_id=batch)
        original = (self.directory/(str(batch)+'.json')).read_bytes()
        with self.assertRaisesRegex(ValueError,'different input'):
            observe('member-ensure',{'members':[1]},batch_id=batch)
        self.assertEqual((self.directory/(str(batch)+'.json')).read_bytes(),original)

    def test_profile_payload_preserves_missing_keys_and_text(self):
        card = MemberCard(2147483648,'中文\x00😀',None,'',None,frozenset(('mid','name','face')))
        self.assertEqual(decode(json.loads(json.dumps(encode(card)))),card)
        self.assertIsNone(decode(encode(None)))

    def test_atomic_publish_race_never_overwrites_an_existing_input(self):
        from db.replay import _save
        path = self.directory/(str(uuid4())+'.json')
        _save(path,{'value':1})
        with self.assertRaises(ValueError):
            _save(path,{'value':2})
        self.assertEqual(json.loads(path.read_text()),{'value':1})

    def test_append_receipt_stores_only_ids_and_rebuilds_original_rows(self):
        from db.models import TddVideoRecord
        from db.replay import _execute
        batch = uuid4()
        obj = TddVideoRecord(added=123,aid=10,view=100,danmaku=0,reply=0,favorite=0,coin=0,share=0,like=0)
        envelope = {'version':1,'batch_id':str(batch),'run_id':None,'operation':'append','payload':encode({'objects':[obj]})}
        def dispatch(operation,payload,session,batch_id):
            payload['objects'][0].id = 42
            return payload['objects']
        receipts = []
        def commit(factory,callback,**kwargs):
            receipts.append(callback(object()))
            return receipts[-1]
        with mock.patch('db.replay._dispatch',side_effect=dispatch),mock.patch('db.ingest.run_batch',side_effect=commit):
            confirmed = _execute(envelope,None)
        self.assertEqual(receipts,[{'append_ids':[42]}])
        with mock.patch('db.replay._dispatch') as callback,mock.patch('db.ingest.run_batch',return_value=receipts[0]):
            replayed = _execute(envelope,None)
        callback.assert_not_called()
        self.assertEqual(vars(confirmed[0]),vars(replayed[0]))
        self.assertEqual((replayed[0].id,replayed[0].view),(42,100))

    def test_cleanup_error_after_confirmed_commit_is_reported_as_confirmed(self):
        with mock.patch('db.ingest.run_batch',return_value=None), \
             mock.patch('db.replay._fsync_directory',side_effect=[None,OSError('directory sync failed')]), \
             self.assertLogs('db.replay',level='WARNING') as logs:
            self.assertIsNone(observe('member-ensure',{'members':[]}))
        self.assertIn('Database commit confirmed',logs.output[0])

    def test_unlink_error_keeps_pending_input_and_forwards_confirmed_batch(self):
        from core import RecordNew
        from queue import Queue
        from job import BatchInsertVideoRecordJob
        downstream = Queue()
        writer = BatchInsertVideoRecordJob('cleanup',Queue(),downstream)
        self.addCleanup(writer.cleanup)
        record = RecordNew(123,10,'1234567890',100,0,0,0,0,0,0)
        with mock.patch('db.ingest.run_batch',return_value={'append_ids':[42]}) as commit, \
             mock.patch('pathlib.Path.unlink',side_effect=OSError('cannot remove pending input')), \
             self.assertLogs('db.replay',level='WARNING'):
            writer._flush([record])
        self.assertEqual(commit.call_count,1)
        self.assertEqual(writer.stat.condition['persisted'],1)
        self.assertEqual(writer.stat.condition['batch_insert_fail'],0)
        self.assertEqual(downstream.get_nowait(),record)
        self.assertEqual(len(list(self.directory.glob('*.json'))),1)
