"""Opt-in real PostgreSQL replay, watermark rollback, and row-lock interleaving.

Uses the same explicitly selected disposable database as test_pg_application.
All rows and pending inputs are synthetic; no HTTP endpoint is contacted.
"""
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
from queue import Queue
import tempfile
import time
import unittest
from unittest import mock
from uuid import uuid4

from db import DBOperation,Session,TddMember,TddVideo
from db.basic import engine
from db.ingest import guard_write_transaction
from db.replay import observe,replay
from task.task import apply_observation
from test_endpoint_get_video_view import expected_result
from util import a2b


@unittest.skipUnless(os.environ.get('TDD_PG_INTEGRATION') == '1',
                     'requires an explicitly selected disposable PostgreSQL database')
class PostgresProfileObservationTest(unittest.TestCase):
    def setUp(self):
        self.run_id = uuid4()
        self.aid = 810000000000000 + self.run_id.int % 1000000000000
        self.mid = 9100000000000 + self.run_id.int % 1000000000
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        environment = mock.patch.dict(os.environ,{'TDD_TRIAL_RUN_ID':str(self.run_id),
                                                  'TDD_REPLAY_DIR':directory.name})
        environment.start()
        self.addCleanup(environment.stop)
        self.view = expected_result()._replace(aid=self.aid,bvid='BV'+a2b(self.aid),staff=[],
                    owner=expected_result().owner._replace(mid=self.mid))
        self.session = Session()
        self.addCleanup(self.session.close)
        with self.session.transaction():
            guard_write_transaction(self.session.connection,self.run_id)
            DBOperation.add(TddMember(added=1,mid=self.mid,name='baseline',face='',sex='',sign='',code=0),self.session)
            values = {field:getattr(self.view,field) for field in
                      ('videos','tid','tname','copyright','pic','title','pubdate','desc','state','attribute','forward')}
            DBOperation.add(TddVideo(added=1,aid=self.aid,bvid=a2b(self.aid),mid=self.mid,code=-404,
                                    hasstaff=0,tags='',**values),self.session)

    def payload(self,added,*,title='new',code=None):
        return dict(aid=self.aid,view=None if code is not None else self.view._replace(title=title),
                    code_error=code,tags='',members=[],added=added)

    def apply_in_transaction(self,payload):
        with self.session.transaction():
            guard_write_transaction(self.session.connection,self.run_id)
            return apply_observation('video-update',payload,self.session,uuid4())

    def test_saved_old_status_replay_cannot_undo_newer_video_or_member(self):
        batch = uuid4()
        with mock.patch('db.replay._execute',side_effect=RuntimeError('injected offline')):
            with self.assertRaisesRegex(RuntimeError,'injected offline'):
                observe('video-update',self.payload(100,code=-404),batch_id=batch)
        observe('video-update',self.payload(200,title='fresh'))
        before = self.session.execute('SELECT count(*) FROM tdd_video_log WHERE aid=%s',(self.aid,))[0][0]
        with self.assertLogs('task',level='WARNING'):
            self.assertEqual(replay(self.directory/(str(batch)+'.json')),[])
        video = DBOperation.query_video_via_aid(self.aid,self.session)
        self.assertEqual((video.code,video.title,video.added),(0,'fresh',1))
        self.assertEqual(self.session.execute('SELECT count(*) FROM tdd_video_log WHERE aid=%s',(self.aid,))[0][0],before)
        from service import MemberCard
        observe('member-update',dict(mid=self.mid,card=MemberCard(self.mid,'fresh','','',''),code=0,added=200))
        with self.assertLogs('task',level='WARNING'):
            observe('member-update',dict(mid=self.mid,card=None,code=-404,added=100))
        member = DBOperation.query_member_via_mid(self.mid,self.session)
        self.assertEqual((member.code,member.name),(0,'fresh'))

    def concurrent(self,first,second):
        """Prove the second transaction waits on the actual first row lock."""
        pid_queue = Queue()
        def waiting_writer():
            with engine.connection() as connection:
                session = Session(connection)
                with session.transaction():
                    guard_write_transaction(connection,self.run_id)
                    pid_queue.put(connection.info.backend_pid)
                    return apply_observation('video-update',second,session,uuid4())
        with ThreadPoolExecutor(max_workers=1) as executor:
            with engine.connection() as connection:
                session = Session(connection)
                with session.transaction():
                    guard_write_transaction(connection,self.run_id)
                    apply_observation('video-update',first,session,uuid4())
                    future = executor.submit(waiting_writer)
                    waiting_pid = pid_queue.get(timeout=5)
                    deadline = time.monotonic()+5
                    blocked = False
                    while time.monotonic() < deadline:
                        blocked = connection.execute('SELECT %s = ANY(pg_blocking_pids(%s))',
                                                     (connection.info.backend_pid,waiting_pid)).fetchone()[0]
                        if blocked:
                            break
                        time.sleep(0.01)
                    self.assertTrue(blocked,'the second writer must wait for the first business-row lock')
            return future.result(timeout=10)

    def test_concurrent_first_watermark_rejects_older_waiter_after_newer_commit(self):
        with self.assertLogs('task',level='WARNING'):
            result = self.concurrent(self.payload(200,title='fresh'),self.payload(100,title='stale'))
        self.assertEqual(result,[])
        video = DBOperation.query_video_via_aid(self.aid,self.session)
        self.assertEqual((video.code,video.title),(0,'fresh'))
        self.assertEqual(self.session.execute('SELECT observed_at FROM tdd_ingest.profile_observation '
                                             'WHERE entity_kind=%s AND entity_id=%s',('video',self.aid))[0][0],200)

    def test_concurrent_newer_waiter_advances_after_older_commit(self):
        self.concurrent(self.payload(100,title='old'),self.payload(200,title='fresh'))
        self.assertEqual(DBOperation.query_video_via_aid(self.aid,self.session).title,'fresh')

    def test_profile_failure_rolls_back_first_watermark_before_older_replay(self):
        with mock.patch('task.task.append_objects',side_effect=RuntimeError('injected log failure')):
            with self.assertRaisesRegex(RuntimeError,'injected log failure'):
                self.apply_in_transaction(self.payload(300,title='rolled back'))
        self.assertEqual(self.session.execute('SELECT count(*) FROM tdd_ingest.profile_observation '
                                             'WHERE entity_kind=%s AND entity_id=%s',('video',self.aid))[0][0],0)
        self.apply_in_transaction(self.payload(200,title='accepted'))
        self.assertEqual(DBOperation.query_video_via_aid(self.aid,self.session).title,'accepted')
