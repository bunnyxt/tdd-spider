"""Opt-in real PostgreSQL delayed observations, watermark rollback, and row-lock interleaving.

Uses the same explicitly selected disposable database as test_pg_application.
All rows and failure inputs are synthetic; no HTTP endpoint is contacted.
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

from db import TddMember,TddVideo
from db.queries import query_video_via_aid, query_member_via_mid
from db.values import database_value
from test_pg_application import read_value
from db.basic import engine
from db.ingest import guard_write_transaction
from db.profiles import update_video_profile, update_member_profile, _update_video_profile
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
                                                  'TDD_FAILURE_DIR':directory.name})
        environment.start()
        self.addCleanup(environment.stop)
        self.view = expected_result()._replace(aid=self.aid,bvid='BV'+a2b(self.aid),staff=[],
                    owner=expected_result().owner._replace(mid=self.mid))
        with engine.connection() as connection, connection.transaction():
            guard_write_transaction(connection,self.run_id)
            member = TddMember(added=1,mid=self.mid,name='baseline',face='',sex='',sign='',code=0)
            values = {field:getattr(self.view,field) for field in
                      ('videos','tid','tname','copyright','pic','title','pubdate','desc','state','attribute','forward')}
            video = TddVideo(added=1,aid=self.aid,bvid=a2b(self.aid),mid=self.mid,code=-404,
                             hasstaff=0,tags='',**values)
            # Seed profiles without a watermark to exercise its first insertion.
            for obj in (member,video):
                fields = [f for f in obj.fields if f not in ('id','last_video_aid')]
                connection.execute('INSERT INTO '+obj.table+' ('+','.join('"'+f+'"' for f in fields)+') '
                                   'VALUES ('+','.join(['%s']*len(fields))+')',
                                   tuple(database_value(f,getattr(obj,f)) for f in fields))

    def payload(self,added,*,title='new',code=None):
        return dict(aid=self.aid,view=None if code is not None else self.view._replace(title=title),
                    code_error=code,tags='',members=[],added=added)

    def apply_in_transaction(self,payload):
        with engine.connection() as connection, connection.transaction():
            guard_write_transaction(connection,self.run_id)
            return _update_video_profile(connection,**payload,run_id=self.run_id)

    def test_delayed_old_status_cannot_undo_newer_video_or_member(self):
        with mock.patch('db.ingest.run_transaction',side_effect=RuntimeError('injected offline')):
            with self.assertRaisesRegex(RuntimeError,'injected offline'):
                update_video_profile(**self.payload(100,code=-404))
        update_video_profile(**self.payload(200,title='fresh'))
        before = read_value('SELECT count(*) FROM tdd_video_log WHERE aid=%s',(self.aid,))
        with self.assertLogs('db.profiles',level='WARNING'):
            self.assertEqual(update_video_profile(**self.payload(100,code=-404)),[])
        video = query_video_via_aid(self.aid)
        self.assertEqual((video.code,video.title,video.added),(0,'fresh',1))
        self.assertEqual(read_value('SELECT count(*) FROM tdd_video_log WHERE aid=%s',(self.aid,)),before)
        from service import MemberCard
        update_member_profile(**dict(mid=self.mid,card=MemberCard(self.mid,'fresh','','',''),code=0,added=200))
        with self.assertLogs('db.profiles',level='WARNING'):
            update_member_profile(**dict(mid=self.mid,card=None,code=-404,added=100))
        member = query_member_via_mid(self.mid)
        self.assertEqual((member.code,member.name),(0,'fresh'))

    def concurrent(self,first,second):
        """Prove the second transaction waits on the actual first row lock."""
        pid_queue = Queue()
        def waiting_writer():
            with engine.connection() as connection:
                with connection.transaction():
                    guard_write_transaction(connection,self.run_id)
                    pid_queue.put(connection.info.backend_pid)
                    return _update_video_profile(connection,**second,run_id=self.run_id)
        with ThreadPoolExecutor(max_workers=1) as executor:
            with engine.connection() as connection:
                with connection.transaction():
                    guard_write_transaction(connection,self.run_id)
                    _update_video_profile(connection,**first,run_id=self.run_id)
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
        with self.assertLogs('db.profiles',level='WARNING'):
            result = self.concurrent(self.payload(200,title='fresh'),self.payload(100,title='stale'))
        self.assertEqual(result,[])
        video = query_video_via_aid(self.aid)
        self.assertEqual((video.code,video.title),(0,'fresh'))
        self.assertEqual(read_value('SELECT observed_at FROM tdd_ingest.profile_observation '
                                             'WHERE entity_kind=%s AND entity_id=%s',('video',self.aid)),200)

    def test_concurrent_newer_waiter_advances_after_older_commit(self):
        self.concurrent(self.payload(100,title='old'),self.payload(200,title='fresh'))
        self.assertEqual(query_video_via_aid(self.aid).title,'fresh')

    def test_profile_failure_rolls_back_first_watermark_before_older_observation(self):
        with mock.patch('db.profiles._append_objects',side_effect=RuntimeError('injected log failure')):
            with self.assertRaisesRegex(RuntimeError,'injected log failure'):
                self.apply_in_transaction(self.payload(300,title='rolled back'))
        self.assertEqual(read_value('SELECT count(*) FROM tdd_ingest.profile_observation '
                                             'WHERE entity_kind=%s AND entity_id=%s',('video',self.aid)),0)
        self.apply_in_transaction(self.payload(200,title='accepted'))
        self.assertEqual(query_video_via_aid(self.aid).title,'accepted')

    def test_member_insert_after_precheck_reports_expected_conflict(self):
        from db.profiles import _insert_member, ensure_members
        from service import MemberCard
        from task import add_member, AlreadyExistError
        mid = self.mid + 1
        pid_queue = Queue()
        service = mock.Mock()
        service.get_member_card.return_value = MemberCard(mid,'second','','','')

        def insert_and_report_pid(conn, member, *, run_id=None):
            pid_queue.put(conn.info.backend_pid)
            return _insert_member(conn,member,run_id=run_id)

        with ThreadPoolExecutor(max_workers=1) as executor:
            with engine.connection() as conn, conn.transaction():
                guard_write_transaction(conn,self.run_id)
                conn.execute('INSERT INTO tdd_member (added,mid,name,face,sex,sign,code) '
                             'VALUES (%s,%s,%s,%s,%s,%s,%s)',
                             (database_value('added',1),mid,'first','','','',0))
                # The uncommitted row is invisible to the task's precheck.
                self.assertIsNone(query_member_via_mid(mid))
                with mock.patch('db.profiles._insert_member',side_effect=insert_and_report_pid):
                    future = executor.submit(add_member,mid,service)
                    waiting_pid = pid_queue.get(timeout=5)
                    deadline = time.monotonic()+5
                    blocked = False
                    while time.monotonic() < deadline:
                        blocked = conn.execute('SELECT %s = ANY(pg_blocking_pids(%s))',
                                               (conn.info.backend_pid,waiting_pid)).fetchone()[0]
                        if blocked:
                            break
                        time.sleep(0.01)
                    self.assertTrue(blocked,'the insert must wait for the competing business key')
            with self.assertRaises(AlreadyExistError):
                future.result(timeout=10)
        self.assertEqual(query_member_via_mid(mid).name,'first')
        ensure_members([TddMember(added=2,mid=mid,name='ignored',face='',sex='',sign='',code=0)])
        self.assertEqual(query_member_via_mid(mid).name,'first')
        self.assertEqual(list(self.directory.glob('*.json')),[])

    def test_video_insert_race_and_unrelated_constraint_are_distinct(self):
        from psycopg.errors import NotNullViolation
        from db.profiles import insert_video
        from task import add_video, AlreadyExistError
        aid = self.aid+1
        video = TddVideo(added=1,aid=aid,bvid=a2b(aid),mid=self.mid,
                         code=0,state=0,title='first',pubdate=100)
        service = mock.Mock()

        def fetch_after_competing_insert(params):
            with ThreadPoolExecutor(max_workers=1) as executor:
                self.assertIsNotNone(executor.submit(insert_video,video,[],[]).result(timeout=10))
            return self.view._replace(aid=aid,bvid='BV'+a2b(aid),title='second')

        service.get_video_view.side_effect = fetch_after_competing_insert
        service.get_video_tags.return_value.tags = []
        self.assertIsNone(query_video_via_aid(aid))
        with self.assertRaises(AlreadyExistError):
            add_video(aid,service)
        self.assertEqual(query_video_via_aid(aid).title,'first')
        self.assertEqual(read_value('SELECT count(*) FROM tdd_video_record WHERE aid=%s',(aid,)),0)
        self.assertEqual(list(self.directory.glob('*.json')),[])
        # A required-field violation is not an expected aid conflict.
        with self.assertRaises(NotNullViolation):
            insert_video(TddVideo(**{**vars(video),'id':None,'aid':aid+1,'title':None}),[],[])
        self.assertIsNone(query_video_via_aid(aid+1))
