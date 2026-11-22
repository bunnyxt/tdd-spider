"""Opt-in PostgreSQL profile serialization and competing creation outcomes.

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

from core import NewVideo
from db import TddMember,TddVideo
from db.queries import query_video_via_aid, query_member_via_mid
from db.values import database_value
from test_pg_application import read_rows, read_value
from db.basic import engine
from db.ingest import guard_write_transaction
from db.profiles import _update_video_profile, _update_member_profile
from service import MemberCard
from test_endpoint_get_video_view import expected_result
from util import a2b


@unittest.skipUnless(os.environ.get('TDD_PG_INTEGRATION') == '1',
                     'requires an explicitly selected disposable PostgreSQL database')
class PostgresProfileSerializationTest(unittest.TestCase):
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
        self.video_view = expected_result()._replace(aid=self.aid,bvid='BV'+a2b(self.aid),staff=[],
                    owner=expected_result().owner._replace(mid=self.mid))
        with engine.connection() as connection, connection.transaction():
            guard_write_transaction(connection,self.run_id)
            member = TddMember(added=1,mid=self.mid,name='baseline',face='',sex='',sign='',code=0)
            values = {field:getattr(self.video_view,field) for field in
                      ('videos','tid','tname','copyright','pic','title','pubdate','desc','state','attribute','forward')}
            video = TddVideo(added=1,aid=self.aid,bvid=a2b(self.aid),mid=self.mid,code=-404,
                             hasstaff=0,tags='',**values)
            for obj in (member,video):
                fields = [f for f in obj.fields if f not in ('id','last_video_aid')]
                connection.execute('INSERT INTO '+obj.table+' ('+','.join('"'+f+'"' for f in fields)+') '
                                   'VALUES ('+','.join(['%s']*len(fields))+')',
                                   tuple(database_value(f,getattr(obj,f)) for f in fields))

    def payload(self,added,*,title='new',code=None):
        return dict(aid=self.aid,video_view=None if code is not None else self.video_view._replace(title=title),
                    code_error=code,tags='',members=[],added=added)

    def concurrent(self,function,first,second):
        """Prove the second transaction waits on the actual first row lock."""
        pid_queue = Queue()
        def waiting_writer():
            with engine.connection() as connection:
                with connection.transaction():
                    guard_write_transaction(connection,self.run_id)
                    pid_queue.put(connection.info.backend_pid)
                    return function(connection,**second,run_id=self.run_id)
        with ThreadPoolExecutor(max_workers=1) as executor:
            with engine.connection() as connection:
                with connection.transaction():
                    guard_write_transaction(connection,self.run_id)
                    function(connection,**first,run_id=self.run_id)
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
                    self.assertFalse(future.done())
            return future.result(timeout=10)

    def test_older_video_input_waits_then_logs_the_preceding_committed_value(self):
        result = self.concurrent(_update_video_profile,
                                 self.payload(200,title='first commit'),
                                 self.payload(100,title='second commit'))
        video = query_video_via_aid(self.aid)
        self.assertEqual((video.code,video.title,video.added),(0,'second commit',1))
        self.assertEqual([(log.attr,log.oldval,log.newval,log.added) for log in result],
                         [('title','first commit','second commit',100)])
        self.assertEqual(read_rows('SELECT oldval,newval,added FROM tdd_video_log '
                                   'WHERE aid=%s AND attr=%s ORDER BY id',(self.aid,'title')),
                         [{'oldval':self.video_view.title,'newval':'first commit','added':200},
                          {'oldval':'first commit','newval':'second commit','added':100}])
        self.assertEqual(list(self.directory.glob('*.json')),[])

    def test_older_member_input_waits_then_logs_the_preceding_committed_value(self):
        first = dict(mid=self.mid,card=MemberCard(self.mid,'first commit','','',''),code=0,added=200)
        second = dict(mid=self.mid,card=MemberCard(self.mid,'second commit','','',''),code=0,added=100)
        result = self.concurrent(_update_member_profile,first,second)
        member = query_member_via_mid(self.mid)
        self.assertEqual((member.code,member.name,member.added),(0,'second commit',1))
        self.assertEqual([(log.attr,log.oldval,log.newval,log.added) for log in result],
                         [('name','first commit','second commit',100)])
        self.assertEqual(read_rows('SELECT oldval,newval,added FROM tdd_member_log '
                                   'WHERE mid=%s AND attr=%s ORDER BY id',(self.mid,'name')),
                         [{'oldval':'baseline','newval':'first commit','added':200},
                          {'oldval':'first commit','newval':'second commit','added':100}])
        self.assertEqual(list(self.directory.glob('*.json')),[])

    def test_member_insert_after_precheck_keeps_profile_and_updates_video(self):
        from db.profiles import _insert_member
        from service import MemberCard, VideoTags, VideoViewStaffItem
        from task import update_video
        mid = self.mid + 1
        pid_queue = Queue()
        service = mock.Mock()
        service.get_member_card.return_value = MemberCard(mid,'second','','','replace')
        service.get_video_view.return_value = self.video_view._replace(
            title='updated',staff=[VideoViewStaffItem(mid,'producer','','')])
        service.get_video_tags.return_value = VideoTags([])

        def insert_and_report_pid(conn, member, *, run_id=None):
            pid_queue.put(conn.info.backend_pid)
            return _insert_member(conn,member,run_id=run_id)

        with ThreadPoolExecutor(max_workers=1) as executor:
            with engine.connection() as conn, conn.transaction():
                guard_write_transaction(conn,self.run_id)
                conn.execute('INSERT INTO tdd_member (added,mid,name,face,sex,sign,code) '
                             'VALUES (%s,%s,%s,%s,%s,%s,%s)',
                             (database_value('added',1),mid,'first','','','keep',0))
                # The uncommitted row is invisible to the task's precheck.
                self.assertIsNone(query_member_via_mid(mid))
                with mock.patch('db.profiles._insert_member',side_effect=insert_and_report_pid), \
                        mock.patch('task.task.get_ts_s',return_value=100):
                    future = executor.submit(update_video,self.aid,service)
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
            logs = future.result(timeout=10)
        self.assertTrue(any(log.attr == 'staff' for log in logs))
        member = query_member_via_mid(mid)
        self.assertEqual((member.name,member.sign,member.added,member.last_video_aid),
                         ('first','keep',1,self.aid))
        self.assertEqual(read_value('SELECT count(*) FROM tdd_member_log WHERE mid=%s',(mid,)),0)
        self.assertEqual(read_value('SELECT count(*) FROM tdd_video_staff WHERE aid=%s AND mid=%s',
                                    (self.aid,mid)),1)
        video = query_video_via_aid(self.aid)
        self.assertEqual((video.code,video.title),(0,'updated'))
        self.assertEqual(list(self.directory.glob('*.json')),[])

    def test_video_insert_race_and_unrelated_constraint_are_distinct(self):
        from psycopg.errors import NotNullViolation
        from db.profiles import create_video
        from task import add_video, AlreadyExistError
        aid = self.aid+1
        video = NewVideo(added=1,aid=aid,bvid=a2b(aid),mid=self.mid,
                         state=0,title='first',pubdate=100)
        service = mock.Mock()

        def fetch_after_competing_insert(params):
            with ThreadPoolExecutor(max_workers=1) as executor:
                self.assertIsNotNone(executor.submit(create_video,video,staff=[],new_members=[]).result(timeout=10))
            return self.video_view._replace(aid=aid,bvid='BV'+a2b(aid),title='second')

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
            create_video(video._replace(aid=aid+1,title=None),staff=[],new_members=[])
        self.assertIsNone(query_video_via_aid(aid+1))
