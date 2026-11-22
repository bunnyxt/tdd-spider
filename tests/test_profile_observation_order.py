"""Delayed profile input cannot undo newer status, metadata, or relationships."""
import unittest
from unittest import mock
from db import TddMember,TddVideo
from core import NewMember
from service import MemberCard,VideoViewStaffItem
from task.task import add_member,add_video,ensure_video_member,update_video
from profile_support import ProfileConnection
from db.profiles import _query_video, _query_member, _query_staff
from db.profiles import update_video_profile, update_member_profile
from test_endpoint_get_video_view import expected_result


class ProfileObservationOrderTest(unittest.TestCase):
    def setUp(self):
        self.conn = ProfileConnection()
        self.addCleanup(self.conn.close)
        self.conn.patch_task_reads(self)
        self.view = expected_result()._replace(bvid='BV1234567890',pic='',staff=[])
        self.aid = self.view.aid
        self.mid = self.view.owner.mid
        self.service = mock.Mock()
        self.service.get_video_view.return_value = self.view
        self.service.get_video_tags.return_value.tags = []
        self.service.get_member_card.return_value = MemberCard(self.mid,'member','','','')
        self.conn.seed(TddMember(added=1,mid=self.mid,name='original',face='',sex='',sign='',code=0))
        values = {field:getattr(self.view,field) for field in
                  ('videos','tid','tname','copyright','pic','title','pubdate','desc','state','attribute','forward')}
        self.conn.seed(TddVideo(added=1,aid=self.aid,bvid='1234567890',mid=self.mid,code=-404,
                                  hasstaff=0,tags='',**values))
        patcher = mock.patch('db.profiles.execute_write',side_effect=self.conn.execute_write)
        patcher.start()
        self.addCleanup(patcher.stop)

    def video_payload(self,added,*,view=None,code=None,tags=''):
        return dict(aid=self.aid,view=self.view if view is None and code is None else view,
                    code_error=code,tags=tags,members=[],added=added)

    def member_payload(self,added,*,card=None,code=0):
        return dict(mid=self.mid,card=card,code=code,added=added)

    def apply(self,function,payload):
        return function(**payload)

    def watermark(self,kind,key):
        row = self.conn.execute('SELECT observed_at FROM tdd_ingest.profile_observation '
                                   'WHERE entity_kind=%s AND entity_id=%s',(kind,key)).fetchone()
        return row[0] if row else None

    def test_older_invalid_status_cannot_undo_newer_success(self):
        self.apply(update_video_profile,self.video_payload(200))
        before = self.conn.execute('SELECT count(*) FROM tdd_video_log').fetchone()[0]
        with self.assertLogs('db.profiles',level='WARNING') as warning:
            self.assertEqual(self.apply(update_video_profile,self.video_payload(100,code=-404)),[])
        self.assertIn('Skip stale profile observation',warning.output[0])
        self.assertEqual(_query_video(self.conn,self.aid).code,0)
        self.assertEqual(self.conn.execute('SELECT count(*) FROM tdd_video_log').fetchone()[0],before)
        self.assertEqual(self.watermark('video',self.aid),200)

    def test_older_success_cannot_undo_newer_invalid_status(self):
        self.apply(update_video_profile,self.video_payload(200,code=62002))
        with self.assertLogs('db.profiles',level='WARNING'):
            self.assertEqual(self.apply(update_video_profile,self.video_payload(100)),[])
        video = _query_video(self.conn,self.aid)
        self.assertEqual((video.code,video.title),(62002,self.view.title))

    def test_older_success_keeps_newer_title_owner_and_staff(self):
        newer = self.view._replace(title='new title',owner=self.view.owner._replace(mid=self.mid+1),
                                  staff=[VideoViewStaffItem(self.mid+2,'new role','','')])
        self.apply(update_video_profile,self.video_payload(200,view=newer,tags='new tags'))
        before = self.conn.execute('SELECT count(*) FROM tdd_video_log').fetchone()[0]
        with self.assertLogs('db.profiles',level='WARNING'):
            self.apply(update_video_profile,self.video_payload(100))
        video = _query_video(self.conn,self.aid)
        self.assertEqual((video.title,video.mid,video.tags,video.added),('new title',self.mid+1,'new tags',1))
        self.assertEqual([(item.mid,item.title) for item in _query_staff(self.conn,self.aid)],
                         [(self.mid+2,'new role')])
        self.assertEqual(self.conn.execute('SELECT count(*) FROM tdd_video_log').fetchone()[0],before)

    def test_older_member_card_or_error_keeps_newer_member(self):
        newest = MemberCard(self.mid,'new name','new sex','new face','new sign')
        self.apply(update_member_profile,self.member_payload(200,card=newest))
        for payload in (self.member_payload(100,code=-404),
                        self.member_payload(150,card=MemberCard(self.mid,'old','','',''))):
            with self.subTest(payload=payload),self.assertLogs('db.profiles',level='WARNING'):
                self.assertEqual(self.apply(update_member_profile,payload),[])
        member = _query_member(self.conn,self.mid)
        self.assertEqual((member.code,member.name,member.sign,member.added),(0,'new name','new sign',1))
        self.assertEqual(self.watermark('member',self.mid),200)

    def test_equal_second_independent_inputs_keep_last_write(self):
        self.apply(update_video_profile,self.video_payload(200,view=self.view._replace(title='first')))
        self.apply(update_video_profile,self.video_payload(200,view=self.view._replace(title='second')))
        self.assertEqual(_query_video(self.conn,self.aid).title,'second')
        self.apply(update_member_profile,self.member_payload(200,card=MemberCard(self.mid,'first','','','')))
        self.apply(update_member_profile,self.member_payload(200,card=MemberCard(self.mid,'second','','','')))
        self.assertEqual(_query_member(self.conn,self.mid).name,'second')

    def test_failed_transaction_does_not_advance_watermark(self):
        self.apply(update_video_profile,self.video_payload(100))
        with mock.patch.object(self.conn,'append',side_effect=RuntimeError('log failure')):
            with self.assertRaisesRegex(RuntimeError,'log failure'):
                self.apply(update_video_profile,self.video_payload(300,view=self.view._replace(title='failed')))
        self.assertEqual(self.watermark('video',self.aid),100)
        self.apply(update_video_profile,self.video_payload(200,view=self.view._replace(title='accepted')))
        self.assertEqual(_query_video(self.conn,self.aid).title,'accepted')

    def test_ignored_403_does_not_block_older_valid_metadata(self):
        self.apply(update_video_profile,self.video_payload(100))
        self.assertEqual(self.apply(update_video_profile,self.video_payload(300,code=-403)),[])
        self.assertEqual(self.watermark('video',self.aid),100)
        self.apply(update_video_profile,self.video_payload(200,view=self.view._replace(title='valid')))
        self.assertEqual(_query_video(self.conn,self.aid).title,'valid')

    def test_view_observation_time_precedes_slow_enrichment(self):
        now = [100]
        def tags(*args,**kwargs):
            now[0] = 300
            return ''
        with mock.patch('task.task.get_ts_s',side_effect=lambda:now[0]), \
             mock.patch('task.task._get_video_tags_str',side_effect=tags):
            update_video(self.aid,self.service)
        self.assertEqual(self.watermark('video',self.aid),100)

    def test_new_member_and_prepared_member_start_with_observation_watermark(self):
        for operation in ('member-add','member-ensure'):
            mid = self.mid + (10 if operation == 'member-add' else 20)
            self.service.get_member_card.return_value = MemberCard(mid,'fresh','','','')
            with mock.patch('task.task.get_ts_s',return_value=200):
                if operation == 'member-add':
                    add_member(mid,self.service)
                else:
                    ensure_video_member(self.aid,mid,self.service)
            old = dict(mid=mid,card=None,code=-404,added=100)
            with self.subTest(operation=operation),self.assertLogs('db.profiles',level='WARNING'):
                self.assertEqual(self.apply(update_member_profile,old),[])
            member = _query_member(self.conn,mid)
            self.assertEqual((member.name,member.code),('fresh',0))
            self.assertEqual(self.watermark('member',mid),200)

    def test_ignored_prepared_existing_member_does_not_poison_watermark(self):
        from db.profiles import _insert_prepared_members
        self.apply(update_member_profile,self.member_payload(100))
        with self.conn.transaction():
            _insert_prepared_members([NewMember(added=300,mid=self.mid,name='ignored',face='',code=-404)],self.conn)
        self.assertEqual(self.watermark('member',self.mid),100)
        self.apply(update_member_profile,self.member_payload(200,card=MemberCard(self.mid,'accepted','','','')))
        self.assertEqual(_query_member(self.conn,self.mid).name,'accepted')

    def test_new_video_starts_with_observation_watermark(self):
        aid = self.aid+1
        self.service.get_video_view.return_value = self.view._replace(aid=aid)
        with mock.patch('task.task.get_ts_s',return_value=200):
            add_video(aid,self.service,commit_video_record=False)
        payload = self.video_payload(100,code=-404)
        payload['aid'] = aid
        with self.assertLogs('db.profiles',level='WARNING'):
            self.assertEqual(self.apply(update_video_profile,payload),[])
        self.assertEqual(_query_video(self.conn,aid).code,0)
        self.assertEqual(self.watermark('video',aid),200)

    def test_baseline_cutoff_fence_rejects_unknown_older_input(self):
        self.conn.execute('INSERT INTO tdd_ingest.profile_observation VALUES (%s,%s,%s)',('video',self.aid,200))
        self.conn.db.commit()
        with self.assertLogs('db.profiles',level='WARNING'):
            self.assertEqual(self.apply(update_video_profile,self.video_payload(100)),[])
        self.assertEqual(_query_video(self.conn,self.aid).code,-404)
        self.apply(update_video_profile,self.video_payload(300))
        self.assertEqual(_query_video(self.conn,self.aid).code,0)
