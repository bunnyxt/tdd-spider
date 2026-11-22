"""Large member IDs and profile failures retain their observed semantics."""
import unittest
from unittest import mock
from db import TddMember,TddVideo
from service import MemberCard,ServiceError
from task.task import _prepare_members,add_video,update_member,update_video
from profile_support import ProfileConnection
from db.profiles import _query_member
from test_endpoint_get_video_view import expected_result


class VideoMemberProfilesTest(unittest.TestCase):
    def setUp(self):
        self.conn = ProfileConnection()
        self.addCleanup(self.conn.close)
        self.conn.patch_task_reads(self)
        self.service = mock.Mock()
        patcher = mock.patch('db.profiles.execute_write',side_effect=self.conn.execute_write)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_member_mid_round_trip_across_int32_boundary(self):
        for mid in (2147483647,2147483648,9000000000000):
            video_view = expected_result()._replace(aid=mid,staff=[])
            self.service.get_video_view.return_value = video_view._replace(
                owner=video_view.owner._replace(mid=mid))
            self.service.get_video_tags.return_value.tags = []
            self.service.get_member_card.return_value = MemberCard(mid,'name','','','')
            add_video(mid,self.service)
            self.assertEqual(_query_member(self.conn,mid).mid,mid)
        self.assertEqual(self.conn.execute('SELECT count(*) FROM tdd_member').fetchone()[0],3)

    def test_profile_failure_is_retried_on_next_refresh(self):
        video_view = expected_result()._replace(staff=[])
        video_view = video_view._replace(owner=video_view.owner._replace(mid=10))
        self.conn.seed(TddVideo(added=1,aid=video_view.aid,bvid=video_view.bvid.removeprefix('BV'),
                               mid=10,title='original',code=0,hasstaff=0,tags=''))
        self.service.get_video_view.return_value = video_view
        self.service.get_video_tags.return_value.tags = []
        self.service.get_member_card.side_effect = [ServiceError(),MemberCard(10,'name','','','')]
        with self.assertLogs('task',level='WARNING'):
            update_video(video_view.aid,self.service)
        self.assertIsNone(_query_member(self.conn,10))
        update_video(video_view.aid,self.service)
        self.assertEqual(_query_member(self.conn,10).name,'name')
        self.assertEqual(self.service.get_member_card.call_count,2)

    def test_member_snapshot_is_unchanged_and_null_empty_values_persist(self):
        self.conn.seed(TddMember(added=1,mid=10,name='name',face='',sex='old',sign='old',code=0))
        snapshot = _query_member(self.conn,10)
        before = vars(snapshot).copy()
        self.service.get_member_card.return_value = MemberCard(10,'name',None,'','\x00')
        with mock.patch('db.profiles._query_member',return_value=snapshot) as read:
            logs = update_member(10,self.service)
        read.assert_called_once_with(self.conn,10)
        self.assertEqual(vars(snapshot),before)
        self.assertEqual({log.attr:(log.oldval,log.newval) for log in logs},{'sex':('old',None),'sign':('old','')})
        member = _query_member(self.conn,10)
        self.assertEqual((member.sex,member.sign,member.added),(None,'',1))

    def test_member_log_failure_rolls_back_profile_without_mutating_snapshot(self):
        self.conn.seed(TddMember(added=1,mid=10,name='old',face='',sex='',sign='',code=0))
        snapshot = _query_member(self.conn,10)
        before = vars(snapshot).copy()
        self.service.get_member_card.return_value = MemberCard(10,'new','','','')
        with mock.patch('db.profiles._query_member',return_value=snapshot),\
             mock.patch.object(self.conn,'append',side_effect=RuntimeError('log failed')):
            with self.assertRaisesRegex(RuntimeError,'log failed'):
                update_member(10,self.service)
        self.assertEqual(vars(snapshot),before)
        self.assertEqual(_query_member(self.conn,10).name,'old')

    def test_query_failure_propagates_without_fetch_or_insert(self):
        with mock.patch.object(self.conn,'execute',side_effect=RuntimeError('query failed')):
            with self.assertRaisesRegex(RuntimeError,'query failed'):
                _prepare_members(1,[10],self.service)
        self.service.get_member_card.assert_not_called()

    def test_invalid_mid_never_fetches_or_writes(self):
        for mid in (0,-1,None,'10'):
            with self.subTest(mid=mid), self.assertLogs('task',level='WARNING'):
                self.assertEqual(_prepare_members(1,[mid],self.service),{})
        self.service.get_member_card.assert_not_called()
