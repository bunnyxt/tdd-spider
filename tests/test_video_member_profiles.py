"""Large member IDs and profile failures retain their observed semantics."""
import unittest
from unittest import mock
from db import DBOperation,TddMember
from service import MemberCard,ServiceError
from task.task import add_member,ensure_video_member,update_member
from profile_support import ProfileSession


class VideoMemberProfilesTest(unittest.TestCase):
    def setUp(self):
        self.session = ProfileSession()
        self.addCleanup(self.session.close)
        self.service = mock.Mock()
        patcher = mock.patch('task.task.observe',side_effect=self.session.observe)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_member_mid_round_trip_across_int32_boundary(self):
        for mid in (2147483647,2147483648,9000000000000):
            self.service.get_member_card.return_value = MemberCard(mid,'name','','','')
            add_member(mid,self.service,self.session)
            self.assertEqual(DBOperation.query_member_via_mid(mid,self.session).mid,mid)
        self.assertEqual(self.session.execute('SELECT count(*) FROM tdd_member')[0][0],3)

    def test_profile_failure_is_retried_on_next_refresh(self):
        self.service.get_member_card.side_effect = [ServiceError(),MemberCard(10,'name','','','')]
        with self.assertLogs('task',level='WARNING'):
            ensure_video_member(1,10,self.service,self.session)
        self.assertIsNone(DBOperation.query_member_via_mid(10,self.session))
        ensure_video_member(1,10,self.service,self.session)
        self.assertEqual(DBOperation.query_member_via_mid(10,self.session).name,'name')
        self.assertEqual(self.service.get_member_card.call_count,2)

    def test_missing_optional_fields_preserve_old_and_explicit_empty_clears(self):
        self.session.seed(TddMember(added=1,mid=10,name='name',face='',sex='old',sign='old',code=0))
        self.service.get_member_card.return_value = MemberCard(10,'name',None,'',None,frozenset(('mid','name','face')))
        self.assertEqual(update_member(10,self.service,self.session),[])
        self.service.get_member_card.return_value = MemberCard(10,'name',None,'','\x00')
        logs = update_member(10,self.service,self.session)
        self.assertEqual({log.attr:(log.oldval,log.newval) for log in logs},{'sex':('old',None),'sign':('old','')})
        member = DBOperation.query_member_via_mid(10,self.session)
        self.assertEqual((member.sex,member.sign),(None,''))

    def test_query_failure_propagates_without_fetch_or_insert(self):
        with mock.patch.object(self.session,'execute',side_effect=RuntimeError('query failed')):
            with self.assertRaisesRegex(RuntimeError,'query failed'):
                add_member(10,self.service,self.session)
        self.service.get_member_card.assert_not_called()

    def test_invalid_mid_never_fetches_or_writes(self):
        for mid in (0,-1,None,'10'):
            with self.subTest(mid=mid), self.assertLogs('task',level='WARNING'):
                ensure_video_member(1,mid,self.service,self.session)
            with self.assertRaises(ValueError):
                add_member(mid,self.service,self.session)
        self.service.get_member_card.assert_not_called()
