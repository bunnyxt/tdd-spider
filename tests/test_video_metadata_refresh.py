"""Profile refresh persists sanitized differences and logs in one transaction."""
import unittest
from unittest import mock
from db import DBOperation,TddVideo,TddVideoStaff,TddMember
from service.endpoints import get_video_view
from task.task import update_video
from test_endpoint_get_video_view import expected_result,valid_response
from profile_support import ProfileSession


class VideoMetadataRefreshTest(unittest.TestCase):
    def setUp(self):
        self.session = ProfileSession()
        self.addCleanup(self.session.close)
        self.view = expected_result()._replace(bvid='BV1234567890',pic='https://example.test/cover.jpg')
        self.service = mock.Mock()
        self.service.get_video_view.return_value = self.view
        self.service.get_video_tags.return_value.tags = []
        self.session.seed(TddMember(added=1,mid=8,name='owner',face='',code=0))
        self.session.seed(TddMember(added=1,mid=10,name='staff',face='',code=0))
        patcher = mock.patch('task.task.observe',side_effect=self.session.observe)
        patcher.start()
        self.addCleanup(patcher.stop)

    def seed(self,**overrides):
        view = self.view
        values = {field:getattr(view,field) for field in ('videos','tid','tname','copyright','pic','title','pubdate','desc','state','attribute','forward')}
        values.update(added=1,aid=view.aid,bvid=view.bvid.removeprefix('BV'),mid=view.owner.mid,hasstaff=1,tags='',code=0)
        values.update(overrides)
        self.session.seed(TddVideo(**values))
        self.session.seed(TddVideoStaff(added=1,aid=view.aid,bvid='1234567890',mid=10,title='producer'))

    def refresh(self,tags=''):
        with mock.patch('task.task.get_video_tags_str',return_value=tags):
            logs = update_video(self.view.aid,self.service,self.session)
        return {log.attr:(log.oldval,log.newval) for log in logs}

    def use_api_response(self,response):
        response['data'].update(bvid=self.view.bvid,pic=self.view.pic)
        self.service.get_video_view.side_effect = lambda params: get_video_view.get(
            lambda *args,**kwargs:response,params=params)

    def test_null_cover_pubdate_owner_and_sorted_tags(self):
        self.seed(pic=None,pubdate=None,mid=None,tags=None)
        changes = self.refresh('music;animation')
        self.assertEqual(changes,{'pic':(None,self.view.pic),'pubdate':(None,'10'),'mid':(None,'8'),'tags':(None,'music;animation')})
        current = DBOperation.query_video_via_aid(7,self.session)
        self.assertEqual((current.pic,current.pubdate,current.mid),(self.view.pic,10,8))
        self.assertEqual(self.session.execute('SELECT count(*) FROM tdd_video_log')[0][0],4)

    def test_equivalent_tags_and_only_actual_nul_do_not_generate_changes(self):
        self.seed(tags='animation;music',title='ti\x00tle',desc='de\x00sc')
        self.assertEqual(self.refresh('music;animation'),{})
        self.assertEqual(self.session.execute('SELECT count(*) FROM tdd_video_log')[0][0],0)

    def test_missing_optional_fields_reset_hasstaff_but_keep_other_metadata(self):
        self.seed()
        response = valid_response()
        for field in ('staff','attribute','forward'):
            del response['data'][field]
        self.use_api_response(response)
        self.assertEqual(self.refresh(),{'hasstaff':('1','0')})
        current = DBOperation.query_video_via_aid(7,self.session)
        self.assertEqual((current.hasstaff,current.attribute,current.forward),(0,16,17))
        # Existing relationship handling is separate from the response flag.
        self.assertEqual(len(DBOperation.query_video_staff_via_aid(7,self.session)),1)

    def test_missing_staff_resolves_unknown_flag_and_only_logs_actual_change(self):
        self.seed(hasstaff=-1)
        response = valid_response()
        del response['data']['staff']
        self.use_api_response(response)
        self.assertEqual(self.refresh(),{'hasstaff':('-1','0')})
        self.assertEqual(DBOperation.query_video_via_aid(7,self.session).hasstaff,0)
        self.assertEqual(self.refresh(),{})
        self.assertEqual(self.session.execute('SELECT count(*) FROM tdd_video_log')[0][0],1)
        self.assertEqual(len(DBOperation.query_video_staff_via_aid(7,self.session)),1)

    def test_present_empty_staff_sets_flag_and_reconciles_relationships(self):
        self.seed(hasstaff=0)
        response = valid_response()
        response['data']['staff'] = []
        self.use_api_response(response)
        self.assertEqual(self.refresh(),{'hasstaff':('0','1'),'staff':('mid: 10; title: producer',None)})
        self.assertEqual(DBOperation.query_video_via_aid(7,self.session).hasstaff,1)
        self.assertEqual(DBOperation.query_video_staff_via_aid(7,self.session),[])

    def test_present_staff_sets_flag_and_preserves_unchanged_relationship(self):
        self.seed(hasstaff=0)
        self.use_api_response(valid_response())
        self.assertEqual(self.refresh(),{'hasstaff':('0','1')})
        self.assertEqual(DBOperation.query_video_via_aid(7,self.session).hasstaff,1)
        self.assertEqual(len(DBOperation.query_video_staff_via_aid(7,self.session)),1)

    def test_code_errors_do_not_derive_hasstaff_or_change_relationships(self):
        self.seed()
        response = valid_response()
        del response['data']['staff']
        response['code'] = 62002
        self.use_api_response(response)
        self.assertEqual(self.refresh(),{'code':('0','62002')})
        response['code'] = -403
        self.assertEqual(self.refresh(),{})
        current = DBOperation.query_video_via_aid(7,self.session)
        self.assertEqual((current.hasstaff,current.code),(1,62002))
        self.assertEqual(len(DBOperation.query_video_staff_via_aid(7,self.session)),1)

    def test_explicit_null_and_literal_escape_are_logged_without_nul(self):
        self.seed(desc='old')
        self.service.get_video_view.return_value = self.view._replace(desc='中\x00文\\u0000',attribute=None)
        changes = self.refresh()
        self.assertEqual(changes['desc'],('old','中文\\u0000'))
        self.assertEqual(changes['attribute'],('16',None))
        self.assertNotIn('\x00',str(self.session.execute('SELECT oldval,newval FROM tdd_video_log')))

    def test_log_failure_rolls_back_profile_and_staff(self):
        self.seed(title='old')
        def fail(*args,**kwargs):
            raise RuntimeError('log insert failed')
        with mock.patch.object(self.session,'append',side_effect=fail), self.assertRaisesRegex(RuntimeError,'log insert'):
            self.refresh()
        self.assertEqual(DBOperation.query_video_via_aid(7,self.session).title,'old')

    def test_new_video_repeated_staff_keeps_first_title_and_commits_video(self):
        from task.task import add_video
        from service import VideoViewStaffItem
        self.service.get_video_view.return_value = self.view._replace(
            staff=[VideoViewStaffItem(10,'first','',''),VideoViewStaffItem(10,'second','','')])
        video = add_video(self.view.aid,self.service,self.session,commit_video_record=False)
        self.assertIsNotNone(video.id)
        self.assertEqual(DBOperation.query_video_via_aid(self.view.aid,self.session).title,self.view.title)
        staff = DBOperation.query_video_staff_via_aid(self.view.aid,self.session)
        self.assertEqual([(item.mid,item.title) for item in staff],[(10,'first')])

    def test_refresh_repeated_existing_and_new_staff_preserves_first_logs_and_other_changes(self):
        from service import VideoViewStaffItem
        self.seed()
        self.session.seed(TddMember(added=1,mid=11,name='staff',face='',code=0))
        self.session.seed(TddVideoStaff(added=1,aid=self.view.aid,bvid='1234567890',mid=12,title='removed'))
        self.service.get_video_view.return_value = self.view._replace(title='changed',staff=[
            VideoViewStaffItem(10,'first-existing','',''),VideoViewStaffItem(10,'second-existing','',''),
            VideoViewStaffItem(11,'first-new','',''),VideoViewStaffItem(11,'second-new','','')])
        with mock.patch('task.task.get_video_tags_str',return_value=''),self.assertLogs('task',level='WARNING'):
            logs = update_video(self.view.aid,self.service,self.session)
        self.assertEqual(DBOperation.query_video_via_aid(self.view.aid,self.session).title,'changed')
        staff = DBOperation.query_video_staff_via_aid(self.view.aid,self.session)
        self.assertEqual({item.mid:item.title for item in staff},{10:'first-existing',11:'first-new'})
        staff_logs = [(item.oldval,item.newval) for item in logs if item.attr == 'staff']
        self.assertEqual(staff_logs,[('mid: 10; title: producer','mid: 10; title: first-existing'),
                                    (None,'mid: 11; title: first-new'),('mid: 12; title: removed',None)])
        self.assertEqual(len(logs),4)
