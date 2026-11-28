"""Profile refresh persists sanitized differences and logs in one transaction."""
import unittest
from unittest import mock
from db import TddVideo,TddVideoStaff,TddMember
from service import FormatError, ServiceError
from service.endpoints import get_video_view
from task.task import update_video
from test_endpoint_get_video_view import expected_result,valid_response
from profile_support import ProfileConnection
from db.profiles import _query_video, _query_member, _query_staff, update_video_profile


class VideoMetadataRefreshTest(unittest.TestCase):
    def setUp(self):
        self.conn = ProfileConnection()
        self.addCleanup(self.conn.close)
        self.conn.patch_task_reads(self)
        self.video_view = expected_result()._replace(bvid='BV1234567890',pic='https://example.test/cover.jpg')
        self.service = mock.Mock()
        self.service.get_video_view.return_value = self.video_view
        self.service.get_video_tags.return_value.tags = []
        self.conn.seed(TddMember(added=1,mid=8,name='owner',face='',code=0))
        self.conn.seed(TddMember(added=1,mid=10,name='staff',face='',code=0))
        patcher = mock.patch('db.profiles.write',side_effect=self.conn.write)
        patcher.start()
        self.addCleanup(patcher.stop)

    def seed(self,**overrides):
        video_view = self.video_view
        values = {field:getattr(video_view,field) for field in ('videos','tid','tname','copyright','pic','title','pubdate','desc','state','attribute','forward')}
        values.update(added=1,aid=video_view.aid,bvid=video_view.bvid.removeprefix('BV'),mid=video_view.owner.mid,hasstaff=1,tags='',code=0)
        values.update(overrides)
        self.conn.seed(TddVideo(**values))
        self.conn.seed(TddVideoStaff(added=1,aid=video_view.aid,bvid='1234567890',mid=10,title='producer'))

    def refresh(self,tags=''):
        with mock.patch('task.task._get_video_tags_str',return_value=tags):
            logs = update_video(self.video_view.aid,self.service)
        return {log.attr:(log.oldval,log.newval) for log in logs}

    def use_api_response(self,response):
        response['data'].update(bvid=self.video_view.bvid,pic=self.video_view.pic)
        self.service.get_video_view.side_effect = lambda params: get_video_view.get(
            lambda *args,**kwargs:response,params=params)

    def test_null_cover_pubdate_owner_and_sorted_tags(self):
        self.seed(pic=None,pubdate=None,mid=None,tags=None)
        changes = self.refresh('music;animation')
        self.assertEqual(changes,{'pic':(None,self.video_view.pic),'pubdate':(None,'10'),'mid':(None,'8'),'tags':(None,'music;animation')})
        current = _query_video(self.conn,7)
        self.assertEqual((current.pic,current.pubdate,current.mid),(self.video_view.pic,10,8))
        self.assertEqual(self.conn.execute('SELECT count(*) FROM tdd_video_log').fetchone()[0],4)

    def test_equivalent_tags_and_only_actual_nul_do_not_generate_changes(self):
        self.seed(tags='animation;music',title='ti\x00tle',desc='de\x00sc')
        self.assertEqual(self.refresh('mu\x00sic;animation'),{})
        self.assertEqual(self.conn.execute('SELECT count(*) FROM tdd_video_log').fetchone()[0],0)

    def test_tag_nul_is_preserved_until_database_comparison_and_storage(self):
        self.seed(tags='old;')
        self.service.get_video_tags.return_value.tags = [mock.Mock(tag_name='mu\x00sic\\u0000')]
        with mock.patch('task.task.update_video_profile',wraps=update_video_profile) as persist:
            logs = update_video(self.video_view.aid,self.service)
        self.assertEqual(persist.call_args.args[3],'mu\x00sic\\u0000;')
        self.assertEqual({log.attr:(log.oldval,log.newval) for log in logs},
                         {'tags':('old;','music\\u0000;')})
        self.assertEqual(_query_video(self.conn,self.video_view.aid).tags,'music\\u0000;')
        self.assertEqual(update_video(self.video_view.aid,self.service),[])

    def test_missing_optional_fields_reset_hasstaff_but_keep_other_metadata(self):
        self.seed()
        response = valid_response()
        for field in ('staff','attribute','forward'):
            del response['data'][field]
        self.use_api_response(response)
        self.assertEqual(self.refresh(),{'hasstaff':('1','0')})
        current = _query_video(self.conn,7)
        self.assertEqual((current.hasstaff,current.attribute,current.forward),(0,16,17))
        # Existing relationship handling is separate from the response flag.
        self.assertEqual(len(_query_staff(self.conn,7)),1)

    def test_optional_scalars_omit_null_and_value_through_parser_and_update(self):
        self.seed()
        response = valid_response()
        del response['data']['attribute']
        response['data']['forward'] = None
        self.use_api_response(response)
        self.assertEqual(self.refresh(), {'forward': ('17', None)})
        stored = _query_video(self.conn,7)
        self.assertEqual((stored.attribute, stored.forward), (16, None))

        response['data']['attribute'] = None
        del response['data']['forward']
        self.assertEqual(self.refresh(), {'attribute': ('16', None)})
        stored = _query_video(self.conn,7)
        self.assertEqual((stored.attribute, stored.forward), (None, None))

        response['data'].update(attribute=18, forward=19)
        self.assertEqual(self.refresh(), {'attribute': (None, '18'), 'forward': (None, '19')})
        stored = _query_video(self.conn,7)
        self.assertEqual((stored.attribute, stored.forward), (18, 19))

    def test_explicit_null_staff_fails_before_any_database_change(self):
        self.seed()
        response = valid_response()
        response['data'].update(title='should not persist', staff=None)
        self.use_api_response(response)
        with self.assertRaises(FormatError):
            self.refresh()
        self.assertEqual(_query_video(self.conn,7).title, self.video_view.title)
        self.assertEqual(len(_query_staff(self.conn,7)), 1)
        self.assertEqual(self.conn.execute('SELECT count(*) FROM tdd_video_log').fetchone()[0], 0)

    def test_missing_title_stops_add_and_refresh_before_enrichment_or_writes(self):
        from task.task import add_video
        response = valid_response()
        del response['data']['title']
        self.use_api_response(response)
        for operation in (add_video,update_video):
            with self.subTest(operation=operation.__name__):
                if operation is update_video:
                    self.seed(title='stored title')
                with mock.patch('task.task.create_video') as create,\
                        mock.patch('task.task.update_video_profile') as update:
                    with self.assertRaises(FormatError):
                        operation(self.video_view.aid,self.service)
                create.assert_not_called()
                update.assert_not_called()
                self.service.get_member_card.assert_not_called()
                self.service.get_video_tags.assert_not_called()
                current = _query_video(self.conn,self.video_view.aid)
                if operation is add_video:
                    self.assertIsNone(current)
                else:
                    self.assertEqual(current.title,'stored title')

    def test_missing_staff_resolves_unknown_flag_and_only_logs_actual_change(self):
        self.seed(hasstaff=-1)
        response = valid_response()
        del response['data']['staff']
        self.use_api_response(response)
        self.assertEqual(self.refresh(),{'hasstaff':('-1','0')})
        self.assertEqual(_query_video(self.conn,7).hasstaff,0)
        self.assertEqual(self.refresh(),{})
        self.assertEqual(self.conn.execute('SELECT count(*) FROM tdd_video_log').fetchone()[0],1)
        self.assertEqual(len(_query_staff(self.conn,7)),1)

    def test_present_empty_staff_sets_flag_and_reconciles_relationships(self):
        self.seed(hasstaff=0)
        response = valid_response()
        response['data']['staff'] = []
        self.use_api_response(response)
        self.assertEqual(self.refresh(),{'hasstaff':('0','1'),'staff':('mid: 10; title: producer',None)})
        self.assertEqual(_query_video(self.conn,7).hasstaff,1)
        self.assertEqual(_query_staff(self.conn,7),[])

    def test_present_staff_sets_flag_and_preserves_unchanged_relationship(self):
        self.seed(hasstaff=0)
        self.use_api_response(valid_response())
        self.assertEqual(self.refresh(),{'hasstaff':('0','1')})
        self.assertEqual(_query_video(self.conn,7).hasstaff,1)
        self.assertEqual(len(_query_staff(self.conn,7)),1)

    def test_code_errors_do_not_derive_hasstaff_or_change_relationships(self):
        self.seed()
        response = valid_response()
        del response['data']['staff']
        response['code'] = 62002
        self.use_api_response(response)
        self.assertEqual(self.refresh(),{'code':('0','62002')})
        response['code'] = -403
        self.assertEqual(self.refresh(),{})
        current = _query_video(self.conn,7)
        self.assertEqual((current.hasstaff,current.code),(1,62002))
        self.assertEqual(len(_query_staff(self.conn,7)),1)

    def test_login_failure_returns_without_database_access_or_enrichment(self):
        self.seed()
        response = valid_response()
        response['code'] = -403
        self.use_api_response(response)
        for aid in (self.video_view.aid,999):
            with self.subTest(aid=aid), \
                    mock.patch('db.profiles.write') as write, \
                    mock.patch.object(self.conn,'execute',side_effect=AssertionError('unexpected database access')):
                self.assertEqual(update_video(aid,self.service),[])
                write.assert_not_called()
        self.service.get_video_tags.assert_not_called()
        self.service.get_member_card.assert_not_called()

    def test_explicit_null_and_literal_escape_are_logged_without_nul(self):
        self.seed(desc='old')
        self.service.get_video_view.return_value = self.video_view._replace(desc='中\x00文\\u0000',attribute=None)
        changes = self.refresh()
        self.assertEqual(changes['desc'],('old','中文\\u0000'))
        self.assertEqual(changes['attribute'],('16',None))
        self.assertNotIn('\x00',str(self.conn.execute('SELECT oldval,newval FROM tdd_video_log')))

    def test_refresh_keeps_locked_profile_and_staff_snapshots_unchanged(self):
        self.seed(title='old',desc='old description')
        snapshot = _query_video(self.conn,self.video_view.aid)
        staff = _query_staff(self.conn,self.video_view.aid)
        before = vars(snapshot).copy()
        staff_before = [vars(item).copy() for item in staff]
        self.service.get_video_view.return_value = self.video_view._replace(
            desc=None,staff=[self.video_view.staff[0]._replace(title='new role')])
        with mock.patch('db.profiles._query_video',return_value=snapshot) as read,\
             mock.patch('db.profiles._query_staff',return_value=staff):
            changes = self.refresh()
        read.assert_called_once_with(self.conn,self.video_view.aid)
        self.assertEqual(vars(snapshot),before)
        self.assertEqual([vars(item) for item in staff],staff_before)
        self.assertEqual(changes,{'title':('old',self.video_view.title),'desc':('old description',None),
                                  'staff':('mid: 10; title: producer','mid: 10; title: new role')})
        stored = _query_video(self.conn,self.video_view.aid)
        self.assertEqual((stored.title,stored.desc,stored.added),(self.video_view.title,None,1))
        self.assertEqual(_query_staff(self.conn,self.video_view.aid)[0].title,'new role')

    def test_log_failure_rolls_back_profile_and_staff(self):
        self.seed(title='old')
        snapshot = _query_video(self.conn,self.video_view.aid)
        before = vars(snapshot).copy()
        self.service.get_video_view.return_value = self.video_view._replace(staff=[])
        def fail(*args,**kwargs):
            raise RuntimeError('log insert failed')
        with mock.patch('db.profiles._query_video',return_value=snapshot),\
             mock.patch.object(self.conn,'append_video_logs',side_effect=fail), self.assertRaisesRegex(RuntimeError,'log insert'):
            self.refresh()
        self.assertEqual(vars(snapshot),before)
        self.assertEqual(_query_video(self.conn,7).title,'old')
        self.assertEqual(len(_query_staff(self.conn,7)),1)

    def test_change_log_time_precedes_slow_enrichment_and_keeps_creation_time(self):
        self.seed(title='before refresh')
        now = [100]
        def slow_tags(*args,**kwargs):
            now[0] = 300
            return ''
        with mock.patch('task.task.get_ts_s',side_effect=lambda:now[0]), \
                mock.patch('task.task._get_video_tags_str',side_effect=slow_tags):
            logs = update_video(self.video_view.aid,self.service)
        self.assertEqual([(log.attr,log.added) for log in logs],[('title',100)])
        self.assertEqual(_query_video(self.conn,self.video_view.aid).added,1)

    def test_new_video_repeated_staff_keeps_first_title_and_commits_video(self):
        from task.task import add_video
        from service import VideoViewStaffItem
        self.service.get_video_view.return_value = self.video_view._replace(
            staff=[VideoViewStaffItem(10,'first','',''),VideoViewStaffItem(10,'second','','')])
        video = add_video(self.video_view.aid,self.service)
        self.assertIsNotNone(video.id)
        self.assertEqual(_query_video(self.conn,self.video_view.aid).title,self.video_view.title)
        staff = _query_staff(self.conn,self.video_view.aid)
        self.assertEqual([(item.mid,item.title) for item in staff],[(10,'first')])

    def test_new_video_tags_failure_aborts_before_database_writes(self):
        from task.task import add_video
        error = ServiceError()
        self.service.get_video_tags.side_effect = error
        with self.assertRaises(ServiceError) as caught:
            add_video(self.video_view.aid,self.service)
        self.assertIs(caught.exception,error)
        self.assertIsNone(_query_video(self.conn,self.video_view.aid))
        self.assertEqual(_query_staff(self.conn,self.video_view.aid),[])
        self.service.get_member_card.assert_not_called()

    def test_new_video_creation_values_preserve_tag_classification_and_defaults(self):
        from task.task import add_video
        for aid,tag,isvc in ((7,'vocaloid中文曲',2),(9,'other',-1)):
            with self.subTest(tag=tag):
                self.service.get_video_view.return_value = self.video_view._replace(aid=aid,staff=None)
                self.service.get_video_tags.return_value.tags = [mock.Mock(tag_name=tag)]
                video = add_video(aid,self.service)
                self.assertEqual((video.code,video.recent,video.freq,video.activity),(0,2,0,0))
                self.assertEqual((video.solo,video.original,video.employed,video.engine),(-1,-1,-1,-1))
                self.assertEqual((video.hasstaff,video.isvc,video.tags),(0,isvc,tag+';'))
                self.assertEqual(vars(video),vars(_query_video(self.conn,aid)))

    def test_refresh_tags_failure_preserves_tags_and_updates_other_fields(self):
        self.seed(title='old title',tags='stored tags;')
        self.service.get_video_tags.side_effect = ServiceError()
        with self.assertLogs('task',level='WARNING'):
            logs = update_video(self.video_view.aid,self.service)
        current = _query_video(self.conn,self.video_view.aid)
        self.assertEqual((current.tags,current.title),('stored tags;',self.video_view.title))
        self.assertEqual({log.attr:(log.oldval,log.newval) for log in logs},
                         {'title':('old title',self.video_view.title)})

    def test_refresh_repeated_existing_and_new_staff_preserves_first_logs_and_other_changes(self):
        from service import VideoViewStaffItem
        self.seed()
        self.conn.seed(TddMember(added=1,mid=11,name='staff',face='',code=0))
        self.conn.seed(TddVideoStaff(added=1,aid=self.video_view.aid,bvid='1234567890',mid=12,title='removed'))
        self.service.get_video_view.return_value = self.video_view._replace(title='changed',staff=[
            VideoViewStaffItem(10,'first-existing','',''),VideoViewStaffItem(10,'second-existing','',''),
            VideoViewStaffItem(11,'first-new','',''),VideoViewStaffItem(11,'second-new','','')])
        with mock.patch('task.task._get_video_tags_str',return_value=''),self.assertLogs('db.profiles',level='WARNING'):
            logs = update_video(self.video_view.aid,self.service)
        self.assertEqual(_query_video(self.conn,self.video_view.aid).title,'changed')
        staff = _query_staff(self.conn,self.video_view.aid)
        self.assertEqual({item.mid:item.title for item in staff},{10:'first-existing',11:'first-new'})
        staff_logs = [(item.oldval,item.newval) for item in logs if item.attr == 'staff']
        self.assertEqual(staff_logs,[('mid: 10; title: producer','mid: 10; title: first-existing'),
                                    (None,'mid: 11; title: first-new'),('mid: 12; title: removed',None)])
        self.assertEqual(len(logs),4)
