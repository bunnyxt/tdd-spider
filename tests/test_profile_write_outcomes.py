"""Tasks interpret actual write outcomes, including inserts racing their prechecks."""
import sqlite3
import unittest
from unittest import mock

from db import TddMember, TddVideo
from core import NewVideo, NewMember
from db.profiles import create_video, insert_member, update_member_profile, _query_member
from service import MemberCard, VideoViewStaffItem
from task.error import AlreadyExistError, NotExistError
from task.task import add_member, add_video, update_member, update_video
from profile_support import ProfileConnection
from test_endpoint_get_video_view import expected_result


class ProfileWriteOutcomesTest(unittest.TestCase):
    def setUp(self):
        self.conn = ProfileConnection()
        self.addCleanup(self.conn.close)
        self.conn.patch_task_reads(self)
        patcher = mock.patch('db.profiles.execute_write', side_effect=self.conn.execute_write)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.service = mock.Mock()
        self.card = MemberCard(7, 'fetched', '', '', '')
        self.service.get_member_card.return_value = self.card
        self.service.get_video_tags.return_value.tags = []
        self.view = expected_result()._replace(staff=[])
        self.service.get_video_view.return_value = self.view

    def test_explicit_member_add_reports_competing_insert_after_precheck(self):
        def fetch(*args, **kwargs):
            self.conn.seed(TddMember(added=1, mid=7, name='concurrent', code=0))
            return self.card
        self.service.get_member_card.side_effect = fetch
        with self.assertRaises(AlreadyExistError):
            add_member(7, self.service)
        self.assertEqual(_query_member(self.conn, 7).name, 'concurrent')
        self.assertEqual(self.conn.execute('SELECT count(*) FROM tdd_ingest.profile_observation').fetchone()[0], 0)

    def test_explicit_video_add_reports_competing_insert_after_precheck(self):
        def fetch(*args, **kwargs):
            self.conn.seed(TddVideo(added=1, aid=self.view.aid, bvid='old', title='concurrent', code=0))
            return self.view
        self.service.get_video_view.side_effect = fetch
        with mock.patch('db.profiles.append_video_records') as append:
            with self.assertRaises(AlreadyExistError):
                add_video(self.view.aid, self.service)
        append.assert_not_called()
        self.assertEqual(self.conn.execute('SELECT title FROM tdd_video').fetchone()[0], 'concurrent')
        self.assertEqual(self.conn.execute('SELECT count(*) FROM tdd_member').fetchone()[0], 0)

    def test_unrelated_not_null_constraint_is_not_an_existing_aid_outcome(self):
        with self.assertRaises(sqlite3.IntegrityError):
            create_video(NewVideo(added=1, aid=self.view.aid,
                         bvid=self.view.bvid.removeprefix('BV'), mid=self.view.owner.mid,
                         title=None, pubdate=self.view.pubdate), staff=[], new_members=[])
        self.assertEqual(self.conn.execute('SELECT count(*) FROM tdd_video').fetchone()[0], 0)

    def test_existing_prepared_member_does_not_block_remaining_creation(self):
        from core import VideoRecord
        self.conn.seed(TddMember(added=1,mid=8,name='existing',face='old-face',code=0))
        video = NewVideo(added=20,aid=7,bvid='1234567890',mid=8,title='video',pubdate=10,hasstaff=1)
        members = [NewMember(added=20,mid=8,name='ignored',face='new-face'),
                   NewMember(added=20,mid=10,name='staff',face='staff-face')]
        first_record = VideoRecord(20,7,'1234567890',1,2,3,4,5,6,7)
        with mock.patch('db.profiles.append_video_records') as append:
            stored = create_video(video,new_members=members,
                staff=[VideoViewStaffItem(10,'producer','staff','staff-face')],first_record=first_record)
        self.assertIsInstance(stored,TddVideo)
        self.assertIsNotNone(stored.id)
        self.assertFalse(hasattr(video,'id'))
        self.assertTrue(all(not hasattr(member,'id') for member in members))
        self.assertEqual(_query_member(self.conn,8).name,'existing')
        self.assertEqual(_query_member(self.conn,10).name,'staff')
        self.assertEqual(self.conn.execute('SELECT mid,title FROM tdd_video_staff WHERE aid=%s',(7,)).fetchall(),
                         [(10,'producer')])
        append.assert_called_once_with(self.conn,[first_record],run_id=None)

    def test_database_reports_absence_and_duplicate_without_task_errors(self):
        row = NewMember(added=1, mid=7, name='first', face='', code=0)
        inserted = insert_member(row)
        self.assertIsInstance(inserted,TddMember)
        self.assertIsNot(inserted,row)
        self.assertIsNotNone(inserted.id)
        self.assertFalse(hasattr(row,'id'))
        self.assertIsNone(insert_member(NewMember(added=2, mid=7, name='ignored', face='', code=0)))
        self.assertIsNone(update_member_profile(8, None, -404, 10))
        self.assertEqual(_query_member(self.conn, 7).name, 'first')

    def test_update_tasks_interpret_missing_profiles(self):
        with self.assertRaises(NotExistError):
            update_member(7, self.service)
        with self.assertRaises(NotExistError):
            update_video(self.view.aid, self.service)

    def test_database_exception_is_not_reclassified_as_profile_absence(self):
        error = TimeoutError('database timeout')
        with mock.patch('task.task.update_member_profile', side_effect=error):
            with self.assertRaises(TimeoutError) as caught:
                update_member(7, self.service)
        self.assertIs(caught.exception, error)
