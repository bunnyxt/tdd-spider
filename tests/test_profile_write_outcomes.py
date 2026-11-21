"""Tasks interpret actual write outcomes, including inserts racing their prechecks."""
import sqlite3
import unittest
from unittest import mock

from db import TddMember, TddVideo
from db.profiles import insert_video, insert_member, update_member_profile, _query_member
from service import MemberCard
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
        invalid = TddVideo(added=1, aid=self.view.aid,
                           bvid=self.view.bvid.removeprefix('BV'), title=None, code=0)
        with self.assertRaises(sqlite3.IntegrityError):
            insert_video(invalid, [], [], record=None)
        self.assertEqual(self.conn.execute('SELECT count(*) FROM tdd_video').fetchone()[0], 0)

    def test_database_reports_absence_and_duplicate_without_task_errors(self):
        row = TddMember(added=1, mid=7, name='first', code=0)
        self.assertIs(insert_member(row), row)
        self.assertIsNone(insert_member(TddMember(added=2, mid=7, name='ignored', code=0)))
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
