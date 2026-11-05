"""Missing owner/staff profiles are retried without changing attribution."""

from types import SimpleNamespace
import unittest
from unittest import mock

from test_remaining_script_run_records import _DEPS

if _DEPS:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from task.task import add_member, ensure_video_member, update_video
    from db import TddMember
    from service import MemberCard, ServiceError
    from test_endpoint_get_video_view import expected_result


@unittest.skipUnless(_DEPS, 'database dependencies unavailable')
class VideoMemberProfilesTest(unittest.TestCase):
    def test_member_mid_round_trip_across_int32_boundary(self):
        engine = create_engine('sqlite://')
        self.addCleanup(engine.dispose)
        # SQLite needs an INTEGER primary key to assign IDs. The mapped mid
        # remains the application's Integer declaration throughout this test.
        engine.execute('CREATE TABLE tdd_member ('
                       'id INTEGER PRIMARY KEY, added INTEGER, mid BIGINT UNIQUE, '
                       'sex TEXT, name TEXT, face TEXT, sign TEXT, code INTEGER, '
                       'video_count INTEGER, last_video BIGINT, '
                       'last_total_stat BIGINT, last_follower BIGINT)')
        session = sessionmaker(bind=engine)()
        self.addCleanup(session.close)
        service = mock.Mock()
        for mid in (2147483647, 2147483648, 9000000000000):
            service.get_member_card.return_value = MemberCard(mid, 'name', '', '', '')
            add_member(mid, service, session)
            actual = session.query(TddMember).filter(TddMember.mid == mid).one()
            self.assertEqual(actual.mid, mid)
        self.assertEqual(session.query(TddMember).count(), 3)

    def test_failed_member_write_is_rolled_back_and_propagates(self):
        session, service = mock.Mock(), mock.Mock()
        service.get_member_card.return_value = MemberCard(10, 'name', '', '', '')
        session.commit.side_effect = RuntimeError('write failed')
        with self.assertRaisesRegex(RuntimeError, 'write failed'):
            add_member(10, service, session, test_exist=False)
        session.rollback.assert_called_once_with()

    def test_profile_failure_is_retried_on_next_refresh(self):
        session, service = mock.Mock(), mock.Mock()
        session.query.return_value.filter.return_value.first.return_value = None
        service.get_member_card.side_effect = [ServiceError(), MemberCard(10, 'name', '', '', '')]
        with self.assertLogs('task', level='WARNING'):
            ensure_video_member(1, 10, service, session)
        session.add.assert_not_called()
        ensure_video_member(1, 10, service, session)
        self.assertEqual(session.add.call_count, 1)
        self.assertEqual(service.get_member_card.call_count, 2)

    def test_metadata_refresh_checks_unchanged_owner_and_staff(self):
        view = expected_result()
        current = SimpleNamespace(**view._asdict(), mid=view.owner.mid, hasstaff=1, tags='', code=0)
        staff = SimpleNamespace(mid=view.staff[0].mid, title=view.staff[0].title)
        session, service = mock.Mock(), mock.Mock()
        service.get_video_view.return_value = view
        with mock.patch('task.task.DBOperation.query_video_via_aid', return_value=current), \
             mock.patch('task.task.DBOperation.query_video_staff_via_aid', return_value=[staff]), \
             mock.patch('task.task.get_video_tags_str', return_value=''), \
             mock.patch('task.task.same_pic_url', return_value=True), \
             mock.patch('task.task.ensure_video_member') as ensure:
            self.assertEqual(update_video(view.aid, service, session), [])
        self.assertEqual({call.args[1] for call in ensure.call_args_list},
                         {view.owner.mid, view.staff[0].mid})

    def test_invalid_mid_never_fetches_or_writes_a_profile(self):
        for mid in (0, -1, None, '10'):
            with self.subTest(mid=mid):
                session, service = mock.Mock(), mock.Mock()
                with self.assertLogs('task', level='WARNING'):
                    ensure_video_member(1, mid, service, session)
                service.get_member_card.assert_not_called()
                session.query.assert_not_called()
                with self.assertRaises(ValueError):
                    add_member(mid, service, session)

    def test_incomplete_stored_metadata_does_not_block_real_pubdate_refresh(self):
        view = expected_result()
        for tags in (None, ''):
            with self.subTest(tags=tags):
                current = SimpleNamespace(**view._asdict(), mid=view.owner.mid,
                                          hasstaff=1, tags=tags, code=0)
                current.pic = current.pubdate = current.mid = None
                staff = SimpleNamespace(mid=view.staff[0].mid, title=view.staff[0].title)
                session, service = mock.Mock(), mock.Mock()
                service.get_video_view.return_value = view
                with mock.patch('task.task.DBOperation.query_video_via_aid', return_value=current), \
                        mock.patch('task.task.DBOperation.query_video_staff_via_aid', return_value=[staff]), \
                        mock.patch('task.task.get_video_tags_str', return_value=''), \
                        mock.patch('task.task.DBOperation.add') as add_log, \
                        mock.patch('task.task.ensure_video_member'):
                    changes = update_video(view.aid, service, session)
                self.assertEqual((current.pic, current.pubdate, current.mid),
                                 (view.pic, view.pubdate, view.owner.mid))
                self.assertEqual({change.attr for change in changes}, {'pic', 'pubdate', 'mid'})
                pubdate_log = next(change for change in changes if change.attr == 'pubdate')
                self.assertEqual((pubdate_log.oldval, pubdate_log.newval), (None, view.pubdate))
                session.commit.assert_called_once_with()
                self.assertEqual(add_log.call_count, 3)


if __name__ == '__main__':
    unittest.main()
