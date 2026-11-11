"""Stored NULL cover and tags must not prevent metadata refresh."""

from types import SimpleNamespace
import unittest
from unittest import mock

from test_remaining_script_run_records import _DEPS

if _DEPS:
    from task.task import update_video
    from test_endpoint_get_video_view import expected_result


@unittest.skipUnless(_DEPS, 'database dependencies unavailable')
class VideoMetadataRefreshTest(unittest.TestCase):
    def refresh(self, current, view, tags):
        staff = SimpleNamespace(mid=view.staff[0].mid, title=view.staff[0].title)
        session, service = mock.Mock(), mock.Mock()
        service.get_video_view.return_value = view
        with mock.patch('task.task.DBOperation.query_video_via_aid', return_value=current), \
                mock.patch('task.task.DBOperation.query_video_staff_via_aid', return_value=[staff]), \
                mock.patch('task.task.get_video_tags_str', return_value=tags), \
                mock.patch('task.task.DBOperation.add') as add_log, \
                mock.patch('task.task.ensure_video_member'):
            changes = update_video(view.aid, service, session)
        session.commit.assert_called_once_with()
        self.assertEqual(add_log.call_count, len(changes))
        return {change.attr: (change.oldval, change.newval) for change in changes}

    def current(self, view, **overrides):
        values = dict(view._asdict(), mid=view.owner.mid, hasstaff=1, tags='', code=0)
        values.update(overrides)
        return SimpleNamespace(**values)

    def test_null_cover_allows_remaining_real_metadata_to_refresh(self):
        view = expected_result()._replace(pic='https://i0.hdslb.com/bfs/archive/cover.jpg')
        current = self.current(view, pic=None, pubdate=None, mid=None)
        changes = self.refresh(current, view, '')
        self.assertEqual(changes, {'pic': (None, view.pic),
                                   'pubdate': (None, view.pubdate),
                                   'mid': (None, view.owner.mid)})
        self.assertEqual((current.pic, current.pubdate, current.mid),
                         (view.pic, view.pubdate, view.owner.mid))

    def test_null_or_empty_tags_accept_real_tags_without_changing_valid_pubdate(self):
        view = expected_result()._replace(pic='https://i0.hdslb.com/bfs/archive/cover.jpg')
        for tags in (None, ''):
            with self.subTest(tags=tags):
                current = self.current(view, tags=tags)
                self.assertEqual(self.refresh(current, view, 'music;animation'),
                                 {'tags': (tags, 'music;animation')})
                self.assertEqual(current.tags, 'music;animation')
                self.assertEqual(current.pubdate, view.pubdate)

    def test_existing_cover_and_equivalent_tags_do_not_generate_changes(self):
        view = expected_result()._replace(pic='https://i0.hdslb.com/bfs/archive/cover.jpg')
        current = self.current(view, tags='animation;music')
        self.assertEqual(self.refresh(current, view, 'music;animation'), {})
        self.assertEqual(current.pic, view.pic)
        self.assertEqual(current.pubdate, view.pubdate)


if __name__ == '__main__':
    unittest.main()
