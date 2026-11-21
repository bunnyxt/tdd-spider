"""A failed lookup must not become an absent-video result."""
import unittest
from unittest import mock
from db import DBOperation, TddVideo
from db.basic import Result, ResultRow


class DatabaseLookupTest(unittest.TestCase):
    def test_aid_lookup_returns_snapshot_or_none(self):
        session = mock.Mock()
        session.execute.return_value = Result([ResultRow(aid=7, id=10, title='stored')])
        video = DBOperation.query_video_via_aid(7, session)
        self.assertIsInstance(video, TddVideo)
        self.assertEqual((video.aid, video.title), (7, 'stored'))
        session.execute.assert_called_once_with('SELECT * FROM tdd_video WHERE aid = %s', (7,))
        session.execute.return_value = Result()
        self.assertIsNone(DBOperation.query_video_via_aid(7, session))

    def test_aid_query_error_propagates_unchanged(self):
        error = RuntimeError('query failed')
        session = mock.Mock()
        session.execute.side_effect = error
        with self.assertRaises(RuntimeError) as caught:
            DBOperation.query_video_via_aid(7, session)
        self.assertIs(caught.exception, error)
