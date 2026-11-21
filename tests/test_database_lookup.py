"""Reads release connections and distinguish missing data from database failures."""
from datetime import datetime, timezone
import unittest
from unittest import mock

from db import TddVideo
from db import queries


class DatabaseLookupTest(unittest.TestCase):
    def setUp(self):
        self.connection = mock.MagicMock()
        self.cursor = self.connection.cursor.return_value.__enter__.return_value
        patcher = mock.patch.object(queries.engine, 'connection')
        self.borrow = patcher.start()
        self.addCleanup(patcher.stop)
        self.borrow.return_value.__enter__.return_value = self.connection

    def test_aid_lookup_returns_snapshot_or_none_and_releases_connection(self):
        self.cursor.fetchone.return_value = dict(aid=7, id=10, title='stored',
            added=datetime(2026, 1, 1, tzinfo=timezone.utc))
        video = queries.query_video_via_aid(7)
        self.assertIsInstance(video, TddVideo)
        self.assertEqual((video.aid, video.title, video.added), (7, 'stored', 1767225600))
        self.cursor.execute.assert_called_once_with('SELECT * FROM tdd_video WHERE aid = %s', (7,))
        self.borrow.return_value.__exit__.assert_called_once()
        self.cursor.fetchone.return_value = None
        self.assertIsNone(queries.query_video_via_aid(7))

    def test_aid_query_error_propagates_unchanged_and_releases_connection(self):
        error = RuntimeError('query failed')
        self.cursor.execute.side_effect = error
        with self.assertRaises(RuntimeError) as caught:
            queries.query_video_via_aid(7)
        self.assertIs(caught.exception, error)
        self.borrow.return_value.__exit__.assert_called_once()

    def test_sprint_window_uses_timestamps_and_detached_tuples(self):
        self.connection.execute.return_value.fetchall.return_value = [(7, 123)]
        self.assertEqual(queries.query_sprint_records_in_window(100, 1900), [(7, 123)])
        bounds = self.connection.execute.call_args.args[1]
        self.assertEqual([int(value.timestamp()) for value in bounds], [100, 1900])
        self.borrow.return_value.__exit__.assert_called_once()

    def test_sprint_first_record_distinguishes_missing_row_from_null_view(self):
        self.connection.execute.return_value.fetchone.return_value = (7, None)
        self.assertEqual(queries.query_first_sprint_record(7), (7, None))
        self.connection.execute.return_value.fetchone.return_value = None
        self.assertIsNone(queries.query_first_sprint_record(7))
