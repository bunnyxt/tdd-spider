"""Staff creation binds input fields and returns the stored row snapshot."""
import unittest
from unittest import mock

from db import TddVideoStaff
from db.profiles import _insert_staff
from db.values import timestamp
from profile_support import ProfileConnection
from util import a2b


class StaffInsertSnapshotTest(unittest.TestCase):
    def test_snapshot_comes_from_full_returning_row(self):
        conn = mock.Mock()
        conn.execute.return_value.fetchone.return_value = (
            90, timestamp(21), 7, a2b(7), 8, 'stored title')
        stored = _insert_staff(conn,20,7,8,'pro\x00ducer')
        sql, params = conn.execute.call_args.args
        self.assertIn('RETURNING id,added,aid,bvid,mid,title',sql)
        self.assertEqual(params,(timestamp(20),7,a2b(7),8,'producer'))
        self.assertIsInstance(stored,TddVideoStaff)
        self.assertEqual((stored.id,stored.added,stored.aid,stored.bvid,stored.mid,stored.title),
                         (90,21,7,a2b(7),8,'stored title'))

    def test_conflict_keeps_original_relationship_and_returns_none(self):
        conn = ProfileConnection()
        self.addCleanup(conn.close)
        inserted = _insert_staff(conn,20,7,8,'pro\x00ducer')
        self.assertEqual((inserted.added,inserted.bvid,inserted.title),(20,a2b(7),'producer'))
        self.assertIsNone(_insert_staff(conn,21,7,8,'ignored'))
        self.assertEqual(conn.execute('SELECT id,added,bvid,title FROM tdd_video_staff').fetchall(),
                         [(inserted.id,20,a2b(7),'producer')])
