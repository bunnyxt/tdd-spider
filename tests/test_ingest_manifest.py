"""Trial manifests preserve complete keys without a receipt or natural-key dedup."""
from datetime import datetime, timezone
import unittest
from unittest import mock
from uuid import uuid4

from psycopg.pq import TransactionStatus

from db.ingest import append_rows

COLUMNS = ('added', 'aid', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'like')
ADDED = datetime(2026, 1, 1, tzinfo=timezone.utc)
ROW = (ADDED, 7, 100, 0, 0, 0, 0, 0, 0)


def connection(keys):
    conn = mock.MagicMock()
    conn.info.transaction_status = TransactionStatus.INTRANS
    conn.execute.return_value.fetchone.return_value = ('read committed',)
    cursors = []
    for returned_keys in keys:
        cursor = mock.MagicMock()
        cursor.__enter__.return_value = cursor
        cursor.fetchone.side_effect = returned_keys
        cursor.nextset.return_value = True
        cursors.append(cursor)
    conn.cursor.side_effect = cursors
    return conn, cursors


class TrialManifestTest(unittest.TestCase):
    def test_same_second_rows_get_distinct_complete_keys_and_typed_manifest(self):
        expected = [(7, ADDED, 10), (7, ADDED, 11)]
        conn, cursors = connection([expected, []])
        run_id = uuid4()
        self.assertEqual(append_rows(conn, 'tdd_video_record', COLUMNS, [ROW, ROW], run_id=run_id), expected)
        cursors[0].executemany.assert_called_once()
        self.assertEqual(cursors[0].executemany.call_args.args[1], [ROW, ROW])
        self.assertTrue(cursors[0].executemany.call_args.kwargs['returning'])
        manifest_rows = cursors[1].executemany.call_args.args[1]
        self.assertEqual([row[1] for row in manifest_rows], [0, 1])
        self.assertEqual({row[0] for row in manifest_rows}, {manifest_rows[0][0]})
        self.assertTrue(all(row[2:4] == (run_id, 'tdd_video_record') for row in manifest_rows))
        self.assertEqual([row[4].obj for row in manifest_rows], [
            {'aid': 7, 'added': {'__tdd_type__': 'datetime', 'value': ADDED.isoformat()}, 'id': 10},
            {'aid': 7, 'added': {'__tdd_type__': 'datetime', 'value': ADDED.isoformat()}, 'id': 11}])
        self.assertEqual([call.args[0] for call in conn.execute.call_args_list], ['SHOW transaction_isolation'])

    def test_each_append_call_gets_independent_trial_group(self):
        conn, cursors = connection([[(7, ADDED, 10)], [], [(7, ADDED, 11)], []])
        for _ in range(2):
            append_rows(conn, 'tdd_video_record', COLUMNS, [ROW], run_id=uuid4())
        groups = [cursors[i].executemany.call_args.args[1][0][0] for i in (1, 3)]
        self.assertNotEqual(*groups)
        self.assertEqual([cursors[i].executemany.call_args.args[1][0][1] for i in (1, 3)], [0, 0])

    def test_formal_append_creates_no_manifest_group_or_receipt(self):
        conn, cursors = connection([[(7, ADDED, 10)]])
        with mock.patch('db.ingest.uuid4', side_effect=AssertionError('formal mode needs no batch UUID')):
            self.assertEqual(append_rows(conn, 'tdd_video_record', COLUMNS, [ROW]), [(7, ADDED, 10)])
        conn.cursor.assert_called_once_with()
        self.assertEqual(cursors[0].executemany.call_count, 1)
        self.assertEqual(conn.execute.call_count, 1)

    def test_manifest_error_propagates_to_callers_transaction(self):
        conn, cursors = connection([[(7, ADDED, 10)], []])
        cursors[1].executemany.side_effect = RuntimeError('manifest failed')
        with self.assertRaisesRegex(RuntimeError, 'manifest failed'):
            append_rows(conn, 'tdd_video_record', COLUMNS, [ROW], run_id=uuid4())
        conn.commit.assert_not_called()

    def test_incomplete_returning_result_rejects_batch(self):
        conn, cursors = connection([[None]])
        with self.assertRaisesRegex(RuntimeError, 'complete key'):
            append_rows(conn, 'tdd_video_record', COLUMNS, [ROW])
        conn.commit.assert_not_called()

    def test_explicit_identity_and_unknown_targets_are_rejected_before_sql(self):
        conn, _ = connection([])
        for table, columns, rows in (
                ('tdd_video', COLUMNS, [ROW]),
                ('tdd_video_record', COLUMNS + ('id',), [ROW + (1,)]),
                ('tdd_video_record', COLUMNS, [ROW[:-1]])):
            with self.subTest(table=table, columns=columns), self.assertRaises(ValueError):
                append_rows(conn, table, columns, rows)
        conn.execute.assert_not_called()
