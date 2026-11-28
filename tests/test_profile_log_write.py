"""Profile logs bind immutable inputs and return actual stored row snapshots."""
import os
import unittest
from unittest import mock
from uuid import uuid4

from core import NewMemberLog, NewVideoLog
from db import TddMemberLog, TddVideoLog
from db.values import timestamp
from db.write import append_member_logs, append_video_logs
from test_ingest_manifest import connection


CASES = (
    (NewVideoLog, TddVideoLog, append_video_logs, 'aid'),
    (NewMemberLog, TddMemberLog, append_member_logs, 'mid'),
)


class ProfileLogWriteTest(unittest.TestCase):
    def setUp(self):
        environment = mock.patch.dict(os.environ, {'TDD_TRIAL_RUN_ID': ''})
        environment.start()
        self.addCleanup(environment.stop)

    def test_returns_stored_values_without_mutating_inputs(self):
        for input_type, row_type, append, key in CASES:
            with self.subTest(table=row_type.table):
                inputs = [input_type(20, 7, 'ti\x00tle', None, ''),
                          input_type(20, 7, 'sign', 'old\x00', 'literal\\u0000')]
                before = list(inputs)
                # Deliberately different returned fields catch implementations
                # that merely attach an identity to the original inputs.
                returned = [(90, timestamp(21), 8, 'stored', '', None),
                            (91, timestamp(22), 9, 'stored sign', None, 'saved')]
                conn, cursors = connection([returned])
                stored = append(conn, inputs)
                sql, bound = cursors[0].executemany.call_args.args
                self.assertIn('RETURNING "id","added","' + key + '","attr","oldval","newval"',
                              sql.as_string())
                self.assertEqual(bound, [(timestamp(20), 7, 'title', None, ''),
                                         (timestamp(20), 7, 'sign', 'old', 'literal\\u0000')])
                self.assertTrue(all(isinstance(row, row_type) for row in stored))
                self.assertEqual([tuple(getattr(row, field) for field in row_type.fields) for row in stored],
                                 [(90, 21, 8, 'stored', '', None),
                                  (91, 22, 9, 'stored sign', None, 'saved')])
                self.assertEqual(inputs, before)
                self.assertTrue(all(not hasattr(item, 'id') for item in inputs))
                with self.assertRaises(AttributeError):
                    inputs[0].added = 99
                conn.commit.assert_not_called()

    def test_trial_manifest_uses_returned_id_only(self):
        run_id = uuid4()
        os.environ['TDD_TRIAL_RUN_ID'] = str(run_id)
        for input_type, row_type, append, _ in CASES:
            with self.subTest(table=row_type.table):
                conn, cursors = connection([[(90, timestamp(21), 7, 'name', None, '')], []])
                stored = append(conn, [input_type(20, 7, 'name', None, '')])
                manifest = cursors[1].executemany.call_args.args[1]
                self.assertEqual(len(manifest), 1)
                self.assertEqual(manifest[0][1:4], (0, run_id, row_type.table))
                self.assertEqual(manifest[0][4].obj, {'id': stored[0].id})
                conn.commit.assert_not_called()

    def test_incomplete_returning_and_manifest_failures_propagate(self):
        for input_type, row_type, append, _ in CASES:
            with self.subTest(table=row_type.table, failure='returning'):
                conn, _ = connection([[None]])
                with self.assertRaisesRegex(RuntimeError, 'complete key'):
                    append(conn, [input_type(20, 7, 'name')])
                conn.commit.assert_not_called()
            with self.subTest(table=row_type.table, failure='manifest'), \
                    mock.patch.dict(os.environ, {'TDD_TRIAL_RUN_ID': str(uuid4())}):
                conn, cursors = connection([[(90, timestamp(21), 7, 'name', None, None)], []])
                cursors[1].executemany.side_effect = RuntimeError('manifest failed')
                with self.assertRaisesRegex(RuntimeError, 'manifest failed'):
                    append(conn, [input_type(20, 7, 'name')])
                conn.commit.assert_not_called()

    def test_empty_input_does_not_write(self):
        for _, row_type, append, _ in CASES:
            with self.subTest(table=row_type.table):
                conn = mock.Mock()
                self.assertEqual(append(conn, []), [])
                self.assertEqual(conn.mock_calls, [])
