"""
Tests for the ``runrecord.track`` context manager -- the shared run-record
lifecycle used by the summary scripts (12_/15_/17_/62_/71_).

``track`` runs the same open / attach-logs / close sequence 51_ drives by hand:
'succeeded' on a clean exit, 'failed' if the body raises, and a row left
'running' (for the query side to derive as 'stale') if the process is killed
before the block exits.

Run from the repo root:

    python tests/test_run_record_track.py
    # or
    python -m unittest discover -s tests
"""

import logging
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from runrecord import track  # noqa: E402
from runrecord._sqlite import sqlite3  # noqa: E402


class FakeStat:
    """Duck-types job.JobStat for add_job_stat_metrics (total_count + condition)."""

    def __init__(self, total_count, condition):
        self.total_count = total_count
        self.condition = condition


def _rows(path, query, params=()):
    conn = sqlite3.connect(path)
    try:
        return conn.execute(query, params).fetchall()
    finally:
        conn.close()


class TrackLifecycleTest(unittest.TestCase):
    def setUp(self):
        self.path = os.path.join(tempfile.mkdtemp(), 'db.sqlite3')

    def _status(self):
        return _rows(self.path, 'SELECT status, finished_at FROM run')[0]

    def test_clean_exit_records_succeeded(self):
        with track('15_update-video-info', db_path=self.path) as rec:
            self.assertTrue(rec.enabled)
        status, finished_at = self._status()
        self.assertEqual(status, 'succeeded')
        self.assertTrue(finished_at)

    def test_exception_records_failed_and_propagates(self):
        class Boom(RuntimeError):
            pass

        with self.assertRaises(Boom):
            with track('15_update-video-info', db_path=self.path):
                raise Boom('kaboom')
        status, finished_at = self._status()
        self.assertEqual(status, 'failed')
        self.assertTrue(finished_at)

    def test_exit_nonzero_records_failed_and_propagates(self):
        # 62_ calls exit(1) when the evocalrank fetch fails -- SystemExit(1),
        # which must still close the run as 'failed', not leave it 'running'.
        with self.assertRaises(SystemExit) as ctx:
            with track('62_add-evocalrank-video', db_path=self.path):
                exit(1)
        self.assertEqual(ctx.exception.code, 1)
        self.assertEqual(self._status()[0], 'failed')

    def test_clean_exit_zero_records_succeeded(self):
        with self.assertRaises(SystemExit):
            with track('s', db_path=self.path):
                sys.exit(0)
        self.assertEqual(self._status()[0], 'succeeded')

    def test_bare_exit_records_succeeded(self):
        with self.assertRaises(SystemExit):
            with track('s', db_path=self.path):
                sys.exit()  # code is None -> a clean exit
        self.assertEqual(self._status()[0], 'succeeded')

    def test_keyboard_interrupt_records_failed(self):
        with self.assertRaises(KeyboardInterrupt):
            with track('s', db_path=self.path):
                raise KeyboardInterrupt
        self.assertEqual(self._status()[0], 'failed')

    def test_metrics_and_logs_are_recorded_from_inside_the_block(self):
        probe = logging.getLogger('track_test_probe')
        probe.handlers = []
        log_path = os.path.join(os.path.dirname(self.path), 's_INFO.log')
        h = logging.FileHandler(log_path, encoding='utf-8')
        h.setLevel(logging.INFO)
        probe.addHandler(h)
        self.addCleanup(h.close)

        with track('17_add-member-follower-record', db_path=self.path) as rec:
            rec.attach_log_locations(source_logger=probe)
            rec.add_job_stat_metrics('follower-fetch', FakeStat(10, {'success': 9, 'exception': 1}))
            rec.add_job_stat_metrics('follower-db-writer', FakeStat(9, {'batch_insert': 1}))

        run_id = _rows(self.path, 'SELECT run_id FROM run')[0][0]
        metrics = {
            (scope, name): value
            for scope, name, value in _rows(
                self.path, 'SELECT scope, name, value FROM run_metric')
        }
        self.assertEqual(metrics[('follower-fetch', 'total_count')], 10.0)
        self.assertEqual(metrics[('follower-fetch', 'success')], 9.0)
        self.assertEqual(metrics[('follower-fetch', 'exception')], 1.0)
        self.assertEqual(metrics[('follower-db-writer', 'batch_insert')], 1.0)
        logs = _rows(self.path, 'SELECT level, path FROM run_log WHERE run_id = ?', (run_id,))
        self.assertEqual(logs, [('INFO', log_path)])


class TrackBestEffortTest(unittest.TestCase):
    def test_unopenable_db_yields_disabled_recorder_and_body_still_runs(self):
        # parent path is a regular file -> the database cannot be opened
        f = tempfile.NamedTemporaryFile(delete=False)
        f.close()
        bad_path = os.path.join(f.name, 'nope', 'db.sqlite3')

        ran = []
        with track('s', db_path=bad_path) as rec:
            self.assertFalse(rec.enabled)
            rec.add_job_stat_metrics('x', FakeStat(1, {'a': 1}))  # no-op
            ran.append(True)
        self.assertEqual(ran, [True])

    def test_disabled_recorder_does_not_swallow_a_body_exception(self):
        f = tempfile.NamedTemporaryFile(delete=False)
        f.close()
        bad_path = os.path.join(f.name, 'nope', 'db.sqlite3')

        with self.assertRaises(ValueError):
            with track('s', db_path=bad_path):
                raise ValueError('still propagates')


if __name__ == '__main__':
    unittest.main(verbosity=2)
