"""Sprint collection distinguishes fetch failures from unconfirmed database writes."""
import os
from queue import Queue
from types import SimpleNamespace
import unittest
from unittest import mock

from core import VideoRecord
from job import AddSprintVideoRecordJob


class SprintRecordJobTest(unittest.TestCase):
    def test_fetch_and_write_failures_are_separate_and_processing_continues(self):
        queue = Queue()
        for aid in (1, 2, 3):
            queue.put(aid)
        job = AddSprintVideoRecordJob('stage-failures', queue, mock.Mock())
        fetched = [VideoRecord(123, aid, '1AbCdEfGhJ', 100, 1, 2, 3, 4, 5, 6)
                   for aid in (2, 3)]
        with mock.patch('job.AddSprintVideoRecordJob.fetch_video_record_via_video_view',
                        side_effect=[RuntimeError('HTTP unavailable'), *fetched]) as fetch, \
                mock.patch('job.AddSprintVideoRecordJob.commit_sprint_video_record',
                           side_effect=[RuntimeError('COMMIT unknown'),
                                        SimpleNamespace(aid=3, view=1000000)]) as commit, \
                mock.patch.dict(os.environ, {'TDD_FAILURE_DIR': '/tmp/test-failed-writes'}), \
                self.assertLogs(job.logger, level='ERROR') as logs:
            job.process()
        self.assertEqual(fetch.call_count, 3)
        self.assertEqual(commit.call_args_list, [mock.call(record) for record in fetched])
        self.assertEqual(job.stat.total_count, 3)
        for metric, expected in {'fetch_success': 2, 'fetch_failed': 1, 'db_write_failed': 1,
                                 'persisted': 1, 'success': 1, 'exception': 2,
                                 'million_success': 1}.items():
            self.assertEqual(job.stat.condition[metric], expected, metric)
        self.assertIn('aid: 1', logs.output[0])
        self.assertIn('No failed-write file', logs.output[0])
        self.assertIn('re-fetch this aid', logs.output[0])
        self.assertIn('aid: 2', logs.output[1])
        self.assertIn('/tmp/test-failed-writes', logs.output[1])
        self.assertIn('best-effort', logs.output[1])
        self.assertIn('Reconcile the database', logs.output[1])
        self.assertTrue(queue.empty())


if __name__ == '__main__':
    unittest.main()
