"""A failed named DB operation must not stop a worker's remaining items."""
from queue import Queue
import unittest
from unittest import mock

from core import VideoRecord
from job import AddVideoRecordJob, UpdateVideoJob
from service import CodeError, VideoView


class VideoJobWriteFailureTest(unittest.TestCase):
    def test_metadata_worker_continues_after_database_failure(self):
        aids = Queue()
        for aid in (7, 8, None):
            aids.put(aid)
        service = object()
        job = UpdateVideoJob('test', aids, service)
        with mock.patch('job.UpdateVideoJob.update_video',
                        side_effect=[RuntimeError('write failed'), []]) as update, \
                self.assertLogs(job.logger, level='ERROR'):
            job.process()
        self.assertEqual(update.call_args_list, [mock.call(7, service), mock.call(8, service)])
        self.assertEqual(job.stat.total_count, 2)
        self.assertEqual(job.stat.condition['update_exception'], 1)
        self.assertEqual(job.stat.condition['0_update'], 1)

    def test_record_worker_continues_after_code_error_refresh_write_failure(self):
        aids, records = Queue(), Queue()
        for aid in (7, 8):
            aids.put(aid)
        service = object()
        job = AddVideoRecordJob('test', aids, records, service)
        code_error = CodeError('get_video_view', VideoView, {'aid': 7}, {'code': -404}, -404)
        record = VideoRecord(123, 8, '1AbCdEfGhJ', 100, 1, 2, 3, 4, 5, 6)
        with mock.patch('job.AddVideoRecordJob.add_video_record_via_video_view',
                        side_effect=[code_error, record]), \
                mock.patch('job.AddVideoRecordJob.update_video',
                           side_effect=RuntimeError('write failed')) as update, \
                self.assertLogs(job.logger, level='ERROR'):
            job.process()
        update.assert_called_once_with(7, service)
        self.assertIs(records.get_nowait(), record)
        self.assertTrue(records.empty())
        self.assertEqual(job.stat.total_count, 2)
        self.assertEqual(job.stat.condition['update_exception'], 1)
        self.assertEqual(job.stat.condition['code_error'], 1)
        self.assertEqual(job.stat.condition['success'], 1)
