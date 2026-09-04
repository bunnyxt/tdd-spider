import logging
from queue import Queue
from unittest import TestCase, mock

from job import (FetchMemberFollowerRecordJob, FetchVideoRecordJob, JobStat,
                 UpdateMemberJob)
from service import RateLimitError


class FakeSession:
    def rollback(self):
        raise AssertionError('rate limiting must not roll back the DB session')


class UpdateMemberJobRateLimitTest(TestCase):
    def test_rate_limit_waits_then_moves_to_next_member(self):
        mid_queue = Queue()
        mid_queue.put(123)
        mid_queue.put(456)
        mid_queue.put(None)

        job = object.__new__(UpdateMemberJob)
        job.mid_queue = mid_queue
        job.service = object()
        job.poll_timeout_s = 0.01
        job.session = FakeSession()
        job.stat = JobStat()
        job.logger = logging.getLogger('test.UpdateMemberJob')

        limited = RateLimitError(
            'get_member_card', 'code_-352', first_seen=95.0, retry_at=105.0)
        with mock.patch('job.UpdateMemberJob.update_member',
                        side_effect=[limited, []]) as update, \
                mock.patch('job.UpdateMemberJob.time.sleep') as sleep, \
                mock.patch('job.UpdateMemberJob.time.monotonic', return_value=100.0):
            job.process()

        self.assertEqual(update.call_args_list, [
            mock.call(123, job.service, job.session),
            mock.call(456, job.service, job.session),
        ])
        sleep.assert_called_once_with(60)
        self.assertEqual(job.stat.total_count, 2)
        self.assertEqual(job.stat.condition['0_update'], 1)
        self.assertEqual(job.stat.condition['rate_limited'], 1)
        self.assertEqual(job.stat.condition['update_exception'], 0)


class FetchJobRateLimitTest(TestCase):
    def test_video_fetch_requeues_aid_and_waits_until_retry(self):
        job = object.__new__(FetchVideoRecordJob)
        job.aid_queue = Queue()
        job.service = object()
        job.code_error_aid_queue = None
        job.duration_limit_due_ts_s = None
        job.stat = JobStat()
        job.logger = logging.getLogger('test.FetchVideoRecordJob')

        limited = RateLimitError(
            'get_video_view_trimmed', 'http_412',
            first_seen=95.0, retry_at=105.0)
        with mock.patch('job.FetchVideoRecordJob.fetch_video_record_via_video_view',
                        side_effect=limited), \
                mock.patch('job.FetchVideoRecordJob.time.monotonic', return_value=100.0), \
                mock.patch('job.FetchVideoRecordJob.time.sleep') as sleep:
            self.assertTrue(job._fetch_single(123))

        self.assertEqual(job.aid_queue.get_nowait(), 123)
        sleep.assert_called_once_with(5.0)
        self.assertEqual(job.stat.condition['rate_limited'], 1)

    def test_follower_fetch_requeues_mid_and_waits_until_retry(self):
        job = object.__new__(FetchMemberFollowerRecordJob)
        job.mid_queue = Queue()
        job.mid_queue.put(123)
        job.record_queue = Queue()
        job.service = object()
        job.duration_limit_s = None
        job.duration_limit_due_ts_s = None
        job.put_timeout_s = 0.01
        job.stat = JobStat()
        job.logger = logging.getLogger('test.FetchMemberFollowerRecordJob')

        limited = RateLimitError(
            'get_member_relation', 'http_412',
            first_seen=95.0, retry_at=105.0)
        with mock.patch('job.FetchMemberFollowerRecordJob.fetch_member_follower_record',
                        side_effect=[limited, RuntimeError('stop')]) as fetch, \
                mock.patch('job.FetchMemberFollowerRecordJob.time.monotonic',
                           return_value=100.0), \
                mock.patch('job.FetchMemberFollowerRecordJob.time.sleep') as sleep:
            job.process()

        self.assertEqual(fetch.call_count, 2)
        sleep.assert_called_once_with(5.0)
        self.assertEqual(job.stat.condition['rate_limited'], 1)
