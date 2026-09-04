import logging
from queue import Queue
from unittest import TestCase, mock

from job import JobStat, UpdateMemberJob
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
                mock.patch('job.UpdateMemberJob.time.sleep') as sleep:
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
