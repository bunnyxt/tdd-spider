import logging
from queue import Queue
from unittest import TestCase, mock

from job import JobStat, UpdateMemberJob
from service import ContentError, MemberCard


class FakeSession:
    def __init__(self):
        self.rollback_count = 0

    def rollback(self):
        self.rollback_count += 1


class UpdateMemberJobEmptyCardTest(TestCase):
    def test_rejected_card_is_counted_apart_and_the_run_continues(self):
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

        blank = ContentError(
            'get_member_card', MemberCard, {'mid': 123},
            {'code': 0, 'message': '0', 'ttl': 1,
             'data': {'card': {'mid': '', 'name': '', 'sex': '',
                               'face': '', 'sign': ''}}},
            'Response data card name should not be empty.')
        with mock.patch('job.UpdateMemberJob.update_member',
                        side_effect=[blank, []]) as update:
            job.process()

        self.assertEqual(update.call_args_list, [
            mock.call(123, job.service, job.session),
            mock.call(456, job.service, job.session),
        ])
        self.assertEqual(job.stat.total_count, 2)
        self.assertEqual(job.stat.condition['empty_card'], 1)
        self.assertEqual(job.stat.condition['0_update'], 1)
        self.assertEqual(job.stat.condition['update_exception'], 0)
        # nothing was written, so there is no poisoned transaction to undo
        self.assertEqual(job.session.rollback_count, 0)
