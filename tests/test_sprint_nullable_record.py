"""Nullable sprint statistics persist without a million-status transition."""
from queue import Queue
from types import SimpleNamespace
import unittest
from unittest import mock
from uuid import uuid4

from db import TddSprintVideoRecord
from job import AddSprintVideoRecordJob
from service.endpoints import get_video_view_trimmed
from task.task import apply_observation
from test_endpoint_get_video_view_trimmed import valid_response


class SprintNullableRecordTest(unittest.TestCase):
    def test_trimmed_parser_accepts_explicit_null_view(self):
        response = valid_response()
        response['data']['stat']['view'] = None
        parsed = get_video_view_trimmed.get(lambda *args,**kwargs:response)
        self.assertIsNone(parsed.stat.view)

    def test_nullable_record_is_appended_without_finished_update(self):
        record = TddSprintVideoRecord(added=123,aid=10,view=None,danmaku=None,reply=None,
                                     favorite=None,coin=None,share=None,like=None)
        session = mock.Mock()
        with mock.patch('task.task.append_objects') as append:
            self.assertIs(apply_observation('sprint-observe',{'record':record},session,uuid4()),record)
        append.assert_called_once()
        self.assertIsNone(append.call_args.args[1][0].view)
        session.execute.assert_not_called()

    def test_job_counts_confirmed_nullable_record_without_million_status(self):
        queue = Queue()
        queue.put(10)
        with mock.patch('job.AddSprintVideoRecordJob.Session',return_value=mock.Mock()):
            job = AddSprintVideoRecordJob('nullable',queue,mock.Mock())
        self.addCleanup(job.cleanup)
        with mock.patch('job.AddSprintVideoRecordJob.add_sprint_video_record_via_video_view',
                        return_value=SimpleNamespace(aid=10,view=None)):
            job.process()
        self.assertEqual(job.stat.condition['persisted'],1)
        self.assertEqual(job.stat.condition['success'],1)
        self.assertEqual(job.stat.condition['million_success'],0)
        self.assertEqual(job.stat.condition['exception'],0)
