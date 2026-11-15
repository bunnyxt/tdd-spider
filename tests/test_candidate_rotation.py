"""Recent-N and weekday grouping use the given candidate order unchanged."""
from queue import Queue
import tempfile
import unittest
from unittest import mock

from job import JobStat
from service.apistat import NullApiStatTracker
from test_remaining_script_run_records import _load


class CandidateRotationTest(unittest.TestCase):
    def test_fixed_unsorted_video_and_member_candidates_keep_legacy_grouping(self):
        for filename,method,query,jobname,limit in (
            ('15_update-video-info.py','update_video_info','query_all_video_bvids','UpdateVideoJob',5000),
            ('16_update-member-info.py','update_member_info','query_all_member_mids','UpdateMemberJob',1000)):
            script = _load(filename)
            candidates = list(range(limit+28,0,-1))
            service = mock.Mock(stats=NullApiStatTracker())
            for weekday in range(7):
                selected = []
                def job(name,queue,service):
                    if not selected:
                        selected.extend(item for item in queue.queue if item is not None)
                    return object()
                pool = mock.Mock()
                pool.join.return_value = JobStat()
                with self.subTest(script=filename,weekday=weekday), tempfile.TemporaryDirectory() as directory, \
                     mock.patch.dict('os.environ',{'TDD_RUN_RECORD_DB':directory+'/runs.sqlite3'}), \
                     mock.patch.object(script,'Service',return_value=service), \
                     mock.patch.object(script,'Session',return_value=mock.Mock()), \
                     mock.patch.object(script.DBOperation,query,return_value=candidates), \
                     mock.patch.object(script,jobname,side_effect=job), \
                     mock.patch.object(script,'JobPool',return_value=pool), \
                     mock.patch.object(script,'get_week_day',return_value=weekday):
                    if filename.startswith('15'):
                        with mock.patch.object(script,'b2a',side_effect=lambda value:value):
                            getattr(script,method)()
                    else:
                        getattr(script,method)()
                expected = candidates[-limit:] + [value for index,value in enumerate(candidates[:-limit]) if index%7 == weekday]
                self.assertEqual(selected,expected)
