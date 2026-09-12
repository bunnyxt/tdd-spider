import logging
import math
import time
from collections import Counter
from unittest import TestCase, mock

from job import JobPool, JobStat

_real_time = time.time


def scripted_time(values):
    # patching the shared `time` module's `time()` also affects stdlib
    # logging (it stamps each LogRecord via time.time()); serve the scripted
    # values to JobPool's own calls and fall back to the real clock for any
    # extra calls logging makes in between.
    it = iter(values)

    def _time():
        return next(it, None) or _real_time()
    return _time


class FakeJob:
    def __init__(self):
        self.stat = JobStat()


class FormatEtaTest(TestCase):
    def test_positive_rate_yields_duration_and_absolute_time(self):
        now = 1_700_000_000.0
        eta = JobPool._format_eta(left=100, rate=10, now=now)

        expected_time = time.strftime('%H:%M:%S', time.localtime(now + 10))
        self.assertEqual(eta, f'ETA 10s ({expected_time})')

    def test_no_work_left_suppresses_eta(self):
        self.assertEqual(JobPool._format_eta(left=0, rate=10, now=0.0), '')
        self.assertEqual(JobPool._format_eta(left=-5, rate=10, now=0.0), '')

    def test_non_positive_rate_suppresses_eta(self):
        self.assertEqual(JobPool._format_eta(left=100, rate=0, now=0.0), '')
        self.assertEqual(JobPool._format_eta(left=100, rate=-1, now=0.0), '')

    def test_non_finite_rate_suppresses_eta(self):
        self.assertEqual(JobPool._format_eta(left=100, rate=math.inf, now=0.0), '')
        self.assertEqual(JobPool._format_eta(left=100, rate=math.nan, now=0.0), '')


class FmtDurationTest(TestCase):
    def test_seconds_minutes_and_hours(self):
        self.assertEqual(JobPool._fmt_duration(9), '9s')
        self.assertEqual(JobPool._fmt_duration(72), '1m12s')
        self.assertEqual(JobPool._fmt_duration(3725), '1h02m')


class ReportProgressLineTest(TestCase):
    def make_pool(self, jobs, **kwargs):
        pool = object.__new__(JobPool)
        pool.jobs = jobs
        pool.progress_label = 'test'
        pool.progress_interval_s = 1.0
        pool.progress_show_conditions = False
        pool.ensure_conditions = []
        pool.logger = logging.getLogger('JobPool')
        for key, value in kwargs.items():
            setattr(pool, key, value)
        return pool

    def test_positive_progress_appends_eta_after_stable_prefix(self):
        job = FakeJob()
        job.stat.total_count = 20
        pool = self.make_pool([job], progress_total=100)
        pool._stop_event = mock.Mock(wait=mock.Mock(return_value=True))

        with mock.patch('job.JobPool.time.time', side_effect=scripted_time([1000.0, 1010.0])), \
                self.assertLogs('JobPool', level=logging.INFO) as logs:
            pool._report_progress()

        self.assertEqual(len(logs.output), 1)
        line = logs.output[0]
        self.assertIn('PROGRESS test: 20 done, 80 left (20.0%), 2/s', line)
        expected_time = time.strftime('%H:%M:%S', time.localtime(1010.0 + 40))
        self.assertIn(f', ETA 40s ({expected_time})', line)

    def test_unknown_total_never_shows_eta(self):
        job = FakeJob()
        job.stat.total_count = 20
        pool = self.make_pool([job], progress_total=None)
        pool._stop_event = mock.Mock(wait=mock.Mock(return_value=True))

        with mock.patch('job.JobPool.time.time', side_effect=scripted_time([1000.0, 1010.0])), \
                self.assertLogs('JobPool', level=logging.INFO) as logs:
            pool._report_progress()

        line = logs.output[0]
        self.assertEqual(line, 'INFO:JobPool:PROGRESS test: 20 done, 2/s')
        self.assertNotIn('ETA', line)

    def test_zero_progress_in_interval_suppresses_eta(self):
        job = FakeJob()
        job.stat.total_count = 20
        pool = self.make_pool([job], progress_total=100)
        # one tick makes progress (last_done starts at 0), a second tick with
        # total_count unchanged then has zero progress in that interval
        pool._stop_event = mock.Mock(wait=mock.Mock(side_effect=[False, True]))

        with mock.patch('job.JobPool.time.time', side_effect=scripted_time([1000.0, 1010.0, 1020.0])), \
                self.assertLogs('JobPool', level=logging.INFO) as logs:
            pool._report_progress()

        self.assertEqual(len(logs.output), 2)
        self.assertIn('ETA', logs.output[0])
        self.assertNotIn('ETA', logs.output[1])

    def test_final_drain_line_keeps_condition_summary_after_eta(self):
        job = FakeJob()
        job.stat.total_count = 20
        job.stat.condition = Counter({'ok': 20})
        pool = self.make_pool([job], progress_total=100, progress_show_conditions=True)
        pool._stop_event = mock.Mock(wait=mock.Mock(return_value=True))

        with mock.patch('job.JobPool.time.time', side_effect=scripted_time([1000.0, 1010.0])), \
                self.assertLogs('JobPool', level=logging.INFO) as logs:
            pool._report_progress()

        line = logs.output[0]
        self.assertIn('PROGRESS test: 20 done, 80 left (20.0%), 2/s, ETA 40s', line)
        self.assertIn('| ok: 20', line)
