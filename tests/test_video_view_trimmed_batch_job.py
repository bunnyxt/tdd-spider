"""
Tests for FetchVideoRecordJob's BATCH mode (batch assembly, per-item retry,
kill-switch, single-path fallback) and the conf switch behind it.

The Service is a scripted mock; the DB layer is stubbed out at import time
(fetch workers never touch the DB anyway -- the stub just satisfies the
``job`` package's sibling imports). Wall-clock colddown sleeps inside the job
module are patched to keep the suite fast.

Run from the repo root:

    python -m unittest discover -s tests
"""

import importlib.util
import os
import sys
import types
import unittest
import unittest.mock
from queue import Queue

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

# the `job` package imports DB-backed siblings (UpdateVideoJob etc.) whose
# `db` import needs a conf.ini; none of that is exercised here, so satisfy
# the imports with placeholder attributes when the real db is not loadable
if 'db' not in sys.modules:
    _fake_db = types.ModuleType('db')
    _fake_db.__getattr__ = lambda name: type(name, (), {})
    sys.modules['db'] = _fake_db

def _load_real_conf_module():
    """
    Load conf/conf.py by file path, bypassing sys.modules: other test modules
    (the run-record script tests) install a stub ``conf`` for the DB-backed
    imports, and this module must test the REAL getter regardless of import
    order. conf/conf.py is stdlib-only, so this always works.
    """
    spec = importlib.util.spec_from_file_location(
        'tdd_real_conf_conf', os.path.join(ROOT, 'conf', 'conf.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


try:
    from service import (  # noqa: E402
        ResponseError, CodeError, MisalignmentError,
        VideoViewTrimmed, VideoViewStat, VideoViewTrimmedBatchItem)
    from job import (  # noqa: E402
        FetchVideoRecordJob, VideoViewTrimmedBatchConfig, VideoViewTrimmedBatchState)
    from task import build_video_record_via_video_view  # noqa: E402
    from util import a2b  # noqa: E402
except ImportError as e:  # pragma: no cover -- e.g. requests not installed
    raise unittest.SkipTest(f'job dependencies unavailable: {e}')

conf_module = _load_real_conf_module()
get_video_view_trimmed_batch_conf = conf_module.get_video_view_trimmed_batch_conf

import configparser  # noqa: E402


def make_view(aid, view=100):
    return VideoViewTrimmed(
        bvid='BV' + a2b(aid), aid=aid,
        stat=VideoViewStat(aid=aid, view=view, danmaku=1, reply=2, favorite=3,
                           coin=4, share=5, now_rank=0, his_rank=0, like=6,
                           dislike=None, vt=None, vv=None))


def ok_item(aid):
    return VideoViewTrimmedBatchItem(aid=aid, view=make_view(aid), error=None)


def code_error_item(aid, code=-404):
    return VideoViewTrimmedBatchItem(
        aid=aid, view=None,
        error=CodeError('video_view_trimmed', {'aid': aid}, {'code': code}, code))


def retryable_item(aid):
    return VideoViewTrimmedBatchItem(
        aid=aid, view=None,
        error=ResponseError('video_view_trimmed_batch_item',
                            {'aid': aid, 'kind': 'item_timeout'}))


class ScriptedService:
    """
    Scripted stand-in for Service. `batch_script` entries are consumed one per
    get_video_view_trimmed_batch call: an Exception instance is raised, a
    callable is called with the aids, anything else is returned as-is. When
    the script runs dry, every item comes back ok. The single path
    (get_video_view_trimmed) always succeeds unless `single_error` is set.
    """

    def __init__(self, batch_script=None, single_error=None):
        self.batch_script = list(batch_script or [])
        self.single_error = single_error
        self.batch_calls = []
        self.single_calls = []

    def get_video_view_trimmed_batch(self, aids, **kwargs):
        self.batch_calls.append(list(aids))
        if self.batch_script:
            entry = self.batch_script.pop(0)
            if isinstance(entry, Exception):
                raise entry
            if callable(entry):
                return entry(aids)
            return entry
        return [ok_item(aid) for aid in aids]

    def get_video_view_trimmed(self, params, **kwargs):
        aid = params['aid']
        self.single_calls.append(aid)
        if self.single_error is not None:
            raise self.single_error
        return make_view(aid)


class NoBatchService(ScriptedService):
    """A service on which any batch call is a test failure."""

    def get_video_view_trimmed_batch(self, aids, **kwargs):
        raise AssertionError('batch path must not be used')


def run_job(service, aids, batch_config, batch_state=None,
            code_error_queue=True):
    """Build a job over a prefilled aid queue, run its loop synchronously
    (no thread), return (job, records, code_error_aids)."""
    aid_queue = Queue()
    for aid in aids:
        aid_queue.put(aid)
    record_queue = Queue()
    code_error_aid_queue = Queue() if code_error_queue else None
    job = FetchVideoRecordJob(
        'job_test', aid_queue, record_queue, service,
        code_error_aid_queue=code_error_aid_queue,
        batch_config=batch_config, batch_state=batch_state)
    job.logger.disabled = True  # the failure paths log loudly on purpose

    # keep the retry colddowns out of the test wall clock (time.perf_counter
    # must stay real -- the job measures batch latency with it)
    import time as real_time
    fake_time = types.SimpleNamespace(
        sleep=lambda s: None, perf_counter=real_time.perf_counter)
    with unittest.mock.patch('job.FetchVideoRecordJob.time', fake_time):
        job.process()

    records = []
    while not record_queue.empty():
        records.append(record_queue.get())
    code_error_aids = []
    if code_error_aid_queue is not None:
        while not code_error_aid_queue.empty():
            code_error_aids.append(code_error_aid_queue.get())
    return job, records, code_error_aids


ALWAYS = VideoViewTrimmedBatchConfig(batch_size=2, batch_fraction=1.0)


class TestBatchHappyPath(unittest.TestCase):

    def test_records_flow_through_batches(self):
        service = ScriptedService()
        aids = [101, 102, 103, 104]
        job, records, _ = run_job(service, aids, ALWAYS, VideoViewTrimmedBatchState())

        self.assertEqual(service.batch_calls, [[101, 102], [103, 104]])
        self.assertEqual(service.single_calls, [])
        self.assertEqual(sorted(r.aid for r in records), aids)
        # RecordNew comes out of the SAME mapping as the single path
        self.assertEqual(records[0].bvid, a2b(records[0].aid))
        self.assertEqual(job.stat.condition['success'], 4)
        self.assertEqual(job.stat.condition['batch_request'], 2)
        self.assertEqual(job.stat.total_count, 4)

    def test_partial_batch_flushes_when_queue_drains(self):
        service = ScriptedService()
        job, records, _ = run_job(
            service, [101, 102, 103], ALWAYS, VideoViewTrimmedBatchState())

        self.assertEqual(service.batch_calls, [[101, 102], [103]])
        self.assertEqual(len(records), 3)

    def test_code_error_routes_to_update_queue(self):
        service = ScriptedService(batch_script=[
            lambda aids: [ok_item(aids[0]), code_error_item(aids[1])]])
        job, records, code_error_aids = run_job(
            service, [101, 102], ALWAYS, VideoViewTrimmedBatchState())

        self.assertEqual([r.aid for r in records], [101])
        self.assertEqual(code_error_aids, [102])
        self.assertEqual(job.stat.condition['code_error'], 1)
        self.assertEqual(job.stat.condition['success'], 1)
        self.assertEqual(job.stat.total_count, 2)

    def test_duplicate_aid_in_buffer_takes_single_path(self):
        service = ScriptedService()
        job, records, _ = run_job(
            service, [101, 101, 102], ALWAYS, VideoViewTrimmedBatchState())

        # never the same token twice in one batch: the second 101 goes single
        self.assertEqual(service.batch_calls, [[101, 102]])
        self.assertEqual(service.single_calls, [101])
        self.assertEqual(job.stat.condition['batch_duplicate_single_path'], 1)
        self.assertEqual(len(records), 3)


class TestPerItemRetry(unittest.TestCase):

    def test_only_failed_items_are_retried(self):
        service = ScriptedService(batch_script=[
            lambda aids: [ok_item(aids[0]), retryable_item(aids[1])]])
        job, records, _ = run_job(
            service, [101, 102], ALWAYS, VideoViewTrimmedBatchState())

        # retry batch contains ONLY the failed item
        self.assertEqual(service.batch_calls, [[101, 102], [102]])
        self.assertEqual(sorted(r.aid for r in records), [101, 102])
        self.assertEqual(job.stat.condition['batch_item_retry'], 1)
        self.assertEqual(job.stat.condition['success'], 2)

    def test_attempts_exhausted_becomes_other_exception(self):
        service = ScriptedService(batch_script=[
            lambda aids: [retryable_item(aids[0])]] * 3)
        job, records, _ = run_job(
            service, [101], ALWAYS, VideoViewTrimmedBatchState())

        # max_attempts=3 total tries, aligned with the single path's retry=3
        self.assertEqual(service.batch_calls, [[101], [101], [101]])
        self.assertEqual(records, [])
        self.assertEqual(service.single_calls, [])  # exhausted != fallback
        self.assertEqual(job.stat.condition['other_exception'], 1)
        self.assertEqual(job.stat.total_count, 1)


class TestWholeBatchFailureAndKillSwitch(unittest.TestCase):

    def test_whole_batch_failure_charges_every_aid_one_attempt(self):
        service = ScriptedService(batch_script=[
            ResponseError('video_view_trimmed_batch', {})])
        state = VideoViewTrimmedBatchState()
        job, records, _ = run_job(service, [101, 102], ALWAYS, state)

        # failure, then a successful retry batch with BOTH aids
        self.assertEqual(service.batch_calls, [[101, 102], [101, 102]])
        self.assertEqual(sorted(r.aid for r in records), [101, 102])
        self.assertEqual(job.stat.condition['batch_whole_failure'], 1)
        self.assertTrue(state.is_enabled())  # one failure does not trip

    def test_three_consecutive_whole_batch_failures_trip_the_kill_switch(self):
        service = ScriptedService(batch_script=[
            ResponseError('video_view_trimmed_batch', {})] * 3)
        state = VideoViewTrimmedBatchState()
        job, records, _ = run_job(service, [101, 102], ALWAYS, state)

        self.assertEqual(job.stat.condition['batch_whole_failure'], 3)
        self.assertFalse(state.is_enabled())
        # the batch's aids came back over the single path -- no data lost
        self.assertEqual(sorted(service.single_calls), [101, 102])
        self.assertEqual(sorted(r.aid for r in records), [101, 102])
        self.assertEqual(job.stat.condition['batch_fallback_single'], 2)

    def test_unexpected_exception_is_a_whole_batch_failure_not_a_dead_thread(self):
        # a bug-shaped exception must degrade like a transport failure, never
        # kill the worker thread mid-run
        service = ScriptedService(batch_script=[ValueError('boom')])
        state = VideoViewTrimmedBatchState()
        job, records, _ = run_job(service, [101, 102], ALWAYS, state)

        self.assertEqual(job.stat.condition['batch_whole_failure'], 1)
        self.assertEqual(sorted(r.aid for r in records), [101, 102])

    def test_batch_success_resets_the_consecutive_counter(self):
        state = VideoViewTrimmedBatchState()
        state.record_whole_batch_failure()
        state.record_whole_batch_failure()
        state.record_batch_success()
        self.assertFalse(state.record_whole_batch_failure())  # back to 1
        self.assertTrue(state.is_enabled())

    def test_misalignment_trips_immediately_and_falls_back(self):
        service = ScriptedService(batch_script=[
            MisalignmentError('video_view_trimmed_batch', {}, {}, 'aid echo mismatch')])
        state = VideoViewTrimmedBatchState()
        job, records, _ = run_job(service, [101, 102, 103, 104], ALWAYS, state)

        # exactly ONE batch call; its aids and every later aid go single path
        self.assertEqual(service.batch_calls, [[101, 102]])
        self.assertFalse(state.is_enabled())
        self.assertEqual(sorted(service.single_calls), [101, 102, 103, 104])
        self.assertEqual(sorted(r.aid for r in records), [101, 102, 103, 104])
        self.assertEqual(job.stat.condition['batch_misalignment'], 1)
        self.assertEqual(job.stat.condition['batch_fallback_single'], 2)

    def test_kill_switch_is_shared_across_jobs(self):
        # a state tripped by one job disables the path for its siblings
        state = VideoViewTrimmedBatchState()
        self.assertTrue(state.trip('misalignment seen by job_0'))
        self.assertFalse(state.trip('job_1 saw it too'))  # only first trips

        service = NoBatchService()
        job, records, _ = run_job(service, [101, 102], ALWAYS, state)
        self.assertEqual(sorted(r.aid for r in records), [101, 102])
        self.assertEqual(service.single_calls, [101, 102])


class TestBatchPathOff(unittest.TestCase):

    def test_no_config_means_single_path_only(self):
        service = NoBatchService()
        job, records, _ = run_job(service, [101, 102], None, None)

        self.assertEqual(service.single_calls, [101, 102])
        self.assertEqual(sorted(r.aid for r in records), [101, 102])
        self.assertEqual(job.stat.condition['success'], 2)
        # no batch counters ever appear when the path is off
        self.assertNotIn('batch_request', job.stat.condition)

    def test_fraction_zero_never_batches(self):
        service = NoBatchService()
        config = VideoViewTrimmedBatchConfig(batch_size=2, batch_fraction=0.0)
        job, records, _ = run_job(
            service, [101, 102], config, VideoViewTrimmedBatchState())

        self.assertEqual(service.single_calls, [101, 102])
        self.assertEqual(len(records), 2)


class TestRecordMapping(unittest.TestCase):

    def test_hidden_view_count_maps_to_minus_one(self):
        record = build_video_record_via_video_view(101, make_view(101, view='--'))
        self.assertEqual(record.view, -1)
        self.assertEqual(record.bvid, a2b(101))  # BV prefix stripped


class TestConf(unittest.TestCase):

    def _with_config(self, ini_text):
        parser = configparser.ConfigParser()
        parser.read_string(ini_text)
        original = conf_module.CONFIG
        conf_module.CONFIG = parser
        self.addCleanup(lambda: setattr(conf_module, 'CONFIG', original))

    def test_missing_section_is_disabled(self):
        self._with_config('[db_mysql]\nuser = u\n')
        self.assertEqual(get_video_view_trimmed_batch_conf(), (0, 0.0))

    def test_enabled_values_pass_through(self):
        self._with_config(
            '[video_view_trimmed_batch]\nbatch_size = 10\nbatch_fraction = 0.05\n')
        self.assertEqual(get_video_view_trimmed_batch_conf(), (10, 0.05))

    def test_zero_size_or_fraction_is_disabled(self):
        self._with_config(
            '[video_view_trimmed_batch]\nbatch_size = 0\nbatch_fraction = 1\n')
        self.assertEqual(get_video_view_trimmed_batch_conf(), (0, 0.0))
        self._with_config(
            '[video_view_trimmed_batch]\nbatch_size = 10\nbatch_fraction = 0\n')
        self.assertEqual(get_video_view_trimmed_batch_conf(), (0, 0.0))

    def test_negative_values_are_disabled(self):
        self._with_config(
            '[video_view_trimmed_batch]\nbatch_size = -5\nbatch_fraction = 0.5\n')
        self.assertEqual(get_video_view_trimmed_batch_conf(), (0, 0.0))

    def test_size_is_capped_at_worker_max(self):
        self._with_config(
            '[video_view_trimmed_batch]\nbatch_size = 100\nbatch_fraction = 1\n')
        self.assertEqual(get_video_view_trimmed_batch_conf(), (50, 1.0))

    def test_fraction_is_clamped_to_one(self):
        self._with_config(
            '[video_view_trimmed_batch]\nbatch_size = 10\nbatch_fraction = 1.5\n')
        self.assertEqual(get_video_view_trimmed_batch_conf(), (10, 1.0))

    def test_unparsable_values_are_disabled(self):
        self._with_config(
            '[video_view_trimmed_batch]\nbatch_size = lots\nbatch_fraction = 1\n')
        self.assertEqual(get_video_view_trimmed_batch_conf(), (0, 0.0))


if __name__ == '__main__':
    unittest.main()
