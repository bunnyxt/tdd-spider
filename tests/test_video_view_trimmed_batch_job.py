"""
Tests for FetchVideoRecordJob's BATCH mode and its pool-shared
VideoViewTrimmedBatchController: batch assembly, per-item routing with
single-path fallback for every failure mode, the concurrency gate on
simultaneous batch invocations, the circuit breaker (including the
in-flight-response race), and the conf switch behind it all.

The Service is a scripted mock; the DB layer is stubbed out at import time
(fetch workers never touch the DB anyway -- the stub just satisfies the
``job`` package's sibling imports). Synchronous tests drive ``process()``
directly; the concurrency and race tests run REAL job threads against
services that block on events.

Run from the repo root:

    python -m unittest discover -s tests
"""

import importlib.util
import os
import sys
import threading
import time
import types
import unittest
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
        FetchVideoRecordJob, VideoViewTrimmedBatchConfig, VideoViewTrimmedBatchController)
    from task import build_video_record_via_video_view  # noqa: E402
    from util import a2b  # noqa: E402
except ImportError as e:  # pragma: no cover -- e.g. requests not installed
    raise unittest.SkipTest(f'job dependencies unavailable: {e}')

conf_module = _load_real_conf_module()
get_video_view_trimmed_batch_conf = conf_module.get_video_view_trimmed_batch_conf

import configparser  # noqa: E402

EVENT_TIMEOUT_S = 10  # generous guard so a broken sync can never hang the suite


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


def transient_item(aid):
    return VideoViewTrimmedBatchItem(
        aid=aid, view=None,
        error=ResponseError('video_view_trimmed_batch_item',
                            {'aid': aid, 'kind': 'item_timeout'}))


def make_controller(batch_size=2, batch_fraction=1.0, max_concurrent_batches=30):
    return VideoViewTrimmedBatchController(VideoViewTrimmedBatchConfig(
        batch_size=batch_size, batch_fraction=batch_fraction,
        max_concurrent_batches=max_concurrent_batches))


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


def make_job(service, aids, controller, duration_limit_s=None):
    """Build a job over its own prefilled aid queue (not started)."""
    aid_queue = Queue()
    for aid in aids:
        aid_queue.put(aid)
    record_queue = Queue()
    code_error_aid_queue = Queue()
    job = FetchVideoRecordJob(
        'job_test', aid_queue, record_queue, service,
        code_error_aid_queue=code_error_aid_queue,
        duration_limit_s=duration_limit_s,
        batch_controller=controller)
    job.logger.disabled = True  # the failure paths log loudly on purpose
    return job, record_queue, code_error_aid_queue


def drain(queue):
    out = []
    while not queue.empty():
        out.append(queue.get())
    return out


def run_job(service, aids, controller):
    """Run one job's loop synchronously (no thread).
    Returns (job, records, code_error_aids)."""
    job, record_queue, code_error_aid_queue = make_job(service, aids, controller)
    job.process()
    return job, drain(record_queue), drain(code_error_aid_queue)


class TestBatchHappyPath(unittest.TestCase):

    def test_records_flow_through_batches(self):
        service = ScriptedService()
        aids = [101, 102, 103, 104]
        job, records, _ = run_job(service, aids, make_controller())

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
        job, records, _ = run_job(service, [101, 102, 103], make_controller())

        self.assertEqual(service.batch_calls, [[101, 102], [103]])
        self.assertEqual(len(records), 3)

    def test_code_error_routes_to_update_queue(self):
        service = ScriptedService(batch_script=[
            lambda aids: [ok_item(aids[0]), code_error_item(aids[1])]])
        job, records, code_error_aids = run_job(service, [101, 102], make_controller())

        self.assertEqual([r.aid for r in records], [101])
        self.assertEqual(code_error_aids, [102])
        self.assertEqual(job.stat.condition['code_error'], 1)
        self.assertEqual(job.stat.condition['success'], 1)
        self.assertEqual(job.stat.total_count, 2)

    def test_duplicate_aid_in_buffer_takes_single_path(self):
        service = ScriptedService()
        job, records, _ = run_job(service, [101, 101, 102], make_controller())

        # never the same token twice in one batch: the second 101 goes single
        self.assertEqual(service.batch_calls, [[101, 102]])
        self.assertEqual(service.single_calls, [101])
        self.assertEqual(job.stat.condition['batch_duplicate_single_path'], 1)
        self.assertEqual(len(records), 3)


class TestFailureFallback(unittest.TestCase):
    """Every batch failure mode ends in the single-aid path; nothing is
    retried inside the batch path and nothing is lost."""

    def test_transient_item_falls_back_to_single_path_alone(self):
        service = ScriptedService(batch_script=[
            lambda aids: [ok_item(aids[0]), transient_item(aids[1])]])
        controller = make_controller()
        job, records, _ = run_job(service, [101, 102, 103, 104], controller)

        # only the failed item went single; the batch path stayed in use
        self.assertEqual(service.batch_calls, [[101, 102], [103, 104]])
        self.assertEqual(service.single_calls, [102])
        self.assertTrue(controller.is_enabled())
        self.assertEqual(sorted(r.aid for r in records), [101, 102, 103, 104])
        self.assertEqual(job.stat.condition['batch_item_fallback_single'], 1)
        self.assertEqual(job.stat.condition['success'], 4)
        self.assertEqual(job.stat.total_count, 4)

    def test_transient_item_fallback_reports_the_single_path_outcome(self):
        # the single path's own retry=3 is the retry policy; if IT gives up,
        # the aid is a plain other_exception exactly like today
        service = ScriptedService(
            batch_script=[lambda aids: [transient_item(aids[0])]],
            single_error=ResponseError('video_view_trimmed', {'aid': 101}))
        job, records, _ = run_job(service, [101], make_controller())

        self.assertEqual(service.single_calls, [101])
        self.assertEqual(records, [])
        self.assertEqual(job.stat.condition['other_exception'], 1)
        self.assertEqual(job.stat.total_count, 1)

    def test_whole_batch_failure_falls_back_and_disables_the_path(self):
        service = ScriptedService(batch_script=[
            ResponseError('video_view_trimmed_batch', {})])
        controller = make_controller()
        job, records, _ = run_job(service, [101, 102, 103, 104], controller)

        # exactly ONE batch call: its aids and every later aid go single path
        self.assertEqual(service.batch_calls, [[101, 102]])
        self.assertFalse(controller.is_enabled())
        self.assertEqual(sorted(service.single_calls), [101, 102, 103, 104])
        self.assertEqual(sorted(r.aid for r in records), [101, 102, 103, 104])
        self.assertEqual(job.stat.condition['batch_whole_failure'], 1)
        self.assertEqual(job.stat.condition['batch_fallback_single'], 2)

    def test_unexpected_exception_is_a_whole_batch_failure_not_a_dead_thread(self):
        # a bug-shaped exception must degrade like a transport failure, never
        # kill the worker thread mid-run
        service = ScriptedService(batch_script=[ValueError('boom')])
        controller = make_controller()
        job, records, _ = run_job(service, [101, 102], controller)

        self.assertEqual(job.stat.condition['batch_whole_failure'], 1)
        self.assertFalse(controller.is_enabled())
        self.assertEqual(sorted(r.aid for r in records), [101, 102])

    def test_misalignment_trips_immediately_and_falls_back(self):
        service = ScriptedService(batch_script=[
            MisalignmentError('video_view_trimmed_batch', {}, {}, 'aid echo mismatch')])
        controller = make_controller()
        job, records, _ = run_job(service, [101, 102, 103, 104], controller)

        self.assertEqual(service.batch_calls, [[101, 102]])
        self.assertFalse(controller.is_enabled())
        self.assertEqual(sorted(service.single_calls), [101, 102, 103, 104])
        self.assertEqual(sorted(r.aid for r in records), [101, 102, 103, 104])
        self.assertEqual(job.stat.condition['batch_misalignment'], 1)
        self.assertEqual(job.stat.condition['batch_fallback_single'], 2)

    def test_breaker_is_shared_across_jobs(self):
        # a controller tripped by one job disables the path for its siblings
        controller = make_controller()
        self.assertTrue(controller.trip('misalignment seen by job_0'))
        self.assertFalse(controller.trip('job_1 saw it too'))  # only first trips

        service = NoBatchService()
        job, records, _ = run_job(service, [101, 102], controller)
        self.assertEqual(sorted(r.aid for r in records), [101, 102])
        self.assertEqual(service.single_calls, [101, 102])


class BlockingBatchService(ScriptedService):
    """Batch calls track in-flight concurrency and block until released."""

    def __init__(self, hold_s=0.0, release_event=None):
        super().__init__()
        self._lock = threading.Lock()
        self._in_flight = 0
        self.max_in_flight = 0
        self.hold_s = hold_s
        self.release_event = release_event
        self.entered = threading.Event()  # set when any batch call is in flight

    def get_video_view_trimmed_batch(self, aids, **kwargs):
        with self._lock:
            self.batch_calls.append(list(aids))
            self._in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self._in_flight)
        self.entered.set()
        try:
            if self.release_event is not None:
                assert self.release_event.wait(EVENT_TIMEOUT_S), 'release never came'
            elif self.hold_s:
                time.sleep(self.hold_s)
            return [ok_item(aid) for aid in aids]
        finally:
            with self._lock:
                self._in_flight -= 1


class TestConcurrencyGate(unittest.TestCase):
    """Real threads: the gate must bound SIMULTANEOUS batch invocations."""

    def test_in_flight_batches_never_exceed_the_cap(self):
        cap = 2
        controller = make_controller(batch_size=2, max_concurrent_batches=cap)
        service = BlockingBatchService(hold_s=0.02)
        aid_queue = Queue()
        for aid in range(1, 49):  # 48 aids -> 24 batches
            aid_queue.put(aid)
        record_queue = Queue()
        jobs = []
        for i in range(6):  # 6 threads contending for 2 slots
            job = FetchVideoRecordJob(
                f'job_{i}', aid_queue, record_queue, service,
                code_error_aid_queue=Queue(), batch_controller=controller)
            job.logger.disabled = True
            jobs.append(job)
        for job in jobs:
            job.start()
        for job in jobs:
            job.join(EVENT_TIMEOUT_S)
            self.assertFalse(job.is_alive(), 'job did not finish')

        self.assertLessEqual(service.max_in_flight, cap)
        self.assertEqual(len(drain(record_queue)), 48)  # nothing lost to the gate
        # observability: with 24 batches over 2 slots the gate must have bitten
        throttled = sum(j.stat.condition['batch_concurrency_throttled'] for j in jobs)
        self.assertGreaterEqual(throttled, 1)

    def test_deadline_while_waiting_for_a_slot_exits_cleanly(self):
        # job A holds the only slot with its batch blocked in flight; job B
        # must give up at its run deadline instead of waiting forever
        release = threading.Event()
        controller = make_controller(batch_size=1, max_concurrent_batches=1)
        service = BlockingBatchService(release_event=release)

        job_a, records_a, _ = make_job(service, [301], controller)
        job_b, records_b, _ = make_job(service, [302], controller,
                                       duration_limit_s=1)
        job_a.start()
        self.assertTrue(service.entered.wait(EVENT_TIMEOUT_S))  # A holds the slot
        job_b.start()
        job_b.join(EVENT_TIMEOUT_S)
        self.assertFalse(job_b.is_alive(), 'job B hung waiting for a slot')
        release.set()
        job_a.join(EVENT_TIMEOUT_S)
        self.assertFalse(job_a.is_alive())

        self.assertEqual(job_b.stat.condition['duration_limit_reached'], 1)
        self.assertEqual(job_b.stat.condition['batch_concurrency_throttled'], 1)
        self.assertEqual(job_b.stat.condition['batch_dropped_at_deadline'], 1)
        self.assertEqual(drain(records_b), [])  # 302 dropped at the deadline
        self.assertEqual([r.aid for r in drain(records_a)], [301])

    def test_deadline_during_final_partial_flush_still_counts_duration_limit(self):
        # queue already EMPTY, job B is flushing its last partial batch (one
        # aid, batch_size 2) and waits for the slot job A holds; the deadline
        # passes during that wait. B breaks straight out after the drop -- the
        # loop top never runs again -- so the drop path itself must record
        # the duration-limit hit, exactly once.
        release = threading.Event()
        controller = make_controller(batch_size=2, max_concurrent_batches=1)
        service = BlockingBatchService(release_event=release)

        job_a, records_a, _ = make_job(service, [301, 302], controller)
        job_b, records_b, _ = make_job(service, [303], controller,
                                       duration_limit_s=1)
        job_a.start()
        self.assertTrue(service.entered.wait(EVENT_TIMEOUT_S))  # A holds the slot
        job_b.start()
        job_b.join(EVENT_TIMEOUT_S)
        self.assertFalse(job_b.is_alive(), 'job B hung waiting for a slot')
        release.set()
        job_a.join(EVENT_TIMEOUT_S)
        self.assertFalse(job_a.is_alive())

        self.assertEqual(job_b.stat.condition['batch_dropped_at_deadline'], 1)
        self.assertEqual(job_b.stat.condition['duration_limit_reached'], 1)
        self.assertEqual(drain(records_b), [])
        self.assertEqual(sorted(r.aid for r in drain(records_a)), [301, 302])


class RaceBatchService(ScriptedService):
    """
    Orchestrates the breaker/in-flight race: job B's batch (aids starting at
    201) blocks in flight; job A's batch (101...) waits until B is in flight,
    then fails with a misalignment, tripping the breaker. B's response then
    returns AFTER the trip and must be discarded by the job.
    """

    def __init__(self):
        super().__init__()
        self._lock = threading.Lock()
        self.b_in_flight = threading.Event()
        self.b_release = threading.Event()

    def get_video_view_trimmed_batch(self, aids, **kwargs):
        with self._lock:
            self.batch_calls.append(list(aids))
        if aids[0] == 201:  # job B
            self.b_in_flight.set()
            assert self.b_release.wait(EVENT_TIMEOUT_S), 'b_release never came'
            return [ok_item(aid) for aid in aids]
        # job A: only misalign once B is provably in flight
        assert self.b_in_flight.wait(EVENT_TIMEOUT_S), 'B never got in flight'
        raise MisalignmentError('video_view_trimmed_batch', {}, {}, 'aid echo mismatch')

    def get_video_view_trimmed(self, params, **kwargs):
        with self._lock:
            self.single_calls.append(params['aid'])
        return make_view(params['aid'])


class TestBreakerInFlightRace(unittest.TestCase):

    def test_response_in_flight_during_trip_is_discarded_and_refetched(self):
        controller = make_controller(batch_size=2, max_concurrent_batches=2)
        service = RaceBatchService()
        job_b, records_b, _ = make_job(service, [201, 202], controller)
        job_a, records_a, _ = make_job(service, [101, 102], controller)

        job_b.start()
        self.assertTrue(service.b_in_flight.wait(EVENT_TIMEOUT_S))
        job_a.start()
        job_a.join(EVENT_TIMEOUT_S)  # A misaligns, trips, falls back, finishes
        self.assertFalse(job_a.is_alive())
        self.assertFalse(controller.is_enabled())
        # only now does B's (healthy-looking) response come back
        service.b_release.set()
        job_b.join(EVENT_TIMEOUT_S)
        self.assertFalse(job_b.is_alive())

        # B re-checked the breaker on return: response discarded wholesale,
        # its aids refetched over the single path -- nothing lost, nothing
        # written from the untrusted response, nothing duplicated
        self.assertEqual(job_b.stat.condition['batch_discarded_after_trip'], 1)
        self.assertEqual(sorted(service.single_calls), [101, 102, 201, 202])
        self.assertEqual(sorted(r.aid for r in drain(records_a)), [101, 102])
        self.assertEqual(sorted(r.aid for r in drain(records_b)), [201, 202])
        self.assertEqual(job_b.stat.condition['batch_fallback_single'], 2)
        # exactly one batch call each: neither job retried the dead path
        self.assertEqual(len(service.batch_calls), 2)


class GatedQueue(Queue):
    """An aid queue whose Nth get_nowait blocks until `gate` is opened -- lets
    a test freeze a job between two dequeues while another job acts."""

    def __init__(self, gate_before_get_number):
        super().__init__()
        self.gate = threading.Event()
        self._gate_before = gate_before_get_number
        self._gets = 0

    def get_nowait(self):
        self._gets += 1
        if self._gets == self._gate_before:
            assert self.gate.wait(EVENT_TIMEOUT_S), 'gate never opened'
        return super().get_nowait()


class TestPartialBufferOnTrip(unittest.TestCase):

    def test_job_holding_a_partial_buffer_flushes_it_when_the_breaker_opens(self):
        # job A buffers 101 (batch_size 3, so no dispatch), then is frozen on
        # its next dequeue; job B trips the breaker meanwhile. When A resumes
        # it must resolve its buffered 101 over the single path BEFORE
        # handling the aid it just dequeued -- not at queue-empty, not at the
        # deadline.
        controller = make_controller(batch_size=3, max_concurrent_batches=2)
        service = RaceBatchService()  # B's 201-batch would block; unused here
        service.get_video_view_trimmed_batch = lambda aids, **kw: (_ for _ in ()).throw(
            MisalignmentError('video_view_trimmed_batch', {}, {}, 'aid echo mismatch'))

        aid_queue_a = GatedQueue(gate_before_get_number=2)
        for aid in (101, 102):
            aid_queue_a.put(aid)
        records_a = Queue()
        job_a = FetchVideoRecordJob('job_a', aid_queue_a, records_a, service,
                                    code_error_aid_queue=Queue(), batch_controller=controller)
        job_a.logger.disabled = True
        job_b, records_b, _ = make_job(service, [201, 202], controller)

        job_a.start()
        # A is now parked inside its second get_nowait with 101 buffered
        job_b.start()
        job_b.join(EVENT_TIMEOUT_S)
        self.assertFalse(job_b.is_alive())
        self.assertFalse(controller.is_enabled())  # B tripped it
        aid_queue_a.gate.set()
        job_a.join(EVENT_TIMEOUT_S)
        self.assertFalse(job_a.is_alive())

        # 101 (the buffer) was refetched before 102 (the fresh dequeue)
        a_singles = [aid for aid in service.single_calls if aid in (101, 102)]
        self.assertEqual(a_singles, [101, 102])
        self.assertEqual(job_a.stat.condition['batch_fallback_single'], 1)
        self.assertNotIn('batch_dropped_at_deadline', job_a.stat.condition)
        self.assertEqual(sorted(r.aid for r in drain(records_a)), [101, 102])
        self.assertEqual(sorted(r.aid for r in drain(records_b)), [201, 202])


class SlotRaceService(ScriptedService):
    """
    Orchestrates the slot/breaker race: job A's batch (301...) holds the only
    slot and blocks until `trip_now` is set, then fails with a misalignment
    (tripping the breaker and releasing the slot). Job B, waiting for that
    slot, then acquires it -- and must NOT send its batch.
    """

    def __init__(self):
        super().__init__()
        self._lock = threading.Lock()
        self.trip_now = threading.Event()

    def get_video_view_trimmed_batch(self, aids, **kwargs):
        with self._lock:
            self.batch_calls.append(list(aids))
        assert self.trip_now.wait(EVENT_TIMEOUT_S), 'trip_now never came'
        raise MisalignmentError('video_view_trimmed_batch', {}, {}, 'aid echo mismatch')

    def get_video_view_trimmed(self, params, **kwargs):
        with self._lock:
            self.single_calls.append(params['aid'])
        return make_view(params['aid'])


class TestSlotAcquiredAfterTrip(unittest.TestCase):

    def test_waiter_that_gets_the_slot_after_a_trip_releases_it_and_falls_back(self):
        controller = make_controller(batch_size=2, max_concurrent_batches=1)
        service = SlotRaceService()
        job_a, records_a, _ = make_job(service, [301, 302], controller)
        job_b, records_b, _ = make_job(service, [303, 304], controller)

        job_a.start()
        deadline = time.time() + EVENT_TIMEOUT_S
        while len(service.batch_calls) < 1:  # A holds the slot, in flight
            self.assertLess(time.time(), deadline, 'A never got in flight')
            time.sleep(0.005)
        job_b.start()
        while job_b.stat.condition['batch_concurrency_throttled'] < 1:  # B waits
            self.assertLess(time.time(), deadline, 'B never waited for the slot')
            time.sleep(0.005)
        # another thread (A) trips the breaker and releases the slot while B
        # is blocked waiting for it
        service.trip_now.set()
        job_a.join(EVENT_TIMEOUT_S)
        job_b.join(EVENT_TIMEOUT_S)
        self.assertFalse(job_a.is_alive())
        self.assertFalse(job_b.is_alive())

        self.assertFalse(controller.is_enabled())
        # B acquired the freed slot but never sent: only A's batch exists
        self.assertEqual(service.batch_calls, [[301, 302]])
        self.assertEqual(job_b.stat.condition['batch_request'], 0)
        self.assertEqual(job_b.stat.condition['batch_fallback_single'], 2)
        self.assertEqual(sorted(r.aid for r in drain(records_b)), [303, 304])
        self.assertEqual(sorted(r.aid for r in drain(records_a)), [301, 302])
        # the slot B abandoned was given back: with cap=1 it is free again
        self.assertTrue(controller.try_acquire_slot(0))
        controller.release_slot()


class TestBatchPathOff(unittest.TestCase):

    def test_no_controller_means_single_path_only(self):
        service = NoBatchService()
        job, records, _ = run_job(service, [101, 102], None)

        self.assertEqual(service.single_calls, [101, 102])
        self.assertEqual(sorted(r.aid for r in records), [101, 102])
        self.assertEqual(job.stat.condition['success'], 2)
        # no batch counters ever appear when the path is off
        self.assertNotIn('batch_request', job.stat.condition)

    def test_fraction_zero_never_batches(self):
        service = NoBatchService()
        job, records, _ = run_job(
            service, [101, 102], make_controller(batch_fraction=0.0))

        self.assertEqual(service.single_calls, [101, 102])
        self.assertEqual(len(records), 2)


class TestRecordMapping(unittest.TestCase):

    def test_hidden_view_count_maps_to_minus_one(self):
        record = build_video_record_via_video_view(101, make_view(101, view='--'))
        self.assertEqual(record.view, -1)
        self.assertEqual(record.bvid, a2b(101))  # BV prefix stripped

    def test_every_field_maps_through(self):
        aid = 456930
        view = VideoViewTrimmed(
            bvid='BV' + a2b(aid), aid=aid,
            stat=VideoViewStat(aid=aid, view=11, danmaku=12, reply=13,
                               favorite=14, coin=15, share=16, now_rank=17,
                               his_rank=18, like=19, dislike=20, vt=21, vv=22))
        record = build_video_record_via_video_view(aid, view)

        self.assertEqual(record.aid, aid)
        self.assertEqual(record.bvid, a2b(aid))
        self.assertEqual(record.view, 11)
        self.assertEqual(record.danmaku, 12)
        self.assertEqual(record.reply, 13)
        self.assertEqual(record.favorite, 14)
        self.assertEqual(record.coin, 15)
        self.assertEqual(record.share, 16)
        self.assertEqual(record.now_rank, 17)
        self.assertEqual(record.his_rank, 18)
        self.assertEqual(record.like, 19)
        self.assertEqual(record.dislike, 20)
        self.assertEqual(record.vt, 21)
        self.assertEqual(record.vv, 22)
        self.assertGreater(record.added, 0)

    def test_bvid_prefix_removal_is_removeprefix_not_lstrip(self):
        # lstrip('BV') would strip any run of leading B/V characters; the
        # mapping must remove exactly one 'BV' prefix
        view = make_view(101)._replace(bvid='BVVB1x')
        self.assertEqual(
            build_video_record_via_video_view(101, view).bvid, 'VB1x')


class TestConf(unittest.TestCase):

    def _with_config(self, ini_text):
        parser = configparser.ConfigParser()
        parser.read_string(ini_text)
        original = conf_module.CONFIG
        conf_module.CONFIG = parser
        self.addCleanup(lambda: setattr(conf_module, 'CONFIG', original))

    def test_missing_section_is_disabled(self):
        self._with_config('[db_mysql]\nuser = u\n')
        self.assertEqual(get_video_view_trimmed_batch_conf(), (0, 0.0, 0))

    def test_enabled_values_pass_through(self):
        self._with_config(
            '[video_view_trimmed_batch]\nbatch_size = 10\nbatch_fraction = 0.05\n'
            'max_concurrent_batches = 20\n')
        self.assertEqual(get_video_view_trimmed_batch_conf(), (10, 0.05, 20))

    def test_max_concurrent_defaults_when_absent(self):
        self._with_config(
            '[video_view_trimmed_batch]\nbatch_size = 10\nbatch_fraction = 0.05\n')
        self.assertEqual(
            get_video_view_trimmed_batch_conf(),
            (10, 0.05, conf_module.VIDEO_VIEW_TRIMMED_BATCH_MAX_CONCURRENT_DEFAULT))

    def test_zero_size_or_fraction_is_disabled(self):
        self._with_config(
            '[video_view_trimmed_batch]\nbatch_size = 0\nbatch_fraction = 1\n')
        self.assertEqual(get_video_view_trimmed_batch_conf(), (0, 0.0, 0))
        self._with_config(
            '[video_view_trimmed_batch]\nbatch_size = 10\nbatch_fraction = 0\n')
        self.assertEqual(get_video_view_trimmed_batch_conf(), (0, 0.0, 0))

    def test_nonpositive_concurrency_disables_the_path(self):
        # a cap <= 0 would deadlock the gate; fail safe to off, never guess
        self._with_config(
            '[video_view_trimmed_batch]\nbatch_size = 10\nbatch_fraction = 1\n'
            'max_concurrent_batches = 0\n')
        self.assertEqual(get_video_view_trimmed_batch_conf(), (0, 0.0, 0))

    def test_negative_values_are_disabled(self):
        self._with_config(
            '[video_view_trimmed_batch]\nbatch_size = -5\nbatch_fraction = 0.5\n')
        self.assertEqual(get_video_view_trimmed_batch_conf(), (0, 0.0, 0))

    def test_size_is_capped_at_worker_max(self):
        self._with_config(
            '[video_view_trimmed_batch]\nbatch_size = 100\nbatch_fraction = 1\n')
        self.assertEqual(get_video_view_trimmed_batch_conf()[0], 50)

    def test_fraction_is_clamped_to_one(self):
        self._with_config(
            '[video_view_trimmed_batch]\nbatch_size = 10\nbatch_fraction = 1.5\n')
        self.assertEqual(get_video_view_trimmed_batch_conf()[1], 1.0)

    def test_unparsable_values_are_disabled(self):
        self._with_config(
            '[video_view_trimmed_batch]\nbatch_size = lots\nbatch_fraction = 1\n')
        self.assertEqual(get_video_view_trimmed_batch_conf(), (0, 0.0, 0))


if __name__ == '__main__':
    unittest.main()
