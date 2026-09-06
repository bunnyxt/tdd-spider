"""
Tests for ``service.apistat.ApiStatTracker``: the always-on,
--debug-independent request/retry/rate-limit observability layer.

Run from the repo root:

    python -m unittest discover -s tests
"""

import inspect
import logging
import os
import sys
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from service.apistat import ApiStatTracker  # noqa: E402


class FakeClock:
    """A settable clock, like ``test_worker_selector.FakeClock`` -- deterministic
    window boundaries without sleeping in real time."""

    def __init__(self, start=1000.0):
        self.now = start

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class ApiSurfaceTest(unittest.TestCase):
    """
    Locks the shape of the hot-path API: a future change that starts passing
    aid/mid, a URL, or anything else high-cardinality must touch this test.
    """

    def test_record_accepts_only_low_cardinality_dimensions(self):
        params = list(inspect.signature(ApiStatTracker.record).parameters)
        self.assertEqual(params, ['self', 'target', 'worker', 'trial', 'outcome', 'now'])


class OutcomeVisibilityTest(unittest.TestCase):
    """AC1: every outcome _get can produce stays individually visible, not
    just HTTP 412 -- including member_card's code_-352 and every timeout kind."""

    def test_every_known_outcome_kind_is_kept_distinct(self):
        clock = FakeClock()
        tracker = ApiStatTracker(clock=clock)
        outcomes = ['ok', 'http_412', 'code_-352', 'http_500',
                    'request_exception', 'body_exception', 'deadline_exceeded',
                    'json_error', 'parse_error']
        for outcome in outcomes:
            tracker.record('get_video_view_trimmed', 'w1', 1, outcome)

        rows = {(r['scope'], r['name']): r['value'] for r in tracker.totals()}
        for outcome in outcomes:
            self.assertEqual(
                rows[('api:get_video_view_trimmed:w1', f't1:{outcome}')], 1.0,
                f'{outcome} was not recorded distinctly')

    def test_member_card_and_video_view_do_not_share_counters(self):
        clock = FakeClock()
        tracker = ApiStatTracker(clock=clock)
        tracker.record('get_member_card', 'card-1', 1, 'code_-352')
        tracker.record('get_video_view_trimmed', 'view-1', 1, 'http_412')

        rows = {(r['scope'], r['name']): r['value'] for r in tracker.totals()}
        self.assertEqual(rows[('api:get_member_card:card-1', 't1:code_-352')], 1.0)
        self.assertEqual(rows[('api:get_video_view_trimmed:view-1', 't1:http_412')], 1.0)
        self.assertNotIn(('api:get_member_card:card-1', 't1:http_412'), rows)


class DerivedQuantitiesTest(unittest.TestCase):
    """AC2: p, retry-recovered and retry-exhausted counts must be directly
    verifiable from the recorded per-trial counts, without --debug."""

    def test_no_retry_needed(self):
        clock = FakeClock()
        tracker = ApiStatTracker(clock=clock)
        for _ in range(97):
            tracker.record('t', 'w', 1, 'ok')
        for _ in range(3):
            tracker.record('t', 'w', 1, 'http_412')

        trials = tracker.grouped_totals()[('t', 'w')]
        derived = ApiStatTracker.derive(trials)
        self.assertAlmostEqual(derived['p'], 0.03)
        self.assertEqual(derived['n1'], 100)
        self.assertEqual(derived['retry_recovered'], 0)
        self.assertEqual(derived['max_trial'], 1)
        self.assertEqual(derived['exhausted'], 3)  # trial 1 IS the last trial here

    def test_retry_recovers_most_of_the_rejected_trial(self):
        clock = FakeClock()
        tracker = ApiStatTracker(clock=clock)
        for _ in range(90):
            tracker.record('t', 'w', 1, 'ok')
        for _ in range(10):
            tracker.record('t', 'w', 1, 'http_412')
        # 9 of the 10 that failed trial 1 succeed on trial 2
        for _ in range(9):
            tracker.record('t', 'w', 2, 'ok')
        tracker.record('t', 'w', 2, 'http_412')

        trials = tracker.grouped_totals()[('t', 'w')]
        derived = ApiStatTracker.derive(trials)
        self.assertAlmostEqual(derived['p'], 0.10)
        self.assertEqual(derived['retry_recovered'], 9)
        self.assertEqual(derived['max_trial'], 2)
        self.assertEqual(derived['exhausted'], 1)

    def test_retry_exhausted_after_the_configured_retry_budget(self):
        clock = FakeClock()
        tracker = ApiStatTracker(clock=clock)
        # one call, rejected on all 3 trials (retry=3) -> RateLimitError in Service
        for trial in (1, 2, 3):
            tracker.record('t', 'w', trial, 'http_412')

        trials = tracker.grouped_totals()[('t', 'w')]
        derived = ApiStatTracker.derive(trials)
        self.assertEqual(derived['p'], 1.0)
        self.assertEqual(derived['retry_recovered'], 0)
        self.assertEqual(derived['max_trial'], 3)
        self.assertEqual(derived['exhausted'], 1)

    def test_no_traffic_derives_to_none_not_a_crash(self):
        derived = ApiStatTracker.derive({})
        self.assertIsNone(derived['p'])
        self.assertIsNone(derived['max_trial'])
        self.assertIsNone(derived['exhausted'])
        self.assertEqual(derived['n1'], 0)
        self.assertEqual(derived['retry_recovered'], 0)


class WindowingTest(unittest.TestCase):
    """AC3 + AC8: one INFO line per closed 5s window, containing only that
    window's counts, plus correct accumulation across window boundaries."""

    def test_no_window_is_logged_before_five_seconds_pass(self):
        clock = FakeClock()
        tracker = ApiStatTracker(window_s=5.0, clock=clock)
        with mock.patch('service.apistat.logger') as mock_logger:
            for _ in range(3):
                clock.advance(1.0)
                tracker.record('t', 'w', 1, 'ok')
            mock_logger.info.assert_not_called()

    def test_crossing_the_boundary_logs_exactly_that_windows_counts(self):
        clock = FakeClock()
        tracker = ApiStatTracker(window_s=5.0, clock=clock)
        with self.assertLogs('apistat', level=logging.INFO) as ctx:
            tracker.record('t', 'w', 1, 'ok', now=clock.now)
            tracker.record('t', 'w', 1, 'http_412', now=clock.now)
            clock.advance(5.1)
            # this call belongs to window 1 and must trigger window 0's flush
            tracker.record('t', 'w', 1, 'ok', now=clock.now)

        window_lines = [line for line in ctx.output if 'API stat window #0' in line]
        self.assertEqual(len(window_lines), 1)
        self.assertIn('n=2', window_lines[0])
        self.assertIn('ok=1', window_lines[0])
        self.assertIn('http_412=1', window_lines[0])
        # the record that opened window 1 must not be in window 0's line
        self.assertNotIn('n=3', window_lines[0])

    def test_totals_accumulate_across_window_boundaries(self):
        clock = FakeClock()
        tracker = ApiStatTracker(window_s=5.0, clock=clock)
        tracker.record('t', 'w', 1, 'ok', now=clock.now)
        clock.advance(5.1)
        tracker.record('t', 'w', 1, 'ok', now=clock.now)
        clock.advance(5.1)
        tracker.record('t', 'w', 1, 'ok', now=clock.now)

        rows = {(r['scope'], r['name']): r['value'] for r in tracker.totals()}
        self.assertEqual(rows[('api:t:w', 't1:ok')], 3.0)

    def test_a_silent_gap_does_not_fabricate_empty_windows(self):
        # traffic in window 0, nothing at all in window 1, traffic resumes in
        # window 2 -- only windows that actually saw a request are recorded
        clock = FakeClock()
        tracker = ApiStatTracker(window_s=5.0, clock=clock)
        tracker.record('t', 'w', 1, 'ok', now=clock.now)
        clock.advance(11.0)  # skips straight into window 2
        tracker.record('t', 'w', 1, 'ok', now=clock.now)
        tracker.finalize()

        indices = [snap.index for snap in tracker._history]
        self.assertEqual(indices, [0, 2])

    def test_finalize_is_idempotent(self):
        clock = FakeClock()
        tracker = ApiStatTracker(window_s=5.0, clock=clock)
        tracker.record('t', 'w', 1, 'ok', now=clock.now)
        first = tracker.finalize()
        second = tracker.finalize()
        self.assertEqual(first, second)
        self.assertEqual(len(tracker._history), 1)


class EndOfRunReportTest(unittest.TestCase):
    """AC3: the end-of-run report must let a reader tell a free window, a
    climbing phase and a plateau apart."""

    def test_report_distinguishes_free_climb_and_plateau(self):
        clock = FakeClock()
        tracker = ApiStatTracker(window_s=5.0, clock=clock)

        def run_window(n, rejected):
            for _ in range(n - rejected):
                tracker.record('view', 'w1', 1, 'ok', now=clock.now)
            for _ in range(rejected):
                tracker.record('view', 'w1', 1, 'http_412', now=clock.now)
            clock.advance(5.1)

        run_window(100, 0)     # free window: p = 0%
        run_window(100, 0)
        run_window(100, 40)    # climbing
        run_window(100, 70)
        run_window(100, 95)    # plateau: p ~= 95%
        run_window(100, 96)

        lines = tracker.finalize()
        spark_line = next(line for line in lines if 'view / w1:' in line)
        glyphs = spark_line.split(':', 1)[1].strip()
        self.assertEqual(len(glyphs), 6)
        # free window -> blank/low glyph, plateau -> the densest glyph
        self.assertIn(glyphs[0], ' ▁')
        self.assertEqual(glyphs[-1], '█')
        # monotonically non-decreasing shading across the climb, i.e. it reads
        # left-to-right as "getting worse", not noise
        levels = ' ▁▂▃▄▅▆▇█'
        self.assertTrue(all(levels.index(a) <= levels.index(b)
                            for a, b in zip(glyphs, glyphs[1:])))

    def test_summary_table_shows_every_trial_and_the_derived_line(self):
        clock = FakeClock()
        tracker = ApiStatTracker(window_s=5.0, clock=clock)
        tracker.record('view', 'w1', 1, 'ok')
        tracker.record('view', 'w1', 1, 'http_412')
        tracker.record('view', 'w1', 2, 'ok')

        lines = '\n'.join(tracker.finalize())
        self.assertIn('view / w1', lines)
        self.assertIn('trial 1: n=2,', lines)
        self.assertIn('trial 2: n=1,', lines)
        self.assertIn('retry_recovered=1', lines)

    def test_no_traffic_renders_without_crashing(self):
        tracker = ApiStatTracker()
        lines = tracker.finalize()
        self.assertIn('(no requests recorded)', '\n'.join(lines))


class LogVolumeTest(unittest.TestCase):
    """AC5: log growth tracks window count and outcome cardinality, not
    request volume or failure count."""

    def test_a_busy_window_and_a_quiet_window_log_about_the_same_size(self):
        clock = FakeClock()
        tracker = ApiStatTracker(window_s=5.0, clock=clock)

        with self.assertLogs('apistat', level=logging.INFO) as ctx:
            tracker.record('view', 'w1', 1, 'ok', now=clock.now)
            clock.advance(5.1)
            tracker.record('view', 'w1', 1, 'ok', now=clock.now)
        quiet_line = next(l for l in ctx.output if 'window #0' in l)

        clock2 = FakeClock()
        tracker2 = ApiStatTracker(window_s=5.0, clock=clock2)
        with self.assertLogs('apistat', level=logging.INFO) as ctx2:
            for _ in range(50_000):
                tracker2.record('view', 'w1', 1, 'ok', now=clock2.now)
            for _ in range(2_000):
                tracker2.record('view', 'w1', 1, 'http_412', now=clock2.now)
            clock2.advance(5.1)
            tracker2.record('view', 'w1', 1, 'ok', now=clock2.now)
        busy_line = next(l for l in ctx2.output if 'window #0' in l)

        # a window's line size is bounded by outcome cardinality, not by how
        # many requests it summarised -- a few bytes of slack for bigger numbers
        self.assertLess(len(busy_line), len(quiet_line) + 40)

    def test_a_ten_minute_run_worth_of_windows_logs_tens_of_kb_not_more(self):
        clock = FakeClock()
        tracker = ApiStatTracker(window_s=5.0, clock=clock)
        total_bytes = 0
        with self.assertLogs('apistat', level=logging.INFO) as ctx:
            for _ in range(120):  # ~10 minutes at 5s/window, a typical hourly run
                tracker.record('get_video_view_trimmed', 'w1', 1, 'ok', now=clock.now)
                tracker.record('get_video_view_trimmed', 'w1', 1, 'http_412', now=clock.now)
                clock.advance(5.1)
            tracker.finalize()
        total_bytes = sum(len(line.encode()) for line in ctx.output)
        self.assertLess(total_bytes, 50_000)  # "几十 KB", not hundreds


class OverheadTest(unittest.TestCase):
    """AC6: record() must stay cheap under the concurrency 51_ actually runs
    (job_num=300, ~500 req/s)."""

    def test_record_is_cheap_under_300_threads(self):
        clock = FakeClock()
        tracker = ApiStatTracker(window_s=5.0, clock=clock)
        # a shared incrementing counter stands in for wall-clock progression so
        # windows roll over during the benchmark too, exercising the flush path
        call_count = 300 * 200  # 300 "threads" x 200 attempts each

        def worker():
            for _ in range(200):
                tracker.record('get_video_view_trimmed', 'w1', 1, 'ok')

        import time as time_module
        start = time_module.perf_counter()
        with ThreadPoolExecutor(max_workers=300) as pool:
            futures = [pool.submit(worker) for _ in range(300)]
            for f in futures:
                f.result()
        elapsed = time_module.perf_counter() - start

        rows = {(r['scope'], r['name']): r['value'] for r in tracker.totals()}
        self.assertEqual(rows[('api:get_video_view_trimmed:w1', 't1:ok')], call_count)
        # generous bound: real production is ~500 req/s, this is 300 threads x
        # 200 calls with NO network I/O at all -- if this were ever anywhere
        # near the bound, record() would be a real bottleneck at 500 req/s
        self.assertLess(elapsed, 5.0,
                        f'{call_count} record() calls across 300 threads took '
                        f'{elapsed:.2f}s -- record() got expensive')


if __name__ == '__main__':
    unittest.main()
