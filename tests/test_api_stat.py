"""Tests for service.apistat.ApiStatTracker."""

import inspect
import logging
import os
import sys
import unittest
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from service.apistat import ApiStatTracker  # noqa: E402


class FakeClock:
    def __init__(self, start=1000.0):
        self.now = start

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def report(tracker):
    log = logging.getLogger('test.apistat.%d' % id(tracker))
    log.setLevel(logging.INFO)
    lines = []
    handler = logging.Handler()
    handler.emit = lambda record: lines.append(record.getMessage())
    log.addHandler(handler)
    try:
        tracker.log_summary(log)
    finally:
        log.removeHandler(handler)
    return lines


class DimensionTest(unittest.TestCase):
    def test_record_accepts_only_low_cardinality_dimensions(self):
        # a future change that starts passing aid/mid or a URL must touch this
        params = list(inspect.signature(ApiStatTracker.record).parameters)
        self.assertEqual(params, ['self', 'target', 'worker', 'trial', 'outcome', 'now'])

    def test_targets_and_workers_do_not_share_counters(self):
        t = ApiStatTracker(clock=FakeClock())
        t.record('get_member_card', 'card-aws', 1, 'code_-352')
        t.record('get_video_view_trimmed', 'vv-aws', 1, 'http_412')
        t.record('get_video_view_trimmed', 'vv-gcp', 1, 'ok')
        rows = {(r['scope'], r['name']): r['value'] for r in t.totals()}
        self.assertEqual(rows['api:get_member_card:card-aws', 't1:code_-352'], 1.0)
        self.assertEqual(rows['api:get_video_view_trimmed:vv-aws', 't1:http_412'], 1.0)
        self.assertEqual(rows['api:get_video_view_trimmed:vv-gcp', 't1:ok'], 1.0)

    def test_every_outcome_kind_stays_distinct(self):
        t = ApiStatTracker(clock=FakeClock())
        kinds = ['ok', 'http_412', 'code_-352', 'http_503', 'request_exception',
                 'body_exception', 'deadline_exceeded', 'json_error', 'parse_error']
        for kind in kinds:
            t.record('view', 'w', 1, kind)
        names = {r['name'] for r in t.totals()}
        self.assertEqual(names, {'t1:%s' % k for k in kinds})


class DerivedTest(unittest.TestCase):
    def test_p_and_retry_recovery_come_from_the_counts(self):
        clock = FakeClock()
        t = ApiStatTracker(clock=clock)
        for _ in range(90):
            t.record('view', 'w', 1, 'ok')
        for _ in range(10):
            t.record('view', 'w', 1, 'http_412')
        for _ in range(8):
            t.record('view', 'w', 2, 'ok')
        for _ in range(2):
            t.record('view', 'w', 2, 'http_412')
        text = '\n'.join(report(t))
        self.assertIn('p=10.00% (n=100)', text)
        self.assertIn('retry_recovered=8', text)

    def test_a_clean_run_reports_zero_p(self):
        t = ApiStatTracker(clock=FakeClock())
        for _ in range(50):
            t.record('view', 'w', 1, 'ok')
        text = '\n'.join(report(t))
        self.assertIn('p=0.00% (n=50)', text)
        self.assertIn('retry_recovered=0', text)

    def test_no_traffic_reports_without_crashing(self):
        text = '\n'.join(report(ApiStatTracker(clock=FakeClock())))
        self.assertIn('no requests recorded', text)


class WindowTest(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.tracker = ApiStatTracker(window_s=5.0, clock=self.clock)
        self.lines = []
        self.handler = logging.Handler()
        self.handler.emit = lambda record: self.lines.append(record.getMessage())
        logging.getLogger('apistat').addHandler(self.handler)
        logging.getLogger('apistat').setLevel(logging.INFO)

    def tearDown(self):
        logging.getLogger('apistat').removeHandler(self.handler)

    def test_a_window_is_logged_only_once_it_is_crossed(self):
        self.tracker.record('view', 'w', 1, 'ok')
        self.clock.advance(4.0)
        self.tracker.record('view', 'w', 1, 'ok')
        self.assertEqual(self.lines, [])
        self.clock.advance(2.0)
        self.tracker.record('view', 'w', 1, 'http_412')
        self.assertEqual(len(self.lines), 1)
        self.assertIn('n=2 ok=2', self.lines[0])

    def test_totals_accumulate_across_windows(self):
        for i in range(4):
            self.tracker.record('view', 'w', 1, 'ok')
            self.clock.advance(5.0)
        rows = {r['name']: r['value'] for r in self.tracker.totals()}
        self.assertEqual(rows['t1:ok'], 4.0)

    def test_an_idle_stretch_does_not_fabricate_windows(self):
        self.tracker.record('view', 'w', 1, 'ok')
        self.clock.advance(60.0)
        self.tracker.record('view', 'w', 1, 'ok')
        self.assertEqual(len(self.lines), 1)

    def test_log_summary_is_safe_to_call_twice(self):
        self.tracker.record('view', 'w', 1, 'ok')
        first = report(self.tracker)
        second = report(self.tracker)
        self.assertEqual(
            [l for l in first if l.startswith('  - trial')],
            [l for l in second if l.startswith('  - trial')])


class ShapeTest(unittest.TestCase):
    def test_the_sparkline_separates_free_climb_and_plateau(self):
        clock = FakeClock()
        t = ApiStatTracker(window_s=5.0, clock=clock)
        for fraction in [0.0, 0.0, 0.0, 0.3, 0.6, 0.8, 0.8, 0.8]:
            rejected = int(round(fraction * 20))
            for _ in range(20 - rejected):
                t.record('view', 'w', 1, 'ok')
            for _ in range(rejected):
                t.record('view', 'w', 1, 'http_412')
            clock.advance(5.0)
        spark = [l for l in report(t) if ' / w: ' in l][0].split(': ')[-1]
        self.assertEqual(len(spark), 8)
        self.assertEqual(spark[0], '▁')
        self.assertGreater(spark.index(spark[-1]), 2)
        self.assertEqual(spark[-3:], spark[-1] * 3)


class CostTest(unittest.TestCase):
    def test_a_ten_minute_run_logs_tens_of_kb_not_more(self):
        clock = FakeClock()
        t = ApiStatTracker(window_s=5.0, clock=clock)
        lines = []
        handler = logging.Handler()
        handler.emit = lambda record: lines.append(record.getMessage())
        log = logging.getLogger('apistat')
        log.addHandler(handler)
        log.setLevel(logging.INFO)
        try:
            for _ in range(120):
                for _ in range(2500):
                    t.record('get_video_view_trimmed', 'vv-aws', 1, 'ok')
                for _ in range(200):
                    t.record('get_video_view_trimmed', 'vv-aws', 1, 'http_412')
                clock.advance(5.0)
            report_lines = report(t)
        finally:
            log.removeHandler(handler)
        size = sum(len(l) for l in lines + report_lines)
        self.assertLess(size, 60_000)
        self.assertEqual(len(lines), 120)

    def test_record_is_safe_and_cheap_under_300_threads(self):
        t = ApiStatTracker()
        with ThreadPoolExecutor(max_workers=300) as pool:
            list(pool.map(lambda i: t.record('view', 'w', 1, 'ok'), range(30_000)))
        rows = {r['name']: r['value'] for r in t.totals()}
        self.assertEqual(rows['t1:ok'], 30_000.0)


if __name__ == '__main__':
    unittest.main()
