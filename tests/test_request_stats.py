import logging
from unittest import TestCase, mock

from service import RateLimitError, RequestStats, Service
from service.stats import RATE_LIMIT_LOG_INTERVAL_S

from test_worker_selector import (ScriptedSession, endpoints_for, response,
                                  worker)


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


class RequestStatsTest(TestCase):
    def test_calls_and_attempts_are_counted_separately(self):
        stats = RequestStats()
        stats.record_call('view', 'ok')
        stats.record_call('view', 'exhausted')
        stats.record_attempt('view', 'a', 'http_412')
        stats.record_attempt('view', 'a', 'http_412')
        stats.record_attempt('view', 'b', 'ok')

        snapshot = stats.snapshot()
        self.assertEqual(snapshot['calls'], {('view', 'ok'): 1,
                                             ('view', 'exhausted'): 1})
        self.assertEqual(snapshot['attempts'], {('view', 'a', 'http_412'): 2,
                                                ('view', 'b', 'ok'): 1})

    def test_direct_mode_attempts_are_bucketed_under_direct(self):
        stats = RequestStats()
        stats.record_attempt('view', None, 'ok')
        self.assertEqual(stats.snapshot()['attempts'], {('view', 'direct', 'ok'): 1})

    def test_format_lines_reports_per_target_and_per_worker(self):
        stats = RequestStats()
        stats.record_call('view', 'ok')
        stats.record_attempt('view', 'a', 'ok')
        stats.record_attempt('view', 'a', 'http_412')

        self.assertEqual(stats.format_lines(), [
            'REQSTAT calls view: total=1 ok=1',
            'REQSTAT attempts view [a]: total=2 http_412=1 ok=1',
        ])

    def test_rate_limit_logging_is_throttled_and_counts_suppressed_hits(self):
        clock = FakeClock()
        stats = RequestStats(clock=clock)

        self.assertEqual(
            stats.record_rate_limit('view', 'a', reason='http_412', action='retry'),
            (True, 0))
        for _ in range(4):
            should_log, _ = stats.record_rate_limit(
                'view', 'a', reason='http_412', action='retry')
            self.assertFalse(should_log)

        clock.now += RATE_LIMIT_LOG_INTERVAL_S
        self.assertEqual(
            stats.record_rate_limit('view', 'a', reason='http_412', action='retry'),
            (True, 4))

    def test_rate_limit_throttle_is_per_target_worker_and_reason(self):
        stats = RequestStats(clock=FakeClock())
        self.assertTrue(stats.record_rate_limit(
            'view', 'a', reason='http_412', action='retry')[0])
        self.assertTrue(stats.record_rate_limit(
            'view', 'b', reason='http_412', action='retry')[0])
        self.assertTrue(stats.record_rate_limit(
            'view', 'a', reason='code_-352', action='retry')[0])
        self.assertFalse(stats.record_rate_limit(
            'view', 'a', reason='http_412', action='retry')[0])

    def test_recorded_events_are_bounded(self):
        clock = FakeClock()
        stats = RequestStats(clock=clock)
        for i in range(600):
            clock.now += RATE_LIMIT_LOG_INTERVAL_S
            stats.record_rate_limit('view', 'a', reason='http_412', action='retry')
        self.assertEqual(len(stats.snapshot()['rate_limit_events']), 500)


class ServiceRequestStatsTest(TestCase):
    def make_service(self, target, workers, responses, **kwargs):
        service = Service(
            mode='worker', retry=3, colddown_factor=0,
            endpoints=endpoints_for(target, workers), **kwargs)
        service._session = ScriptedSession(responses)
        return service

    def attempts(self, service):
        return service.request_stats.snapshot()['attempts']

    def calls(self, service):
        return service.request_stats.snapshot()['calls']

    @mock.patch('service.worker.random.choice', side_effect=lambda items: items[0])
    def test_412_then_success_records_both_attempts_and_one_call(self, _choice):
        service = self.make_service('view', [
            worker('a', 'https://a.invalid/'),
            worker('b', 'https://b.invalid/'),
        ], {
            'https://a.invalid/': [response(412, b'blocked')],
            'https://b.invalid/': [response(200, {'code': 0})],
        })

        with mock.patch('service.Service.time.sleep'):
            self.assertEqual(service._get('view', 'worker'), {'code': 0})

        self.assertEqual(self.attempts(service), {
            ('view', 'a', 'rate_limited:http_412'): 1,
            ('view', 'b', 'ok'): 1,
        })
        self.assertEqual(self.calls(service), {('view', 'ok'): 1})

    def test_exhausted_call_is_recorded_once_with_every_attempt(self):
        service = self.make_service('view', [
            worker('only', 'https://only.invalid/'),
        ], {'https://only.invalid/': [response(500, b'boom')] * 3})

        with mock.patch('service.Service.time.sleep'):
            self.assertIsNone(service._get('view', 'worker', retry=3))

        self.assertEqual(self.attempts(service),
                         {('view', 'only', 'http_500'): 3})
        self.assertEqual(self.calls(service), {('view', 'exhausted'): 1})

    def test_single_worker_412_counts_every_hit_as_rate_limited(self):
        service = self.make_service('view', [
            worker('only', 'https://only.invalid/'),
        ], {'https://only.invalid/': [response(412, b'blocked')] * 3})

        with mock.patch('service.Service.time.sleep'):
            self.assertIsNone(service._get('view', 'worker', retry=3))

        self.assertEqual(self.attempts(service),
                         {('view', 'only', 'rate_limited:http_412'): 3})
        self.assertEqual(self.calls(service), {('view', 'exhausted'): 1})

    @mock.patch('service.worker.random.choice', side_effect=lambda items: items[0])
    def test_pool_exhaustion_records_the_call_as_rate_limited(self, _choice):
        service = self.make_service('view', [
            worker('a', 'https://a.invalid/'),
            worker('b', 'https://b.invalid/'),
        ], {
            'https://a.invalid/': [response(412, b'blocked')],
            'https://b.invalid/': [response(412, b'blocked')],
        })

        with mock.patch('service.Service.time.sleep'):
            with self.assertRaises(RateLimitError):
                service._get('view', 'worker', retry=5)

        self.assertEqual(self.attempts(service), {
            ('view', 'a', 'rate_limited:http_412'): 1,
            ('view', 'b', 'rate_limited:http_412'): 1,
        })
        self.assertEqual(self.calls(service), {('view', 'rate_limited'): 1})

    def test_rate_limit_log_line_names_the_action_taken(self):
        service = self.make_service('view', [
            worker('only', 'https://only.invalid/'),
        ], {'https://only.invalid/': [response(412, b'blocked')]})

        with mock.patch('service.Service.time.sleep'), \
                self.assertLogs('Service', level=logging.WARNING) as logs:
            service._get('view', 'worker', retry=1)

        line = '\n'.join(logs.output)
        self.assertIn('RATE_LIMIT target=view worker_id=only', line)
        self.assertIn('reason=http_412', line)
        self.assertIn('status=412', line)
        self.assertIn('action=retry_same_worker_no_failover', line)

    @mock.patch('service.worker.random.choice', side_effect=lambda items: items[0])
    def test_rate_limit_log_line_names_the_cooldown_action(self, _choice):
        service = self.make_service('view', [
            worker('a', 'https://a.invalid/'),
            worker('b', 'https://b.invalid/'),
        ], {
            'https://a.invalid/': [response(412, b'blocked')],
            'https://b.invalid/': [response(200, {'code': 0})],
        })

        with mock.patch('service.Service.time.sleep'), \
                self.assertLogs('Service', level=logging.WARNING) as logs:
            service._get('view', 'worker')

        self.assertIn('action=cooldown_1800s_then_other_worker',
                      '\n'.join(logs.output))

    def test_direct_mode_rate_limit_is_counted_and_logged(self):
        service = Service(mode='direct', retry=3, colddown_factor=0,
                          endpoints=endpoints_for('view', [
                              worker('a', 'https://a.invalid/')]))
        service._session = ScriptedSession(
            {'https://direct.invalid/': [response(412, b'blocked')]})

        with mock.patch('service.Service.time.sleep'), \
                self.assertLogs('Service', level=logging.WARNING) as logs:
            with self.assertRaises(RateLimitError):
                service._get('view', 'direct')

        self.assertIn('action=raise_direct_mode', '\n'.join(logs.output))
        self.assertEqual(self.attempts(service),
                         {('view', 'direct', 'rate_limited:http_412'): 1})
        self.assertEqual(self.calls(service), {('view', 'rate_limited'): 1})
