"""
Tests that ``Service._get`` actually feeds its ``ApiStatTracker``: every
attempt outcome it can produce must reach the tracker with the exact
same classification the existing per-attempt DEBUG line already uses.

Run from the repo root:

    python -m unittest discover -s tests
"""

import os
import sys
import unittest
from unittest import mock

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from service import RateLimitError, ResponseError, Service  # noqa: E402
from service.apistat import ApiStatTracker, NullApiStat  # noqa: E402
from test_worker_selector import ScriptedSession, endpoints_for, response, worker  # noqa: E402


def totals_dict(tracker):
    return {(row['scope'], row['name']): row['value'] for row in tracker.totals()}


def raising_response(exc):
    """A Response whose body read raises ``exc`` -- simulates the connection
    dying mid-download (Service's ``body_exception`` branch)."""
    r = response(200, b'placeholder')
    r.headers = {}

    def _iter_content(chunk_size=1):
        raise exc
        yield  # pragma: no cover - makes this a generator function

    r.iter_content = _iter_content
    return r


class ServiceApiStatWiringTest(unittest.TestCase):
    def make_service(self, target, workers, responses, **kwargs):
        service = Service(mode='worker', retry=kwargs.pop('retry', 3),
                          colddown_factor=0,
                          endpoints=endpoints_for(target, workers), **kwargs)
        service._session = ScriptedSession(responses)
        return service

    def test_success_on_first_trial_is_recorded_ok(self):
        service = self.make_service('view', [worker('w1', 'https://w1.invalid/')],
                                    {'https://w1.invalid/': [response(200, {'code': 0})]})
        service._get('view', 'worker')
        self.assertEqual(totals_dict(service.stats),
                         {('api:view:w1', 't1:ok'): 1.0})

    def test_recovery_after_one_rejection_is_visible_at_both_trials(self):
        service = self.make_service('view', [worker('w1', 'https://w1.invalid/')], {
            'https://w1.invalid/': [response(412, b'blocked'), response(200, {'code': 0})]})
        with mock.patch('service.Service.time.sleep'):
            service._get('view', 'worker')
        self.assertEqual(totals_dict(service.stats), {
            ('api:view:w1', 't1:http_412'): 1.0,
            ('api:view:w1', 't2:ok'): 1.0,
        })

    def test_exhausted_rate_limit_records_every_trial_and_raises(self):
        service = self.make_service('view', [worker('w1', 'https://w1.invalid/')], {
            'https://w1.invalid/': [response(412, b'blocked')] * 3})
        with mock.patch('service.Service.time.sleep'):
            with self.assertRaises(RateLimitError):
                service._get('view', 'worker', retry=3)
        self.assertEqual(totals_dict(service.stats), {
            ('api:view:w1', 't1:http_412'): 1.0,
            ('api:view:w1', 't2:http_412'): 1.0,
            ('api:view:w1', 't3:http_412'): 1.0,
        })

    def test_member_card_in_body_rate_limit_records_its_own_reason(self):
        service = self.make_service('get_member_card', [worker('c1', 'https://c1.invalid/')],
                                    {'https://c1.invalid/': [response(200, {'code': -352})]})
        with mock.patch('service.Service.time.sleep'):
            with self.assertRaises(RateLimitError):
                service._get('get_member_card', 'worker', retry=1)
        self.assertEqual(totals_dict(service.stats),
                         {('api:get_member_card:c1', 't1:code_-352'): 1.0})

    def test_non_rate_limited_non_200_is_recorded_as_http_code(self):
        service = self.make_service('view', [worker('w1', 'https://w1.invalid/')],
                                    {'https://w1.invalid/': [response(500, b'oops')]})
        with mock.patch('service.Service.time.sleep'):
            with self.assertRaises(ResponseError):
                service._get('view', 'worker', retry=1)
        self.assertEqual(totals_dict(service.stats),
                         {('api:view:w1', 't1:http_500'): 1.0})

    def test_json_decode_failure_is_recorded(self):
        service = self.make_service('view', [worker('w1', 'https://w1.invalid/')],
                                    {'https://w1.invalid/': [response(200, b'not json')]})
        with mock.patch('service.Service.time.sleep'):
            with self.assertRaises(ResponseError):
                service._get('view', 'worker', retry=1)
        self.assertEqual(totals_dict(service.stats),
                         {('api:view:w1', 't1:json_error'): 1.0})

    def test_request_exception_is_recorded(self):
        service = self.make_service('view', [worker('w1', 'https://w1.invalid/')], {})

        class RaisingSession:
            def get(self, url, **kwargs):
                raise requests.exceptions.ConnectionError('refused')

        service._session = RaisingSession()
        with mock.patch('service.Service.time.sleep'):
            with self.assertRaises(ResponseError):
                service._get('view', 'worker', retry=1)
        self.assertEqual(totals_dict(service.stats),
                         {('api:view:w1', 't1:request_exception'): 1.0})

    def test_body_exception_is_recorded(self):
        service = self.make_service('view', [worker('w1', 'https://w1.invalid/')],
                                    {'https://w1.invalid/':
                                         [raising_response(requests.exceptions.ChunkedEncodingError())]})
        with mock.patch('service.Service.time.sleep'):
            with self.assertRaises(ResponseError):
                service._get('view', 'worker', retry=1)
        self.assertEqual(totals_dict(service.stats),
                         {('api:view:w1', 't1:body_exception'): 1.0})

    def test_deadline_exceeded_is_recorded(self):
        service = self.make_service('view', [worker('w1', 'https://w1.invalid/')],
                                    {'https://w1.invalid/': [response(200, b'x' * 10)]})
        # trial_start, one over-deadline check inside the chunk loop, then the
        # duration calc after breaking out -- three perf_counter() reads for
        # this single trial
        with mock.patch('service.Service.time.sleep'), \
                mock.patch('service.Service.time.perf_counter',
                           side_effect=[0.0, 100.0, 100.0]):
            with self.assertRaises(ResponseError):
                service._get('view', 'worker', retry=1)
        self.assertEqual(totals_dict(service.stats),
                         {('api:view:w1', 't1:deadline_exceeded'): 1.0})

    def test_direct_mode_records_worker_as_direct(self):
        service = Service(mode='direct', retry=1, colddown_factor=0,
                          endpoints=endpoints_for('view', [worker('a', 'https://a.invalid/')]))
        service._session = ScriptedSession(
            {'https://direct.invalid/': [response(200, {'code': 0})]})
        service._get('view', 'direct')
        self.assertEqual(totals_dict(service.stats),
                         {('api:view:direct', 't1:ok'): 1.0})

    def test_stats_false_records_nothing_and_needs_no_none_check(self):
        # callers never test `stats is not None`; disabling swaps in a no-op
        service = self.make_service('view', [worker('w1', 'https://w1.invalid/')],
                                    {'https://w1.invalid/': [response(200, {'code': 0})]},
                                    stats=False)
        self.assertIsInstance(service.stats, NullApiStat)
        service._get('view', 'worker')
        self.assertEqual(service.stats.totals(), [])
        service.stats.log_summary()

    def test_a_shared_tracker_can_be_injected(self):
        shared = ApiStatTracker()
        service = self.make_service('view', [worker('w1', 'https://w1.invalid/')],
                                    {'https://w1.invalid/': [response(200, {'code': 0})]},
                                    stats=shared)
        self.assertIs(service.stats, shared)
        service._get('view', 'worker')
        self.assertEqual(totals_dict(shared), {('api:view:w1', 't1:ok'): 1.0})

    def test_default_service_creates_its_own_tracker(self):
        service = self.make_service('view', [worker('w1', 'https://w1.invalid/')],
                                    {'https://w1.invalid/': [response(200, {'code': 0})]})
        self.assertIsInstance(service.stats, ApiStatTracker)


if __name__ == '__main__':
    unittest.main()
