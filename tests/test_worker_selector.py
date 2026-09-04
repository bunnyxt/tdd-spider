import json
import logging
from concurrent.futures import ThreadPoolExecutor
from unittest import TestCase, mock

import requests

from service import (Service, RateLimitError,
                     WorkerSelector, default_rate_limit, member_card_rate_limit)


def worker(worker_id, url, platform='test', weight=1, enabled=True):
    return {
        'id': worker_id,
        'url': url,
        'platform': platform,
        'weight': weight,
        'enabled': enabled,
    }


def endpoints_for(target, workers):
    return {target: {'direct': 'https://direct.invalid/', 'workers': workers}}


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def response(status, body):
    item = requests.Response()
    item.status_code = status
    item.headers = {}
    item._content = (body if isinstance(body, bytes)
                     else json.dumps(body).encode())
    item._content_consumed = True
    item.url = 'https://worker.invalid/'
    return item


class ScriptedSession:
    def __init__(self, responses_by_url):
        self.responses_by_url = {
            url: list(responses) for url, responses in responses_by_url.items()
        }
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append(url)
        return self.responses_by_url[url].pop(0)


class WorkerSelectorConfigTest(TestCase):
    def test_legacy_url_and_new_objects_are_supported(self):
        selector = WorkerSelector(endpoints_for('view', [
            'https://legacy.invalid/',
            worker('new', 'https://new.invalid/', weight=2),
            worker('off', 'https://off.invalid/', enabled=False),
        ]))

        with mock.patch('service.worker.random.choice',
                        side_effect=lambda items: items[0]) as choose:
            selector.select('view')
        available = choose.call_args.args[0]
        self.assertEqual([item.id for item in available].count('new'), 2)
        self.assertEqual(sum(item.url == 'https://legacy.invalid/'
                             for item in available), 1)
        self.assertNotIn('off', {item.id for item in available})

    def test_invalid_new_worker_fields_are_rejected(self):
        invalid = [
            worker('', 'https://worker.invalid/'),
            worker('w', ''),
            worker('w', 'https://worker.invalid/', platform=''),
            worker('w', 'https://worker.invalid/', weight=0),
            worker('w', 'https://worker.invalid/', weight=True),
            worker('w', 'https://worker.invalid/', enabled='yes'),
        ]
        for item in invalid:
            with self.subTest(item=item), self.assertRaises(ValueError):
                WorkerSelector(endpoints_for('view', [item]))

    def test_unknown_new_worker_field_is_rejected(self):
        item = worker('w', 'https://worker.invalid/')
        item['enable'] = False
        with self.assertRaisesRegex(ValueError, 'Unknown worker field'):
            WorkerSelector(endpoints_for('view', [item]))

    def test_worker_ids_only_need_to_be_unique_within_a_target(self):
        endpoints = {
            'view': {'workers': [worker('same', 'https://one.invalid/')]},
            'card': {'workers': [worker('same', 'https://two.invalid/')]},
        }
        WorkerSelector(endpoints)

        endpoints['view']['workers'].append(
            worker('same', 'https://three.invalid/'))
        with self.assertRaisesRegex(ValueError, 'Duplicate worker id'):
            WorkerSelector(endpoints)

    def test_service_exits_on_invalid_worker_configuration(self):
        endpoints = endpoints_for('view', [
            worker('same', 'https://one.invalid/'),
            worker('same', 'https://two.invalid/'),
        ])
        with self.assertRaises(SystemExit):
            Service(mode='worker', endpoints=endpoints)

    def test_service_exits_on_invalid_request_mode(self):
        with self.assertRaises(SystemExit):
            Service(mode='invalid', endpoints={})

    def test_worker_configuration_is_validated_when_target_is_used(self):
        service = Service(mode='worker', endpoints={
            'unused': {'workers': []},
            'view': {'workers': [worker('view-a', 'https://a.invalid/')]},
        })
        self.assertEqual(service._worker_selector.select('view').id, 'view-a')
        with self.assertRaises(ValueError):
            service._worker_selector.select('unused')


class WorkerSelectorStateTest(TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.selector = WorkerSelector({
            'view': {'workers': [
                worker('view-a', 'https://a.invalid/'),
                worker('view-b', 'https://b.invalid/'),
            ]},
            'card': {'workers': [
                worker('card-a', 'https://card.invalid/'),
            ]},
        }, clock=self.clock)

    def test_rate_limit_is_target_scoped_and_cooldown_restores_directly(self):
        view_a = next(item for item in self.selector._workers['view']
                      if item.id == 'view-a')
        self.selector.mark_rate_limited(
            'view', view_a, reason='http_412', cooldown_s=30)

        self.assertEqual(self.selector.select('view').id, 'view-b')
        self.assertEqual(self.selector.select('card').id, 'card-a')
        self.clock.now += 30
        with self.assertLogs('Service', logging.INFO) as captured, \
                mock.patch('service.worker.random.choice', side_effect=lambda items: items[0]):
            self.assertEqual(self.selector.select('view').id, 'view-a')
        self.assertIn('worker_rate_limit_cleared', '\n'.join(captured.output))

    def test_empty_pool_is_reported(self):
        selected = self.selector.select('card')
        with self.assertLogs('Service', logging.ERROR) as captured:
            with self.assertRaises(RateLimitError) as raised:
                self.selector.mark_rate_limited(
                    'card', selected, reason='code_-352', cooldown_s=30)
        self.assertEqual(raised.exception.target, 'card')
        self.assertEqual(raised.exception.first_seen, 1000.0)
        self.assertEqual(raised.exception.retry_at, 1030.0)
        self.assertIn('worker_pool_rate_limited', '\n'.join(captured.output))

    def test_repeated_rate_limit_preserves_first_seen_and_extends_retry_at(self):
        view_a = next(item for item in self.selector._workers['view']
                      if item.id == 'view-a')
        self.selector.mark_rate_limited(
            'view', view_a, reason='http_412', cooldown_s=30)
        self.clock.now += 10
        self.selector.mark_rate_limited(
            'view', view_a, reason='http_412', cooldown_s=30)

        first_seen, retry_at = self.selector.rate_limit_window('view')
        self.assertEqual(first_seen, 1000.0)
        self.assertEqual(retry_at, 1040.0)

    def test_expired_other_worker_does_not_cause_pool_exhaustion(self):
        view_a, view_b = self.selector._workers['view']
        self.selector.mark_rate_limited(
            'view', view_a, reason='http_412', cooldown_s=30)
        self.clock.now += 31

        self.selector.mark_rate_limited(
            'view', view_b, reason='http_412', cooldown_s=30)

        self.assertEqual(self.selector.select('view').id, 'view-a')

    def test_selection_uses_state_lock(self):
        state_lock = mock.MagicMock()
        self.selector._lock = state_lock
        self.selector.select('view')
        state_lock.__enter__.assert_called_once()

    def test_300_threads_select_safely_after_transition(self):
        view_a = next(item for item in self.selector._workers['view']
                      if item.id == 'view-a')
        self.selector.mark_rate_limited(
            'view', view_a, reason='http_412', cooldown_s=30)
        with ThreadPoolExecutor(max_workers=300) as pool:
            selected = list(pool.map(lambda _: self.selector.select('view'), range(3000)))
        self.assertEqual({item.id for item in selected}, {'view-b'})

    def test_50_concurrent_rate_limit_calls_leave_consistent_state(self):
        view_a = next(item for item in self.selector._workers['view']
                      if item.id == 'view-a')
        with ThreadPoolExecutor(max_workers=50) as pool:
            list(pool.map(lambda _: self.selector.mark_rate_limited(
                'view', view_a, reason='http_412', cooldown_s=30), range(50)))
        self.assertEqual(self.selector.select('view').id, 'view-b')


class RateLimitCheckerTest(TestCase):
    def test_default_checker_only_handles_http_412(self):
        limited = default_rate_limit(response(412, b'blocked'))
        self.assertEqual((limited.reason, limited.cooldown_s), ('http_412', 1800))
        self.assertIsNone(default_rate_limit(response(200, {'code': -412})))
        self.assertIsNone(default_rate_limit(response(200, {'code': -352})))

    def test_member_card_extends_default_with_json_352(self):
        limited = member_card_rate_limit(response(200, {'code': -352}))
        self.assertEqual((limited.reason, limited.cooldown_s), ('code_-352', 300))
        self.assertEqual(member_card_rate_limit(response(412, b'blocked')).reason,
                         'http_412')
        self.assertIsNone(member_card_rate_limit(response(200, {'code': -404})))
        self.assertIsNone(member_card_rate_limit(response(500, {'code': -352})))
        self.assertIsNone(member_card_rate_limit(response(200, b'not json')))


class ServiceWorkerRoutingTest(TestCase):
    def make_service(self, target, workers, responses):
        service = Service(
            mode='worker', retry=3, colddown_factor=0,
            endpoints=endpoints_for(target, workers))
        service._session = ScriptedSession(responses)
        return service

    @mock.patch('service.worker.random.choice', side_effect=lambda items: items[0])
    def test_http_412_disables_worker_and_retry_uses_another(self, _choice):
        service = self.make_service('view', [
            worker('a', 'https://a.invalid/'),
            worker('b', 'https://b.invalid/'),
        ], {
            'https://a.invalid/': [response(412, b'blocked')],
            'https://b.invalid/': [response(200, {'code': 0})],
        })

        with mock.patch('service.Service.time.sleep'):
            result = service._get('view', 'worker')

        self.assertEqual(result, {'code': 0})
        self.assertEqual(service._session.calls,
                         ['https://a.invalid/', 'https://b.invalid/'])

    @mock.patch('service.worker.random.choice', side_effect=lambda items: items[0])
    def test_json_352_uses_member_card_checker_without_60_second_sleep(self, _choice):
        service = self.make_service('get_member_card', [
            worker('a', 'https://a.invalid/'),
            worker('b', 'https://b.invalid/'),
        ], {
            'https://a.invalid/': [response(200, {'code': -352})],
            'https://b.invalid/': [response(200, {'code': 0})],
        })

        with mock.patch('service.Service.time.sleep') as sleep:
            result = service._get('get_member_card', 'worker')

        self.assertEqual(result, {'code': 0})
        self.assertNotIn(mock.call(60), sleep.mock_calls)
        self.assertEqual(service._session.calls,
                         ['https://a.invalid/', 'https://b.invalid/'])

    def test_member_card_uses_generic_json_parser_and_retries_non_json(self):
        service = self.make_service('get_member_card', [
            worker('a', 'https://a.invalid/'),
        ], {'https://a.invalid/': [
            response(200, b'not json'),
            response(200, {'code': 0}),
        ]})

        with mock.patch('service.Service.time.sleep'):
            result = service._get('get_member_card', 'worker')

        self.assertEqual(result, {'code': 0})
        self.assertEqual(service._session.calls,
                         ['https://a.invalid/', 'https://a.invalid/'])

    @mock.patch('service.worker.random.choice', side_effect=lambda items: items[0])
    def test_member_card_public_method_uses_special_checker(self, _choice):
        valid = {
            'code': 0, 'message': '0', 'ttl': 1,
            'data': {'card': {
                'mid': '123', 'name': 'name', 'sex': '保密',
                'face': 'https://image.invalid/', 'sign': '',
            }},
        }
        service = self.make_service('get_member_card', [
            worker('a', 'https://a.invalid/'),
            worker('b', 'https://b.invalid/'),
        ], {
            'https://a.invalid/': [response(200, {'code': -352})],
            'https://b.invalid/': [response(200, valid)],
        })

        with mock.patch('service.Service.time.sleep') as sleep:
            card = service.get_member_card({'mid': 123})

        self.assertEqual((card.mid, card.name), ('123', 'name'))
        self.assertNotIn(mock.call(60), sleep.mock_calls)

    def test_single_rate_limited_worker_raises_pool_exhausted_immediately(self):
        service = self.make_service('view', [
            worker('a', 'https://a.invalid/'),
        ], {'https://a.invalid/': [response(412, b'blocked')]})

        with mock.patch('service.Service.time.sleep') as sleep:
            with self.assertRaises(RateLimitError):
                service._get('view', 'worker', retry=20)

        sleep.assert_not_called()
        self.assertEqual(service._session.calls, ['https://a.invalid/'])

    def test_ordinary_failure_can_retry_the_same_worker(self):
        service = self.make_service('view', [
            worker('a', 'https://a.invalid/'),
        ], {'https://a.invalid/': [
            response(500, b'upstream failed'),
            response(200, {'code': 0}),
        ]})

        with mock.patch('service.Service.time.sleep'):
            result = service._get('view', 'worker')

        self.assertEqual(result, {'code': 0})
        self.assertEqual(service._session.calls,
                         ['https://a.invalid/', 'https://a.invalid/'])

    def test_direct_rate_limit_does_not_use_selector(self):
        service = Service(mode='direct', endpoints=endpoints_for('view', []))
        service._session = ScriptedSession({
            'https://direct.invalid/': [response(412, b'blocked')],
        })
        with mock.patch.object(service._worker_selector, 'select') as select:
            with self.assertRaises(RateLimitError) as raised:
                service._get('view', 'direct')
        self.assertEqual(raised.exception.reason, 'http_412')
        self.assertGreater(raised.exception.retry_at, 0)
        select.assert_not_called()
