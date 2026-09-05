import json
import logging
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from unittest import TestCase, mock

import requests

from service import (Service, RateLimitError, WorkerConfigurationError,
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


class WorkerSelectorSelectionTest(TestCase):
    def setUp(self):
        self.selector = WorkerSelector({
            'view': {'workers': [
                worker('view-a', 'https://a.invalid/', weight=1),
                worker('view-b', 'https://b.invalid/', weight=2),
                worker('view-c', 'https://c.invalid/', weight=3),
                worker('view-off', 'https://off.invalid/', enabled=False),
            ]},
            'card': {'workers': [
                worker('card-a', 'https://card.invalid/'),
            ]},
        })

    def test_weighted_selection_follows_the_configured_ratio(self):
        picked = Counter()
        with mock.patch('service.worker.random.choice',
                        side_effect=lambda items: items[0]) as choose:
            self.selector.select('view')
        pool = choose.call_args.args[0]
        for item in pool:
            picked[item.id] += 1
        self.assertEqual(picked, Counter({'view-c': 3, 'view-b': 2, 'view-a': 1}))

    def test_disabled_workers_are_never_selected(self):
        with mock.patch('service.worker.random.choice',
                        side_effect=lambda items: items[0]) as choose:
            self.selector.select('view')
        self.assertNotIn('view-off',
                         {item.id for item in choose.call_args.args[0]})

    def test_empty_pool_is_reported(self):
        selector = WorkerSelector({'view': {'workers': []}})
        with self.assertRaises(WorkerConfigurationError):
            selector.select('view')

    def test_a_rate_limit_never_takes_a_worker_out_of_the_pool(self):
        # the selector is stateless: nothing a response says can shrink the
        # candidate set, so a limited worker keeps taking its share of traffic
        seen = {self.selector.select('view').id for _ in range(300)}
        self.assertEqual(seen, {'view-a', 'view-b', 'view-c'})

    def test_300_threads_select_safely(self):
        with ThreadPoolExecutor(max_workers=300) as pool:
            selected = list(pool.map(lambda _: self.selector.select('card'),
                                     range(3000)))
        self.assertEqual({item.id for item in selected}, {'card-a'})


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

    def test_http_412_keeps_the_worker_in_the_pool(self):
        # a 412 used to cool this worker down and hand the retry to the other
        # one; it is now just a retry, and the limited worker stays eligible
        service = self.make_service('view', [
            worker('a', 'https://a.invalid/'),
            worker('b', 'https://b.invalid/'),
        ], {
            'https://a.invalid/': [response(412, b'blocked'),
                                   response(200, {'code': 0})],
            'https://b.invalid/': [response(412, b'blocked'),
                                   response(200, {'code': 0})],
        })

        with mock.patch('service.Service.time.sleep'), \
                mock.patch('service.worker.random.choice',
                           side_effect=lambda items: items[0]) as choose:
            result = service._get('view', 'worker')

        self.assertEqual(result, {'code': 0})
        self.assertEqual(service._session.calls,
                         ['https://a.invalid/', 'https://a.invalid/'])
        # the second selection still saw both workers as candidates
        self.assertEqual({item.id for item in choose.call_args.args[0]},
                         {'a', 'b'})

    def test_http_412_on_single_worker_uses_normal_retry_without_cooldown(self):
        service = self.make_service('view', [
            worker('only', 'https://only.invalid/'),
        ], {
            'https://only.invalid/': [
                response(412, b'blocked'),
                response(412, b'blocked'),
                response(412, b'blocked'),
                response(200, {'code': 0}),
            ],
        })

        with mock.patch('service.Service.time.sleep'):
            self.assertIsNone(service._get('view', 'worker', retry=3))
            self.assertEqual(service._get('view', 'worker', retry=1),
                             {'code': 0})

        self.assertEqual(service._session.calls,
                         ['https://only.invalid/'] * 4)

    def test_json_352_on_single_worker_retries_instead_of_returning_body(self):
        service = self.make_service('get_member_card', [
            worker('only', 'https://only.invalid/'),
        ], {
            'https://only.invalid/': [
                response(200, {'code': -352}),
                response(200, {'code': -352}),
                response(200, {'code': 0}),
            ],
        })

        with mock.patch('service.Service.time.sleep'):
            self.assertIsNone(service._get('get_member_card', 'worker', retry=2))
            self.assertEqual(service._get('get_member_card', 'worker', retry=1),
                             {'code': 0})

        self.assertEqual(service._session.calls,
                         ['https://only.invalid/'] * 3)

    @mock.patch('service.worker.random.choice', side_effect=lambda items: items[0])
    def test_json_352_uses_member_card_checker_without_60_second_sleep(self, _choice):
        service = self.make_service('get_member_card', [
            worker('a', 'https://a.invalid/'),
            worker('b', 'https://b.invalid/'),
        ], {
            'https://a.invalid/': [response(200, {'code': -352}),
                                   response(200, {'code': 0})],
            'https://b.invalid/': [response(200, {'code': 0})],
        })

        with mock.patch('service.Service.time.sleep') as sleep:
            result = service._get('get_member_card', 'worker')

        self.assertEqual(result, {'code': 0})
        self.assertNotIn(mock.call(60), sleep.mock_calls)
        self.assertEqual(service._session.calls,
                         ['https://a.invalid/', 'https://a.invalid/'])

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
            'https://a.invalid/': [response(200, {'code': -352}),
                                   response(200, valid)],
            'https://b.invalid/': [response(200, valid)],
        })

        with mock.patch('service.Service.time.sleep') as sleep:
            card = service.get_member_card({'mid': 123})

        self.assertEqual((card.mid, card.name), ('123', 'name'))
        self.assertNotIn(mock.call(60), sleep.mock_calls)

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
