import logging
from unittest import TestCase, mock

from service import RateLimitError, Service

from test_worker_selector import (ScriptedSession, endpoints_for, response,
                                  worker)


class ApiDebugLineTest(TestCase):
    def make_service(self, target, workers, responses):
        service = Service(
            mode='worker', retry=3, colddown_factor=0,
            endpoints=endpoints_for(target, workers))
        service._session = ScriptedSession(responses)
        return service

    def test_one_line_per_attempt_names_target_worker_and_result(self):
        service = self.make_service('view', [
            worker('only', 'https://only.invalid/'),
        ], {'https://only.invalid/': [
            response(500, b'upstream failed'),
            response(200, {'code': 0}),
        ]})

        with mock.patch('service.Service.time.sleep'), \
                self.assertLogs('Service', level=logging.DEBUG) as logs:
            service._get('view', 'worker')

        api_lines = [line for line in logs.output if 'API target: ' in line]
        self.assertEqual(len(api_lines), 2)
        self.assertIn('target: view, worker: only, status: 500, result: ok',
                      api_lines[0])
        self.assertIn('target: view, worker: only, status: 200, result: ok',
                      api_lines[1])

    def test_the_line_is_written_before_a_rate_limit_aborts_the_call(self):
        # the raise happens right after this line, so the reason a call ended
        # is still on record
        service = self.make_service('view', [
            worker('only', 'https://only.invalid/'),
        ], {'https://only.invalid/': [response(412, b'blocked')]})

        with mock.patch('service.Service.time.sleep'), \
                self.assertLogs('Service', level=logging.DEBUG) as logs:
            with self.assertRaises(RateLimitError):
                service._get('view', 'worker')

        api_lines = [line for line in logs.output if 'API target: ' in line]
        self.assertEqual(len(api_lines), 1)
        self.assertIn('status: 412, result: http_412', api_lines[0])

    def test_in_body_rate_limit_is_reported_as_the_result(self):
        service = self.make_service('get_member_card', [
            worker('card', 'https://card.invalid/'),
        ], {'https://card.invalid/': [response(200, {'code': -352})]})

        with mock.patch('service.Service.time.sleep'), \
                self.assertLogs('Service', level=logging.DEBUG) as logs:
            with self.assertRaises(RateLimitError):
                service._get('get_member_card', 'worker', retry=1)

        api_lines = [line for line in logs.output if 'API target: ' in line]
        self.assertEqual(len(api_lines), 1)
        self.assertIn('status: 200, result: code_-352', api_lines[0])

    def test_direct_mode_reports_the_worker_as_direct(self):
        service = Service(mode='direct', retry=1, colddown_factor=0,
                          endpoints=endpoints_for('view', [
                              worker('a', 'https://a.invalid/')]))
        service._session = ScriptedSession(
            {'https://direct.invalid/': [response(200, {'code': 0})]})

        with self.assertLogs('Service', level=logging.DEBUG) as logs:
            service._get('view', 'direct')

        api_lines = [line for line in logs.output if 'API target: ' in line]
        self.assertEqual(len(api_lines), 1)
        self.assertIn('worker: direct, status: 200, result: ok', api_lines[0])
