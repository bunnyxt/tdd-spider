import logging
from unittest import TestCase, mock

from service import RateLimitError, Service

from test_worker_selector import (ScriptedSession, endpoints_for, response,
                                  worker)


class ApiDebugLineTest(TestCase):
    def make_service(self, endpoint, workers, responses):
        service = Service(
            mode='worker', retry=3, colddown_factor=0,
            endpoints=endpoints_for(endpoint, workers))
        service._session = ScriptedSession(responses)
        return service

    def test_one_line_per_attempt_names_endpoint_worker_and_result(self):
        service = self.make_service('view', [
            worker('only', 'https://only.invalid/'),
        ], {'https://only.invalid/': [
            response(500, b'upstream failed'),
            response(200, {'code': 0}),
        ]})

        with mock.patch('service.Service.time.sleep'), \
                self.assertLogs('Service', level=logging.DEBUG) as logs:
            service._get('view')

        api_lines = [line for line in logs.output if 'API endpoint=' in line]
        self.assertEqual(len(api_lines), 2)
        self.assertIn('endpoint=view worker=only trial=1 outcome=http_500 status=500',
                      api_lines[0])
        self.assertIn('endpoint=view worker=only trial=2 outcome=ok status=200',
                      api_lines[1])

    def test_every_rate_limited_attempt_gets_its_own_line(self):
        service = self.make_service('view', [
            worker('only', 'https://only.invalid/'),
        ], {'https://only.invalid/': [response(412, b'blocked')] * 3})

        with mock.patch('service.Service.time.sleep'), \
                self.assertLogs('Service', level=logging.DEBUG) as logs:
            with self.assertRaises(RateLimitError):
                service._get('view', retry=3)

        api_lines = [line for line in logs.output if 'API endpoint=' in line]
        self.assertEqual(len(api_lines), 3)
        self.assertTrue(all('status=412' in line and 'outcome=http_412' in line
                            for line in api_lines))

    def test_in_body_rate_limit_is_reported_as_the_result(self):
        service = self.make_service('get_member_card', [
            worker('card', 'https://card.invalid/'),
        ], {'https://card.invalid/': [response(200, {'code': -352})]})

        with mock.patch('service.Service.time.sleep'), \
                self.assertLogs('Service', level=logging.DEBUG) as logs:
            with self.assertRaises(RateLimitError):
                service._get('get_member_card', retry=1)

        api_lines = [line for line in logs.output if 'API endpoint=' in line]
        self.assertEqual(len(api_lines), 1)
        self.assertIn('status=200', api_lines[0])
        self.assertIn('outcome=code_-352', api_lines[0])

    def test_direct_mode_reports_the_worker_as_direct(self):
        service = Service(mode='direct', retry=1, colddown_factor=0,
                          endpoints=endpoints_for('view', [
                              worker('a', 'https://a.invalid/')]))
        service._session = ScriptedSession(
            {'https://direct.invalid/': [response(200, {'code': 0})]})

        with self.assertLogs('Service', level=logging.DEBUG) as logs:
            service._get('view')

        api_lines = [line for line in logs.output if 'API endpoint=' in line]
        self.assertEqual(len(api_lines), 1)
        self.assertIn('worker=direct trial=1 outcome=ok status=200', api_lines[0])
