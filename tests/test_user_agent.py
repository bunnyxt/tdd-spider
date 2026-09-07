import json
from unittest import TestCase, mock

import requests

from service import Service

from test_worker_selector import endpoints_for, response, worker


class RecordingSession:
    def __init__(self, reply):
        self.reply = reply
        self.sent_agents = []

    def get(self, url, **kwargs):
        self.sent_agents.append(kwargs['headers']['User-Agent'])
        item = requests.Response()
        item.status_code = 200
        item.headers = {}
        item._content = json.dumps(self.reply).encode()
        item._content_consumed = True
        item.url = url
        return item


class UserAgentTest(TestCase):
    def make_service(self, **kwargs):
        service = Service(
            mode='worker', colddown_factor=0,
            endpoints=endpoints_for('view', [worker('a', 'https://a.invalid/')]),
            **kwargs)
        service._session = RecordingSession({'code': 0})
        return service

    def test_the_agent_is_drawn_again_for_every_request(self):
        # the agent used to be written into self._headers on the first request,
        # so a process picked one at startup and sent it for its whole run
        service = self.make_service()
        for _ in range(40):
            service._get('view')

        self.assertEqual(len(service._session.sent_agents), 40)
        self.assertGreater(len(set(service._session.sent_agents)), 1)

    def test_default_headers_are_not_mutated_by_a_request(self):
        service = self.make_service()
        service._get('view')
        self.assertEqual(service._headers, {})
        self.assertEqual(service.get_default_headers(), {})

    def test_configured_headers_survive_and_are_not_mutated(self):
        service = self.make_service(headers={'Referer': 'https://ref.invalid/'})
        service._get('view')
        service._get('view')
        self.assertEqual(service._headers, {'Referer': 'https://ref.invalid/'})

    def test_an_explicit_agent_is_left_alone(self):
        service = self.make_service()
        for _ in range(5):
            service._get('view', headers={'User-Agent': 'mine/1.0'})
        self.assertEqual(set(service._session.sent_agents), {'mine/1.0'})

    def test_the_symbian_agent_is_not_offered(self):
        # measured 20 rejections in 50 requests (40%); every other agent in the
        # list returned 50/50 clean in the same interleaved run
        service = self.make_service()
        self.assertFalse(any('Symbian' in agent for agent in service._ua_list))
        self.assertFalse(any('NokiaN97' in agent for agent in service._ua_list))
