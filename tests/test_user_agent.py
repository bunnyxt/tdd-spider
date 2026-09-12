import json
from unittest import TestCase, mock

import requests

from service import Service
from service.ua import UA_LIST

from test_worker_selector import ScriptedSession, endpoints_for, response, worker


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
        # removed from the list that used to live in Service; UA_LIST still
        # ships, so the exclusion still applies
        self.assertFalse(any('Symbian' in agent for agent in UA_LIST))
        self.assertFalse(any('NokiaN97' in agent for agent in UA_LIST))


class UserAgentPoolTest(TestCase):
    """The pool comes from endpoints.json; UA_LIST is what an endpoint that
    configures none uses."""

    def endpoints_with_agents(self, agents):
        endpoints = endpoints_for('view', [worker('a', 'https://a.invalid/')])
        endpoints['view']['user_agents'] = agents
        return endpoints

    def make_service(self, endpoints):
        service = Service(mode='worker', colddown_factor=0, endpoints=endpoints)
        service._session = RecordingSession({'code': 0})
        return service

    def test_a_configured_pool_replaces_ua_list(self):
        service = self.make_service(self.endpoints_with_agents(['x-1', 'x-2']))
        for _ in range(40):
            service._get('view')
        self.assertEqual(set(service._session.sent_agents), {'x-1', 'x-2'})

    def test_an_endpoint_without_a_pool_uses_ua_list(self):
        service = self.make_service(
            endpoints_for('view', [worker('a', 'https://a.invalid/')]))
        for _ in range(40):
            service._get('view')
        self.assertTrue(set(service._session.sent_agents) <= set(UA_LIST))
        self.assertGreater(len(set(service._session.sent_agents)), 1)

    def test_every_attempt_draws_again(self):
        endpoints = self.endpoints_with_agents(['x-1', 'x-2', 'x-3'])
        service = Service(mode='worker', retry=3, colddown_factor=0,
                          endpoints=endpoints)
        service._session = ScriptedSession(
            {'https://a.invalid/': [response(412, b'no'), response(412, b'no'),
                                    response(200, {'code': 0})]})
        with mock.patch('service.Service.time.sleep'):
            service._get('view')
        sent = service._session.agents
        self.assertEqual(len(sent), 3)
        self.assertTrue(set(sent) <= {'x-1', 'x-2', 'x-3'})

    def test_a_single_entry_pool_repeats(self):
        endpoints = self.endpoints_with_agents(['only-one'])
        service = Service(mode='worker', retry=2, colddown_factor=0,
                          endpoints=endpoints)
        service._session = ScriptedSession(
            {'https://a.invalid/': [response(412, b'no'), response(200, {'code': 0})]})
        with mock.patch('service.Service.time.sleep'):
            service._get('view')
        self.assertEqual(service._session.agents, ['only-one', 'only-one'])

    def test_a_caller_supplied_agent_survives_every_retry(self):
        endpoints = self.endpoints_with_agents(['x-1', 'x-2'])
        service = Service(mode='worker', retry=3, colddown_factor=0,
                          endpoints=endpoints)
        service._session = ScriptedSession(
            {'https://a.invalid/': [response(412, b'no'), response(412, b'no'),
                                    response(200, {'code': 0})]})
        with mock.patch('service.Service.time.sleep'):
            service._get('view', headers={'User-Agent': 'mine'})
        self.assertEqual(service._session.agents, ['mine', 'mine', 'mine'])

    def test_an_empty_pool_uses_ua_list(self):
        service = self.make_service(self.endpoints_with_agents([]))
        for _ in range(20):
            service._get('view')
        self.assertTrue(set(service._session.sent_agents) <= set(UA_LIST))
