import unittest
from unittest import mock

from service import CodeError, FormatError, Newlist, Service
from service.endpoints import get_newlist


def valid_response():
    return {
        'code': 0, 'message': '0',
        'data': {
            'archives': [{
                'aid': 7, 'videos': 1, 'tid': 3, 'tname': 'music', 'copyright': 1,
                'pic': 'pic', 'title': 'title', 'bvid': 'BV1', 'desc': 'desc',
                'stat': {'aid': 7, 'view': 1, 'danmaku': 2, 'reply': 3, 'favorite': 4,
                         'coin': 5, 'share': 6, 'now_rank': 0, 'his_rank': 0,
                         'like': 7, 'dislike': 0, 'vt': 8, 'vv': 9},
                'owner': {'mid': 8, 'name': 'owner', 'face': 'face'},
            }],
            'page': {'count': 1, 'num': 1, 'size': 50},
        },
    }


class GetNewlistEndpointTest(unittest.TestCase):
    def test_adapts_response_and_passes_options(self):
        request = mock.Mock(return_value=valid_response())
        result = get_newlist.get(request, params={'rid': 3, 'pn': 1, 'ps': 50},
                                 headers={'X-Test': 'yes'}, retry=2, timeout=3.0,
                                 colddown_factor=0.0)
        self.assertIsInstance(result, Newlist)
        self.assertEqual(result.archives[0].owner.name, 'owner')
        self.assertEqual(result.archives[0].stat.vv, 9)
        self.assertEqual(result.page.size, 50)
        self.assertEqual(request.call_args.args, ('get_newlist',))
        self.assertEqual(request.call_args.kwargs['parser'], get_newlist.parse)

    def test_parser_retries_invalid_json_and_configured_code(self):
        self.assertIsNone(get_newlist.parse('not json'))
        self.assertIsNone(get_newlist.parse('{"code": -40002}'))
        self.assertEqual(get_newlist.parse('{"code": 0}'), {'code': 0})

    def test_nonzero_code_keeps_business_error(self):
        response = valid_response()
        response['code'] = -404
        with self.assertRaises(CodeError) as raised:
            get_newlist.get(lambda *args, **kwargs: response)
        self.assertEqual((raised.exception.endpoint, raised.exception.result_type,
                          raised.exception.code), ('get_newlist', Newlist, -404))

    def test_invalid_shape_keeps_format_error(self):
        response = valid_response()
        del response['data']['archives'][0]['owner']['face']
        with self.assertRaises(FormatError) as raised:
            get_newlist.get(lambda *args, **kwargs: response)
        self.assertEqual(raised.exception.endpoint, 'get_newlist')
        self.assertIs(raised.exception.result_type, Newlist)

    def test_service_method_delegates_to_adapter(self):
        service = Service(mode='direct', endpoints={})
        service._get = mock.Mock(return_value=valid_response())
        result = service.get_newlist({'rid': 3, 'pn': 1, 'ps': 50})
        self.assertIsInstance(result, Newlist)
        self.assertEqual(service._get.call_args.args, ('get_newlist',))
        self.assertEqual(service._get.call_args.kwargs['parser'], get_newlist.parse)


if __name__ == '__main__':
    unittest.main()
