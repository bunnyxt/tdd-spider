import unittest
from unittest import mock

from service import CodeError, FormatError, Service, VideoTag, VideoTags
from service.endpoints import get_video_tags


def valid_response():
    return {
        'code': 0,
        'message': '0',
        'ttl': 1,
        'data': [
            {'tag_id': 7, 'tag_name': 'tag'},
            {'tag_id': 11, 'tag_name': 'next'},
        ],
    }


class GetVideoTagsEndpointTest(unittest.TestCase):
    def test_adapts_a_valid_response_and_passes_request_options(self):
        request = mock.Mock(return_value=valid_response())

        result = get_video_tags.get(
            request, params={'aid': 7}, headers={'X-Test': 'yes'}, retry=2,
            timeout=3.0, colddown_factor=0.0)

        self.assertEqual(result, VideoTags([
            VideoTag(7, 'tag'), VideoTag(11, 'next')]))
        self.assertEqual(request.call_args.args, ('get_video_tags',))
        self.assertEqual(request.call_args.kwargs, {
            'params': {'aid': 7}, 'headers': {'X-Test': 'yes'}, 'retry': 2,
            'timeout': 3.0, 'colddown_factor': 0.0,
            'parser': get_video_tags.parse,
        })

    def test_parser_retries_invalid_json_and_configured_codes(self):
        self.assertIsNone(get_video_tags.parse('not json'))
        self.assertIsNone(get_video_tags.parse('{"code": -500}'))
        self.assertIsNone(get_video_tags.parse('{"code": -504}'))
        self.assertEqual(get_video_tags.parse('{"code": 0}'), {'code': 0})

    def test_nonzero_api_code_keeps_the_existing_business_error(self):
        response = valid_response()
        response['code'] = -404

        with self.assertRaises(CodeError) as raised:
            get_video_tags.get(lambda *args, **kwargs: response,
                               params={'aid': 7})

        self.assertEqual((raised.exception.endpoint, raised.exception.result_type,
                          raised.exception.code),
                         ('get_video_tags', VideoTags, -404))

    def test_invalid_shape_keeps_the_existing_format_error(self):
        response = valid_response()
        del response['data'][0]['tag_name']

        with self.assertRaises(FormatError) as raised:
            get_video_tags.get(lambda *args, **kwargs: response,
                               params={'aid': 7})

        self.assertEqual(raised.exception.endpoint, 'get_video_tags')
        self.assertIs(raised.exception.result_type, VideoTags)

    def test_service_public_method_delegates_to_the_adapter(self):
        service = Service(mode='direct', endpoints={})
        service._get = mock.Mock(return_value=valid_response())

        result = service.get_video_tags({'aid': 7})

        self.assertEqual(result, VideoTags([
            VideoTag(7, 'tag'), VideoTag(11, 'next')]))
        self.assertEqual(service._get.call_args.args, ('get_video_tags',))
        self.assertEqual(service._get.call_args.kwargs, {
            'params': {'aid': 7}, 'headers': None, 'retry': None,
            'timeout': None, 'colddown_factor': None,
            'parser': get_video_tags.parse,
        })


if __name__ == '__main__':
    unittest.main()
