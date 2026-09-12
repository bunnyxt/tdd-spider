import unittest
from unittest import mock

from service import CodeError, FormatError, Service, VideoViewStat, VideoViewTrimmed
from service.endpoints import get_video_view_trimmed


def valid_response():
    return {
        'code': 0, 'message': '0', 'ttl': 1,
        'data': {
            'bvid': 'BV1', 'aid': 7,
            'stat': {
                'aid': 7, 'view': 1, 'danmaku': 2, 'reply': 3, 'favorite': 4,
                'coin': 5, 'share': 6, 'now_rank': 0, 'his_rank': 0, 'like': 7,
                'dislike': 0, 'vt': 8, 'vv': 9,
            },
        },
    }


class GetVideoViewTrimmedEndpointTest(unittest.TestCase):
    def test_adapts_valid_response_and_passes_request_options(self):
        request = mock.Mock(return_value=valid_response())

        result = get_video_view_trimmed.get(
            request, params={'aid': 7}, headers={'X-Test': 'yes'}, retry=2,
            timeout=3.0, colddown_factor=0.0)

        self.assertEqual(result, VideoViewTrimmed(
            'BV1', 7, VideoViewStat(7, 1, 2, 3, 4, 5, 6, 0, 0, 7, 0, 8, 9)))
        request.assert_called_once_with(
            'get_video_view_trimmed', params={'aid': 7}, headers={'X-Test': 'yes'},
            retry=2, timeout=3.0, colddown_factor=0.0)

    def test_optional_stat_values_are_none_when_absent(self):
        response = valid_response()
        del response['data']['stat']['vt']
        del response['data']['stat']['vv']

        result = get_video_view_trimmed.get(lambda *args, **kwargs: response)

        self.assertIsNone(result.stat.vt)
        self.assertIsNone(result.stat.vv)

    def test_nonzero_api_code_keeps_existing_business_error(self):
        response = valid_response()
        response['code'] = -404

        with self.assertRaises(CodeError) as raised:
            get_video_view_trimmed.get(lambda *args, **kwargs: response)

        self.assertEqual((raised.exception.endpoint, raised.exception.result_type,
                          raised.exception.code),
                         ('get_video_view_trimmed', VideoViewTrimmed, -404))

    def test_invalid_shape_keeps_existing_format_error(self):
        response = valid_response()
        del response['data']['stat']['view']

        with self.assertRaises(FormatError) as raised:
            get_video_view_trimmed.get(lambda *args, **kwargs: response)

        self.assertEqual(raised.exception.endpoint, 'get_video_view_trimmed')
        self.assertIs(raised.exception.result_type, VideoViewTrimmed)

    def test_direct_mode_remains_rejected_before_request(self):
        service = Service(mode='direct', endpoints={})
        service._get = mock.Mock(return_value=valid_response())

        with self.assertRaises(SystemExit):
            service.get_video_view_trimmed({'aid': 7})

        service._get.assert_not_called()


if __name__ == '__main__':
    unittest.main()
