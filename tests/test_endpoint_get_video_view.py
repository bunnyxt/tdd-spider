import unittest
from unittest import mock

from service import (CodeError, FormatError, Service, VideoView, VideoViewOwner,
                     VideoViewStaffItem, VideoViewStat)
from service.endpoints import get_video_view


def valid_response():
    return {
        'code': 0,
        'message': '0',
        'ttl': 1,
        'data': {
            'bvid': 'BV1', 'aid': 7, 'videos': 1, 'tid': 3, 'tname': 'music',
            'copyright': 1, 'pic': 'pic-url', 'title': 'title', 'pubdate': 10,
            'ctime': 9, 'desc': 'desc', 'state': 0, 'duration': 60,
            'owner': {'mid': 8, 'name': 'owner', 'face': 'face-url'},
            'stat': {
                'aid': 7, 'view': 1, 'danmaku': 2, 'reply': 3, 'favorite': 4,
                'coin': 5, 'share': 6, 'now_rank': 0, 'his_rank': 0, 'like': 7,
                'dislike': 0, 'vt': 8, 'vv': 9,
            },
            'attribute': 16,
            'forward': 17,
            'staff': [{'mid': 10, 'title': 'producer', 'name': 'staff', 'face': 'staff-face'}],
        },
    }


def expected_result():
    return VideoView(
        bvid='BV1', aid=7, videos=1, tid=3, tname='music', copyright=1,
        pic='pic-url', title='title', pubdate=10, ctime=9, desc='desc', state=0,
        duration=60, owner=VideoViewOwner(8, 'owner', 'face-url'),
        stat=VideoViewStat(7, 1, 2, 3, 4, 5, 6, 0, 0, 7, 0, 8, 9),
        attribute=16, forward=17,
        staff=[VideoViewStaffItem(10, 'producer', 'staff', 'staff-face')])


class GetVideoViewEndpointTest(unittest.TestCase):
    def test_adapts_a_valid_response_and_passes_request_options(self):
        request = mock.Mock(return_value=valid_response())

        result = get_video_view.get(
            request, params={'aid': 7}, headers={'X-Test': 'yes'}, retry=2,
            timeout=3.0, colddown_factor=0.0)

        self.assertEqual(result, expected_result())
        request.assert_called_once_with(
            'get_video_view', params={'aid': 7}, headers={'X-Test': 'yes'},
            retry=2, timeout=3.0, colddown_factor=0.0)

    def test_missing_optional_values_leave_none_and_no_staff(self):
        response = valid_response()
        del response['data']['attribute']
        del response['data']['forward']
        del response['data']['staff']
        del response['data']['stat']['vt']
        del response['data']['stat']['vv']

        result = get_video_view.get(lambda *args, **kwargs: response)

        self.assertIsNone(result.attribute)
        self.assertIsNone(result.forward)
        self.assertIsNone(result.staff)
        self.assertIsNone(result.stat.vt)
        self.assertIsNone(result.stat.vv)

    def test_nonzero_api_code_keeps_the_existing_business_error(self):
        response = valid_response()
        response['code'] = -404

        with self.assertRaises(CodeError) as raised:
            get_video_view.get(lambda *args, **kwargs: response, params={'aid': 7})

        self.assertEqual((raised.exception.endpoint, raised.exception.result_type,
                          raised.exception.code),
                         ('get_video_view', VideoView, -404))

    def test_invalid_shape_keeps_the_existing_format_error(self):
        response = valid_response()
        del response['data']['staff'][0]['face']

        with self.assertRaises(FormatError) as raised:
            get_video_view.get(lambda *args, **kwargs: response, params={'aid': 7})

        self.assertEqual(raised.exception.endpoint, 'get_video_view')
        self.assertIs(raised.exception.result_type, VideoView)

    def test_service_public_method_delegates_to_the_adapter(self):
        service = Service(mode='direct', endpoints={})
        service._get = mock.Mock(return_value=valid_response())

        self.assertEqual(service.get_video_view({'aid': 7}), expected_result())
        service._get.assert_called_once_with(
            'get_video_view', params={'aid': 7}, headers=None,
            retry=None, timeout=None, colddown_factor=None)


if __name__ == '__main__':
    unittest.main()
