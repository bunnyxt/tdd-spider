import unittest
from unittest import mock

from service import CodeError, FormatError, MemberRelation, Service
from service.endpoints import get_member_relation


def valid_response():
    return {
        'code': 0,
        'message': '0',
        'ttl': 1,
        'data': {'mid': 7, 'following': 11, 'follower': 13},
    }


class GetMemberRelationEndpointTest(unittest.TestCase):
    def test_adapts_a_valid_response_and_passes_request_options(self):
        request = mock.Mock(return_value=valid_response())

        result = get_member_relation.get(
            request, params={'vmid': 7}, headers={'X-Test': 'yes'}, retry=2,
            timeout=3.0, colddown_factor=0.0)

        self.assertEqual(result, MemberRelation(7, 11, 13))
        request.assert_called_once_with(
            'get_member_relation', params={'vmid': 7}, headers={'X-Test': 'yes'},
            retry=2, timeout=3.0, colddown_factor=0.0)

    def test_nonzero_api_code_keeps_the_existing_business_error(self):
        response = valid_response()
        response['code'] = -404

        with self.assertRaises(CodeError) as raised:
            get_member_relation.get(lambda *args, **kwargs: response,
                                    params={'vmid': 7})

        self.assertEqual((raised.exception.target, raised.exception.code),
                         ('member_relation', -404))

    def test_invalid_shape_keeps_the_existing_format_error(self):
        response = valid_response()
        del response['data']['follower']

        with self.assertRaises(FormatError) as raised:
            get_member_relation.get(lambda *args, **kwargs: response,
                                    params={'vmid': 7})

        self.assertEqual(raised.exception.target, 'member_relation')

    def test_service_public_method_delegates_to_the_adapter(self):
        service = Service(mode='direct', endpoints={})
        service._get = mock.Mock(return_value=valid_response())

        result = service.get_member_relation({'vmid': 7})

        self.assertEqual(result, MemberRelation(7, 11, 13))
        service._get.assert_called_once_with(
            'get_member_relation', params={'vmid': 7}, headers=None,
            retry=None, timeout=None, colddown_factor=None)


if __name__ == '__main__':
    unittest.main()
