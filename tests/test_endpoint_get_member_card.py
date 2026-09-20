import unittest
from unittest import mock

from service import (CodeError, ContentError, FormatError, MemberCard,
                     Service)
from service.endpoints import get_member_card


def valid_response():
    return {
        'code': 0,
        'message': '0',
        'ttl': 1,
        'data': {
            'card': {
                'mid': 7,
                'name': 'name',
                'sex': '保密',
                'face': 'face-url',
                'sign': 'sign',
            },
        },
    }


class GetMemberCardEndpointTest(unittest.TestCase):
    def test_adapts_a_valid_response_and_passes_request_options(self):
        request = mock.Mock(return_value=valid_response())

        result = get_member_card.get(
            request, params={'mid': 7}, headers={'X-Test': 'yes'}, retry=2,
            timeout=3.0, colddown_factor=0.0)

        self.assertEqual(result, MemberCard(7, 'name', '保密', 'face-url', 'sign'))
        request.assert_called_once_with(
            'get_member_card', params={'mid': 7}, headers={'X-Test': 'yes'},
            retry=2, timeout=3.0, colddown_factor=0.0)

    def test_nonzero_api_code_keeps_the_existing_business_error(self):
        response = valid_response()
        response['code'] = -404

        with self.assertRaises(CodeError) as raised:
            get_member_card.get(lambda *args, **kwargs: response,
                                params={'mid': 7})

        self.assertEqual((raised.exception.endpoint, raised.exception.result_type,
                          raised.exception.code),
                         ('get_member_card', MemberCard, -404))

    def test_invalid_shape_keeps_the_existing_format_error(self):
        response = valid_response()
        del response['data']['card']['face']

        with self.assertRaises(FormatError) as raised:
            get_member_card.get(lambda *args, **kwargs: response,
                                params={'mid': 7})

        self.assertEqual(raised.exception.endpoint, 'get_member_card')
        self.assertIs(raised.exception.result_type, MemberCard)

    def test_a_blank_card_is_rejected_instead_of_adapted(self):
        response = valid_response()
        response['data']['card'] = {
            'mid': '', 'name': '', 'sex': '', 'face': '', 'sign': ''}

        with self.assertRaises(ContentError) as raised:
            get_member_card.get(lambda *args, **kwargs: response,
                                params={'mid': 7})

        self.assertEqual((raised.exception.endpoint, raised.exception.result_type),
                         ('get_member_card', MemberCard))
        self.assertEqual(raised.exception.response, response)

    def test_an_empty_name_alone_is_rejected(self):
        response = valid_response()
        response['data']['card']['name'] = ''

        with self.assertRaises(ContentError):
            get_member_card.get(lambda *args, **kwargs: response,
                                params={'mid': 7})

    def test_a_member_who_cleared_optional_fields_is_still_adapted(self):
        response = valid_response()
        response['data']['card']['sign'] = ''
        response['data']['card']['face'] = ''

        result = get_member_card.get(lambda *args, **kwargs: response,
                                     params={'mid': 7})

        self.assertEqual(result, MemberCard(7, 'name', '保密', '', ''))

    def test_service_public_method_delegates_to_the_adapter(self):
        service = Service(mode='direct', endpoints={})
        service._get = mock.Mock(return_value=valid_response())

        result = service.get_member_card({'mid': 7})

        self.assertEqual(result, MemberCard(7, 'name', '保密', 'face-url', 'sign'))
        service._get.assert_called_once_with(
            'get_member_card', params={'mid': 7}, headers=None,
            retry=None, timeout=None, colddown_factor=None)


if __name__ == '__main__':
    unittest.main()
