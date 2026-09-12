import unittest

from service import (CodeError, FormatError, MemberCard, RateLimitError,
                     ResponseError)


class ServiceErrorTest(unittest.TestCase):
    def test_response_and_rate_limit_errors_identify_only_the_endpoint(self):
        response = ResponseError('get_member_card', {'mid': 7}, 'http_503', 3)
        rate_limit = RateLimitError('get_member_card', 'code_-352')

        self.assertEqual(response.endpoint, 'get_member_card')
        self.assertEqual(rate_limit.endpoint, 'get_member_card')
        self.assertNotIn('target=', str(response))
        self.assertNotIn('target=', str(rate_limit))

    def test_validation_errors_identify_endpoint_and_result_type(self):
        response = {'code': -404}
        code_error = CodeError(
            'get_member_card', MemberCard, {'mid': 7}, response, -404)
        format_error = FormatError(
            'get_member_card', MemberCard, {'mid': 7}, response,
            'Response data should be a dict.')

        for error in (code_error, format_error):
            self.assertEqual(error.endpoint, 'get_member_card')
            self.assertIs(error.result_type, MemberCard)
            self.assertIn('endpoint=get_member_card', str(error))
            self.assertIn('result_type=MemberCard', str(error))
            self.assertNotIn('target=', str(error))


if __name__ == '__main__':
    unittest.main()
