from typing import Optional

from core import TddError

__all__ = ['ServiceError', 'ResponseError', 'RateLimitError',
           'ValidationError', 'FormatError', 'CodeError']


class ServiceError(TddError):
    def __init__(self):
        super().__init__()

    def __str__(self):
        return '<ServiceError>'


class ResponseError(ServiceError):
    def __init__(self, endpoint: str, params: dict,
                 reason: Optional[str] = None, trials: int = 0):
        super().__init__()
        self.endpoint = endpoint
        self.params = params
        self.reason = reason
        self.trials = trials

    def __str__(self):
        return (f'<ResponseError(endpoint={self.endpoint},params={self.params},'
                f'reason={self.reason},trials={self.trials})>')


class RateLimitError(ServiceError):
    def __init__(self, endpoint: str, reason: str):
        super().__init__()
        self.endpoint = endpoint
        self.reason = reason

    def __str__(self):
        return f'<RateLimitError(endpoint={self.endpoint},reason={self.reason})>'


class ValidationError(ServiceError):
    def __init__(self, endpoint: str, result_type: type, params: dict,
                 response: dict):
        super().__init__()
        self.endpoint = endpoint
        self.result_type = result_type
        self.params = params
        self.response = response

    def __str__(self):
        return (f'<ValidationError(endpoint={self.endpoint},'
                f'result_type={self.result_type.__name__},params={self.params},'
                f'response={self.response})>')


class FormatError(ValidationError):
    def __init__(self, endpoint: str, result_type: type, params: dict,
                 response: dict, message: str):
        super().__init__(endpoint, result_type, params, response)
        self.message = message

    def __str__(self):
        return (f'<FormatError(endpoint={self.endpoint},'
                f'result_type={self.result_type.__name__},params={self.params},'
                f'response={self.response},message={self.message})>')


class CodeError(ValidationError):
    def __init__(self, endpoint: str, result_type: type, params: dict,
                 response: dict, code: int):
        super().__init__(endpoint, result_type, params, response)
        self.code = code

    def __str__(self):
        return (f'<CodeError(endpoint={self.endpoint},'
                f'result_type={self.result_type.__name__},params={self.params},'
                f'response={self.response},code={self.code})>')
