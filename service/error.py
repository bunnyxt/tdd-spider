from core import TddError

__all__ = ['ServiceError', 'ResponseError', 'ValidationError', 'FormatError', 'CodeError']


class ServiceError(TddError):
    def __init__(self):
        super().__init__()

    def __str__(self):
        return '<ServiceError>'


class ResponseError(ServiceError):
    def __init__(self, target: str, params: dict):
        super().__init__()
        self.target = target
        self.params = params

    def __str__(self):
        return f'<ResponseError(target={self.target},params={self.params})>'


class ValidationError(ServiceError):
    def __init__(self, target: str, params: dict, response: dict):
        super().__init__()
        self.target = target
        self.params = params
        self.response = response

    def __str__(self):
        return f'<ValidationError(target={self.target},params={self.params},response={self.response})>'


class FormatError(ValidationError):
    def __init__(self, target: str, params: dict, response: dict, message: str):
        super().__init__(target, params, response)
        self.message = message

    def __str__(self):
        return f'<FormatError(target={self.target},params={self.params},response={self.response},message={self.message})>'


class CodeError(ValidationError):
    def __init__(self, target: str, params: dict, response: dict, code: int):
        super().__init__(target, params, response)
        self.code = code

    def __str__(self):
        return f'<CodeError(target={self.target},params={self.params},response={self.response},code={self.code})>'
