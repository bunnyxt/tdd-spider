from core import TddError

__all__ = ['ServiceError', 'ResponseError', 'RateLimitError',
           'ValidationError', 'FormatError', 'CodeError']


class ServiceError(TddError):
    def __init__(self):
        super().__init__()

    def __str__(self):
        return '<ServiceError>'


class ResponseError(ServiceError):
    # `reason` and `trials` say WHY the retry budget ran out -- a timeout, a
    # 502, an unparseable body. Without them an exhausted request is
    # indistinguishable from any other in a caller's stats, and answering
    # "what actually failed" means re-running with debug logging on.
    def __init__(self, target: str, params: dict,
                 reason: str = 'unknown', trials: int = 0):
        super().__init__()
        self.target = target
        self.params = params
        self.reason = reason
        self.trials = trials

    def __str__(self):
        return (f'<ResponseError(target={self.target},params={self.params},'
                f'reason={self.reason},trials={self.trials})>')


class RateLimitError(ServiceError):
    # Raised as soon as an upstream rate limit is seen. It carries no timing:
    # the per-worker cooldown that used to predict a retry time is gone, and
    # how long to wait is the caller's decision, not the Service's.
    def __init__(self, target: str, reason: str):
        super().__init__()
        self.target = target
        self.reason = reason

    def __str__(self):
        return f'<RateLimitError(target={self.target},reason={self.reason})>'


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
