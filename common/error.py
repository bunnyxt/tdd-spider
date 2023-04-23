__all__ = ['TddError',
           'TddCommonError', 'AlreadyExistError', 'NotExistError', 'InvalidObjError', 'InvalidObjCodeError',
           'InvalidParamError']


class TddError(Exception):

    def __init__(self):
        super().__init__(self)

    def __str__(self):
        return '<TddError>'


class TddCommonError(Exception):

    def __init__(self):
        super().__init__(self)

    def __str__(self):
        return '<TddCommonError>'


class AlreadyExistError(TddCommonError):

    def __init__(self, table_name, params):
        super().__init__()
        self.table_name = table_name
        self.params = params

    def __str__(self):
        return 'Params {0} in table {1} already exist!'.format(self.params, self.table_name)


class NotExistError(TddCommonError):

    def __init__(self, table_name, params):
        super().__init__()
        self.table_name = table_name
        self.params = params

    def __str__(self):
        return 'Params {0} in table {1} not exist!'.format(self.params, self.table_name)


class InvalidObjError(TddCommonError):

    def __init__(self, obj_name, params):
        super().__init__()
        self.obj_name = obj_name
        self.params = params

    def __str__(self):
        return 'Invalid {0} obj get with params {1}!'.format(self.obj_name, self.params)


class InvalidObjCodeError(TddCommonError):

    def __init__(self, obj_name, code):
        super().__init__()
        self.obj_name = obj_name
        self.code = code

    def __str__(self):
        return 'Invalid code {0} get for {1} obj!'.format(self.code, self.obj_name)


class InvalidParamError(TddCommonError):

    def __init__(self, params):
        super().__init__()
        self.params = params

    def __str__(self):
        return 'Invalid given params {0}!'.format(self.params)
