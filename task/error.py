from common.error import TddError

__all__ = ['TaskError', 'NotExistError', 'AlreadyExistError']


class TaskError(TddError):
    def __init__(self):
        super().__init__()

    def __str__(self):
        return '<TaskError>'


class NotExistError(TaskError):
    def __init__(self, table, params):
        super().__init__()
        self.table = table
        self.params = params

    def __str__(self):
        return f'<NotExistError(table={self.table},params={self.params})>'


class AlreadyExistError(TaskError):
    def __init__(self, table, params):
        super().__init__()
        self.table = table
        self.params = params

    def __str__(self):
        return f'<AlreadyExistError(table={self.table},params={self.params})>'
