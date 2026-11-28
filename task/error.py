from core import TddError

__all__ = ['TaskError', 'NotExistError', 'AlreadyExistError']


class TaskError(TddError):
    def __init__(self) -> None:
        super().__init__()

    def __str__(self) -> str:
        return '<TaskError>'


class NotExistError(TaskError):
    def __init__(self, table: str, params: dict[str, object]) -> None:
        super().__init__()
        self.table = table
        self.params = params

    def __str__(self) -> str:
        return f'<NotExistError(table={self.table},params={self.params})>'


class AlreadyExistError(TaskError):
    def __init__(self, table: str, params: dict[str, object]) -> None:
        super().__init__()
        self.table = table
        self.params = params

    def __str__(self) -> str:
        return f'<AlreadyExistError(table={self.table},params={self.params})>'
