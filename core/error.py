__all__ = ['TddError']


class TddError(Exception):

    def __init__(self):
        super().__init__(self)

    def __str__(self):
        return '<TddError>'
