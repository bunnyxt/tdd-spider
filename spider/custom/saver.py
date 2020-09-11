from ..instances import Saver
from db import DBOperation


class FileSaver(Saver):
    def __init__(self, filename, mode='a'):
        Saver.__init__(self)
        self._file = open(filename, mode)

    def item_save(self, priority: int, url: str, keys: dict, deep: int, item: dict):
        self._file.write('%s\n' % str(item))
        return 1, None


class DbSaver(Saver):
    def __init__(self, get_session):
        Saver.__init__(self)
        self._session = get_session()

    def item_save(self, priority: int, url: str, keys: dict, deep: int, item: dict):
        DBOperation.add(item, self._session)
        return 1, None
