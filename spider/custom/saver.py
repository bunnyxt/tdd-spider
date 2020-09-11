from ..instances import Saver
from db import DBOperation


class DbSaver(Saver):
    def __init__(self, get_session):
        Saver.__init__(self)
        self._session = get_session()

    def item_save(self, priority: int, url: str, keys: dict, deep: int, item: dict):
        DBOperation.add(item, self._session)
        return 1, None
