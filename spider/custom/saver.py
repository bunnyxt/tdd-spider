from ..instances import Saver
from db import DBOperation, TddMemberLog
from common import NotExistError
from util import get_ts_s
import re
import logging

logger = logging.getLogger('saver')


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
        try:
            self._session.add(item)
            self._session.commit()
        except Exception as e:
            self._session.rollback()
            raise e
        return 1, None

    def __del__(self):
        self._session.close()


class DbListSaver(Saver):
    def __init__(self, get_session):
        Saver.__init__(self)
        self._session = get_session()

    def item_save(self, priority: int, url: str, keys: dict, deep: int, item: dict):
        try:
            for single_item in item:
                self._session.add(single_item)
                self._session.commit()
        except Exception as e:
            self._session.rollback()
            raise e
        return 1, None

    def __del__(self):
        self._session.close()
