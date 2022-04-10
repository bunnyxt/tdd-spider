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


class UpdateMemberInfoSaver(Saver):
    def __init__(self, get_session):
        Saver.__init__(self)
        self._session = get_session()

    def item_save(self, priority: int, url: str, keys: dict, deep: int, item: dict):
        # rename item
        member_obj = item

        # get mid from member_obj, or url
        if member_obj['code'] != 0:
            # url format: http://api.bilibili.com/x/space/acc/info?mid=123456
            match_obj = re.match(r'.*mid=(\d+)$', url)
            if match_obj:
                mid = int(match_obj.group(1))  # group(1) can be safely transformed to int
            else:
                raise RuntimeError('Fail to parse mid from member info request url %s.' % url)
        else:
            mid = member_obj['data']['mid']

        # get old member obj from db
        old_obj = DBOperation.query_member_via_mid(mid, self._session)
        if old_obj is None:
            # mid not exist in db
            raise NotExistError(table_name='tdd_member', params={'mid': mid})

        # used for update log added timestamp
        added = get_ts_s()

        try:
            # all update log
            member_update_logs = []

            if member_obj['code'] != 0:
                # code maybe -404
                if member_obj['code'] != old_obj.code:
                    member_update_logs.append(
                        TddMemberLog(added, mid, 'code', old_obj.code, member_obj['code']))
                    old_obj.code = member_obj['code']
            else:
                if member_obj['data']['sex'] != old_obj.sex:
                    member_update_logs.append(
                        TddMemberLog(added, mid, 'sex', old_obj.sex, member_obj['data']['sex']))
                    old_obj.sex = member_obj['data']['sex']
                if member_obj['data']['name'] != old_obj.name:
                    member_update_logs.append(
                        TddMemberLog(added, mid, 'name', old_obj.name, member_obj['data']['name']))
                    old_obj.name = member_obj['data']['name']
                if member_obj['data']['face'][-44:] != old_obj.face[-44:]:  # remove prefix, just compare last 44 characters
                    member_update_logs.append(
                        TddMemberLog(added, mid, 'face', old_obj.face, member_obj['data']['face']))
                    old_obj.face = member_obj['data']['face']
                if member_obj['data']['sign'] != old_obj.sign:
                    member_update_logs.append(
                        TddMemberLog(added, mid, 'sign', old_obj.sign, member_obj['data']['sign']))
                    old_obj.sign = member_obj['data']['sign']
            self._session.commit()  # commit TddMember object changes

            # add logs
            for log in member_update_logs:
                self._session.add(log)
                self._session.commit()
                logger.info('%d, %s, %s -> %s' % (log.mid, log.attr, log.oldval, log.newval))
        except Exception as e:
            self._session.rollback()
            raise RuntimeError('Fail to update info of member mid = %d! Detail: %s' % (mid, e))

        return 1, None  # save success

    def __del__(self):
        self._session.close()
