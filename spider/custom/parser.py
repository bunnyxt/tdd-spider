from ..instances import Parser
import json
from util import get_ts_s
from db import TddMemberFollowerRecord, TddVideoRecord, TddMemberLog, DBOperation
from common import NotExistError
import logging
logger = logging.getLogger('parser')


class TddMemberFollowerRecordParser(Parser):
    def htm_parse(self, priority: int, url: str, keys: dict, deep: int, content: object):
        status_code, url_now, html_text = content
        obj = json.loads(html_text)
        item = TddMemberFollowerRecord()
        item.mid = obj['data']['mid']
        item.added = get_ts_s()
        if obj['code'] == 0:
            item.follower = obj['data']['follower']
        else:
            raise RuntimeError('api return code != 0')  # TODO
        return 1, [], item  # parse_state (1: success), url_list (do not add new urls), item (obj to be saved)


class TddVideoRecordParser(Parser):
    def htm_parse(self, priority: int, url: str, keys: dict, deep: int, content: object) -> (int, list, object):
        status_code, url_now, html_text = content
        obj = json.loads(html_text)

        item = TddVideoRecord()
        item.added = get_ts_s()
        if obj['code'] == 0:
            item.aid = obj['data']['aid']
            item.view = -1 if obj['data']['view'] == '--' else obj['data']['view']
            item.danmaku = obj['data']['danmaku']
            item.reply = obj['data']['reply']
            item.favorite = obj['data']['favorite']
            item.coin = obj['data']['coin']
            item.share = obj['data']['share']
            item.like = obj['data']['like']
        else:
            raise RuntimeError('api return code != 0')  # TODO
        return 1, [], item  # parse_state (1: success), url_list (do not add new urls), item (obj to be saved)


class TddMemberParserForUpdate(Parser):
    def __init__(self, get_session):
        Parser.__init__(self)
        self._session = get_session()

    def htm_parse(self, priority: int, url: str, keys: dict, deep: int, content: object) -> (int, list, object):
        status_code, url_now, html_text = content
        member_obj = json.loads(html_text)

        member_update_logs = []
        if member_obj['code'] == 0:

            mid = member_obj['data']

            # get old member obj from db
            old_obj = DBOperation.query_member_via_mid(mid, self._session)
            if old_obj is None:
                # mid not exist in db
                raise NotExistError(table_name='tdd_member', params={'mid': mid})  # TODO

            added = get_ts_s()

            try:
                if member_obj['data']['sex'] != old_obj.sex:
                    member_update_logs.append(
                        TddMemberLog(added, mid, 'sex', old_obj.sex, member_obj['data']['sex']))
                    old_obj.sex = member_obj['data']['sex']
                if member_obj['data']['name'] != old_obj.name:
                    member_update_logs.append(
                        TddMemberLog(added, mid, 'name', old_obj.name, member_obj['data']['name']))
                    old_obj.name = member_obj['data']['name']
                if member_obj['data']['face'][-44:] != old_obj.face[-44:]:  # remove prefix, just compare last 44 character
                    member_update_logs.append(
                        TddMemberLog(added, mid, 'face', old_obj.face, member_obj['data']['face']))
                    old_obj.face = member_obj['data']['face']
                if member_obj['data']['sign'] != old_obj.sign:
                    member_update_logs.append(
                        TddMemberLog(added, mid, 'sign', old_obj.sign, member_obj['data']['sign']))
                    old_obj.sign = member_obj['data']['sign']
                self._session.commit()  # commit changes

                for log in member_update_logs:
                    logger.info('%d, %s, %s -> %s' % (log.mid, log.attr, log.oldval, log.newval))
            except Exception as e:
                self._session.rollback()
                raise RuntimeError('fail to update member mid = %d, %s' % (mid, e))  # TODO
        else:
            raise RuntimeError('api return code != 0')  # TODO
        return 1, [], member_update_logs  # parse_state (1: success), url_list (do not add new urls), member_update_logs (objs to be saved)

    def __del__(self):
        self._session.close()
