from ..instances import Parser
import json
from util import get_ts_s
from db import TddMemberFollowerRecord, TddVideoRecord
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


class JsonParserWithCodeZeroChecker(Parser):
    def __init__(self):
        Parser.__init__(self)

    def htm_parse(self, priority: int, url: str, keys: dict, deep: int, content: object) -> (int, list, object):
        status_code, url_now, html_text = content
        obj = json.loads(html_text)
        if obj['code'] != 0:
            raise RuntimeError('api return code != 0')  # TODO
        return 1, [], obj  # parse_state (1: success), url_list (do not add new urls), obj (obj to be saved)

