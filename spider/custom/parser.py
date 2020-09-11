from ..instances import Parser
import json
from util import get_ts_s
from db import TddMemberFollowerRecord


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
