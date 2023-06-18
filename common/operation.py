from .validation import get_valid, test_video_stat
from .error import *
from util import get_ts_s
from db import DBOperation, TddVideoRecord

__all__ = ['add_video_record_via_awesome_stat', 'add_video_record_via_stat_api']


def add_video_record_via_awesome_stat(added, stat, session):
    new_video_record = TddVideoRecord()

    # set attr from awesome stat
    try:
        new_video_record.aid = stat['aid']
        new_video_record.added = added
        new_video_record.view = -1 if stat['view'] == '--' else stat['view']
        new_video_record.danmaku = stat['danmaku']
        new_video_record.reply = stat['reply']
        new_video_record.favorite = stat['favorite']
        new_video_record.coin = stat['coin']
        new_video_record.share = stat['share']
        new_video_record.like = stat['like']
        new_video_record.dislike = stat.get('dislike', None)
        new_video_record.now_rank = stat.get('now_rank', None)
        new_video_record.his_rank = stat.get('his_rank', None)
        new_video_record.vt = stat.get('vt', None)
        new_video_record.vv = stat.get('vv', None)
    except Exception:
        raise InvalidParamError({'stat', stat})

    # add to db
    DBOperation.add(new_video_record, session)

    return new_video_record


def add_video_record_via_stat_api(aid, bapi, session):
    # get stat_obj
    stat_obj = get_valid(bapi.get_video_stat, (aid,), test_video_stat)
    if stat_obj is None:
        # fail to get valid view_obj
        raise InvalidObjError(obj_name='stat', params={'aid': aid})

    new_video_record = TddVideoRecord()

    # set basic attr
    new_video_record.aid = aid
    new_video_record.added = get_ts_s()

    # set attr from stat_obj
    if stat_obj['code'] == 0:
        new_video_record.view = -1 if stat_obj['data']['view'] == '--' else stat_obj['data']['view']
        new_video_record.danmaku = stat_obj['data']['danmaku']
        new_video_record.reply = stat_obj['data']['reply']
        new_video_record.favorite = stat_obj['data']['favorite']
        new_video_record.coin = stat_obj['data']['coin']
        new_video_record.share = stat_obj['data']['share']
        new_video_record.like = stat_obj['data']['like']
        new_video_record.dislike = stat_obj['data'].get('dislike', None)
        new_video_record.now_rank = stat_obj['data'].get('now_rank', None)
        new_video_record.his_rank = stat_obj['data'].get('his_rank', None)
        new_video_record.vt = stat_obj['data'].get('vt', None)
        new_video_record.vv = stat_obj['data'].get('vv', None)
    else:
        # stat code != 0
        raise InvalidObjCodeError(obj_name='stat', code=stat_obj['code'])

    # add to db
    DBOperation.add(new_video_record, session)

    return new_video_record
