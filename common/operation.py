from .validation import get_valid, test_video_view, test_video_tags, test_member, test_video_stat
from .error import *
from util import get_ts_s
from db import TddVideo, TddVideoStaff, TddMember, DBOperation, TddVideoRecord, TddVideoLog
import time

__all__ = ['add_video', 'update_video', 'add_member', 'add_staff', 'add_video_record_via_awesome_stat',
           'add_video_record_via_stat_api', 'get_tags_str']


def add_video(aid, bapi, session, test_exist=True, params=None, set_recent=True,
              set_isvc=True, add_video_owner=True, add_video_staff=True):
    # test exist
    if test_exist:
        video = DBOperation.query_video_via_aid(aid, session)
        if video is not None:
            # video already exist
            raise AlreadyExistError(table_name='tdd_video', params={'aid': aid})

    # get view_obj
    view_obj = get_valid(bapi.get_video_view, (aid,), test_video_view)
    if view_obj is None:
        # fail to get valid view_obj
        raise InvalidObjError(obj_name='view', params={'aid': aid})

    new_video = TddVideo()

    # set params
    if params:
        for key in params.keys():
            if key[:2] == '__' or key[-2:] == '__':
                # cannot set internal attribute
                continue
            if key in dir(new_video):
                new_video.__setattr__(key, params[key])

    # set basic attr
    new_video.aid = aid
    new_video.added = get_ts_s()

    # set attr from view_obj
    if view_obj['code'] == 0:
        new_video.videos = view_obj['data']['videos']
        new_video.tid = view_obj['data']['tid']
        new_video.tname = view_obj['data']['tname']
        new_video.copyright = view_obj['data']['copyright']
        new_video.pic = view_obj['data']['pic']
        new_video.title = view_obj['data']['title']
        new_video.pubdate = view_obj['data']['pubdate']
        new_video.desc = view_obj['data']['desc']
        new_video.mid = view_obj['data']['owner']['mid']
        new_video.code = view_obj['code']
    else:
        # video code != 0
        raise InvalidObjCodeError(obj_name='view', code=view_obj['code'])

    # set tags
    new_video.tags = get_tags_str(aid, bapi)

    # set recent
    if set_recent:
        new_video.recent = 2

    # set isvc
    if set_isvc:
        for tag in new_video.tags.split(';'):
            if tag.upper() == 'VOCALOID中文曲':
                new_video.isvc = 2
                break

    # add member
    if add_video_owner:
        try:
            add_member(new_video.mid, bapi, session)
        except TddCommonError as e:
            print(e)

    # add staff
    if add_video_staff:
        if 'staff' in view_obj['data'].keys():
            new_video.hasstaff = 1
            for staff in view_obj['data']['staff']:
                try:
                    add_member(staff['mid'], bapi, session)
                except TddCommonError as e:
                    print(e)
                try:
                    add_staff(new_video.added, aid, staff['mid'], staff['title'], session)
                except TddCommonError as e:
                    print(e)
                time.sleep(0.2)
        else:
            new_video.hasstaff = 0

    # add to db
    DBOperation.add(new_video, session)

    return new_video


def update_video(aid, bapi, session):
    # get view_obj
    view_obj = get_valid(bapi.get_video_view, (aid,), test_video_view)
    if view_obj is None:
        # fail to get valid view_obj
        raise InvalidObjError(obj_name='view', params={'aid': aid})

    # get old video obj from db
    old_obj = DBOperation.query_video_via_aid(aid, session)
    if old_obj is None:
        # aid not exist in db
        raise NotExistError(table_name='tdd_video', params={'aid': aid})

    video_update_logs = []
    added = get_ts_s()

    # check following attr
    try:
        if view_obj['code'] != old_obj.code:
            video_update_logs.append(TddVideoLog(added, aid, 'code', old_obj.code, view_obj['code']))
            old_obj.code = view_obj['code']
        if view_obj['code'] == 0:
            if view_obj['data']['videos'] != old_obj.videos:
                video_update_logs.append(TddVideoLog(added, aid, 'videos', old_obj.videos, view_obj['data']['videos']))
                old_obj.videos = view_obj['data']['videos']
            if view_obj['data']['tid'] != old_obj.tid:
                video_update_logs.append(TddVideoLog(added, aid, 'tid', old_obj.tid, view_obj['data']['tid']))
                old_obj.tid = view_obj['data']['tid']
            if view_obj['data']['tname'] != old_obj.tname:
                video_update_logs.append(TddVideoLog(added, aid, 'tname', old_obj.tname, view_obj['data']['tname']))
                old_obj.tname = view_obj['data']['tname']
            if view_obj['data']['copyright'] != old_obj.copyright:
                video_update_logs.append(TddVideoLog(added, aid, 'copyright', old_obj.copyright, view_obj['data']['copyright']))
                old_obj.copyright = view_obj['data']['copyright']
            if view_obj['data']['pic'] != old_obj.pic:
                video_update_logs.append(TddVideoLog(added, aid, 'pic', old_obj.pic, view_obj['data']['pic']))
                old_obj.pic = view_obj['data']['pic']
            if view_obj['data']['title'] != old_obj.title:
                video_update_logs.append(TddVideoLog(added, aid, 'title', old_obj.title, view_obj['data']['title']))
                old_obj.title = view_obj['data']['title']
            if view_obj['data']['pubdate'] != old_obj.pubdate:
                video_update_logs.append(TddVideoLog(added, aid, 'pubdate', old_obj.pubdate, view_obj['data']['pubdate']))
                old_obj.pubdate = view_obj['data']['pubdate']
            if view_obj['data']['desc'] != old_obj.desc:
                video_update_logs.append(TddVideoLog(added, aid, 'desc', old_obj.desc, view_obj['data']['desc']))
                old_obj.desc = view_obj['data']['desc']
            if view_obj['data']['owner']['mid'] != old_obj.mid:
                video_update_logs.append(TddVideoLog(added, aid, 'mid', old_obj.mid, view_obj['data']['owner']['mid']))
                old_obj.mid = view_obj['data']['owner']['mid']
            # has staff
            new_hasstaff = 0
            if 'staff' in view_obj['data'].keys():
                new_hasstaff = 1
            if new_hasstaff != old_obj.hasstaff:
                video_update_logs.append(TddVideoLog(added, aid, 'hasstaff', old_obj.hasstaff, new_hasstaff))
                old_obj.hasstaff = new_hasstaff
            # TODO update staff list

            session.commit()  # commit changes
    except Exception as e:
        print(e)

    # add to db
    for log in video_update_logs:
        DBOperation.add(log, session)

    return video_update_logs


def add_member(mid, bapi, session, test_exist=True):
    # test exist
    if test_exist:
        member = DBOperation.query_member_via_mid(mid, session)
        if member is not None:
            # member already exist
            raise AlreadyExistError(table_name='tdd_member', params={'mid': mid})

    # get member_obj
    member_obj = get_valid(bapi.get_member, (mid,), test_member)
    if member_obj is None:
        # fail to get valid member_obj
        raise InvalidObjError(obj_name='member', params={'mid': mid})

    new_member = TddMember()

    # set basic attr
    new_member.mid = mid
    new_member.added = get_ts_s()

    # set attr from member_obj
    if member_obj['code'] == 0:
        new_member.sex = member_obj['data']['sex']
        new_member.name = member_obj['data']['name']
        new_member.face = member_obj['data']['face']
        new_member.sign = member_obj['data']['sign']
    else:
        # member_obj code != 0
        raise InvalidObjCodeError(obj_name='member', code=member_obj['code'])

    # add to db
    DBOperation.add(new_member, session)

    return new_member


def add_staff(added, aid, mid, title, session):
    new_staff = TddVideoStaff()

    # set attr
    new_staff.added = added
    new_staff.aid = aid
    new_staff.mid = mid
    new_staff.title = title

    # add to db
    DBOperation.add(new_staff, session)

    return new_staff


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
    else:
        # stat code != 0
        raise InvalidObjCodeError(obj_name='stat', code=stat_obj['code'])

    # add to db
    DBOperation.add(new_video_record, session)

    return new_video_record


def get_tags_str(aid, bapi):
    # get tags_obj
    tags_obj = get_valid(bapi.get_video_tags, (aid,), test_video_tags)
    if tags_obj is None:
        # fail to get valid test_video_tags
        raise InvalidObjError(obj_name='tags', params={'aid': aid})

    tags_str = ''
    try:
        for tag in tags_obj['data']:
            tags_str += tag['tag_name']
            tags_str += ';'
    except Exception as e:
        print(e)

    return tags_str
