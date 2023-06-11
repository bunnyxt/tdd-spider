from .validation import get_valid, test_video_view, test_video_view_via_bvid, test_video_tags, test_video_tags_via_bvid, \
    test_member, test_video_stat, test_member_relation
from .error import *
from util import get_ts_s, b2a, a2b
from db import TddVideo, TddVideoStaff, TddMember, DBOperation, TddVideoRecord, \
    TddVideoLog, TddMemberLog, TddMemberFollowerRecord
import time

__all__ = ['add_video_via_bvid',
           'add_member',
           'add_video_record_via_awesome_stat', 'add_video_record_via_stat_api']


# bvid version
def add_video_via_bvid(bvid, bapi, session, test_exist=True, params=None, set_recent=True,
                       set_isvc=True, add_video_owner=True, add_video_staff=True,
                       update_member_last_video=True, update_member_video_count=True):
    # test exist
    if test_exist:
        video = DBOperation.query_video_via_bvid(bvid, session)
        if video is not None:
            # video already exist
            raise AlreadyExistError(table_name='tdd_video', params={'bvid': bvid})

    # get view_obj
    view_obj = get_valid(bapi.get_video_view_via_bvid, (bvid,), test_video_view_via_bvid)
    if view_obj is None:
        # fail to get valid view_obj
        raise InvalidObjError(obj_name='view', params={'bvid': bvid})

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
    new_video.bvid = bvid
    new_video.added = get_ts_s()

    # set attr from view_obj
    if view_obj['code'] == 0:
        new_video.aid = view_obj['data']['aid']
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
        if 'attribute' in view_obj['data'].keys():
            new_video.attribute = view_obj['data']['attribute']
        new_video.state = view_obj['data']['state']
        if 'forward' in view_obj['data'].keys():
            new_video.forward = view_obj['data']['forward']
    else:
        # video code != 0
        raise InvalidObjCodeError(obj_name='view', code=view_obj['code'])

    # set tags
    new_video.tags = get_tags_str_via_bvid(bvid, bapi)

    # set recent
    if set_recent:
        new_video.recent = 2

    # set isvc
    if set_isvc:
        for tag in new_video.tags.split(';'):
            if tag.upper() == 'VOCALOID中文曲':
                new_video.isvc = 2
                break

    member_mid_set = set()
    member_mid_set.add(new_video.mid)

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
                member_mid_set.add(staff['mid'])
                try:
                    add_member(staff['mid'], bapi, session)
                except TddCommonError as e:
                    print(e)
                try:
                    add_staff_via_bvid(new_video.added, bvid, staff['mid'], staff['title'], session)
                except TddCommonError as e:
                    print(e)
                time.sleep(0.2)
        else:
            new_video.hasstaff = 0

    # add to db
    DBOperation.add(new_video, session)

    # get new video id
    new_video = DBOperation.query_video_via_bvid(bvid, session)

    if update_member_last_video or update_member_video_count:
        for mid in member_mid_set:
            member = DBOperation.query_member_via_mid(mid, session)
            # update member last video
            if update_member_last_video:
                member.last_video = new_video.id
            # update member video count
            if update_member_video_count:
                member.video_count += 1
            session.commit()

    return new_video


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
        # of course we only add member with code = 0
        new_member.sex = member_obj['data']['sex']
        new_member.name = member_obj['data']['name']
        new_member.face = member_obj['data']['face']
        new_member.sign = member_obj['data']['sign']
        new_member.code = 0
    else:
        # member_obj code != 0
        raise InvalidObjCodeError(obj_name='member', code=member_obj['code'])

    # add to db
    DBOperation.add(new_member, session)

    return new_member


# bvid version
def add_staff_via_bvid(added, bvid, mid, title, session, test_exist=True):
    # test exist
    if test_exist:
        staff = DBOperation.query_video_staff_via_bvid_mid(bvid, mid, session)
        if staff is not None:
            # staff already exist
            raise AlreadyExistError(table_name='tdd_video_staff', params={'bvid': bvid, 'mid': mid})

    new_staff = TddVideoStaff()

    # set attr
    new_staff.added = added
    new_staff.aid = b2a(bvid)
    new_staff.bvid = bvid
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


# bvid version
def get_tags_str_via_bvid(bvid, bapi):
    # get tags_obj
    tags_obj = get_valid(bapi.get_video_tags_via_bvid, (bvid,), test_video_tags_via_bvid)
    if tags_obj is None:
        # fail to get valid test_video_tags
        raise InvalidObjError(obj_name='tags', params={'bvid': bvid})

    tags_str = ''
    try:
        for tag in tags_obj['data']:
            tags_str += tag['tag_name']
            tags_str += ';'
    except Exception as e:
        print(e)

    return tags_str
