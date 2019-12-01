from .validation import get_valid, test_video_view, test_video_tags, test_member
from util import get_ts_s
from db import TddVideo, TddVideoStaff, TddMember, DBOperation
import time

__all__ = ['add_video', 'add_member']


def add_video(aid, bapi, session, test_exist=True, params=None,
              set_isvc=True, add_video_owner=True, add_video_staff=True):
    # test exist
    if test_exist:
        video = DBOperation.query_video_via_aid(aid, session)
        if video is not None:
            # video already exist
            return 1  # TODO replace error code with exception

    # get view_obj
    view_obj = get_valid(bapi.get_video_view, (aid,), test_video_view)
    if view_obj is None:
        # fail to get valid view_obj
        return 2

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
        return 3

    # set tags
    new_video.tags = get_tags_str(aid, bapi)

    # set isvc
    if set_isvc:
        for tag in new_video.tags.split(';'):
            if tag == 'VOCALOID中文曲':
                new_video.isvc = 2
                break

    # add member
    if add_video_owner:
        add_member(new_video.mid, bapi, session)

    # add staff
    if add_video_staff:
        if 'staff' in view_obj['data'].keys():
            new_video.hasstaff = 1
            for staff in view_obj['data']['staff']:
                add_member(staff['mid'], bapi, session)
                add_staff(new_video.added, aid, staff['mid'], staff['title'], session)
                time.sleep(0.2)
        else:
            new_video.hasstaff = 0

    # add to db
    DBOperation.add(new_video, session)

    return 0


def add_member(mid, bapi, session, test_exist=True):
    # test exist
    if test_exist:
        member = DBOperation.query_member_via_mid(mid, session)
        if member is not None:
            # member already exist
            return 1  # TODO replace error code with exception

    # get member_obj
    member_obj = get_valid(bapi.get_member, (mid,), test_member)
    if member_obj is None:
        # fail to get valid member_obj
        return 2

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
        return 3

    # add to db
    DBOperation.add(new_member, session)

    return 0


def add_staff(added, aid, mid, title, session):
    new_staff = TddVideoStaff()

    # set attr
    new_staff.added = added
    new_staff.aid = aid
    new_staff.mid = mid
    new_staff.title = title

    # add to db
    DBOperation.add(new_staff, session)

    return 0


def get_tags_str(aid, bapi):
    # get tags_obj
    tags_obj = get_valid(bapi.get_video_tags, (aid,), test_video_tags)
    if tags_obj is None:
        # fail to get valid test_video_tags
        # return 2
        return ''

    tags_str = ''
    try:
        for tag in tags_obj['data']:
            tags_str += tag['tag_name']
            tags_str += ';'
    except Exception as e:
        # return e.message
        pass

    return tags_str
