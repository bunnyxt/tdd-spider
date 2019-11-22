from .validation import get_valid, test_video_view
from util import get_ts_s
from db import TddVideo, DBOperation

__all__ = ['add_video']


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
        new_video.copyright = view_obj['data']['tname']
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
        else:
            new_video.hasstaff = 0

    # add to db
    DBOperation.add(new_video, session)

    return 0


def add_member(mid, bapi, session, test_exist=True):
    # TODO
    pass


def add_staff(added, aid, mid, title, session):
    # TODO
    pass


def get_tags_str(aid, bapi):
    # TODO
    return ''
