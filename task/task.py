from service import Service, ServiceError, CodeError, ArchiveRankByPartionArchiveStat
from sqlalchemy.orm.session import Session
from db import DBOperation, TddVideo, TddVideoRecord, TddVideoLog, TddVideoStaff, TddMember, TddMemberFollowerRecord, \
    TddMemberLog
from util import get_ts_s, a2b, same_pic_url
from typing import List
from common.error import TddError
from .error import AlreadyExistError, NotExistError

import logging

logger = logging.getLogger('task')

__all__ = ['add_video_record', 'commit_video_record_via_archive_stat',
           'add_video', 'update_video',
           'add_member', 'update_member', 'commit_staff', 'add_member_follower_record',
           'get_video_tags_str']


def add_video_record(aid: int, service: Service, session: Session) -> TddVideoRecord:
    # get video stat
    try:
        video_stat = service.get_video_stat({'aid': aid})
    except ServiceError as e:
        raise e

    # assemble video record
    new_video_record = TddVideoRecord(
        aid=aid,
        added=get_ts_s(),
        view=-1 if video_stat.view == '--' else video_stat.view,
        danmaku=video_stat.danmaku,
        reply=video_stat.reply,
        favorite=video_stat.favorite,
        coin=video_stat.coin,
        share=video_stat.share,
        like=video_stat.like,
        dislike=video_stat.dislike,
        now_rank=video_stat.now_rank,
        his_rank=video_stat.his_rank,
        vt=video_stat.vt,
        vv=video_stat.vv,
    )

    # add to db
    # TODO: use new db operation which can raise exception
    DBOperation.add(new_video_record, session)

    return new_video_record


def commit_video_record_via_archive_stat(stat: ArchiveRankByPartionArchiveStat, session: Session) -> TddVideoRecord:
    # assemble video record
    new_video_record = TddVideoRecord(
        aid=stat.aid,
        added=get_ts_s(),
        view=-1 if stat.view == '--' else stat.view,
        danmaku=stat.danmaku,
        reply=stat.reply,
        favorite=stat.favorite,
        coin=stat.coin,
        share=stat.share,
        like=stat.like,
        dislike=stat.dislike,
        now_rank=stat.now_rank,
        his_rank=stat.his_rank,
        vt=stat.vt,
        vv=stat.vv,
    )

    # add to db
    # TODO: use new db operation which can raise exception
    DBOperation.add(new_video_record, session)

    return new_video_record


def add_video(aid: int, service: Service, session: Session, test_exist=True) -> TddVideo:
    # TODO: rollback already committed changes if error occurs
    # test exist
    if test_exist:
        video = DBOperation.query_video_via_aid(aid, session)
        if video is not None:
            # video already exist
            raise AlreadyExistError(table='tdd_video', params={'aid': aid})

    # get video view
    try:
        video_view = service.get_video_view({'aid': aid})
    except ServiceError as e:
        raise e

    # assemble video
    new_video = TddVideo(
        added=get_ts_s(),
        aid=aid,
        bvid=video_view.bvid[2:],  # remove 'BV' prefix
        videos=video_view.videos,
        tid=video_view.tid,
        tname=video_view.tname,
        copyright=video_view.copyright,
        pic=video_view.pic,
        title=video_view.title,
        pubdate=video_view.pubdate,
        desc=video_view.desc,
        mid=video_view.owner.mid,
        code=0,
        state=video_view.state,
    )

    # optional attributes
    if video_view.attribute is not None:
        new_video.attribute = video_view.attribute
    if video_view.forward is not None:
        new_video.forward = video_view.forward

    # set tags
    new_video.tags = get_video_tags_str(aid, service)

    # set recent
    new_video.recent = 2

    # set isvc
    for tag in new_video.tags.split(';'):
        if tag.upper() == 'VOCALOID中文曲':
            new_video.isvc = 2
            break

    # create member mid set
    member_mid_set: set[int] = {new_video.mid}

    # add member
    try:
        add_member(new_video.mid, service, session)
    except AlreadyExistError as e:
        logger.debug(f'Uploader member of new video already exist! '
                     f'video: {new_video}, mid: {new_video.mid}, error: {e}')
    except Exception as e:
        logger.error(f'Fail to add uploader member when add new video! '
                     f'video: {new_video}, mid: {new_video.mid}, error: {e}')
        raise e

    # add staff
    if video_view.staff is not None:
        new_video.hasstaff = 1
        for staff_item in video_view.staff:
            member_mid_set.add(staff_item.mid)
            try:
                add_member(staff_item.mid, service, session)
            except AlreadyExistError:
                logger.debug(f'Staff member of new video already exist! '
                             f'video: {new_video}, mid: {staff_item.mid}')
            except Exception as e:
                logger.error(f'Fail to add staff member when add new video! '
                             f'video: {new_video}, mid: {staff_item.mid}, error: {e}')
                raise e
            try:
                commit_staff(new_video.added, aid, staff_item.mid, staff_item.title, session)
            except Exception as e:
                logger.error(f'Fail to add staff member when add new video! '
                             f'video: {new_video}, mid: {staff_item.mid}, title: {staff_item.title}, error: {e}')
                raise e
    else:
        new_video.hasstaff = 0

    # add to db
    # TODO: use new db operation which can raise exception
    DBOperation.add(new_video, session)

    # update member last_video and video_count
    try:
        for mid in member_mid_set:
            member = DBOperation.query_member_via_mid(mid, session)
            member.last_video = new_video.id
            member.video_count += 1
            session.commit()
    except Exception as e:
        logger.error(f'Fail to update member last_video and video_count! '
                     f'video: {new_video}, error: {e}')
        session.rollback()
        raise e

    return new_video


def update_video(aid: int, service: Service, session: Session) -> List[TddVideoLog]:
    # get current video
    # TODO: use new db operation which can raise exception
    curr_video: TddVideo = DBOperation.query_video_via_aid(aid, session)
    if curr_video is None:
        raise NotExistError(table='tdd_video', params={'aid': aid})

    video_update_logs: List[TddVideoLog] = []
    bvid = a2b(aid)

    # get video view
    try:
        video_view = service.get_video_view({'aid': aid})
    except CodeError as e:
        if e.code != curr_video.code:
            if e.code != -403:  # just due to not logged in, actually it's okay
                video_update_logs.append(
                    TddVideoLog(get_ts_s(), aid, bvid,
                                'code', curr_video.code, e.code))
                curr_video.code = e.code
    except ServiceError as e:
        raise e
    else:
        # check attributes
        added = get_ts_s()
        # code
        if 0 != curr_video.code:
            video_update_logs.append(
                TddVideoLog(added, aid, bvid,
                            'code', curr_video.code, 0))
            curr_video.code = 0
        # videos
        if video_view.videos != curr_video.videos:
            video_update_logs.append(
                TddVideoLog(added, aid, bvid,
                            'videos', curr_video.videos, video_view.videos))
            curr_video.videos = video_view.videos
        # tid
        if video_view.tid != curr_video.tid:
            video_update_logs.append(
                TddVideoLog(added, aid, bvid,
                            'tid', curr_video.tid, video_view.tid))
            curr_video.tid = video_view.tid
        # tname
        if video_view.tname != curr_video.tname:
            video_update_logs.append(
                TddVideoLog(added, aid, bvid,
                            'tname', curr_video.tname, video_view.tname))
            curr_video.tname = video_view.tname
        # copyright
        if video_view.copyright != curr_video.copyright:
            video_update_logs.append(
                TddVideoLog(added, aid, bvid,
                            'copyright', curr_video.copyright, video_view.copyright))
            curr_video.copyright = video_view.copyright
        # pic
        if not same_pic_url(video_view.pic, curr_video.pic):
            video_update_logs.append(
                TddVideoLog(added, aid, bvid,
                            'pic', curr_video.pic, video_view.pic))
            curr_video.pic = video_view.pic
        # title
        if video_view.title != curr_video.title:
            video_update_logs.append(
                TddVideoLog(added, aid, bvid,
                            'title', curr_video.title, video_view.title))
            curr_video.title = video_view.title
        # pubdate
        if video_view.pubdate != curr_video.pubdate:
            video_update_logs.append(
                TddVideoLog(added, aid, bvid,
                            'pubdate', curr_video.pubdate, video_view.pubdate))
            curr_video.pubdate = video_view.pubdate
        # desc
        if video_view.desc != curr_video.desc:
            video_update_logs.append(
                TddVideoLog(added, aid, bvid,
                            'desc', curr_video.desc, video_view.desc))
            curr_video.desc = video_view.desc
        # mid
        if video_view.owner.mid != curr_video.mid:
            video_update_logs.append(
                TddVideoLog(added, aid, bvid,
                            'mid', curr_video.mid, video_view.owner.mid))
            curr_video.mid = video_view.owner.mid
        # attribute
        if video_view.attribute is not None:
            if video_view.attribute != curr_video.attribute:
                video_update_logs.append(
                    TddVideoLog(added, aid, bvid,
                                'attribute', curr_video.attribute, video_view.attribute))
                curr_video.attribute = video_view.attribute
        # state
        if video_view.state != curr_video.state:
            video_update_logs.append(
                TddVideoLog(added, aid, bvid,
                            'state', curr_video.state, video_view.state))
            curr_video.state = video_view.state
        # forward
        if video_view.forward is not None:
            if video_view.forward != curr_video.forward:
                video_update_logs.append(
                    TddVideoLog(added, aid, bvid,
                                'forward', curr_video.forward, video_view.forward))
                curr_video.forward = video_view.forward
        # hasstaff
        new_hasstaff = 1 if video_view.staff is not None else 0
        if new_hasstaff != curr_video.hasstaff:
            video_update_logs.append(
                TddVideoLog(added, aid, bvid,
                            'hasstaff', curr_video.hasstaff, new_hasstaff))
            curr_video.hasstaff = new_hasstaff
        # update staff list
        # TODO: update staff last_video and video_count if changed
        curr_staff_list = DBOperation.query_video_staff_via_aid(aid, session)
        if new_hasstaff == 1:
            for staff_item in video_view.staff:
                # find staff in db
                curr_staff_item_found = None
                for curr_staff_item in curr_staff_list:
                    if curr_staff_item.mid == staff_item.mid:
                        curr_staff_item_found = curr_staff_item
                        curr_staff_list.remove(curr_staff_item)
                if curr_staff_item_found:
                    # staff exist, check it
                    if staff_item.title != curr_staff_item_found.title:
                        video_update_logs.append(
                            TddVideoLog(added, aid, bvid,
                                        'staff',
                                        f'mid: {curr_staff_item_found.mid}; title: {curr_staff_item_found.title}',
                                        f'mid: {staff_item.mid}; title: {staff_item.title}'))
                        curr_staff_item_found.title = staff_item.title
                else:
                    # staff not exist, add it
                    # add member if not exists
                    try:
                        add_member(staff_item.mid, service, session)
                    except AlreadyExistError:
                        logger.debug(f'Staff member of video already exist! '
                                     f'mid: {staff_item.mid}')
                    except TddError as e:
                        logger.warning(f'Fail to add new member before add new staff!'
                                       f'aid: {aid}, mid: {staff_item.mid}, error: {e}')
                    # add staff
                    try:
                        new_staff = commit_staff(added, aid, staff_item.mid, staff_item.title, session)
                    except TddError as e:
                        logger.warning(f'Fail to add new staff!'
                                       f'aid: {aid}, mid: {staff_item.mid}, title: {staff_item.title}, error: {e}')
                    else:
                        video_update_logs.append(
                            TddVideoLog(added, aid, bvid,
                                        'staff', None, f'mid: {new_staff.mid}; title: {new_staff.title}'))
            # remove staff left in old_staff_list
            for curr_staff_item in curr_staff_list:
                DBOperation.delete_video_staff_via_id(curr_staff_item.id, session)
                video_update_logs.append(
                    TddVideoLog(added, aid, bvid,
                                'staff', f'mid: {curr_staff_item.mid}; title: {curr_staff_item.title}', None))
        # update tags string
        new_tags = get_video_tags_str(aid, service)
        new_tags_sorted = ';'.join(sorted(new_tags.split(';')))
        curr_tags_sorted = ';'.join(sorted(curr_video.tags.split(';')))
        if new_tags_sorted != curr_tags_sorted:
            video_update_logs.append(
                TddVideoLog(added, aid, bvid,
                            'tags', curr_video.tags, new_tags))
            curr_video.tags = new_tags

    # commit changes
    try:
        session.commit()
    except Exception as e:
        logger.error(f'Fail to commit changes! object: {curr_video}, error: {e}')
        session.rollback()
        raise e

    # add to db
    # TODO: use new db operation which can raise exception
    for log in video_update_logs:
        DBOperation.add(log, session)

    return video_update_logs


def add_member(mid: int, service: Service, session: Session, test_exist=True):
    # test exist
    if test_exist:
        member = DBOperation.query_member_via_mid(mid, session)
        if member is not None:
            # member already exist
            raise AlreadyExistError(table='member', params={'mid': mid})

    # get member space
    try:
        member_space = service.get_member_space({'mid': mid})
    except ServiceError as e:
        raise e

    # assemble member
    new_member = TddMember(
        mid=mid,
        added=get_ts_s(),
        sex=member_space.sex,
        name=member_space.name,
        face=member_space.face,
        sign=member_space.sign,
        code=0
    )

    # add to db
    # TODO: use new db operation which can raise exception
    DBOperation.add(new_member, session)

    return new_member


def update_member(mid: int, service: Service, session: Session):
    # get current member
    # TODO: use new db operation which can raise exception
    curr_member: TddMember = DBOperation.query_member_via_mid(mid, session)
    if curr_member is None:
        raise NotExistError(table='tdd_member', params={'mid': mid})

    member_update_logs: List[TddMemberLog] = []

    # get member space
    try:
        member_space = service.get_member_space({'mid': mid})
    except CodeError as e:
        # code maybe -404, otherwise anti-crawler triggered, raise error
        if e.code != -404:
            raise e
        if e.code != curr_member.code:
            member_update_logs.append(
                TddMemberLog(get_ts_s(), mid,
                             'code', curr_member.code, e.code))
            curr_member.code = e.code
    except ServiceError as e:
        raise e
    else:
        # check attributes
        added = get_ts_s()
        # code
        if 0 != curr_member.code:
            member_update_logs.append(
                TddMemberLog(added, mid,
                             'code', curr_member.code, 0))
            curr_member.code = 0
        # sex
        if member_space.sex != curr_member.sex:
            member_update_logs.append(
                TddMemberLog(added, mid,
                             'sex', curr_member.sex, member_space.sex))
            curr_member.sex = member_space.sex
        # name
        if member_space.name != curr_member.name:
            member_update_logs.append(
                TddMemberLog(added, mid,
                             'name', curr_member.name, member_space.name))
            curr_member.name = member_space.name
        # face
        if not same_pic_url(member_space.face, curr_member.face):
            member_update_logs.append(
                TddMemberLog(added, mid,
                             'face', curr_member.face, member_space.face))
            curr_member.face = member_space.face
        # sign
        if member_space.sign != curr_member.sign:
            member_update_logs.append(
                TddMemberLog(added, mid,
                             'sign', curr_member.sign, member_space.sign))
            curr_member.sign = member_space.sign

    # commit changes
    try:
        session.commit()
    except Exception as e:
        logger.error(f'Fail to commit changes! object: {curr_member}, error: {e}')
        session.rollback()
        raise e

    # add to db
    # TODO: use new db operation which can raise exception
    for log in member_update_logs:
        DBOperation.add(log, session)

    return member_update_logs


def commit_staff(added: int, aid: int, mid: int, title: str, session: Session, test_exist=True):
    bvid = a2b(aid)

    # test exist
    if test_exist:
        staff = DBOperation.query_video_staff_via_aid_mid(aid, mid, session)
        if staff is not None:
            # staff already exist
            raise AlreadyExistError(table='video_staff', params={'aid': aid, 'mid': mid})

    # assemble staff
    new_staff = TddVideoStaff(
        added=added,
        aid=aid,
        bvid=bvid,
        mid=mid,
        title=title
    )

    # add to db
    # TODO: use new db operation which can raise exception
    DBOperation.add(new_staff, session)

    return new_staff


def add_member_follower_record(mid: int, service: Service, session: Session):
    # get member relation
    try:
        member_relation = service.get_member_relation({'vmid': mid})
    except ServiceError as e:
        raise e

    # assemble follower record
    new_follower_record = TddMemberFollowerRecord(
        mid=mid,
        added=get_ts_s(),
        follower=member_relation.follower,
    )

    # add to db
    # TODO: use new db operation which can raise exception
    DBOperation.add(new_follower_record, session)

    return new_follower_record


def get_video_tags_str(aid: int, service: Service) -> str:
    # get video tags
    try:
        video_tags = service.get_video_tags({'aid': aid})
    except ServiceError as e:
        raise e

    # assemble tags string
    tags_str = ''
    for tag in video_tags.tags:
        tags_str += tag.tag_name
        tags_str += ';'

    return tags_str
