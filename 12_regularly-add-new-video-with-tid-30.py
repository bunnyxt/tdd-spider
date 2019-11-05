from pybiliapi import BiliApi
from logger import logger_12
from db import Session, DBOperation, TddVideo, TddMember, TddVideoStaff
from util import get_ts_s
import math
import time
import schedule
import threading


def regularly_add_new_video():
    logger_12.info('Now start add new video with tid 30...')

    bapi = BiliApi()
    session = Session()

    # get last added aids
    last_15_videos = DBOperation.query_last_x_video(15, session)
    last_15_aids = list(map(lambda x: x.aid, last_15_videos))
    logger_12.info('Last 15 aids: %s' % last_15_aids)

    # get page total
    obj = bapi.get_archive_rank_by_partion(30, 1, 50)
    page_total = math.ceil(obj['data']['page']['count'] / 50)
    logger_12.info('%d page(s) found.' % page_total)

    # add add video
    page_num = 1
    goon = True
    last_aid_count = 0
    added_aid_count = 0
    while page_num <= page_total and goon:
        # get obj via awesome api
        obj = bapi.get_archive_rank_by_partion(30, page_num, 50)
        is_valid = False
        re_count = 1
        while True:
            # ensure obj is valid
            try:
                for _ in obj['data']['archives']:
                    pass
                is_valid = True
                break
            except TypeError:
                logger_12.warning('TypeError caught, re-call page_num = %d, re_count = %d' % (page_num, re_count))
                re_count += 1
                if re_count == 5:
                    logger_12.warning('Fail to get valid obj with page_num %d, continue to next page' % page_num)
                    break
                time.sleep(1)
                obj = bapi.get_archive_rank_by_partion(30, page_num, 50)
        if not is_valid:
            page_num += 1
            continue

        # process each video
        try:
            for arch in obj['data']['archives']:
                aid = arch['aid']

                # check in last aids or not
                if last_aid_count >= 5:
                    logger_12.info('5 last aids meet, now break')
                    goon = False
                    break

                if aid in last_15_aids:
                    last_aid_count += 1
                    logger_12.info('Aid %d in last aids, count %d / 5, now continue' % (aid, last_aid_count))
                    continue

                # check aid added or not
                video = DBOperation.query_video_via_aid(aid, session)
                if video is None:
                    # get other param
                    added = get_ts_s()
                    videos = arch['videos']
                    tid = arch['tid']
                    tname = arch['tname']
                    copyright = arch['copyright']
                    pic = arch['pic']
                    title = arch['title']
                    desc = arch['description']
                    mid = arch['mid']

                    # get pubdate ts
                    view_obj = bapi.get_video_view(aid)
                    is_valid = False
                    re_count = 1
                    while True:
                        # ensure view_obj is valid
                        try:
                            _ = view_obj['data']['pubdate']
                            _ = view_obj['code']
                            is_valid = True
                            break
                        except Exception as e:
                            logger_12.warning(
                                'Exception %s, re-call view api aid = %d, re_count = %d' % (e, aid, re_count),
                                exc_info=True)
                            re_count += 1
                            if re_count == 5:
                                logger_12.warning(
                                    'Fail to get valid view with aid %d, continue to next aid' % aid)
                                break
                            time.sleep(1)
                            view_obj = bapi.get_video_view(aid)
                    if not is_valid:
                        continue
                    pubdate = view_obj['data']['pubdate']
                    code = view_obj['code']

                    # get tags and check isvc
                    tags = ''
                    isvc = -1
                    tags_obj = bapi.get_video_tags(aid)
                    is_valid = False
                    re_count = 1
                    while True:
                        # ensure tags_obj is valid
                        try:
                            for _ in tags_obj['data']:
                                pass
                            is_valid = True
                            break
                        except Exception as e:
                            logger_12.warning(
                                'Exception %s, re-call view api aid = %d, re_count = %d' % (e, aid, re_count),
                                exc_info=True)
                            re_count += 1
                            if re_count == 5:
                                logger_12.warning('Fail to get valid tags with aid %d, continue to next aid' % aid)
                                break
                            time.sleep(1)
                            tags_obj = bapi.get_video_tags(aid)
                    if not is_valid:
                        continue
                    try:
                        tags_str = ''
                        for tag in tags_obj['data']:
                            tag_name = tag['tag_name']
                            if tag_name == 'VOCALOID中文曲':
                                isvc = 2
                                logger_12.info('VOCALOID中文曲 tag detected, set isvc = 2.')
                            tags_str += tag_name
                            tags_str += ';'
                        tags = tags_str
                    except Exception as e:
                        logger_12.warning(
                            'Exception %s, fail to get tags of aid %d, got tags obj %s.' % (e, aid, tags_obj))

                    # add member and staff if isvc = 2
                    if isvc == 2:
                        # add member
                        member = DBOperation.query_member_via_mid(mid, session)
                        if member is None:
                            member_obj = bapi.get_member(mid)
                            is_valid = False
                            re_count = 1
                            while True:
                                # ensure member_obj is valid
                                try:
                                    _ = member_obj['data']
                                    is_valid = True
                                    break
                                except Exception as e:
                                    logger_12.warning(
                                        'Exception %s, re-call view api mid = %d, re_count = %d' % (e, mid, re_count),
                                        exc_info=True)
                                    re_count += 1
                                    if re_count == 5:
                                        logger_12.warning(
                                            'Fail to get valid member obj with mid %d!' % mid)
                                        break
                                    time.sleep(1)
                                    member_obj = bapi.get_member(mid)
                            if is_valid:
                                if member_obj['code'] == 0:
                                    member_data = member_obj['data']
                                    member_added = get_ts_s()
                                    member_mid = member_data['mid']
                                    member_sex = member_data['sex']
                                    member_name = member_data['name']
                                    member_face = member_data['face']
                                    member_sign = member_data['sign']

                                    new_member = TddMember()
                                    new_member.added = member_added
                                    new_member.mid = member_mid
                                    new_member.sex = member_sex
                                    new_member.name = member_name
                                    new_member.face = member_face
                                    new_member.sign = member_sign

                                    DBOperation.add(new_member, session)
                                    logger_12.info('Add new member %s.' % new_member)
                                else:
                                    logger_12.warning(
                                        'Member mid = %d has code %d, do not add to db.' % (mid, member_obj['code']))
                            else:
                                pass
                        else:
                            logger_12.info('UP mid %d already exist!' % mid)
                        time.sleep(0.2)

                        # add staff
                        if 'staff' in view_obj['data'].keys():
                            hasstaff = 1
                            for staff in view_obj['data']['staff']:
                                staff_mid = staff['mid']
                                staff_title = staff['title']

                                new_staff = TddVideoStaff()
                                new_staff.added = added
                                new_staff.aid = aid
                                new_staff.mid = staff_mid
                                new_staff.title = staff_title

                                DBOperation.add(new_staff, session)
                                logger_12.info('Add new video staff %s.' % new_staff)

                                # add staff member
                                member = DBOperation.query_member_via_mid(staff_mid, session)
                                if member is None:
                                    member_obj = bapi.get_member(staff_mid)
                                    is_valid = False
                                    re_count = 1
                                    while True:
                                        # ensure member_obj is valid
                                        try:
                                            _ = member_obj['data']
                                            is_valid = True
                                            break
                                        except Exception as e:
                                            logger_12.warning(
                                                'Exception %s, re-call view api mid = %d, re_count = %d' % (
                                                    e, staff_mid, re_count),
                                                exc_info=True)
                                            re_count += 1
                                            if re_count == 5:
                                                logger_12.warning(
                                                    'Fail to get valid member obj with mid %d!' % staff_mid)
                                                break
                                            time.sleep(1)
                                            member_obj = bapi.get_member(staff_mid)
                                    if is_valid:
                                        if member_obj['code'] == 0:
                                            member_data = member_obj['data']
                                            member_added = get_ts_s()
                                            member_mid = member_data['mid']
                                            member_sex = member_data['sex']
                                            member_name = member_data['name']
                                            member_face = member_data['face']
                                            member_sign = member_data['sign']

                                            new_member = TddMember()
                                            new_member.added = member_added
                                            new_member.mid = member_mid
                                            new_member.sex = member_sex
                                            new_member.name = member_name
                                            new_member.face = member_face
                                            new_member.sign = member_sign

                                            DBOperation.add(new_member, session)
                                            logger_12.info('Add new member %s.' % new_member)
                                        else:
                                            logger_12.warning('Member mid = %d has code %d, do not add to db.' % (
                                                staff_mid, member_obj['code']))
                                    else:
                                        pass
                                else:
                                    logger_12.info('Staff mid %d already exist!' % mid)
                                time.sleep(0.2)
                        else:
                            hasstaff = 0
                    else:
                        hasstaff = -1

                    new_video = TddVideo()
                    new_video.added = added
                    new_video.aid = aid
                    new_video.videos = videos
                    new_video.tid = tid
                    new_video.tname = tname
                    new_video.copyright = copyright
                    new_video.pic = pic
                    new_video.title = title
                    new_video.pubdate = pubdate
                    new_video.desc = desc
                    new_video.tags = tags
                    new_video.mid = mid
                    new_video.code = code
                    new_video.hasstaff = hasstaff
                    new_video.isvc = isvc

                    DBOperation.add(new_video, session)
                    logger_12.info('Add new video %s.' % new_video)
                    added_aid_count += 1

                    time.sleep(0.2)  # since used view api and tag api, need sleep to avoid ip being banned
                else:
                    logger_12.info('Aid %d has already added!' % aid)
                    time.sleep(0.05)
        except Exception as e:
            logger_12.error('Exception caught. Detail: %s' % e)

        # update page num
        logger_12.info('Page %d / %d done.' % (page_num, page_total))
        page_total = math.ceil(obj['data']['page']['count'] / 50)
        page_num += 1

    logger_12.info('%d new video(s) added.' % added_aid_count)
    logger_12.info('Finish add new video with tid 30!')


def regularly_add_new_video_task():
    threading.Thread(target=regularly_add_new_video).start()


def main():
    logger_12.info('Regularly add new video with tid 30 registered.')
    regularly_add_new_video_task()
    schedule.every(10).minutes.do(regularly_add_new_video_task)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    main()
