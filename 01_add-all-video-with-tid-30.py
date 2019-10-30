from pybiliapi import BiliApi
from logger import logger_01
from db import Session, DBOperation, TddVideo
from util import get_ts_s
import math
import time


def main():
    logger_01.info('Now start add all video with tid 30...')

    bapi = BiliApi()
    session = Session()

    # get page total
    obj = bapi.get_archive_rank_by_partion(30, 1, 50)
    page_total = math.ceil(obj['data']['page']['count'] / 50)
    logger_01.info('%d page(s) found.' % page_total)

    # add add video
    page_num = 1
    while page_num <= page_total:
        # get obj via awesome api
        obj = bapi.get_archive_rank_by_partion(30, page_num, 50)
        while True:
            # ensure obj is valid
            try:
                for _ in obj['data']['archives']:
                    pass
                break
            except TypeError:
                logger_01.warning('TypeError caught, re-call page_num = %d' % page_num)
                time.sleep(1)
                obj = bapi.get_archive_rank_by_partion(30, page_num, 50)

        # process each video
        try:
            for arch in obj['data']['archives']:
                aid = arch['aid']
                # check aid added or not
                video = DBOperation.query_tdd_video_via_aid(aid, session)
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
                    while True:
                        # ensure view_obj is valid
                        try:
                            _ = view_obj['data']['pubdate']
                            _ = view_obj['code']
                            break
                        except Exception as e:
                            logger_01.warning('Exception %s, re-call view api aid = %d' % (e, aid), exc_info=True)
                            time.sleep(1)
                            view_obj = bapi.get_video_view(aid)
                    pubdate = view_obj['data']['pubdate']
                    code = view_obj['code']

                    # get tags and check isvc
                    tags = ''
                    isvc = -1
                    tags_obj = bapi.get_video_tags(aid)
                    while True:
                        # ensure tags_obj is valid
                        try:
                            for _ in tags_obj['data']:
                                pass
                            break
                        except Exception as e:
                            logger_01.warning('Exception %s, re-call view api aid = %d' % (e, aid), exc_info=True)
                            time.sleep(1)
                            tags_obj = bapi.get_video_tags(aid)
                    try:
                        tags_str = ''
                        for tag in tags_obj['data']:
                            tag_name = tag['tag_name']
                            if tag_name == 'VOCALOID中文曲':
                                isvc = 2
                                logger_01.info('VOCALOID中文曲 tag detected, set isvc = 2.')
                            tags_str += tag_name
                            tags_str += ';'
                        tags = tags_str
                    except Exception as e:
                        logger_01.warning(
                            'Exception %s, fail to get tags of aid %d, got tags obj %s.' % (e, aid, tags_obj))

                    # do not add staff now, even with isvc = 2
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
                    logger_01.info('Add new video %s.' % new_video)

                    time.sleep(0.2)  # since used view api and tag api, need sleep to avoid ip being banned
                else:
                    logger_01.info('Aid %d has already added!' % aid)
        except Exception as e:
            logger_01.error('Exception caught. Detail: %s' % e)

        # update page num
        logger_01.info('Page %d / %d done.' % (page_num, page_total))
        page_total = math.ceil(obj['data']['page']['count'] / 50)
        page_num += 1

    logger_01.info('Finish add all video with tid 30!')


if __name__ == '__main__':
    main()
