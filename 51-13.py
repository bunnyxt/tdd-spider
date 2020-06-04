from logger import logger_51
from util import ts_s_to_str, get_ts_s, get_week_day, str_to_ts_s, zk_calc
from db import Session, DBOperation


def main():
    logger_51.info('51-13: video record rank weekly')

    hour_label = ts_s_to_str(get_ts_s())[:13] + ':00'
    time_label = hour_label[11:]
    logger_51.info('hour_label: ' + hour_label)

    session = Session()

    logger_51.info('13-1: update tdd_video_record_rank_weekly_base')

    if time_label == '03:00' and get_week_day() == 5:
        try:
            # TODO test validity
            # create table from tdd_video_record_hourly

            # TODO move to history before drop
            drop_tmp_table_sql = 'drop table if exists tdd_video_record_rank_weekly_base_tmp'
            session.execute(drop_tmp_table_sql)
            logger_51.info(drop_tmp_table_sql)

            hour_start_ts = str_to_ts_s(ts_s_to_str(get_ts_s())[:11] + '03:00:00')
            create_tmp_table_sql = 'create table tdd_video_record_rank_weekly_base_tmp ' + \
                                   'select * from tdd_video_record_hourly where added >= %d' % hour_start_ts
            session.execute(create_tmp_table_sql)
            logger_51.info(create_tmp_table_sql)

            drop_old_table_sql = 'drop table if exists tdd_video_record_rank_weekly_base'
            session.execute(drop_old_table_sql)
            logger_51.info(drop_old_table_sql)

            rename_tmp_table_sql = 'rename table tdd_video_record_rank_weekly_base_tmp to ' + \
                                   'tdd_video_record_rank_weekly_base'
            session.execute(rename_tmp_table_sql)
            logger_51.info(rename_tmp_table_sql)
        except Exception as e:
            session.rollback()
            logger_51.warning('Error occur when executing update tdd_video_record_rank_weekly_base. Detail: %s' % e)
    else:
        logger_51.info('13-1 done! not Sat 03:00 pass, no need to update tdd_video_record_rank_weekly_base')

    logger_51.info('13-2: update tdd_video_record_rank_weekly_current')

    # get record base dict
    video_record_base_dict = DBOperation.query_video_record_rank_weekly_base_dict(session)
    logger_51.info('video_record_base_dict got from db')

    # load video_record_now_list from file
    data_folder = 'data/'
    file_name = hour_label + '.csv'
    video_record_now_list = []
    with open(data_folder + file_name, 'r') as f:
        f.readline()  # ignore first line
        while True:
            line = f.readline()
            if not line:
                break
            line = line.rstrip('\n')
            line_arr = line.split(',')
            if len(line_arr) == 9:
                video_record_now_list.append(tuple(line_arr))
            else:
                logger_51.warning('incorrect line format, line: ' + line)
    logger_51.info('video_record_now_list loaded from file')

    # get video videos(page) dict from db
    video_videos_dict = DBOperation.query_video_videos_dict(session)
    logger_51.info('video_videos_dict got from db')

    try:
        drop_tmp_table_sql = 'drop table if exists tdd_video_record_rank_weekly_current_tmp'
        session.execute(drop_tmp_table_sql)
        logger_51.info(drop_tmp_table_sql)

        create_tmp_table_sql = 'create table tdd_video_record_rank_weekly_current_tmp ' + \
                               'like tdd_video_record_rank_weekly_current'
        session.execute(create_tmp_table_sql)
        logger_51.info(create_tmp_table_sql)
    except Exception as e:
        session.rollback()
        logger_51.warning('Error occur when executing update tdd_video_record_rank_weekly_base. Detail: %s' % e)

    logger_51.info('now making video_record_weekly_curr_list...')
    video_record_weekly_curr_list = []
    video_record_weekly_curr_made_count = 0
    for rn in video_record_now_list:
        # check bvid exists in base or not
        bvid = rn[1]
        if bvid in video_record_base_dict.keys():
            rb = video_record_base_dict[bvid]
        else:
            rb = (0, 0, 0, 0, 0, 0, 0, 0)
        d_view = rn[2] - rb[1]  # maybe occur -1?
        d_danmaku = rn[3] - rb[2]
        d_reply = rn[4] - rb[3]
        d_favorite = rn[5] - rb[4]
        d_coin = rn[6] - rb[5]
        d_share = rn[7] - rb[6]
        d_like = rn[8] - rb[7]
        page = 1
        if bvid in video_videos_dict.keys():
            page = video_videos_dict[bvid]
        point, xiua, xiub = zk_calc(d_view, d_danmaku, d_reply, d_favorite, page=page)
        # append to list
        video_record_weekly_curr_list.append([bvid, rb[0], rn[0],
                                              rn[2], rn[3], rn[4], rn[5], rn[6], rn[7], rn[8],
                                              d_view, d_danmaku, d_reply, d_favorite, d_coin, d_share, d_like,
                                              point, xiua, xiub])
        video_record_weekly_curr_made_count += 1
        if video_record_weekly_curr_made_count % 10000 == 0:
            logger_51.info('make %d / %d done' % (video_record_weekly_curr_made_count, len(video_record_now_list)))
    logger_51.info('make %d / %d done' % (video_record_weekly_curr_made_count, len(video_record_now_list)))
    logger_51.info('finish make video_record_weekly_curr_list')

    # sort via point
    video_record_weekly_curr_list.sort(key=lambda x: (x[17], x[10])).reverse()  # TODO if point equals?
    logger_51.info('finish sort video_record_weekly_curr_list')

    logger_51.info('now inserting video_record_weekly_curr_list...')
    video_record_weekly_curr_added_count = 0
    rank = 1
    for c in video_record_weekly_curr_list:
        # dont use sql alchemy in order to save memory
        sql = 'insert into tdd_video_record_rank_weekly_current_tmp' \
              'values("%s", %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %f, %f, %f, %d)' % \
              (c[0], c[1], c[2],
               c[3], c[4], c[5], c[6], c[7], c[8], c[9],
               c[10], c[11], c[12], c[13], c[14], c[15], c[16],
               c[17], c[18], c[19],
               rank)
        rank += 1
        session.execute(sql)
        video_record_weekly_curr_added_count += 1
        if video_record_weekly_curr_added_count % 10000 == 0:
            session.commit()
            logger_51.info('insert %d / %d done' % (video_record_weekly_curr_added_count,
                                                    len(video_record_weekly_curr_list)))
    session.commit()
    logger_51.info('insert %d / %d done' % (video_record_weekly_curr_added_count, len(video_record_weekly_curr_list)))

    try:
        drop_old_table_sql = 'drop table if exists tdd_video_record_rank_weekly_current'
        session.execute(drop_old_table_sql)
        logger_51.info(drop_old_table_sql)

        rename_tmp_table_sql = 'rename table tdd_video_record_rank_weekly_current_tmp to' + \
                               'tdd_video_record_rank_weekly_current'
        session.execute(rename_tmp_table_sql)
        logger_51.info(rename_tmp_table_sql)
    except Exception as e:
        session.rollback()
        logger_51.warning('Error occur when executing update tdd_video_record_rank_weekly_base. Detail: %s' % e)

    logger_51.info('13-2: done! Finish updating tdd_video_record_rank_weekly_current')

    session.close()


if __name__ == '__main__':
    main()
