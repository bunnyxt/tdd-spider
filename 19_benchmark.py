from collections import defaultdict, namedtuple
from db import DBOperation, Session, TddVideoRecordAbnormalChange
from util import get_ts_s, ts_s_to_str


def prt(s):
    ss = '[%s] %s' % (ts_s_to_str(get_ts_s()), s)
    print(ss)
    with open('19_output.log', 'a') as f:
        f.write('%s\n' % ss)


def main():
    session = Session()

    prt('07: load records from history files')

    # load records from history files
    history_filename_list = []
    index_filename = 'data2/index.txt'
    with open(index_filename, 'r') as f:
        lines = f.readlines()
        for line in lines[-13:]:
            history_filename_list.append(line.rstrip('\n'))
    prt('Will load records from file list %r' % history_filename_list)

    VideoRecord = namedtuple("VideoRecord",
                             ['aid', 'added', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'like'])
    history_record_dict = defaultdict(list)
    history_record_count = 0
    for filename in history_filename_list:
        try:
            with open(filename, 'r') as f:
                lines = f.readlines()
                for line in lines[1:]:
                    try:
                        line_list = line.rstrip('\n').split(',')
                        video_record = VideoRecord(
                            int(line_list[0]),
                            int(line_list[1]),
                            int(line_list[2]),
                            int(line_list[3]),
                            int(line_list[4]),
                            int(line_list[5]),
                            int(line_list[6]),
                            int(line_list[7]),
                            int(line_list[8]),
                        )
                        video_record_list = history_record_dict[video_record.aid]
                        video_record_list.append(video_record)
                    except Exception as e:
                        prt('Fail to make video record from line: %s. Exception caught. Detail: %s'
                                          % (line, e))
                    finally:
                        history_record_count += 1
            prt('Finish load records from file %s' % filename)
        except Exception as e:
            prt('Fail to read load records from file %s. Exception caught. Detail: %s' % (filename, e))

    prt('07 done! loaded %d history records from %d files'
                   % (history_record_count, len(history_filename_list)))

    prt('08: check params of history video records')

    # get video pubdate
    video_pubdate_list = DBOperation.query_video_pubdate_all(session)
    video_pubdate_dict = dict()
    for (aid, pubdate) in video_pubdate_list:
        video_pubdate_dict[aid] = pubdate
    prt('Finish make video pubdate dict with %d aids.' % len(video_pubdate_dict))

    # check record
    aids = history_record_dict.keys()
    check_total_count = len(aids)
    check_visited_count = 0
    for aid in aids[:1000]:
        video_record_list = history_record_dict[aid]
        if len(video_record_list) <= 2:  # at least require 3 record
            continue

        video_record_list.sort(key=lambda r: r.added)

        record = video_record_list[-1]
        # remove all zero situation
        if record.view == 0 and record.danmaku == 0 and record.reply == 0 and record.favorite == 0 and \
                record.coin == 0 and record.share == 0 and record.like == 0:
            prt('%d got all params of record = 0, maybe API bug, continue' % record.aid)
            continue

        # remove abnormal all zero VideoRecord
        abnormal_all_zero_index_list = []
        for i in range(len(video_record_list)):
            video_record = video_record_list[i]
            if video_record.view == 0 and video_record.danmaku == 0 and video_record.reply == 0 and \
                    video_record.favorite == 0 and video_record.coin == 0 and video_record.share == 0 and \
                    video_record.like == 0:
                if i == 0:
                    abnormal_all_zero_index_list.append(i)  # start from all zero, remove it
                else:
                    video_record_last = video_record_list[i - 1]
                    if video_record_last.view == 0 and video_record_last.danmaku == 0 and video_record_last.reply == 0 and \
                            video_record_last.favorite == 0 and video_record_last.coin == 0 and video_record_last.share == 0 and \
                            video_record_last.like == 0:
                        pass
                    else:
                        abnormal_all_zero_index_list.append(i)  # from not all zero to zero, remove it
        for i in reversed(abnormal_all_zero_index_list):
            prt('%d found abnormal all zero video record at %d, delete it'
                              % (aid, video_record_list[i].added))
            del video_record_list[i]

        if len(video_record_list) <= 2:  # at least require 3 record
            continue

        timespan_now = video_record_list[-1].added - video_record_list[-2].added
        if timespan_now == 0:
            prt('%d got timespan_now = 0, continue' % aid)
            continue
        speed_now_dict = dict()
        speed_now_dict['view'] = (video_record_list[-1].view - video_record_list[-2].view) / timespan_now * 3600
        speed_now_dict['danmaku'] = (video_record_list[-1].danmaku - video_record_list[-2].danmaku) / timespan_now * 3600
        speed_now_dict['reply'] = (video_record_list[-1].reply - video_record_list[-2].reply) / timespan_now * 3600
        speed_now_dict['favorite'] = (video_record_list[-1].favorite - video_record_list[-2].favorite) / timespan_now * 3600
        speed_now_dict['coin'] = (video_record_list[-1].coin - video_record_list[-2].coin) / timespan_now * 3600
        speed_now_dict['share'] = (video_record_list[-1].share - video_record_list[-2].share) / timespan_now * 3600
        speed_now_dict['like'] = (video_record_list[-1].like - video_record_list[-2].like) / timespan_now * 3600

        timespan_last = video_record_list[-2].added - video_record_list[-3].added
        if timespan_last == 0:
            prt('%d got timespan_last = 0, continue'% aid)
            continue
        speed_last_dict = dict()
        speed_last_dict['view'] = (video_record_list[-2].view - video_record_list[-3].view) / timespan_last * 3600
        speed_last_dict['danmaku'] = (video_record_list[-2].danmaku - video_record_list[-3].danmaku) / timespan_last * 3600
        speed_last_dict['reply'] = (video_record_list[-2].reply - video_record_list[-3].reply) / timespan_last * 3600
        speed_last_dict['favorite'] = (video_record_list[-2].favorite - video_record_list[-3].favorite) / timespan_last * 3600
        speed_last_dict['coin'] = (video_record_list[-2].coin - video_record_list[-3].coin) / timespan_last * 3600
        speed_last_dict['share'] = (video_record_list[-2].share - video_record_list[-3].share) / timespan_last * 3600
        speed_last_dict['like'] = (video_record_list[-2].like - video_record_list[-3].like) / timespan_last * 3600

        # use magic number 99999999 to represent infinity
        speed_now_incr_rate_dict = dict()
        speed_now_incr_rate_dict['view'] = (speed_now_dict['view'] - speed_last_dict['view']) \
            / speed_last_dict['view'] if speed_last_dict['view'] != 0 else \
            99999999 * 1 if (speed_now_dict['view'] - speed_last_dict['view']) > 0 else -1
            # float('inf') * (speed_now_dict['view'] - speed_last_dict['view'])
        speed_now_incr_rate_dict['danmaku'] = (speed_now_dict['danmaku'] - speed_last_dict['danmaku']) \
            / speed_last_dict['danmaku'] if speed_last_dict['danmaku'] != 0 else \
            99999999 * 1 if (speed_now_dict['danmaku'] - speed_last_dict['danmaku']) > 0 else -1
            # float('inf') * (speed_now_dict['danmaku'] - speed_last_dict['danmaku'])
        speed_now_incr_rate_dict['reply'] = (speed_now_dict['reply'] - speed_last_dict['reply']) \
            / speed_last_dict['reply'] if speed_last_dict['reply'] != 0 else \
            99999999 * 1 if (speed_now_dict['reply'] - speed_last_dict['reply']) > 0 else -1
            # float('inf') * (speed_now_dict['reply'] - speed_last_dict['reply'])
        speed_now_incr_rate_dict['favorite'] = (speed_now_dict['favorite'] - speed_last_dict['favorite']) \
            / speed_last_dict['favorite'] if speed_last_dict['favorite'] != 0 else \
            99999999 * 1 if (speed_now_dict['favorite'] - speed_last_dict['favorite']) > 0 else -1
            # float('inf') * (speed_now_dict['favorite'] - speed_last_dict['favorite'])
        speed_now_incr_rate_dict['coin'] = (speed_now_dict['coin'] - speed_last_dict['coin']) \
            / speed_last_dict['coin'] if speed_last_dict['coin'] != 0 else \
            99999999 * 1 if (speed_now_dict['coin'] - speed_last_dict['coin']) > 0 else -1
            # float('inf') * (speed_now_dict['coin'] - speed_last_dict['coin'])
        speed_now_incr_rate_dict['share'] = (speed_now_dict['share'] - speed_last_dict['share']) \
            / speed_last_dict['share'] if speed_last_dict['share'] != 0 else \
            99999999 * 1 if (speed_now_dict['share'] - speed_last_dict['share']) > 0 else -1
            # float('inf') * (speed_now_dict['share'] - speed_last_dict['share'])
        speed_now_incr_rate_dict['like'] = (speed_now_dict['like'] - speed_last_dict['like']) \
            / speed_last_dict['like'] if speed_last_dict['like'] != 0 else \
            99999999 * 1 if (speed_now_dict['like'] - speed_last_dict['like']) > 0 else -1
            # float('inf') * (speed_now_dict['like'] - speed_last_dict['like'])

        period_range = video_record_list[-1].added - video_record_list[0].added
        if period_range == 0:
            prt('%d got period_range = 0, continue' % aid)
            continue

        speed_period_dict = dict()
        speed_period_dict['view'] = (video_record_list[-1].view - video_record_list[0].view) / period_range * 3600
        speed_period_dict['danmaku'] = (video_record_list[-1].danmaku - video_record_list[0].danmaku) / period_range * 3600
        speed_period_dict['reply'] = (video_record_list[-1].reply - video_record_list[0].reply) / period_range * 3600
        speed_period_dict['favorite'] = (video_record_list[-1].favorite - video_record_list[0].favorite) / period_range * 3600
        speed_period_dict['coin'] = (video_record_list[-1].coin - video_record_list[0].coin) / period_range * 3600
        speed_period_dict['share'] = (video_record_list[-1].share - video_record_list[0].share) / period_range * 3600
        speed_period_dict['like'] = (video_record_list[-1].like - video_record_list[0].like) / period_range * 3600

        overall_range = video_record_list[-1].added
        if aid in video_pubdate_dict.keys() and video_pubdate_dict[aid]:
            overall_range -= video_pubdate_dict[aid]
        if overall_range == 0:
            prt('%d got overall_range = 0, continue' % aid)
            continue

        speed_overall_dict = dict()
        speed_overall_dict['view'] = video_record_list[-1].view / overall_range * 3600
        speed_overall_dict['danmaku'] = video_record_list[-1].danmaku / overall_range * 3600
        speed_overall_dict['reply'] = video_record_list[-1].reply / overall_range * 3600
        speed_overall_dict['favorite'] = video_record_list[-1].favorite / overall_range * 3600
        speed_overall_dict['coin'] = video_record_list[-1].coin / overall_range * 3600
        speed_overall_dict['share'] = video_record_list[-1].share / overall_range * 3600
        speed_overall_dict['like'] = video_record_list[-1].like / overall_range * 3600

        has_abnormal_change = False
        new_change_list = []

        # check unexpected drop
        for (key, value) in speed_now_dict.items():
            if value < -50:
                new_change = TddVideoRecordAbnormalChange()
                new_change.added = video_record_list[-1].added
                new_change.aid =aid
                new_change.attr = key
                new_change.speed_now = speed_now_dict[key]
                new_change.speed_last = speed_last_dict[key]
                new_change.speed_now_incr_rate = speed_now_incr_rate_dict[key]
                new_change.period_range = period_range
                new_change.speed_period = speed_period_dict[key]
                new_change.speed_overall = speed_overall_dict[key]
                new_change.this_added = video_record_list[-1].added
                new_change.this_view = video_record_list[-1].view
                new_change.this_danmaku = video_record_list[-1].danmaku
                new_change.this_reply = video_record_list[-1].reply
                new_change.this_favorite = video_record_list[-1].favorite
                new_change.this_coin = video_record_list[-1].coin
                new_change.this_share = video_record_list[-1].share
                new_change.this_like = video_record_list[-1].like
                new_change.last_added = video_record_list[-2].added
                new_change.last_view = video_record_list[-2].view
                new_change.last_danmaku = video_record_list[-2].danmaku
                new_change.last_reply = video_record_list[-2].reply
                new_change.last_favorite = video_record_list[-2].favorite
                new_change.last_coin = video_record_list[-2].coin
                new_change.last_share = video_record_list[-2].share
                new_change.last_like = video_record_list[-2].like
                new_change.description = 'unexpected drop detected, speed now of %s is %f, < -50' % (key, value)
                prt('%d change: %s' % (aid, new_change.description))
                has_abnormal_change = True
                new_change_list.append(new_change)

        # check unexpected increase speed
        for (key, value) in speed_now_incr_rate_dict.items():
            if value > 2 and speed_now_dict[key] > 50:
                new_change = TddVideoRecordAbnormalChange()
                new_change.added = video_record_list[-1].added
                new_change.aid = aid
                new_change.attr = key
                new_change.speed_now = speed_now_dict[key]
                new_change.speed_last = speed_last_dict[key]
                new_change.speed_now_incr_rate = speed_now_incr_rate_dict[key]
                new_change.period_range = period_range
                new_change.speed_period = speed_period_dict[key]
                new_change.speed_overall = speed_overall_dict[key]
                new_change.this_added = video_record_list[-1].added
                new_change.this_view = video_record_list[-1].view
                new_change.this_danmaku = video_record_list[-1].danmaku
                new_change.this_reply = video_record_list[-1].reply
                new_change.this_favorite = video_record_list[-1].favorite
                new_change.this_coin = video_record_list[-1].coin
                new_change.this_share = video_record_list[-1].share
                new_change.this_like = video_record_list[-1].like
                new_change.last_added = video_record_list[-2].added
                new_change.last_view = video_record_list[-2].view
                new_change.last_danmaku = video_record_list[-2].danmaku
                new_change.last_reply = video_record_list[-2].reply
                new_change.last_favorite = video_record_list[-2].favorite
                new_change.last_coin = video_record_list[-2].coin
                new_change.last_share = video_record_list[-2].share
                new_change.last_like = video_record_list[-2].like
                if value == 99999999:
                    speed_now_str = 'inf'
                elif value == -99999999:
                    speed_now_str = '-inf'
                else:
                    speed_now_str = '{0}%'.format(value * 100)
                new_change.description = 'unexpected increase speed detected, speed now of {0} is {1}, > 200%'.format(
                    key, speed_now_str)
                prt('%d change: %s' % (aid, new_change.description))
                has_abnormal_change = True
                new_change_list.append(new_change)

        # if has_abnormal_change and record.id is None:
        #     DBOperation.add(record, session)
        #     logger_19.info('Add video record %s' % record)

        # TODO change freq

        # try:
        #     for new_change in new_change_list:
        #         new_change.this_record_id = record.id
        #         # TODO make add last record to tdd_video_record
        #         session.add(new_change)
        #     session.commit()
        # except Exception as e:
        #     logger_19.error('Fail to add new change list with aid %d. Exception caught. Detail: %s' % (record.aid, e))

        check_visited_count += 1
        if check_visited_count % 100 == 0:
            prt('check %d / %d done' % (check_visited_count, check_total_count))

    prt('check %d / %d done' % (check_visited_count, check_total_count))

    prt('08 done! Finish check params of history video records')


if __name__ == '__main__':
    main()
