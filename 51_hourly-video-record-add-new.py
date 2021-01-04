import logging
from logutils import logging_init
from pybiliapi import BiliApi
from db import Session, DBOperation
from threading import Thread
from queue import Queue
from common import get_valid, test_archive_rank_by_partion
from util import get_ts_s, get_ts_s_str
import math
from serverchan import sc_send
from collections import namedtuple

Record = namedtuple('Record', ['added', 'aid', 'bvid', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'like'])


def get_need_insert_aid_list(time_label, is_tid_30, session):
    if time_label == '04:00':
        # return total
        return DBOperation.query_all_update_video_aids(is_tid_30, session)

    # add 1 hour aids
    aid_list = DBOperation.query_freq_update_video_aids(2, is_tid_30, session)  # freq = 2

    if time_label in ['00:00', '04:00', '08:00', '12:00', '16:00', '20:00']:
        # add 4 hour aids
        aid_list += DBOperation.query_freq_update_video_aids(1, is_tid_30, session)  # freq = 1

    return aid_list


class EndOfFetcher:
    def __init__(self):
        pass

    def __repr__(self):
        return "<EndOfFetcher>"


class AwesomeApiFetcher(Thread):
    def __init__(self, name, page_num_queue, content_queue, bapi=None):
        super().__init__()
        self.name = name
        self.page_num_queue = page_num_queue
        self.content_queue = content_queue
        self.bapi = bapi if bapi is not None else BiliApi()

    def run(self):
        logging.info('fetcher %s, start' % self.name)
        while not self.page_num_queue.empty():
            page_num = self.page_num_queue.get()
            page_obj = get_valid(self.bapi.get_archive_rank_by_partion, (30, page_num, 50),
                                 test_archive_rank_by_partion)
            added = get_ts_s()
            if page_obj is None:
                logging.warning('fetcher %s, pn %d fail' % (self.name, page_num))
                self.page_num_queue.put(page_num)
            else:
                logging.debug('fetcher %s, pn %d success' % (self.name, page_num))
                self.content_queue.put({'added': added, 'content': page_obj})
        self.content_queue.put(EndOfFetcher())
        logging.info('fetcher %s, end' % self.name)


class AwesomeApiRecordParser(Thread):
    def __init__(self, name, content_queue, record_queue, eof_total_num):
        super().__init__()
        self.name = name
        self.content_queue = content_queue
        self.record_queue = record_queue
        self.eof_total_num = eof_total_num  # TODO use better way to stop thread

    def run(self):
        logging.info('parser %s, start' % self.name)
        eof_num = 0
        while eof_num < self.eof_total_num:
            content = self.content_queue.get()
            if isinstance(content, EndOfFetcher):
                eof_num += 1
                logging.info('parser %s, get %d eof' % (self.name, eof_num))
                continue
            added = content['added']
            page_obj = content['content']
            for arch in page_obj['data']['archives']:
                arch_stat = arch['stat']
                record = Record(
                    added, arch['aid'], arch['bvid'].lstrip('BV'),
                    -1 if arch_stat['view'] == '--' else arch_stat['view'], arch_stat['danmaku'], arch_stat['reply'],
                    arch_stat['favorite'], arch_stat['coin'], arch_stat['share'], arch_stat['like']
                )
                self.record_queue.put(record)
        logging.info('parser %s, end' % self.name)


def run_c30_video_pipeline(time_label):
    logging.info('[01-c30] c30 video pipeline start')

    bapi = BiliApi()

    # get page total
    obj = get_valid(bapi.get_archive_rank_by_partion, (30, 1, 50), test_archive_rank_by_partion)
    if obj is None:
        raise RuntimeError('[01-c30] Fail to get page total via awesome api!')
    page_total = math.ceil(obj['data']['page']['count'] / 50)
    logging.info('[01-c30] %d page(s) found' % page_total)

    # put page num into page_num_queue
    page_num_queue = Queue()  # store pn for awesome api fetcher to consume
    for pn in range(1, page_total + 1):
        page_num_queue.put(pn)
    logging.info('[01-c30] %d page(s) put in page_num_queue' % page_num_queue.qsize())

    # create fetcher
    content_queue = Queue()  # store api returned object (json parsed) content for parser consume
    fetcher_total_num = 5  # can be modified, default 5 is reasonable
    awesome_api_fetcher_list = []
    for i in range(fetcher_total_num):
        awesome_api_fetcher_list.append(AwesomeApiFetcher('fetcher_%d' % i, page_num_queue, content_queue))
    logging.info('[01-c30] %d awesome api fetcher(s) created' % len(awesome_api_fetcher_list))

    # create parser
    record_queue = Queue()  # store parsed record
    parser = AwesomeApiRecordParser('parser_0', content_queue, record_queue, fetcher_total_num)
    logging.info('[01-c30] awesome api record parser created')

    # start fetcher
    for fetcher in awesome_api_fetcher_list:
        fetcher.start()
    logging.info('[01-c30] %d awesome api fetcher(s) started' % len(awesome_api_fetcher_list))

    # start parser
    parser.start()
    logging.info('[01-c30] awesome api record parser started')

    # join fetcher and parser
    for fetcher in awesome_api_fetcher_list:
        fetcher.join()
    parser.join()

    # finish multi thread fetching and parsing
    logging.info('[01-c30] %d record(s) parsed' % record_queue.qsize())

    # remove duplicate and record queue -> aid record dict
    aid_record_dict = {}
    while not record_queue.empty():
        record = record_queue.get()
        aid_record_dict[record.aid] = record
    logging.info('[01-c30] %d record(s) left after remove duplication' % len(aid_record_dict))

    # get need insert aid list
    session = Session()
    need_insert_aid_list = get_need_insert_aid_list(time_label, True, session)
    logging.info('[01-c30] %d aid(s) need insert for time label %s' % (len(need_insert_aid_list), time_label))

    # insert records
    logging.info('[01-c30] Now start inserting records...')
    # use sql directly, combine 1000 records into one sql to execute and commit
    # TODO debug table tdd_video_record_2, create table tdd_video_record_2 like tdd_video_record
    sql_prefix = 'insert into ' \
                 'tdd_video_record_2(added, aid, `view`, danmaku, reply, favorite, coin, share, `like`) ' \
                 'values '
    sql = sql_prefix
    need_insert_and_succeed_count = 0
    need_insert_but_record_not_found_aid_list = []
    log_gap = 1000 * max(1, (len(need_insert_aid_list) // 1000 // 10))
    for aid in need_insert_aid_list:
        record = aid_record_dict.get(aid, None)
        if not record:
            need_insert_but_record_not_found_aid_list.append(aid)
            continue
        sql += '(%d, %d, %d, %d, %d, %d, %d, %d, %d), ' % (
            record.added, record.aid,
            record.view, record.danmaku, record.reply, record.favorite, record.coin, record.share, record.like
        )
        need_insert_and_succeed_count += 1
        if need_insert_and_succeed_count % 1000 == 0:
            sql = sql[:-2]  # remove ending comma and space
            session.execute(sql)
            session.commit()
            sql = sql_prefix
            if need_insert_and_succeed_count % log_gap == 0:
                logging.info('[01-c30] %d inserted' % need_insert_and_succeed_count)
    if sql != sql_prefix:
        sql = sql[:-2]  # remove ending comma and space
        session.execute(sql)
        session.commit()
    logging.info('[01-c30] Finish inserting records! %d records added, %d aids left'
                 % (need_insert_and_succeed_count, len(need_insert_but_record_not_found_aid_list)))

    # TODO next two module use sub thread to execute, using proxy pool to fetch api and update video info

    # check need insert but not found aid list
    # these aids should have record in aid_record_dict, but not found at present
    # possible reasons:
    # - now video tid != 30
    # - now video code != 0
    # - ...
    logging.info('[01-c30] Now start checking need add but not found aids...')
    for aid in need_insert_but_record_not_found_aid_list:
        # TODO
        pass
    logging.info('[01-c30] Finish checking need add but not found aids!')

    # check no need insert records
    # if time label is 04:00, we need to add all video records into tdd_video_record table,
    # therefore need_insert_aid_list contains all c30 aids in db, however, still not cover all records
    # possible reasons:
    # - some video moved into c30
    # - some video code changed to 0
    # - ...
    logging.info('[01-c30] Now start checking no need insert records...')
    no_need_insert_aid_list = list(set(aid_record_dict.keys()) - set(need_insert_aid_list))
    logging.info('[01-c30] %d no need insert aid get' % len(no_need_insert_aid_list))
    for aid in no_need_insert_aid_list:
        # TODO
        pass
    logging.info('[01-c30] Finish checking no need insert records!')

    session.close()
    logging.info('[01-c30] c30 video pipeline done')

    return [record for record in aid_record_dict.values()]


def run_c0_video_pipeline(time_label):
    # TODO
    pass


def run_hourly_video_record_add(time_task):
    time_label = time_task[-5:]  # current time, ex: 19:00
    # time_label = '04:00'  # DEBUG
    logging.info('Now start hourly video record add, time label: %s..' % time_label)

    # bapi = BiliApi()
    # session = Session()

    run_c30_video_pipeline(time_label)  # TODO use new thread
    run_c0_video_pipeline(time_label)  # TODO use new thread


def main():
    logging.info('51: hourly video record add (new)')

    time_task = '%s:00' % get_ts_s_str()[:13]  # current time task, ex: 2013-01-31 19:00
    logging.info('Now start, time task: %s' % time_task)
    try:
        run_hourly_video_record_add(time_task)
    except Exception as e:
        logging.critical(e)
        sc_send('51: Critical exception occurred!', 'send time: %s, exception description: %s' % (get_ts_s_str(), e))
    logging.info('Done! time task: %s' % time_task)


if __name__ == '__main__':
    logging_init(file_prefix='51_new')
    main()
