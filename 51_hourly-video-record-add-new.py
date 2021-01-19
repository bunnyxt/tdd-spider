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
import logging
logger = logging.getLogger('51')

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
        self.logger = logging.getLogger('AwesomeApiFetcher')

    def run(self):
        self.logger.info('fetcher %s, start' % self.name)
        while not self.page_num_queue.empty():
            page_num = self.page_num_queue.get()
            page_obj = get_valid(self.bapi.get_archive_rank_by_partion, (30, page_num, 50),
                                 test_archive_rank_by_partion)
            added = get_ts_s()
            if page_obj is None:
                self.logger.warning('fetcher %s, pn %d fail' % (self.name, page_num))
                self.page_num_queue.put(page_num)
            else:
                self.logger.debug('fetcher %s, pn %d success' % (self.name, page_num))
                self.content_queue.put({'added': added, 'content': page_obj})
        self.content_queue.put(EndOfFetcher())
        self.logger.info('fetcher %s, end' % self.name)


class AwesomeApiRecordParser(Thread):
    def __init__(self, name, content_queue, record_queue, eof_total_num):
        super().__init__()
        self.name = name
        self.content_queue = content_queue
        self.record_queue = record_queue
        self.eof_total_num = eof_total_num  # TODO use better way to stop thread
        self.logger = logging.getLogger('AwesomeApiRecordParser')

    def run(self):
        self.logger.info('parser %s, start' % self.name)
        eof_num = 0
        while eof_num < self.eof_total_num:
            content = self.content_queue.get()
            if isinstance(content, EndOfFetcher):
                eof_num += 1
                self.logger.info('parser %s, get %d eof' % (self.name, eof_num))
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
        self.logger.info('parser %s, end' % self.name)


class C30NeedAddButNotFoundAidsChecker(Thread):
    def __init__(self, need_insert_but_record_not_found_aid_list):
        super().__init__()
        self.need_insert_but_record_not_found_aid_list = need_insert_but_record_not_found_aid_list
        self.logger = logging.getLogger('C30NeedAddButNotFoundAidsChecker')

    def run(self):
        # check need insert but not found aid list
        # these aids should have record in aid_record_dict, but not found at present
        # possible reasons:
        # - now video tid != 30
        # - now video code != 0
        # - ...
        self.logger.info('Now start checking need add but not found aids...')
        for aid in self.need_insert_but_record_not_found_aid_list:
            # TODO
            self.logger.warning('TODO: aid %d' % aid)
        self.logger.info('Finish checking need add but not found aids!')


class C30NoNeedInsertAidsChecker(Thread):
    def __init__(self, no_need_insert_aid_list):
        super().__init__()
        self.no_need_insert_aid_list = no_need_insert_aid_list
        self.logger = logging.getLogger('C30NoNeedInsertAidsChecker')

    def run(self):
        # check no need insert records
        # if time label is 04:00, we need to add all video records into tdd_video_record table,
        # therefore need_insert_aid_list contains all c30 aids in db, however, still not cover all records
        # possible reasons:
        # - some video moved into c30
        # - some video code changed to 0
        # - ...
        self.logger.info('Now start checking no need insert records...')
        for aid in self.no_need_insert_aid_list:
            # TODO
            self.logger.warning('TODO: aid %d' % aid)
        self.logger.info('Finish checking no need insert records!')


class C30PipelineRunner(Thread):
    def __init__(self, time_label):
        super().__init__()
        self.time_label = time_label
        self.return_record_list = None
        self.logger = logging.getLogger('C30PipelineRunner')

    def run(self):
        self.logger.info('c30 video pipeline start')

        bapi = BiliApi()

        # get page total
        obj = get_valid(bapi.get_archive_rank_by_partion, (30, 1, 50), test_archive_rank_by_partion)
        if obj is None:
            raise RuntimeError('Fail to get page total via awesome api!')
        page_total = math.ceil(obj['data']['page']['count'] / 50)
        self.logger.info('%d page(s) found' % page_total)

        # put page num into page_num_queue
        page_num_queue = Queue()  # store pn for awesome api fetcher to consume
        for pn in range(1, page_total + 1):
            page_num_queue.put(pn)
        self.logger.info('%d page(s) put in page_num_queue' % page_num_queue.qsize())

        # create fetcher
        content_queue = Queue()  # store api returned object (json parsed) content for parser consume
        fetcher_total_num = 5  # can be modified, default 5 is reasonable
        awesome_api_fetcher_list = []
        for i in range(fetcher_total_num):
            awesome_api_fetcher_list.append(AwesomeApiFetcher('fetcher_%d' % i, page_num_queue, content_queue))
        self.logger.info('%d awesome api fetcher(s) created' % len(awesome_api_fetcher_list))

        # create parser
        record_queue = Queue()  # store parsed record
        parser = AwesomeApiRecordParser('parser_0', content_queue, record_queue, fetcher_total_num)
        self.logger.info('awesome api record parser created')

        # start fetcher
        for fetcher in awesome_api_fetcher_list:
            fetcher.start()
        self.logger.info('%d awesome api fetcher(s) started' % len(awesome_api_fetcher_list))

        # start parser
        parser.start()
        self.logger.info('awesome api record parser started')

        # join fetcher and parser
        for fetcher in awesome_api_fetcher_list:
            fetcher.join()
        parser.join()

        # finish multi thread fetching and parsing
        self.logger.info('%d record(s) parsed' % record_queue.qsize())

        # remove duplicate and record queue -> aid record dict
        aid_record_dict = {}
        while not record_queue.empty():
            record = record_queue.get()
            aid_record_dict[record.aid] = record
        self.logger.info('%d record(s) left after remove duplication' % len(aid_record_dict))

        # get need insert aid list
        session = Session()
        need_insert_aid_list = get_need_insert_aid_list(self.time_label, True, session)
        self.logger.info('%d aid(s) need insert for time label %s' % (len(need_insert_aid_list), self.time_label))

        # insert records
        self.logger.info('Now start inserting records...')
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
                    self.logger.info('%d inserted' % need_insert_and_succeed_count)
        if sql != sql_prefix:
            sql = sql[:-2]  # remove ending comma and space
            session.execute(sql)
            session.commit()
        self.logger.info('Finish inserting records! %d records added, %d aids left'
                         % (need_insert_and_succeed_count, len(need_insert_but_record_not_found_aid_list)))

        # check need insert but not found aid list
        # these aids should have record in aid_record_dict, but not found at present
        # possible reasons:
        # - now video tid != 30
        # - now video code != 0
        # - ...
        self.logger.info('%d c30 need add but not found aids got' % len(need_insert_but_record_not_found_aid_list))
        self.logger.info('Now start a branch thread for checking need add but not found aids...')
        c30_need_add_but_not_found_aids_checker = C30NeedAddButNotFoundAidsChecker(need_insert_but_record_not_found_aid_list)
        c30_need_add_but_not_found_aids_checker.start()

        # check no need insert records
        # if time label is 04:00, we need to add all video records into tdd_video_record table,
        # therefore need_insert_aid_list contains all c30 aids in db, however, still not cover all records
        # possible reasons:
        # - some video moved into c30
        # - some video code changed to 0
        # - ...
        no_need_insert_aid_list = list(set(aid_record_dict.keys()) - set(need_insert_aid_list))
        self.logger.info('%d c30 no need insert records got' % len(no_need_insert_aid_list))
        self.logger.info('Now start a branch thread for checking need no need insert aids...')
        c30_no_need_insert_aids_checker = C30NoNeedInsertAidsChecker(no_need_insert_aid_list)
        c30_no_need_insert_aids_checker.start()

        session.close()
        self.logger.info('c30 video pipeline done! return %d records' % len(aid_record_dict))

        self.return_record_list = [record for record in aid_record_dict.values()]


class C0PipelineRunner(Thread):
    def __init__(self, time_label):
        super().__init__()
        self.time_label = time_label
        self.return_record_list = None
        self.logger = logging.getLogger('C0PipelineRunner')

    def run(self):
        self.logger.info('c0 video pipeline start')
        # TODO


def run_hourly_video_record_add(time_task):
    time_label = time_task[-5:]  # current time, ex: 19:00
    # time_label = '04:00'  # DEBUG
    logger.info('Now start hourly video record add, time label: %s..' % time_label)

    # upstream data acquisition pipeline, c30 and c0 pipeline runner, init -> start -> join -> records
    logger.info('Now start upstream data acquisition pipelines...')

    data_acquisition_pipeline_runner_list = [
        C30PipelineRunner(time_label),
        C0PipelineRunner(time_label),
    ]
    for runner in data_acquisition_pipeline_runner_list:
        runner.start()
    for runner in data_acquisition_pipeline_runner_list:
        runner.join()

    records = []
    for runner in data_acquisition_pipeline_runner_list:
        if runner.return_record_list:
            records += runner.return_record_list
        else:
            logger.error('Fail to get valid return_record_list from pipeline runner %s' % runner)

    logger.info('Finish upstream data acquisition pipelines! %d records received' % len(records))


def main():
    logger.info('51: hourly video record add (new)')

    time_task = '%s:00' % get_ts_s_str()[:13]  # current time task, ex: 2013-01-31 19:00
    logger.info('Now start, time task: %s' % time_task)
    try:
        run_hourly_video_record_add(time_task)
    except Exception as e:
        logger.critical(e)
        sc_send('51: Critical exception occurred!', 'send time: %s, exception description: %s' % (get_ts_s_str(), e))
    logger.info('Done! time task: %s' % time_task)


if __name__ == '__main__':
    logging_init(file_prefix='51_new')
    main()
