from logutils import logging_init
from pybiliapi import BiliApi
from db import Session, DBOperation, TddVideoRecordAbnormalChange
from threading import Thread
from queue import Queue
from common import get_valid, test_archive_rank_by_partion, test_video_view, test_video_stat, \
    add_video_record_via_stat_api, update_video, add_video_via_bvid, \
    InvalidObjCodeError, TddCommonError, AlreadyExistError
from util import get_ts_s, get_ts_s_str, a2b, is_all_zero_record, str_to_ts_s
import math
import time
import datetime
import os
import re
from conf import get_proxy_pool_url
from serverchan import sc_send
from collections import namedtuple, defaultdict
import logging
logger = logging.getLogger('51')

Record = namedtuple('Record', ['added', 'aid', 'bvid', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'like'])
RecordSpeed = namedtuple('RecordSpeed', [
    'start_ts', 'end_ts', 'timespan', 'per_seconds', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'like'])
RecordSpeedRatio = namedtuple('RecordSpeedRatio', [
    'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'like', 'inf_magic_num'])


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
        # - now video state = -4, forward = another video aid
        # - ...
        self.logger.info('Now start checking need add but not found aids...')
        session = Session()
        bapi_with_proxy = BiliApi(get_proxy_pool_url())
        result_status_dict = defaultdict(list)
        for idx, aid in enumerate(self.need_insert_but_record_not_found_aid_list, 1):
            # try update video
            try:
                tdd_video_logs = update_video(aid, bapi_with_proxy, session)
            except TddCommonError as e2:
                self.logger.warning('Fail to update video aid %d. Exception caught. Detail: %s' % (aid, e2))
                result_status_dict['fail_aids'].append(aid)
            except Exception as e2:
                self.logger.error('Fail to update video aid %d. Exception caught. Detail: %s' % (aid, e2))
                result_status_dict['fail_aids'].append(aid)
            else:
                # init change flags
                code_change_flag = False
                tid_change_flag = False
                # check update logs
                for log in tdd_video_logs:
                    if log.attr == 'code':
                        code_change_flag = True
                    if log.attr == 'tid':
                        tid_change_flag = True
                    self.logger.info('Update video aid %d, attr: %s, oldval: %s, newval: %s'
                                     % (log.aid, log.attr, log.oldval, log.newval))
                # set result status
                # NOTE: here maybe code_change_aids and tid_change_aids both +1 aid
                if code_change_flag:
                    result_status_dict['code_change_aids'].append(aid)
                if tid_change_flag:
                    result_status_dict['tid_change_aids'].append(aid)
                if not code_change_flag and not code_change_flag:
                    self.logger.warning('No code or tid change found for video aid %d, need further check' % aid)
                    result_status_dict['no_change_found_aids'].append(aid)
            finally:
                if idx % 10 == 0:
                    self.logger.info('%d / %d done' % (idx, len(self.need_insert_but_record_not_found_aid_list)))
        self.logger.info('%d / %d done' % (len(self.need_insert_but_record_not_found_aid_list),
                                           len(self.need_insert_but_record_not_found_aid_list)))
        session.close()
        self.logger.info('Finish checking need add but not found aids! %s' %
                         ', '.join(['%s: %d' % (k, len(v)) for (k, v) in dict(result_status_dict).items()]))
        self.logger.warning('fail_aids: %r' % result_status_dict['fail_aids'])
        self.logger.warning('no_change_found_aids: %r' % result_status_dict['no_change_found_aids'])


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
        # - some video has -403 code, awesome api will also get these video, login session not required
        # - ...
        self.logger.info('Now start checking no need insert records...')
        session = Session()
        bapi_with_proxy = BiliApi(get_proxy_pool_url())
        _403_aids = DBOperation.query_403_video_aids()
        result_status_dict = defaultdict(list)
        for idx, aid in enumerate(self.no_need_insert_aid_list, 1):
            # check whether -403 video
            if aid in _403_aids:
                self.logger.info('-403 video aid %d detected, skip' % aid)
                result_status_dict['-403_aids'].append(aid)
                continue

            # try add new video first
            video_already_exist_flag = False
            try:
                new_video = add_video_via_bvid(a2b(aid), bapi_with_proxy, session)
            except AlreadyExistError:
                # video already exist, which is absolutely common
                self.logger.debug('Video aid %d already exists' % aid)
                video_already_exist_flag = True
            except TddCommonError as e:
                self.logger.warning('Fail to add video aid %d. Exception caught. Detail: %s' % (aid, e))
            else:
                self.logger.info('Add new video %s' % new_video)
                result_status_dict['add_new_video_aids'].append(aid)

            if video_already_exist_flag:
                # try update video
                try:
                    tdd_video_logs = update_video(aid, bapi_with_proxy, session)
                except TddCommonError as e2:
                    self.logger.warning('Fail to update video aid %d. Exception caught. Detail: %s' % (aid, e2))
                    result_status_dict['fail_aids'].append(aid)
                except Exception as e2:
                    self.logger.error('Fail to update video aid %d. Exception caught. Detail: %s' % (aid, e2))
                    result_status_dict['fail_aids'].append(aid)
                else:
                    # init change flags
                    code_change_flag = False
                    # check update logs
                    for log in tdd_video_logs:
                        if log.attr == 'code':
                            code_change_flag = True
                        self.logger.info('Update video aid %d, attr: %s, oldval: %s, newval: %s'
                                         % (log.aid, log.attr, log.oldval, log.newval))
                    # set result status
                    # NOTE: here maybe code_change_aids and tid_change_aids both +1 aid
                    if code_change_flag:
                        result_status_dict['code_change_aids'].append(aid)
                    else:
                        self.logger.warning('No code change found for video aid %d, need further check' % aid)
                        result_status_dict['no_change_found_aids'].append(aid)

            if idx % 10 == 0:
                self.logger.info('%d / %d done' % (idx, len(self.no_need_insert_aid_list)))
        self.logger.info('%d / %d done' % (len(self.no_need_insert_aid_list), len(self.no_need_insert_aid_list)))
        self.logger.info('Finish checking no need insert records! %s' %
                         ', '.join(['%s: %d' % (k, len(v)) for (k, v) in dict(result_status_dict).items()]))
        self.logger.warning('fail_aids: %r' % result_status_dict['fail_aids'])
        self.logger.warning('no_change_found_aids: %r' % result_status_dict['no_change_found_aids'])


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

        # check all zero records
        bapi_with_proxy = BiliApi(proxy_pool_url=get_proxy_pool_url())
        for aid, record in aid_record_dict.items():
            if is_all_zero_record(record):
                self.logger.warning('All zero record of video aid %d detected! Try get video record again...' % aid)
                # get stat_obj
                stat_obj = get_valid(bapi_with_proxy.get_video_stat, (aid,), test_video_stat)
                if stat_obj is None:
                    self.logger.warning('Fail to get valid stat obj of video aid %d!' % aid)
                    continue
                if stat_obj['code'] != 0:
                    self.logger.warning('Fail to get stat obj with code 0 of video aid %d! code %s detected' % (
                        aid, stat_obj['code']))
                    continue
                # assemble new record
                new_record = Record(
                    get_ts_s(), aid, a2b(aid),
                    -1 if stat_obj['data']['view'] == '--' else stat_obj['data']['view'], stat_obj['data']['danmaku'],
                    stat_obj['data']['reply'], stat_obj['data']['favorite'], stat_obj['data']['coin'],
                    stat_obj['data']['share'], stat_obj['data']['like']
                )
                if is_all_zero_record(new_record):
                    self.logger.warning('Get all zero record of video aid %d again!' % aid)
                    continue
                aid_record_dict[aid] = new_record
                self.logger.warning('Use new not all zero record %s instead.' % str(new_record))

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
        # - now video state = -4, forward = another video aid
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
        # - some video has -403 code, awesome api will also get these video, login session not required
        # - ...
        if self.time_label == '04:00':
            no_need_insert_aid_list = list(set(aid_record_dict.keys()) - set(need_insert_aid_list))
            self.logger.info('%d c30 no need insert records got' % len(no_need_insert_aid_list))
            self.logger.info('Now start a branch thread for checking need no need insert aids...')
            c30_no_need_insert_aids_checker = C30NoNeedInsertAidsChecker(no_need_insert_aid_list)
            c30_no_need_insert_aids_checker.start()

        self.logger.info('c30 video pipeline done! return %d records' % len(aid_record_dict))
        self.return_record_list = [record for record in aid_record_dict.values()]
        session.close()


class C0PipelineRunner(Thread):
    def __init__(self, time_label):
        super().__init__()
        self.time_label = time_label
        self.return_record_list = None
        self.logger = logging.getLogger('C0PipelineRunner')

    def run(self):
        self.logger.info('c0 video pipeline start')

        # get need insert aid list
        session = Session()
        need_insert_aid_list = get_need_insert_aid_list(self.time_label, False, session)
        self.logger.info('%d aid(s) need insert for time label %s' % (len(need_insert_aid_list), self.time_label))

        # fetch and insert records
        # TODO use multi thread to accelerate
        self.logger.info('Now start fetching and inserting records...')
        bapi_with_proxy = BiliApi(proxy_pool_url=get_proxy_pool_url())
        fail_aids = []
        new_video_record_list = []
        for idx, aid in enumerate(need_insert_aid_list, 1):
            # add video record
            try:
                new_video_record = add_video_record_via_stat_api(aid, bapi_with_proxy, session)
                new_video_record_list.append(new_video_record)
                self.logger.debug('Add new record %s' % new_video_record)
            except InvalidObjCodeError as e:
                self.logger.warning('Fail to add video record aid %d. Exception caught. Detail: %s', (aid, e))
                try:
                    tdd_video_logs = update_video(aid, bapi_with_proxy, session)
                except TddCommonError as e2:
                    self.logger.warning('Fail to update video aid %d. Exception caught. Detail: %s' % (aid, e2))
                except Exception as e2:
                    self.logger.error('Fail to update video aid %d. Exception caught. Detail: %s' % (aid, e2))
                else:
                    for log in tdd_video_logs:
                        self.logger.info('Update video aid %d, attr: %s, oldval: %s, newval: %s'
                                         % (log.aid, log.attr, log.oldval, log.newval))
                fail_aids.append(aid)
            except TddCommonError as e:
                self.logger.warning('Fail to add video record aid %d. Exception caught. Detail: %s', (aid, e))
                fail_aids.append(aid)
            if idx % 10 == 0:
                self.logger.info('%d / %d done' % (idx, len(need_insert_aid_list)))
        self.logger.info('%d / %d done' % (len(need_insert_aid_list), len(need_insert_aid_list)))
        self.logger.info('Finish fetching and inserting records! %d records added, %d aids fail'
                         % (len(new_video_record_list), len(fail_aids)))
        self.logger.warning('fail_aids: %r' % fail_aids)

        record_list = [
            # 'added', 'aid', 'bvid', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'like'
            Record(r.added, r.aid, a2b(r.aid), r.view, r.danmaku, r.reply, r.favorite, r.coin, r.share, r.like)
            for r in new_video_record_list
        ]
        self.logger.info('c0 video pipeline done! return %d records' % len(record_list))
        self.return_record_list = record_list
        session.close()


class RecordsSaveToFileRunner(Thread):
    def __init__(self, records, time_task, data_folder='data/'):
        super().__init__()
        self.records = records
        self.time_task = time_task
        self.data_folder = data_folder.rstrip('/') + '/'
        self.current_filename = '%s.csv' % self.time_task
        self.logger = logging.getLogger('RecordsSaveToFileRunner')

    def run(self):
        self.logger.info('Now start saving records to file...')
        current_filename_path = self.data_folder + self.current_filename
        self.logger.info('will save %d records into file %s' % (len(self.records), current_filename_path))
        with open(current_filename_path, 'w') as f:
            f.write('added,aid,bvid,view,danmaku,reply,favorite,coin,share,like\n')
            for idx, record in enumerate(self.records, 1):
                f.write('%d,%d,%s,%d,%d,%d,%d,%d,%d,%d\n' % (
                    record.added, record.aid, record.bvid, record.view, record.danmaku, record.reply, record.favorite,
                    record.coin, record.share, record.like))
                if idx % 20000 == 0:
                    self.logger.info('%d / %d done' % (idx, len(self.records)))
            self.logger.info('%d / %d done' % (len(self.records), len(self.records)))
        self.logger.info('Finish save %d records into file %s!' % (len(self.records), current_filename_path))


class RecordsSaveToDbRunner(Thread):
    def __init__(self, records):
        super().__init__()
        self.records = records
        self.logger = logging.getLogger('RecordsSaveToDbRunner')

    def run(self):
        self.logger.info('Now start saving records to db...')
        session = Session()
        sql_prefix = 'insert into ' \
                     'tdd_video_record_hourly(added, bvid, `view`, danmaku, reply, favorite, coin, share, `like`) ' \
                     'values '
        sql = sql_prefix
        for idx, record in enumerate(self.records, 1):
            sql += '(%d, "%s", %d, %d, %d, %d, %d, %d, %d), ' % (
                record.added, record.bvid,
                record.view, record.danmaku, record.reply, record.favorite, record.coin, record.share, record.like
            )
            if idx % 1000 == 0:
                sql = sql[:-2]  # remove ending comma and space
                session.execute(sql)
                session.commit()
                sql = sql_prefix
                if idx % 10000 == 0:
                    self.logger.info('%d / %d done' % (idx, len(self.records)))
        if sql != sql_prefix:
            sql = sql[:-2]  # remove ending comma and space
            session.execute(sql)
            session.commit()
        self.logger.info('%d / %d done' % (len(self.records), len(self.records)))
        session.close()
        self.logger.info('Finish save %d records into db!' % len(self.records))


class RecentRecordsAnalystRunner(Thread):
    def __init__(self, records, time_task, data_folder='data/', recent_file_num=2):
        super().__init__()
        self.records = records
        self.time_task = time_task
        self.data_folder = data_folder.rstrip('/') + '/'
        self.current_filename = '%s.csv' % self.time_task
        self.recent_file_num = recent_file_num
        self.logger = logging.getLogger('RecentRecordsAnalystRunner')

    def _calc_record_speed(self, record_start, record_end, per_seconds=3600):
        # record_start and record_end should be namedtuple Record
        timespan = record_end.added - record_start.added
        if timespan == 0:
            raise ZeroDivisionError('timespan between two records should not be zero')
        return RecordSpeed(
            start_ts=record_start.added,
            end_ts=record_end.added,
            timespan=timespan,
            per_seconds=per_seconds,
            view=(record_end.view - record_start.view) / timespan * per_seconds,
            danmaku=(record_end.danmaku - record_start.danmaku) / timespan * per_seconds,
            reply=(record_end.reply - record_start.reply) / timespan * per_seconds,
            favorite=(record_end.favorite - record_start.favorite) / timespan * per_seconds,
            coin=(record_end.coin - record_start.coin) / timespan * per_seconds,
            share=(record_end.share - record_start.share) / timespan * per_seconds,
            like=(record_end.like - record_start.like) / timespan * per_seconds
        )

    def _calc_record_speed_ratio(self, record_speed_start, record_speed_end, inf_magic_num=99999999):
        # record_speed_start and record_speed_end should be namedtuple RecordSpeedRatio
        return RecordSpeedRatio(
            view=(record_speed_end.view - record_speed_start.view) / record_speed_start.view
            if record_speed_start.view != 0 else inf_magic_num * 1
            if (record_speed_end.view - record_speed_start.view) > 0 else -1,
            danmaku=(record_speed_end.danmaku - record_speed_start.danmaku) / record_speed_start.danmaku
            if record_speed_start.danmaku != 0 else inf_magic_num * 1
            if (record_speed_end.danmaku - record_speed_start.danmaku) > 0 else -1,
            reply=(record_speed_end.reply - record_speed_start.reply) / record_speed_start.reply
            if record_speed_start.reply != 0 else inf_magic_num * 1
            if (record_speed_end.reply - record_speed_start.reply) > 0 else -1,
            favorite=(record_speed_end.favorite - record_speed_start.favorite) / record_speed_start.favorite
            if record_speed_start.favorite != 0 else inf_magic_num * 1
            if (record_speed_end.favorite - record_speed_start.favorite) > 0 else -1,
            coin=(record_speed_end.coin - record_speed_start.coin) / record_speed_start.coin
            if record_speed_start.coin != 0 else inf_magic_num * 1
            if (record_speed_end.coin - record_speed_start.coin) > 0 else -1,
            share=(record_speed_end.share - record_speed_start.share) / record_speed_start.share
            if record_speed_start.share != 0 else inf_magic_num * 1
            if (record_speed_end.share - record_speed_start.share) > 0 else -1,
            like=(record_speed_end.like - record_speed_start.like) / record_speed_start.like
            if record_speed_start.like != 0 else inf_magic_num * 1
            if (record_speed_end.like - record_speed_start.like) > 0 else -1,
            inf_magic_num=inf_magic_num
        )

    def _assemble_record_abnormal_change(self, added, aid, attr,
                                         speed_now, speed_last, speed_now_incr_rate,
                                         period_range, speed_period, speed_overall,
                                         this_record, last_record, description):
        change_obj = TddVideoRecordAbnormalChange()
        change_obj.added = added
        change_obj.aid = aid
        change_obj.attr = attr
        change_obj.speed_now = speed_now
        change_obj.speed_last = speed_last
        change_obj.speed_now_incr_rate = speed_now_incr_rate
        change_obj.period_range = period_range
        change_obj.speed_period = speed_period
        change_obj.speed_overall = speed_overall
        change_obj.this_added = this_record.added
        change_obj.this_view = this_record.view
        change_obj.this_danmaku = this_record.danmaku
        change_obj.this_reply = this_record.reply
        change_obj.this_favorite = this_record.favorite
        change_obj.this_coin = this_record.coin
        change_obj.this_share = this_record.share
        change_obj.this_like = this_record.like
        change_obj.last_added = last_record.added
        change_obj.last_view = last_record.view
        change_obj.last_danmaku = last_record.danmaku
        change_obj.last_reply = last_record.reply
        change_obj.last_favorite = last_record.favorite
        change_obj.last_coin = last_record.coin
        change_obj.last_share = last_record.share
        change_obj.last_like = last_record.like
        change_obj.description = description
        return change_obj

    def run(self):
        self.logger.info('Now start analysing recent records...')

        # get recent records filenames
        filenames = os.listdir(self.data_folder)
        if self.current_filename in filenames:
            filenames.remove(self.current_filename)  # remove current filename to avoid duplicate
        recent_records_filenames = sorted(
            list(filter(lambda file: re.search(r'^\d{4}-\d{2}-\d{2} \d{2}:00\.csv$', file), filenames)),
            key=lambda x: str_to_ts_s(x[:-4] + ':00')
        )[-self.recent_file_num:]

        # load records from recent files
        self.logger.info('Will load records from recent %d files, filenames: %r' % (
            self.recent_file_num, recent_records_filenames))
        aid_recent_records_dict = defaultdict(list)
        total_records = 0
        for filename in recent_records_filenames:
            file_records = 0
            filename_path = self.data_folder + filename
            with open(filename_path, 'r') as f:
                f.readline()
                for line in f:
                    try:
                        line_arr = line.rstrip('\n').split(',')
                        # 'added', 'aid', 'bvid', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'like'
                        record = Record(int(line_arr[0]), int(line_arr[1]), line_arr[2],
                                        int(line_arr[3]), int(line_arr[4]), int(line_arr[5]), int(line_arr[6]),
                                        int(line_arr[7]), int(line_arr[8]), int(line_arr[9]))
                        aid_recent_records_dict[record.aid].append(record)
                        file_records += 1
                    except Exception as e:
                        self.logger.warning('Fail to parse line %s into record, exception occurred, detail: %s' % (
                            line, e))
                self.logger.info('%d records loaded from file %s' % (file_records, filename))
                total_records += file_records

        # add this round records
        for record in self.records:
            aid_recent_records_dict[record.aid].append(record)
        aid_recent_records_dict = dict(aid_recent_records_dict)
        self.logger.info('Total %d records of total %d aids from recent %d files loaded' % (
            total_records, len(aid_recent_records_dict.keys()), self.recent_file_num))

        # get video aid pubdate dict, since the following check process requires
        session = Session()
        self.logger.info('Now get pubdate of all videos from db...')
        aid_pubdate_list = DBOperation.query_video_pubdate_all(session)
        if not aid_pubdate_list:
            self.logger.error('Fail to get pubdate of all videos from db!')
            return
        aid_pubdate_dict = {}
        for aid, pubdate in aid_pubdate_list:
            if pubdate is None:
                continue
            aid_pubdate_dict[aid] = pubdate
        del aid_pubdate_list
        self.logger.info('Finish get valid pubdate from %d videos!' % len(aid_pubdate_dict))

        # check recent records
        self.logger.info('Now check recent records...')
        result_status_dict = defaultdict(list)
        for idx, (aid, records) in enumerate(aid_recent_records_dict.items(), 1):
            # get pubdate from aid_pubdate_dict
            pubdate = aid_pubdate_dict.get(aid, None)
            if pubdate is None:
                self.logger.warning('Fail to get pubdate of video aid %d, continue' % aid)
                result_status_dict['no_valid_pubdate'].append(aid)
                continue
            pubdate_record = Record(pubdate, aid, a2b(aid), 0, 0, 0, 0, 0, 0, 0)

            records.sort(key=lambda r: r.added)  # sort by added

            # TODO should be refactored in the future to support more check logic
            # ensure at least 3 records
            if len(records) <= 2:
                # very common and not harmful to system, set to debug level is enough
                self.logger.debug('Records len of video aid %d less than 3, continue' % aid)
                result_status_dict['records_len_less_than_3'].append(aid)
                continue

            # ensure no all zero record (except the first record of video, which may be all zero)
            has_all_zero_record = False
            for idx, record in records:
                if is_all_zero_record(record):
                    if idx == 0:
                        # check if this is the first record of video
                        if len(DBOperation.query_video_records_of_given_aid_added_before_given_ts(
                                aid, record.added, session)) > 0:
                            # has records of this video added before this record
                            continue
                    self.logger.warning('Abnormal all zero record %s of video aid %d detected, continue' % (
                        str(record), aid))
                    has_all_zero_record = True
                    break
            if has_all_zero_record:
                result_status_dict['has_all_zero_record'].append(aid)
                continue

            # calc record speed
            # actually we can just delete one of duplicate records (with same added) instead of continue and skip check
            # however, here the adjacent timespan of records should be around 1 hour
            # so here we think zero timespan is error and should skip
            speed_now = speed_last = None
            try:
                speed_now = self._calc_record_speed(records[-2], records[-1])
                speed_last = self._calc_record_speed(records[-3], records[-2])
                speed_period = self._calc_record_speed(records[0], records[-1])
                speed_overall = self._calc_record_speed(pubdate_record, records[-1])
            except ZeroDivisionError:
                self.logger.warning('Zero timespan between adjacent records of video aid %d detected, continue' % aid)
                result_status_dict['zero_timespan_between_adjacent_records'].append(aid)
                continue

            # calc record speed ratio
            speed_ratio = self._calc_record_speed_ratio(speed_last, speed_now)

            record_abnormal_change_list = []

            # check unexpected speed now value drop
            # rule: speed_now.prop < -10
            for idx, prop in enumerate(RecordSpeed._fields[4:], 4):
                value = speed_now[idx]
                if value < -10:
                    change_obj = self._assemble_record_abnormal_change(
                        added=records[-1].added, aid=aid, attr=prop,
                        speed_now=speed_now[idx], speed_last=speed_last[idx], speed_now_incr_rate=speed_ratio[idx-4],
                        period_range=speed_period.timespan, speed_period=speed_period[idx],
                        speed_overall=speed_overall[idx],
                        this_record=records[-1], last_record=records[-2],
                        description='unexpected drop detected, speed now of prop %s is %.2f, < -10' % (prop, value)
                    )
                    self.logger.info('Found unexpected drop of video aid %d, description: %s' % (
                        aid, change_obj.description))
                    result_status_dict['unexpected_drop'].append(aid)
                    record_abnormal_change_list.append(change_obj)

            # check unexpected speed now value increase
            # rule: speed_ratio.prop > 2 and speed_now.prop > 50
            for idx, prop in enumerate(RecordSpeedRatio._fields[:7]):
                value = speed_ratio[idx]
                if value > 2 and speed_now[idx+4] > 50:
                    change_obj = self._assemble_record_abnormal_change(
                        added=records[-1].added, aid=aid, attr=prop,
                        speed_now=speed_now[idx+4], speed_last=speed_last[idx+4], speed_now_incr_rate=speed_ratio[idx],
                        period_range=speed_period.timespan, speed_period=speed_period[idx+4],
                        speed_overall=speed_overall[idx+4],
                        this_record=records[-1], last_record=records[-2],
                        description='unexpected increase detected, speed now of prop %s is %s, > -200%%' % (
                            prop, '%.2f' % value if abs(value) is not 99999999 else '%sinf' % '-' if value < 0 else '')
                    )
                    self.logger.info('Found unexpected increase of video aid %d, description: %s' % (
                        aid, change_obj.description))
                    result_status_dict['unexpected_increase'].append(aid)
                    record_abnormal_change_list.append(change_obj)

            # commit changes
            try:
                for change_obj in record_abnormal_change_list:
                    session.add(change_obj)
                    session.commit()
            except Exception as e:
                self.logger.error('Fail to add abnormal change of video aid %d to db. Exception caught. Detail: %s' % (
                    aid, e))

            if idx % 10000 == 0:
                self.logger.info('%d / %d done' % (idx, len(aid_recent_records_dict)))
        self.logger.info('%d / %d done' % (len(aid_recent_records_dict), len(aid_recent_records_dict)))

        session.close()
        self.logger.info('Finish analysing recent records! %s' %
                         ', '.join(['%s: %d' % (k, len(v)) for (k, v) in dict(result_status_dict).items()]))


class RecentActivityFreqUpdateRunner(Thread):
    def __init__(self, time_label):
        super().__init__()
        self.time_label = time_label
        self.logger = logging.getLogger('RecordsSaveToFileRunner')

    def _update_recent(self, session):
        self.logger.info('Now start update recent field...')
        try:
            now_ts = get_ts_s()
            last_1d_ts = now_ts - 1 * 24 * 60 * 60
            last_7d_ts = now_ts - 7 * 24 * 60 * 60
            session.execute('update tdd_video set recent = 0 where added < %d' % last_7d_ts)
            session.execute('update tdd_video set recent = 1 where added >= %d && added < %d' % (
                last_7d_ts, last_1d_ts))
            session.execute('update tdd_video set recent = 2 where added >= %d' % last_1d_ts)
            session.commit()
            self.logger.info('Finish update recent field!')
        except Exception as e:
            self.logger.info('Fail to update recent field. Exception caught. Detail: %s' % e)
            session.rollback()

    def _update_activity(self, session, active_threshold=1000, hot_threshold=5000):
        self.logger.info('Now start update activity field...')
        try:
            this_week_ts_begin = int(time.mktime(time.strptime(str(datetime.date.today()), '%Y-%m-%d'))) + 4 * 60 * 60
            this_week_ts_end = this_week_ts_begin + 30 * 60
            this_week_results = session.execute(
                'select r.`aid`, `view` from tdd_video_record r join tdd_video v on r.aid = v.aid ' +
                'where r.added >= %d && r.added <= %d' % (this_week_ts_begin, this_week_ts_end))
            this_week_records = {}
            for result in this_week_results:
                aid = result[0]
                view = result[1]
                if aid in this_week_records.keys():
                    last_view = this_week_records[aid]
                    if view > last_view:
                        this_week_records[aid] = view
                else:
                    this_week_records[aid] = view

            last_week_ts_begin = this_week_ts_begin - 7 * 24 * 60 * 60
            last_week_ts_end = last_week_ts_begin + 30 * 60
            last_week_results = session.execute(
                'select r.`aid`, `view` from tdd_video_record r join tdd_video v on r.aid = v.aid ' +
                'where r.added >= %d && r.added <= %d' % (last_week_ts_begin, last_week_ts_end))
            last_week_records = {}
            for result in last_week_results:
                aid = result[0]
                view = result[1]
                if aid in last_week_records.keys():
                    last_view = last_week_records[aid]
                    if view < last_view:
                        last_week_records[aid] = view
                else:
                    last_week_records[aid] = view

            last_week_record_keys = last_week_records.keys()
            diff_records = {}
            for aid in this_week_records.keys():
                if aid in last_week_record_keys:
                    diff_records[aid] = this_week_records[aid] - last_week_records[aid]
                else:
                    diff_records[aid] = this_week_records[aid]

            active_aids = []
            hot_aids = []
            for aid, view in diff_records.items():
                if view >= hot_threshold:
                    hot_aids.append(aid)
                elif view >= active_threshold:
                    active_aids.append(aid)

            session.execute('update tdd_video set activity = 0')
            for aid in active_aids:
                session.execute('update tdd_video set activity = 1 where aid = %d' % aid)
            for aid in hot_aids:
                session.execute('update tdd_video set activity = 2 where aid = %d' % aid)
            session.commit()

            self.logger.info('Finish update activity field! %d active videos and %d hot videos set.' % (
                len(active_aids), len(hot_aids)))
        except Exception as e:
            self.logger.info('Fail to update activity field. Exception caught. Detail: %s' % e)
            session.rollback()

    def _update_freq(self, session):
        self.logger.info('Now start update freq field...')
        try:
            session.execute('update tdd_video set freq = 0')
            session.execute('update tdd_video set freq = 1 where activity = 1')
            session.execute('update tdd_video set freq = 2 where activity = 2 || recent = 1')
            session.commit()
            self.logger.info('Finish update freq field!')
        except Exception as e:
            self.logger.info('Fail to update freq field. Exception caught. Detail: %s' % e)
            session.rollback()

    def run(self):
        self.logger.info('Now start updating recent, activity, freq fields of video...')
        session = Session()
        self._update_recent(session)
        if self.time_label == '04:00':
            self._update_activity(session)
        self._update_freq(session)
        session.close()
        self.logger.info('Finish update recent, activity, freq fields of video!')


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
    del data_acquisition_pipeline_runner_list  # release memory

    # downstream data analysis pipeline
    logger.info('Now start downstream data analysis pipelines...')
    data_analysis_pipeline_runner_list = [
        RecordsSaveToFileRunner(records, time_task),
        RecordsSaveToDbRunner(records),
        RecentRecordsAnalystRunner(records, time_task),
        RecentActivityFreqUpdateRunner(time_label),

    ]
    for runner in data_analysis_pipeline_runner_list:
        runner.start()
    for runner in data_analysis_pipeline_runner_list:
        runner.join()

    logger.info('Finish downstream data analysis pipelines!')
    del data_analysis_pipeline_runner_list  # release memory


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
