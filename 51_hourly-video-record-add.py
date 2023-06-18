from logutils import logging_init
from pybiliapi import BiliApi
from db import Session, DBOperation, TddVideoRecordAbnormalChange
from threading import Thread
from queue import Queue
from common import get_valid, test_archive_rank_by_partion, test_video_stat, \
    add_video_record_via_stat_api, \
    InvalidObjCodeError, TddCommonError, AlreadyExistError as CommonAlreadyExistError
from util import get_ts_s, get_ts_s_str, a2b, is_all_zero_record, null_or_str, \
    str_to_ts_s, ts_s_to_str, b2a, zk_calc, get_week_day
import math
import time
import datetime
import os
import re
from conf import get_proxy_pool_url
from serverchan import sc_send
from collections import namedtuple, defaultdict, Counter
from common.error import TddError
from service import Service, CodeError
# from proxypool import get_proxy_url
from task import add_video_record, update_video, add_video, AlreadyExistError
import logging

logger = logging.getLogger('51')

# TODO: remove old record
Record = namedtuple('Record', [
    'added', 'aid', 'bvid', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'like'])
RecordNew = namedtuple('RecordNew', [
    'added', 'aid', 'bvid', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'like',
    'dislike', 'now_rank', 'his_rank', 'vt', 'vv'])
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
                page_obj_videos = len(page_obj['data']['archives'])
                if page_obj_videos < 50:
                    self.logger.warning(
                        'fetcher %s, pn %d, only %d videos found' % (self.name, page_num, page_obj_videos))
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
                # TODO: remove old record
                # record = Record(
                #     added, arch['aid'], arch['bvid'].lstrip('BV'),
                #     -1 if arch_stat['view'] == '--' else arch_stat['view'], arch_stat['danmaku'], arch_stat['reply'],
                #     arch_stat['favorite'], arch_stat['coin'], arch_stat['share'], arch_stat['like']
                # )
                record = RecordNew(
                    added, arch['aid'], arch['bvid'].lstrip('BV'),
                    -1 if arch_stat['view'] == '--' else arch_stat['view'], arch_stat['danmaku'], arch_stat['reply'],
                    arch_stat['favorite'], arch_stat['coin'], arch_stat['share'], arch_stat['like'],
                    arch_stat.get('dislike', None), arch_stat.get('now_rank', None), arch_stat.get('his_rank', None),
                    arch_stat.get('vt', None), arch_stat.get('vv', None)
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
        # self.logger.error('%s' % self.need_insert_but_record_not_found_aid_list)  # TMP
        self.logger.error('TMP stop add affected video record, count: %d' % len(
            self.need_insert_but_record_not_found_aid_list))  # TMP
        sc_send('affected video found', 'send time: %s, count: %d' % (
            get_ts_s_str(), len(self.need_insert_but_record_not_found_aid_list)))  # TMP
        # for idx, aid in enumerate(self.need_insert_but_record_not_found_aid_list, 1):
        #     # try update video
        #     try:
        #         TODO: use new update_video
        #         tdd_video_logs = update_video(aid, bapi_with_proxy, session)
        #     except TddCommonError as e2:
        #         self.logger.warning('Fail to update video aid %d. Exception caught. Detail: %s' % (aid, e2))
        #         result_status_dict['fail_aids'].append(aid)
        #     except Exception as e2:
        #         self.logger.error('Fail to update video aid %d. Exception caught. Detail: %s' % (aid, e2))
        #         result_status_dict['fail_aids'].append(aid)
        #     else:
        #         # check update logs
        #         for log in tdd_video_logs:
        #             self.logger.info('Update video aid %d, attr: %s, oldval: %s, newval: %s'
        #                              % (log.aid, log.attr, log.oldval, log.newval))
        #         # set result status
        #         # NOTE: here maybe code_change_aids and tid_change_aids both +1 aid
        #         tdd_video_logs_attr_list = [ log.attr for log in tdd_video_logs]
        #         expected_change_found = False
        #         if 'code' in tdd_video_logs_attr_list:
        #             result_status_dict['code_change_aids'].append(aid)
        #             expected_change_found = True
        #         if 'tid' in tdd_video_logs_attr_list:
        #             result_status_dict['tid_change_aids'].append(aid)
        #             expected_change_found = True
        #         if 'state' in tdd_video_logs_attr_list and 'forward' in tdd_video_logs_attr_list:
        #             result_status_dict['state_and_forward_change_aids'].append(aid)
        #             expected_change_found = True
        #         if not expected_change_found:
        #             self.logger.warning('No expected change (code / tid / state & forward) found for video aid %d, need further check' % aid)
        #             # TMP START
        #             try:
        #                 new_video_record = add_video_record_via_stat_api(aid, bapi_with_proxy, session)
        #                 self.logger.warning('TMP add affected video record %s' % new_video_record)
        #             except Exception as e3:
        #                 self.logger.warning('TMP Fail to add video record aid %d. Exception caught. Detail: %s' % (aid, e3))
        #             # TMP END
        #             result_status_dict['no_expected_change_found_aids'].append(aid)
        #     finally:
        #         if idx % 10 == 0:
        #             self.logger.info('%d / %d done' % (idx, len(self.need_insert_but_record_not_found_aid_list)))
        # self.logger.info('%d / %d done' % (len(self.need_insert_but_record_not_found_aid_list),
        #                                    len(self.need_insert_but_record_not_found_aid_list)))
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
        service = Service(mode='worker')
        bapi_with_proxy = BiliApi(get_proxy_pool_url())
        _403_aids = DBOperation.query_403_video_aids(session)
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
                new_video = add_video(aid, service, session)
            except AlreadyExistError:
                # video already exist, which is absolutely common
                self.logger.debug('Video aid %d already exists' % aid)
                video_already_exist_flag = True
            except TddError as e:
                self.logger.warning('Fail to add video aid %d. Exception caught. Detail: %s' % (aid, e))
            else:
                self.logger.info('Add new video %s' % new_video)
                result_status_dict['add_new_video_aids'].append(aid)

            if video_already_exist_flag:
                # try update video
                try:
                    tdd_video_logs = update_video(aid, service, session)
                except TddError as e2:
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

        # check all zero record
        self.logger.info('Now start checking all zero record...')
        check_all_zero_record_start_ts = get_ts_s()
        check_all_zero_record_latest_end_ts = check_all_zero_record_start_ts + 3 * 60
        self.logger.info('Limit this process end before %s.' % ts_s_to_str(check_all_zero_record_latest_end_ts))
        check_all_zero_record_all_zero_record_count = 0
        check_all_zero_record_all_zero_record_again_count = 0
        check_all_zero_record_not_all_zero_record_count = 0
        check_all_zero_record_fail_fetch_again_count = 0
        bapi_with_proxy = BiliApi(proxy_pool_url=get_proxy_pool_url())
        for aid, record in aid_record_dict.items():
            if is_all_zero_record(record):
                # check within this if statement in order to reduce check end time consumption
                if get_ts_s() > check_all_zero_record_latest_end_ts:
                    self.logger.warning('Exceed limit end time! Now break!')
                    break
                check_all_zero_record_all_zero_record_count += 1
                self.logger.warning('All zero record of video aid %d detected! Try get video record again...' % aid)
                # get stat_obj
                stat_obj = get_valid(bapi_with_proxy.get_video_stat, (aid,), test_video_stat)
                if stat_obj is None:
                    self.logger.warning('Fail to get valid stat obj of video aid %d!' % aid)
                    check_all_zero_record_fail_fetch_again_count += 1
                    continue
                if stat_obj['code'] != 0:
                    self.logger.warning('Fail to get stat obj with code 0 of video aid %d! code %s detected' % (
                        aid, stat_obj['code']))
                    check_all_zero_record_fail_fetch_again_count += 1
                    continue
                # assemble new record
                # TODO: remove old record
                # new_record = Record(
                #     get_ts_s(), aid, a2b(aid),
                #     -1 if stat_obj['data']['view'] == '--' else stat_obj['data']['view'], stat_obj['data']['danmaku'],
                #     stat_obj['data']['reply'], stat_obj['data']['favorite'], stat_obj['data']['coin'],
                #     stat_obj['data']['share'], stat_obj['data']['like']
                # )
                new_record = RecordNew(
                    get_ts_s(), aid, a2b(aid),
                    -1 if stat_obj['data']['view'] == '--' else stat_obj['data']['view'], stat_obj['data']['danmaku'],
                    stat_obj['data']['reply'], stat_obj['data']['favorite'], stat_obj['data']['coin'],
                    stat_obj['data']['share'], stat_obj['data']['like'],
                    stat_obj['data'].get('dislike', None),
                    stat_obj['data'].get('now_rank', None), stat_obj['data'].get('his_rank', None),
                    stat_obj['data'].get('vt', None), stat_obj['data'].get('vv', None),
                )
                if is_all_zero_record(new_record):
                    self.logger.warning('Get all zero record of video aid %d again!' % aid)
                    check_all_zero_record_all_zero_record_again_count += 1
                    continue
                aid_record_dict[aid] = new_record
                self.logger.warning('Use new not all zero record %s instead.' % str(new_record))
                check_all_zero_record_not_all_zero_record_count += 1
        self.logger.info(
            'Finish checking all zero record! ' +
            '%d all zero record found, ' % check_all_zero_record_all_zero_record_count +
            '%d got all zero record again, ' % check_all_zero_record_all_zero_record_again_count +
            '%d got new not all zero record, ' % check_all_zero_record_not_all_zero_record_count +
            '%d fail to fetch again.' % check_all_zero_record_fail_fetch_again_count
        )

        # get need insert aid list
        session = Session()
        need_insert_aid_list = get_need_insert_aid_list(self.time_label, True, session)
        self.logger.info('%d aid(s) need insert for time label %s' % (len(need_insert_aid_list), self.time_label))

        # insert records
        self.logger.info('Now start inserting records...')
        # use sql directly, combine 1000 records into one sql to execute and commit
        # TODO: remove old record
        # sql_prefix = 'insert into ' \
        #              'tdd_video_record(added, aid, `view`, danmaku, reply, favorite, coin, share, `like`) ' \
        #              'values '
        sql_prefix = 'insert into ' \
                     'tdd_video_record(added, aid, `view`, danmaku, reply, favorite, coin, share, `like`, ' \
                     'dislike, now_rank, his_rank, vt, vv) ' \
                     'values '
        sql = sql_prefix
        need_insert_but_record_not_found_aid_list = []
        log_gap = 1000 * max(1, (len(need_insert_aid_list) // 1000 // 10))
        for idx, aid in enumerate(need_insert_aid_list, 1):
            record = aid_record_dict.get(aid, None)
            if not record:
                need_insert_but_record_not_found_aid_list.append(aid)
                continue
            # TODO: remove old record
            # sql += '(%d, %d, %d, %d, %d, %d, %d, %d, %d), ' % (
            #     record.added, record.aid,
            #     record.view, record.danmaku, record.reply, record.favorite, record.coin, record.share, record.like
            # )
            sql += '(%d, %d, %d, %d, %d, %d, %d, %d, %d, %s, %s, %s, %s, %s), ' % (
                record.added, record.aid,
                record.view, record.danmaku, record.reply, record.favorite, record.coin, record.share, record.like,
                null_or_str(record.dislike), null_or_str(record.now_rank), null_or_str(record.his_rank),
                null_or_str(record.vt), null_or_str(record.vv)
            )
            if idx % 1000 == 0:
                sql = sql[:-2]  # remove ending comma and space
                try:
                    session.execute(sql)
                    session.commit()
                except Exception as e:
                    self.logger.error('Fail to execute sql: %s...%s' % (sql[:100], sql[-100:]))
                    self.logger.error('Exception: %s' % str(e))
                sql = sql_prefix
                if idx % log_gap == 0:
                    self.logger.info('%d / %d done' % (idx, len(need_insert_aid_list)))
        if sql != sql_prefix:
            sql = sql[:-2]  # remove ending comma and space
            try:
                session.execute(sql)
                session.commit()
            except Exception as e:
                self.logger.error('Fail to execute sql: %s...%s' % (sql[:100], sql[-100:]))
                self.logger.error('Exception: %s' % str(e))
        self.logger.info('%d / %d done' % (len(need_insert_aid_list), len(need_insert_aid_list)))
        self.logger.info('Finish inserting records! %d records added, %d aids left' % (
            len(need_insert_aid_list), len(need_insert_but_record_not_found_aid_list)))

        # check need insert but not found aid list
        # these aids should have record in aid_record_dict, but not found at present
        # possible reasons:
        # - now video tid != 30
        # - now video code != 0
        # - now video state = -4, forward = another video aid
        # - ...
        self.logger.info('%d c30 need add but not found aids got' % len(need_insert_but_record_not_found_aid_list))
        self.logger.info('Now start a branch thread for checking need add but not found aids...')
        c30_need_add_but_not_found_aids_checker = C30NeedAddButNotFoundAidsChecker(
            need_insert_but_record_not_found_aid_list)
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
        # bapi_with_proxy = BiliApi(proxy_pool_url=get_proxy_pool_url())
        service = Service(mode='worker')
        fail_aids = []
        new_video_record_list = []
        for idx, aid in enumerate(need_insert_aid_list, 1):
            # add video record
            try:
                # new_video_record = add_video_record_via_stat_api(aid, bapi_with_proxy, session)
                new_video_record = add_video_record(aid, service, session)
                new_video_record_list.append(new_video_record)
                self.logger.debug('Add new record %s' % new_video_record)
            # except InvalidObjCodeError as e:
            except CodeError as e:
                self.logger.warning('Fail to add video record aid %d. Exception caught. Detail: %s' % (aid, e))
                try:
                    tdd_video_logs = update_video(aid, service, session)
                except TddError as e2:
                    self.logger.warning('Fail to update video aid %d. Exception caught. Detail: %s' % (aid, e2))
                except Exception as e2:
                    self.logger.error('Fail to update video aid %d. Exception caught. Detail: %s' % (aid, e2))
                else:
                    for log in tdd_video_logs:
                        self.logger.info('Update video aid %d, attr: %s, oldval: %s, newval: %s'
                                         % (log.aid, log.attr, log.oldval, log.newval))
                fail_aids.append(aid)
            except TddError as e:
                self.logger.warning('Fail to add video record aid %d. Exception caught. Detail: %s' % (aid, e))
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
            # TODO: use following record new
            # RecordNew(
            #     r.added, r.aid, a2b(r.aid), r.view, r.danmaku, r.reply, r.favorite, r.coin, r.share, r.like,
            #     r.dislike, r.now_rank, r.his_rank, r.vt, r.vv)
            for r in new_video_record_list
        ]
        self.logger.info('c0 video pipeline done! return %d records' % len(record_list))
        self.return_record_list = record_list
        session.close()


# TODO: change to record new
class RecordsSaveToFileRunner(Thread):
    def __init__(self, records, time_task, data_folder='data/'):
        super().__init__()
        self.records = records
        self.time_task = time_task
        self.time_label = time_task[-5:]
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

        # TODO ugly design, should be separated into another class
        if self.time_label == '23:00':
            try:
                # get today filename prefix
                day_prefix = ts_s_to_str(get_ts_s())[:10]
                day_prefix_path = self.data_folder + day_prefix

                # pack today file
                self.logger.info('pack %s*.csv into %s.tar.gz' % (day_prefix_path, day_prefix_path))
                pack_result = os.popen(
                    'cd %s && mkdir %s && cp %s*.csv %s && tar -zcvf %s.tar.gz %s && rm -r %s && cd ..' % (
                        self.data_folder, day_prefix, day_prefix, day_prefix, day_prefix, day_prefix, day_prefix
                    )
                )
                for line in pack_result:
                    self.logger.info(line.rstrip('\n'))

                # get 3 day before filename prefix
                day_prefix_3d_before = ts_s_to_str(get_ts_s() - 3 * 24 * 60 * 60)[:10]
                day_prefix_3d_before_path = self.data_folder + day_prefix_3d_before

                # remove 3 day before csv file
                self.logger.info('remove %s*.csv' % day_prefix_3d_before_path)
                pack_result = os.popen('rm %s*.csv' % day_prefix_3d_before_path)
                for line in pack_result:
                    self.logger.info(line.rstrip('\n'))
            except Exception as e:
                self.logger.error('Error occur when executing packing files shell scripts. Detail: %s' % e)
            else:
                self.logger.info('Finish execute packing files shell scripts!')


# TODO: change to record new
class RecordsSaveToDbRunner(Thread):
    def __init__(self, records, time_label):
        super().__init__()
        self.records = records
        self.time_label = time_label
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

        # TODO ugly design, should be separated into another class
        if self.time_label == '23:00':
            try:
                session.execute('drop table if exists tdd_video_record_hourly_4')
                self.logger.info('drop table tdd_video_record_hourly_4')

                session.execute('rename table tdd_video_record_hourly_3 to tdd_video_record_hourly_4')
                self.logger.info('rename table tdd_video_record_hourly_3 to tdd_video_record_hourly_4')

                session.execute('rename table tdd_video_record_hourly_2 to tdd_video_record_hourly_3')
                self.logger.info('rename table tdd_video_record_hourly_2 to tdd_video_record_hourly_3')

                session.execute('rename table tdd_video_record_hourly to tdd_video_record_hourly_2')
                self.logger.info('rename table tdd_video_record_hourly to tdd_video_record_hourly_2')

                session.execute('create table tdd_video_record_hourly like tdd_video_record_hourly_2')
                self.logger.info('create table tdd_video_record_hourly like tdd_video_record_hourly_2')
            except Exception as e:
                session.rollback()
                self.logger.error('Error occur when executing change tdd_video_record_hourly table. Detail: %s' % e)
            else:
                self.logger.info('Finish change tdd_video_record_hourly table!')


# TODO: change to record new
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
            for idx2, record in enumerate(records):
                if is_all_zero_record(record):
                    if idx2 == 0:
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
            for idx2, prop in enumerate(RecordSpeed._fields[4:], 4):
                value = speed_now[idx2]
                if value < -10:
                    change_obj = self._assemble_record_abnormal_change(
                        added=records[-1].added, aid=aid, attr=prop,
                        speed_now=speed_now[idx2], speed_last=speed_last[idx2],
                        speed_now_incr_rate=speed_ratio[idx2 - 4],
                        period_range=speed_period.timespan, speed_period=speed_period[idx2],
                        speed_overall=speed_overall[idx2],
                        this_record=records[-1], last_record=records[-2],
                        description='unexpected drop detected, speed now of prop %s is %.2f, < -10' % (prop, value)
                    )
                    self.logger.info('Found unexpected drop of video aid %d, description: %s' % (
                        aid, change_obj.description))
                    result_status_dict['unexpected_drop'].append(aid)
                    record_abnormal_change_list.append(change_obj)

            # check unexpected speed now value increase
            # rule: speed_ratio.prop > 2 and speed_now.prop > 50
            for idx2, prop in enumerate(RecordSpeedRatio._fields[:7]):
                value = speed_ratio[idx2]
                if value > 2 and speed_now[idx2 + 4] > 50:
                    change_obj = self._assemble_record_abnormal_change(
                        added=records[-1].added, aid=aid, attr=prop,
                        speed_now=speed_now[idx2 + 4], speed_last=speed_last[idx2 + 4],
                        speed_now_incr_rate=speed_ratio[idx2],
                        period_range=speed_period.timespan, speed_period=speed_period[idx2 + 4],
                        speed_overall=speed_overall[idx2 + 4],
                        this_record=records[-1], last_record=records[-2],
                        description='unexpected increase detected, speed now of prop %s is %s, > 200%%' % (
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
        self.logger = logging.getLogger('RecentActivityFreqUpdateRunner')

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


# TODO: update after all-video hourly task fixed
class RankWeeklyUpdateRunner(Thread):
    def __init__(self, records, time_task):
        super().__init__()
        self.records = records
        self.time_task = time_task
        self.time_label = time_task[-5:]
        self.logger = logging.getLogger('RankWeeklyUpdateRunner')

    def run(self):
        self.logger.info('Now start updating rank weekly...')
        session = Session()

        # TODO check why the following two db query need at least 1 min to finish and optimize it

        # get record base dict
        # bvid -> added, view, danmaku, reply, favorite, coin, share, like
        bvid_base_record_dict = DBOperation.query_video_record_rank_weekly_base_dict(session)
        # change format to Record namedtuple
        for bvid, record in bvid_base_record_dict.items():
            bvid_base_record_dict[bvid] = Record(
                record[0], b2a(bvid), bvid, record[1], record[2], record[3], record[4], record[5], record[6], record[7])
        self.logger.info('bvid_base_record_dict with %d records got from db' % len(bvid_base_record_dict))

        # get videos (page), pubdate dict
        # bvid -> videos, pubdate, maybe have None
        bvid_videos_pubdate_dict = DBOperation.query_video_videos_pubdate_dict(session)
        self.logger.info('bvid_videos_pubdate_dict with %d videos got from db' % len(bvid_videos_pubdate_dict))

        # make current issue list
        self.logger.info('Now create video increment list...')
        video_increment_list = []
        base_records_begin_ts = min(map(lambda r: r.added, bvid_base_record_dict.values()))
        for idx, record in enumerate(self.records, 1):
            bvid = record.bvid
            try:
                # get videos (page) and pubdate
                page, pubdate = bvid_videos_pubdate_dict.get(bvid, (None, None))
                if pubdate is None or page is None or page < 1:
                    self.logger.warning('Invalid pubdate %s or page %s of video bvid %s detected, continue' % (
                        str(pubdate), str(page), bvid))
                    continue

                # get base record
                base_record = bvid_base_record_dict.get(bvid, None)
                if base_record is None:
                    # fail to get base record, check pubdate
                    if pubdate >= base_records_begin_ts:
                        # new video, published in this week, set base_record.added to pubdate
                        base_record = Record(pubdate, record.aid, record.bvid, 0, 0, 0, 0, 0, 0, 0)
                    else:
                        # old video, published before this week, should have base record, so here mush be an error
                        self.logger.warning('Fail to get base record of old video bvid %s, continue' % bvid)
                        # TODO need to insert the nearest into base
                        continue

                # calc delta
                d_view = record.view - base_record.view  # maybe occur -1?
                d_danmaku = record.danmaku - base_record.danmaku
                d_reply = record.reply - base_record.reply
                d_favorite = record.favorite - base_record.favorite
                d_coin = record.coin - base_record.coin
                d_share = record.share - base_record.share
                d_like = record.like - base_record.like

                # calc point
                point, xiua, xiub = zk_calc(d_view, d_danmaku, d_reply, d_favorite, page=page)

                # append to video increment list
                video_increment_list.append((
                    bvid, base_record.added, record.added,
                    record.view, record.danmaku, record.reply, record.favorite, record.coin, record.share, record.like,
                    d_view, d_danmaku, d_reply, d_favorite, d_coin, d_share, d_like,
                    point, xiua, xiub))
            except Exception as e:
                self.logger.warning('Fail to create increment of video bvid %s. Exception caught. Detail: %s' % (
                    bvid, e))
            finally:
                if idx % 10000 == 0:
                    self.logger.info('%d / %d done' % (idx, len(self.records)))
        self.logger.info('%d / %d done' % (len(self.records), len(self.records)))
        self.logger.info('Finish create video increment list with %d increments!' % len(video_increment_list))

        # sort via point
        video_increment_list.sort(key=lambda x: (x[17], x[10]))  # TODO if point equals?
        video_increment_list.reverse()
        self.logger.info('Finish sort video increment list!')

        # select top 10000
        video_increment_top_list = video_increment_list[:10000]

        # update sql
        self.logger.info('Now execute update sql...')
        try:
            drop_tmp_table_sql = 'drop table if exists tdd_video_record_rank_weekly_current_tmp'
            session.execute(drop_tmp_table_sql)
            self.logger.info(drop_tmp_table_sql)

            create_tmp_table_sql = 'create table tdd_video_record_rank_weekly_current_tmp ' + \
                                   'like tdd_video_record_rank_weekly_current'
            session.execute(create_tmp_table_sql)
            self.logger.info(create_tmp_table_sql)

            for rank, c in enumerate(video_increment_top_list, 1):
                sql = 'insert into tdd_video_record_rank_weekly_current_tmp values(' \
                      '"%s", %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %f, %f, %f, %d)' % \
                      (c[0], c[1], c[2],
                       c[3], c[4], c[5], c[6], c[7], c[8], c[9],
                       c[10], c[11], c[12], c[13], c[14], c[15], c[16],
                       c[17], c[18], c[19],
                       rank)
                session.execute(sql)
            session.commit()
            self.logger.info('Top 10000 increments added to tdd_video_record_rank_weekly_current_tmp!')

            drop_old_table_sql = 'drop table if exists tdd_video_record_rank_weekly_current'
            session.execute(drop_old_table_sql)
            self.logger.info(drop_old_table_sql)

            rename_tmp_table_sql = 'rename table tdd_video_record_rank_weekly_current_tmp to ' + \
                                   'tdd_video_record_rank_weekly_current'
            session.execute(rename_tmp_table_sql)
            self.logger.info(rename_tmp_table_sql)
        except Exception as e:
            self.logger.error('Fail to execute update sql! Exception caught. Detail: %s' % e)
            session.rollback()
        self.logger.info('Finish execute update sql!')

        self.logger.info('Now update color...')
        color_dict = {
            10: 'incr_view',
            11: 'incr_danmaku',
            12: 'incr_reply',
            13: 'incr_favorite',
            14: 'incr_coin',
            15: 'incr_share',
            16: 'incr_like',
            17: 'point',
        }
        for prop_idx, prop in color_dict.items():
            prop_list = sorted(list(map(lambda x: x[prop_idx], video_increment_top_list)))
            # a
            value = float(prop_list[5000])
            session.execute('update tdd_video_record_rank_weekly_current_color set a = %f ' % value +
                            'where property = "%s"' % prop)
            # b
            value = float(prop_list[9000])
            session.execute('update tdd_video_record_rank_weekly_current_color set b = %f ' % value +
                            'where property = "%s"' % prop)
            # c
            value = float(prop_list[9900])
            session.execute('update tdd_video_record_rank_weekly_current_color set c = %f ' % value +
                            'where property = "%s"' % prop)
            session.commit()
            # d
            value = float(prop_list[9990])
            session.execute('update tdd_video_record_rank_weekly_current_color set d = %f ' % value +
                            'where property = "%s"' % prop)
            session.commit()
        self.logger.info('Finish update color!')

        if self.time_label == '03:00' and get_week_day() == 5:
            self.logger.info('Now archive this week data and start a new week...')
            try:
                # calc archive overview
                ts_str = ts_s_to_str(get_ts_s())
                end_ts = str_to_ts_s(ts_str[:11] + '03:00:00')
                start_ts = end_ts - 7 * 24 * 60 * 60
                issue_num = (start_ts - 1599850800) // (7 * 24 * 60 * 60) + 424
                arch_name = 'W' + ts_str[:4] + ts_str[5:7] + ts_str[8:10] + ' - #' + str(issue_num)
                session.execute(
                    'insert into tdd_video_record_rank_weekly_archive_overview (`name`, start_ts, end_ts) ' +
                    'values ("%s", %d, %d)' % (arch_name, start_ts, end_ts))
                session.commit()
                self.logger.info('Archive overview saved to db! name: %s, start_ts: %d (%s), end_ts: %d (%s)' % (
                    arch_name, start_ts, ts_s_to_str(start_ts), end_ts, ts_s_to_str(end_ts)
                ))

                # get arch id
                result = session.execute('select `id` from tdd_video_record_rank_weekly_archive_overview ' +
                                         'where `name` = "%s"' % arch_name)
                arch_id = 0
                for r in result:
                    arch_id = int(r[0])
                self.logger.info('Archive arch id is %d.' % arch_id)

                # archive increments, just like add current increments, just add 1 more column called arch_id
                self.logger.info('Now archiving increments...')
                for rank, c in enumerate(video_increment_top_list, 1):
                    sql = 'insert into tdd_video_record_rank_weekly_archive values(' \
                          '%d, "%s", %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %f, %f, %f, %d)' % \
                          (arch_id, c[0], c[1], c[2],
                           c[3], c[4], c[5], c[6], c[7], c[8], c[9],
                           c[10], c[11], c[12], c[13], c[14], c[15], c[16],
                           c[17], c[18], c[19],
                           rank)
                    session.execute(sql)
                session.commit()
                self.logger.info('Finish archive current top 10000 increments!')

                # archive color
                self.logger.info('Now archiving color...')
                result = session.execute('select * from tdd_video_record_rank_weekly_current_color')
                for r in result:
                    prop = str(r[0])
                    a = float(r[1])
                    b = float(r[2])
                    c = float(r[3])
                    d = float(r[4])
                    session.execute('insert into tdd_video_record_rank_weekly_archive_color values(' +
                                    '%d, "%s", %f, %f, %f, %f)' % (arch_id, prop, a, b, c, d))
                session.commit()
                self.logger.info('Finish archive color!')

                # update base
                self.logger.info('Now updating base...')
                drop_tmp_table_sql = 'drop table if exists tdd_video_record_rank_weekly_base_tmp'
                session.execute(drop_tmp_table_sql)
                self.logger.info(drop_tmp_table_sql)

                hour_start_ts = str_to_ts_s(ts_s_to_str(get_ts_s())[:11] + '03:00:00')
                create_tmp_table_sql = 'create table tdd_video_record_rank_weekly_base_tmp ' + \
                                       'select * from tdd_video_record_hourly where added >= %d' % hour_start_ts
                session.execute(create_tmp_table_sql)  # create table from tdd_video_record_hourly
                self.logger.info(create_tmp_table_sql)

                # WARNING some c0 videos not have records since this is 03:00 round, not 04:00, which includes all
                # so we need to add c0
                # of cause, some c30 videos maybe not have their records this round due to some error
                # but we do not consider it here, since in 04:00 this will also happen
                # we will handle it at another place
                result = session.execute('select aid from tdd_video where tid != 30 and code = 0 and state = 0')
                all_c0_video_aid_list = [r[0] for r in result]
                result = session.execute('select bvid from tdd_video_record_rank_weekly_base_tmp')
                all_video_aid_in_base_tmp_list = [b2a(r[0]) for r in result]
                no_base_c0_video_aid_list = list(set(all_c0_video_aid_list) - set(all_video_aid_in_base_tmp_list))
                self.logger.info('%d no base c0 video found, now add base record for them...'
                                 % len(no_base_c0_video_aid_list))
                bapi_with_proxy = BiliApi(proxy_pool_url=get_proxy_pool_url())
                fail_aids = []
                success_aids = []
                for idx, aid in enumerate(no_base_c0_video_aid_list, 1):
                    # get stat_obj
                    stat_obj = get_valid(bapi_with_proxy.get_video_stat, (aid,), test_video_stat)
                    if stat_obj is None:
                        self.logger.warning('Fail to get valid stat obj of video aid %d!' % aid)
                        fail_aids.append(aid)
                        continue
                    if stat_obj['code'] != 0:
                        self.logger.warning('Fail to get stat obj with code 0 of video aid %d! code %s detected' % (
                            aid, stat_obj['code']))
                        fail_aids.append(aid)
                        continue
                    # add record to base
                    add_record_to_base_sql = 'insert into tdd_video_record_rank_weekly_base_tmp values ' + \
                                             '(%d, \'%s\', %d, %d, %d, %d, %d, %d, %d)' \
                                             % (get_ts_s(), a2b(aid),
                                                -1 if stat_obj['data']['view'] == '--' else stat_obj['data']['view'],
                                                stat_obj['data']['danmaku'], stat_obj['data']['reply'],
                                                stat_obj['data']['favorite'], stat_obj['data']['coin'],
                                                stat_obj['data']['share'], stat_obj['data']['like'])
                    session.execute(add_record_to_base_sql)
                    success_aids.append(aid)
                    if idx % 10 == 0:
                        self.logger.info('%d / %d done' % (idx, len(no_base_c0_video_aid_list)))
                        session.commit()
                self.logger.info('%d / %d done' % (len(no_base_c0_video_aid_list), len(no_base_c0_video_aid_list)))
                session.commit()
                self.logger.info('Finish adding no base c0 video base records! %d records added, %d aids fail'
                                 % (len(success_aids), len(fail_aids)))
                self.logger.warning('fail_aids: %r' % fail_aids)

                drop_old_table_sql = 'drop table if exists tdd_video_record_rank_weekly_base'
                session.execute(drop_old_table_sql)
                self.logger.info(drop_old_table_sql)

                rename_tmp_table_sql = 'rename table tdd_video_record_rank_weekly_base_tmp to ' + \
                                       'tdd_video_record_rank_weekly_base'
                session.execute(rename_tmp_table_sql)
                self.logger.info(rename_tmp_table_sql)
                self.logger.info('Finish update base!')
            except Exception as e:
                session.rollback()
                self.logger.warning(
                    'Fail to archive this week data and start a new week. Exception caught. Detail: %s' % e)
            else:
                self.logger.info('Finish archive this week data and start a new week!')

        session.close()
        self.logger.info('Finish update rank weekly!')


# TODO: update after all-video hourly task fixed
class RankMonthlyUpdateRunner(Thread):
    def __init__(self, records, time_task):
        super().__init__()
        self.records = records
        self.time_task = time_task
        self.time_label = time_task[-5:]
        self.day_num = time_task[8:10]  # ADD
        self.logger = logging.getLogger('RankMonthlyUpdateRunner')  # change

    def run(self):
        self.logger.info('Now start updating rank monthly...')  # change
        session = Session()

        # TODO check why the following two db query need at least 1 min to finish and optimize it

        # get record base dict
        # bvid -> added, view, danmaku, reply, favorite, coin, share, like
        bvid_base_record_dict = DBOperation.query_video_record_rank_monthly_base_dict(session)  # CHANGE
        # change format to Record namedtuple
        for bvid, record in bvid_base_record_dict.items():
            bvid_base_record_dict[bvid] = Record(
                record[0], b2a(bvid), bvid, record[1], record[2], record[3], record[4], record[5], record[6], record[7])
        self.logger.info('bvid_base_record_dict with %d records got from db' % len(bvid_base_record_dict))

        # get videos (page), pubdate dict
        # bvid -> videos, pubdate, maybe have None
        bvid_videos_pubdate_dict = DBOperation.query_video_videos_pubdate_dict(session)
        self.logger.info('bvid_videos_pubdate_dict with %d videos got from db' % len(bvid_videos_pubdate_dict))

        # make current issue list
        self.logger.info('Now create video increment list...')
        video_increment_list = []
        base_records_begin_ts = min(map(lambda r: r.added, bvid_base_record_dict.values()))
        for idx, record in enumerate(self.records, 1):
            bvid = record.bvid
            try:
                # get videos (page) and pubdate
                page, pubdate = bvid_videos_pubdate_dict.get(bvid, (None, None))
                if pubdate is None or page is None or page < 1:
                    self.logger.warning('Invalid pubdate %s or page %s of video bvid %s detected, continue' % (
                        str(pubdate), str(page), bvid))
                    continue

                # get base record
                base_record = bvid_base_record_dict.get(bvid, None)
                if base_record is None:
                    # fail to get base record, check pubdate
                    if pubdate >= base_records_begin_ts:
                        # new video, published in this month, set base_record.added to pubdate  # CHANGE
                        base_record = Record(pubdate, record.aid, record.bvid, 0, 0, 0, 0, 0, 0, 0)
                    else:
                        # old video, published before this month, should have base record, so here mush be an error  # CHANGE
                        self.logger.warning('Fail to get base record of old video bvid %s, continue' % bvid)
                        # TODO need to insert the nearest into base
                        continue

                # calc delta
                d_view = record.view - base_record.view  # maybe occur -1?
                d_danmaku = record.danmaku - base_record.danmaku
                d_reply = record.reply - base_record.reply
                d_favorite = record.favorite - base_record.favorite
                d_coin = record.coin - base_record.coin
                d_share = record.share - base_record.share
                d_like = record.like - base_record.like

                # calc point
                point, xiua, xiub = zk_calc(d_view, d_danmaku, d_reply, d_favorite, page=page)

                # append to video increment list
                video_increment_list.append((
                    bvid, base_record.added, record.added,
                    record.view, record.danmaku, record.reply, record.favorite, record.coin, record.share, record.like,
                    d_view, d_danmaku, d_reply, d_favorite, d_coin, d_share, d_like,
                    point, xiua, xiub))
            except Exception as e:
                self.logger.warning('Fail to create increment of video bvid %s. Exception caught. Detail: %s' % (
                    bvid, e))
            finally:
                if idx % 10000 == 0:
                    self.logger.info('%d / %d done' % (idx, len(self.records)))
        self.logger.info('%d / %d done' % (len(self.records), len(self.records)))
        self.logger.info('Finish create video increment list with %d increments!' % len(video_increment_list))

        # sort via point
        video_increment_list.sort(key=lambda x: (x[17], x[10]))  # TODO if point equals?
        video_increment_list.reverse()
        self.logger.info('Finish sort video increment list!')

        # select top 10000
        video_increment_top_list = video_increment_list[:10000]

        # update sql
        self.logger.info('Now execute update sql...')
        try:
            drop_tmp_table_sql = 'drop table if exists tdd_video_record_rank_monthly_current_tmp'  # CHANGE
            session.execute(drop_tmp_table_sql)
            self.logger.info(drop_tmp_table_sql)

            create_tmp_table_sql = 'create table tdd_video_record_rank_monthly_current_tmp ' + \
                                   'like tdd_video_record_rank_monthly_current'  # CHANGE
            session.execute(create_tmp_table_sql)
            self.logger.info(create_tmp_table_sql)

            for rank, c in enumerate(video_increment_top_list, 1):
                sql = 'insert into tdd_video_record_rank_monthly_current_tmp values(' \
                      '"%s", %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %f, %f, %f, %d)' % \
                      (c[0], c[1], c[2],
                       c[3], c[4], c[5], c[6], c[7], c[8], c[9],
                       c[10], c[11], c[12], c[13], c[14], c[15], c[16],
                       c[17], c[18], c[19],
                       rank)  # CHANGE
                session.execute(sql)
            session.commit()
            self.logger.info('Top 10000 increments added to tdd_video_record_rank_monthly_current_tmp!')  # CHANGE

            drop_old_table_sql = 'drop table if exists tdd_video_record_rank_monthly_current'  # CHANGE
            session.execute(drop_old_table_sql)
            self.logger.info(drop_old_table_sql)

            rename_tmp_table_sql = 'rename table tdd_video_record_rank_monthly_current_tmp to ' + \
                                   'tdd_video_record_rank_monthly_current'  # CHANGE
            session.execute(rename_tmp_table_sql)
            self.logger.info(rename_tmp_table_sql)
        except Exception as e:
            self.logger.error('Fail to execute update sql! Exception caught. Detail: %s' % e)
            session.rollback()
        self.logger.info('Finish execute update sql!')

        self.logger.info('Now update color...')
        color_dict = {
            10: 'incr_view',
            11: 'incr_danmaku',
            12: 'incr_reply',
            13: 'incr_favorite',
            14: 'incr_coin',
            15: 'incr_share',
            16: 'incr_like',
            17: 'point',
        }
        for prop_idx, prop in color_dict.items():
            prop_list = sorted(list(map(lambda x: x[prop_idx], video_increment_top_list)))
            # a
            value = float(prop_list[5000])
            session.execute('update tdd_video_record_rank_monthly_current_color set a = %f ' % value +
                            'where property = "%s"' % prop)  # CHANGE
            # b
            value = float(prop_list[9000])
            session.execute('update tdd_video_record_rank_monthly_current_color set b = %f ' % value +
                            'where property = "%s"' % prop)  # CHANGE
            # c
            value = float(prop_list[9900])
            session.execute('update tdd_video_record_rank_monthly_current_color set c = %f ' % value +
                            'where property = "%s"' % prop)  # CHANGE
            session.commit()
            # d
            value = float(prop_list[9990])
            session.execute('update tdd_video_record_rank_monthly_current_color set d = %f ' % value +
                            'where property = "%s"' % prop)  # CHANGE
            session.commit()
        self.logger.info('Finish update color!')

        if self.time_label == '04:00' and self.day_num == '01':  # CHANGE
            self.logger.info('Now archive this month data and start a new month...')  # CHANGE
            try:
                # calc archive overview
                ts_str = ts_s_to_str(get_ts_s())
                end_ts = str_to_ts_s(ts_str[:11] + '04:00:00')  # CHANGE
                # CHANGE
                # get last month day 01 time 04:00:00
                this_year_num = int(ts_str[:4])
                this_month_num = int(ts_str[5:7])
                if this_month_num == 0:
                    last_year_num = this_year_num - 1
                    last_month_num = 12
                else:
                    last_year_num = this_year_num
                    last_month_num = this_month_num - 1
                last_month_str = str(last_month_num) if last_month_num > 9 else '0' + str(last_month_num)
                start_ts_str = str(last_year_num) + '-' + last_month_str + '-01 04:00:00'
                start_ts = str_to_ts_s(start_ts_str)
                arch_name = 'M' + ts_str[:4] + ts_str[5:7] + ts_str[8:10]
                session.execute(
                    'insert into tdd_video_record_rank_monthly_archive_overview (`name`, start_ts, end_ts) ' +
                    'values ("%s", %d, %d)' % (arch_name, start_ts, end_ts))  # CHANGE
                session.commit()
                self.logger.info('Archive overview saved to db! name: %s, start_ts: %d (%s), end_ts: %d (%s)' % (
                    arch_name, start_ts, ts_s_to_str(start_ts), end_ts, ts_s_to_str(end_ts)
                ))

                # get arch id
                result = session.execute('select `id` from tdd_video_record_rank_monthly_archive_overview ' +
                                         'where `name` = "%s"' % arch_name)  # CHANGE
                arch_id = 0
                for r in result:
                    arch_id = int(r[0])
                self.logger.info('Archive arch id is %d.' % arch_id)

                # archive increments, just like add current increments, just add 1 more column called arch_id
                self.logger.info('Now archiving increments...')
                for rank, c in enumerate(video_increment_top_list, 1):
                    sql = 'insert into tdd_video_record_rank_monthly_archive values(' \
                          '%d, "%s", %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %f, %f, %f, %d)' % \
                          (arch_id, c[0], c[1], c[2],
                           c[3], c[4], c[5], c[6], c[7], c[8], c[9],
                           c[10], c[11], c[12], c[13], c[14], c[15], c[16],
                           c[17], c[18], c[19],
                           rank)  # CHANGE
                    session.execute(sql)
                session.commit()
                self.logger.info('Finish archive current top 10000 increments!')

                # archive color
                self.logger.info('Now archiving color...')
                result = session.execute('select * from tdd_video_record_rank_monthly_current_color')  # CHANGE
                for r in result:
                    prop = str(r[0])
                    a = float(r[1])
                    b = float(r[2])
                    c = float(r[3])
                    d = float(r[4])
                    session.execute('insert into tdd_video_record_rank_monthly_archive_color values(' +
                                    '%d, "%s", %f, %f, %f, %f)' % (arch_id, prop, a, b, c, d))  # CHANGE
                session.commit()
                self.logger.info('Finish archive color!')

                # update base
                self.logger.info('Now updating base...')
                drop_tmp_table_sql = 'drop table if exists tdd_video_record_rank_monthly_base_tmp'  # CHANGE
                session.execute(drop_tmp_table_sql)
                self.logger.info(drop_tmp_table_sql)

                hour_start_ts = str_to_ts_s(ts_s_to_str(get_ts_s())[:11] + '04:00:00')  # CHANGE
                create_tmp_table_sql = 'create table tdd_video_record_rank_monthly_base_tmp ' + \
                                       'select * from tdd_video_record_hourly where added >= %d' % hour_start_ts  # CHANGE
                session.execute(create_tmp_table_sql)  # create table from tdd_video_record_hourly
                self.logger.info(create_tmp_table_sql)

                drop_old_table_sql = 'drop table if exists tdd_video_record_rank_monthly_base'  # CHANGE
                session.execute(drop_old_table_sql)
                self.logger.info(drop_old_table_sql)

                rename_tmp_table_sql = 'rename table tdd_video_record_rank_monthly_base_tmp to ' + \
                                       'tdd_video_record_rank_monthly_base'  # CHANGE
                session.execute(rename_tmp_table_sql)
                self.logger.info(rename_tmp_table_sql)
                self.logger.info('Finish update base!')
            except Exception as e:
                session.rollback()
                self.logger.warning(
                    'Fail to archive this month data and start a new month. Exception caught. Detail: %s' % e)  # CHANGE
            else:
                self.logger.info('Finish archive this month data and start a new month!')  # CHANGE

        session.close()
        self.logger.info('Finish update rank monthly!')  # CHANGE


def run_hourly_video_record_add(time_task):
    time_label = time_task[-5:]  # current time, ex: 19:00
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

    # remove duplicate records
    logger.info('Now check duplicate records...')
    duplicate_records_item_list = list(
        filter(lambda item: item[1] > 1, Counter(map(lambda record: record.bvid, records)).items())
    )  # bvid -> count of records of video with this bvid
    if len(duplicate_records_item_list) == 0:
        logger.info('No duplicate records detected!')
    else:
        logger.warning('Duplicate records detected! %d videos with %d records in total!' % (
            len(duplicate_records_item_list), sum(map(lambda item: item[1], duplicate_records_item_list))
        ))
        removed_records_count = 0
        for bvid, count in duplicate_records_item_list:
            logger.warning('Video bvid %s have total %d records!' % (bvid, count))
            for record in sorted(
                    filter(lambda record: record.bvid == bvid, records),  # records from video with the same bvid
                    key=lambda record: record.added  # sorted by added, asc
            )[1:]:  # remain the first one, i.e. the earliest record
                records.remove(record)
                removed_records_count += 1
        logger.warning('Finish remove duplicate records! Total %d duplicate records removed!' % removed_records_count)

    logger.info('Finish upstream data acquisition pipelines! %d records received' % len(records))
    del data_acquisition_pipeline_runner_list  # release memory

    # downstream data analysis pipeline
    logger.info('Now start downstream data analysis pipelines...')
    data_analysis_pipeline_runner_list = [
        RecordsSaveToFileRunner(records, time_task),
        RecordsSaveToDbRunner(records, time_label),
        RecentRecordsAnalystRunner(records, time_task),
        RecentActivityFreqUpdateRunner(time_label),
        # RankWeeklyUpdateRunner(records, time_task),
        # RankMonthlyUpdateRunner(records, time_task),
    ]
    for runner in data_analysis_pipeline_runner_list:
        runner.start()
    for runner in data_analysis_pipeline_runner_list:
        runner.join()

    logger.info('Finish downstream data analysis pipelines!')
    del data_analysis_pipeline_runner_list  # release memory


def main():
    logger.info('51: hourly video record add')

    time_task = '%s:00' % get_ts_s_str()[:13]  # current time task, ex: 2013-01-31 19:00
    logger.info('Now start, time task: %s' % time_task)
    try:
        run_hourly_video_record_add(time_task)
    except Exception as e:
        logger.critical(e)
        sc_send('51: Critical exception occurred!', 'send time: %s, exception description: %s' % (get_ts_s_str(), e))
    logger.info('Done! time task: %s' % time_task)


if __name__ == '__main__':
    # current time task, only number, ex: 201301311900
    time_task_simple = ('%s:00' % get_ts_s_str()[:13]).replace('-', '').replace(' ', '').replace(':', '')
    logging_init(file_prefix='51_%s' % time_task_simple)
    main()
