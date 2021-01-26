from logutils import logging_init
from pybiliapi import BiliApi
from db import Session, DBOperation
from threading import Thread
from queue import Queue
from common import get_valid, test_archive_rank_by_partion, test_video_view, test_video_stat, \
    add_video_record_via_stat_api, update_video, add_video_via_bvid, \
    InvalidObjCodeError, TddCommonError, AlreadyExistError
from util import get_ts_s, get_ts_s_str, a2b, is_all_zero_record
import math
from conf import get_proxy_pool_url
from serverchan import sc_send
from collections import namedtuple, defaultdict
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
