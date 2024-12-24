from db import Session, DBOperation, TddVideoRecordAbnormalChange, TddVideoRecord
from threading import Thread
from queue import Queue
from util import get_ts_s, get_ts_s_str, a2b, is_all_zero_record, null_or_str, \
    str_to_ts_s, ts_s_to_str, b2a, zk_calc, get_week_day, logging_init, fullname, get_current_line_no, format_ts_ms
import math
import time
import datetime
import os
import re
from serverchan import sc_send_summary, sc_send_critical
from collections import namedtuple, defaultdict, Counter
from core import TddError
from service import Service, NewlistArchive, VideoView
from job import GetNewlistArchiveJob, JobStat, AddVideoRecordJob, Job
from typing import NamedTuple, Optional
from task import update_video, add_video, AlreadyExistError
from timer import Timer
import logging

script_id = '51'
script_name = 'hourly-video-record-add'
script_fullname = fullname(script_id, script_name)
logger = logging.getLogger(script_id)

# TODO: remove old record
Record = namedtuple('Record', [
    'added', 'aid', 'bvid', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'like'])
# RecordNew = namedtuple('RecordNew', [
#     'added', 'aid', 'bvid', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'like',
#     'dislike', 'now_rank', 'his_rank', 'vt', 'vv'])
RecordSpeed = namedtuple('RecordSpeed', [
    'start_ts', 'end_ts', 'timespan', 'per_seconds', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'like'])
RecordSpeedRatio = namedtuple('RecordSpeedRatio', [
    'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'like', 'inf_magic_num'])


# TODO: rename back to Record after all old Record is removed
class RecordNew(NamedTuple):
    added: int
    aid: int
    bvid: str
    view: int
    danmaku: int
    reply: int
    favorite: int
    coin: int
    share: int
    like: int
    dislike: int
    now_rank: int
    his_rank: int
    vt: Optional[int]
    vv: Optional[int]


def get_need_insert_aid_list(time_label, is_tid_30, session):
    if time_label == '04:00':
        # return total
        return DBOperation.query_all_update_video_aids(is_tid_30, session)

    # add 1 hour aids
    aid_list = DBOperation.query_freq_update_video_aids(
        2, is_tid_30, session)  # freq = 2

    if time_label in ['00:00', '04:00', '08:00', '12:00', '16:00', '20:00']:
        # add 4 hour aids
        # freq = 1
        aid_list += DBOperation.query_freq_update_video_aids(
            1, is_tid_30, session)

    return aid_list


class CheckC30NeedInsertButNotFoundAidsJob(Job):
    """
    Check c30 need insert but not found aids.
    It is expected that record of those aids already fetched, however not found at present.
    Possible reasons:
    - now video tid != 30
    - now video code != 0
    - now video state = -4, forward = another video aid
    - missing video from api (e.g. partion archive api)
    - ...
    """

    def __init__(self, name: str, aid_queue: Queue[int], record_queue: Queue[RecordNew], service: Service):
        super().__init__(name)
        self.aid_queue = aid_queue
        self.record_queue = record_queue
        self.service = service
        self.session = Session()
        self._duration_limit_s = 60 * 40  # 40 minutes

    def process(self):
        # TMP duration limit
        duration_limit_due_ts = get_ts_s() + self._duration_limit_s
        self.logger.info(
            f'Duration limit due at {ts_s_to_str(duration_limit_due_ts)}.')

        while not self.aid_queue.empty():
            if get_ts_s() > duration_limit_due_ts:
                self.logger.warning('Duration limit reached. Exit.')
                break

            aid = self.aid_queue.get()
            self.logger.debug(
                f'Now check c30 need insert but not found aid. aid: {aid}')
            timer = Timer()
            timer.start()

            try:
                update_video_context = {}
                tdd_video_logs = update_video(
                    aid, self.service, self.session, out_context=update_video_context)
                video_view: VideoView = update_video_context['video_view']
            except Exception as e:
                self.logger.error(
                    f'Fail to update video info. aid: {aid}, error: {e}')
                self.stat.condition['update_exception'] += 1
            else:
                for log in tdd_video_logs:
                    self.logger.info(
                        f'Update video info. aid: {aid}, attr: {log.attr}, {log.oldval} -> {log.newval}')
                self.logger.debug(
                    f'{len(tdd_video_logs)} log(s) found. aid: {aid}')
                self.stat.condition[f'{len(tdd_video_logs)}_update'] += 1

                log_attr_log_dict = {log.attr: log for log in tdd_video_logs}
                expected_change_found = False
                if 'tid' in log_attr_log_dict:
                    if log_attr_log_dict['tid'].oldval == '30' and log_attr_log_dict['tid'].newval != '30':
                        expected_change_found = True
                        self.stat.condition['tid_not_30'] += 1
                if 'code' in log_attr_log_dict:
                    if log_attr_log_dict['code'].oldval == '0' and log_attr_log_dict['code'].newval != '0':
                        expected_change_found = True
                        self.stat.condition['code_not_0'] += 1
                if 'state' in log_attr_log_dict and 'forward' in log_attr_log_dict:
                    if log_attr_log_dict['state'].oldval == '0' and log_attr_log_dict['state'].newval == '-4' and \
                            log_attr_log_dict['forward'].newval != str(aid):
                        expected_change_found = True
                        self.stat.condition['state_-4'] += 1

                if expected_change_found:
                    self.stat.condition['expected_change_found'] += 1
                else:
                    # Due to the bug of awesome api, some video may not be fetched from the batch api.
                    # In this case, we need to retrieve video record from video view.
                    # So much such missing video existed, therefore, to simplify the log,
                    # we downgrade the log level from warning to debug.
                    self.logger.debug(
                        f'Expected change not found, maybe missing video from api. aid: {aid}')
                    self.stat.condition['expected_change_not_found'] += 1

                    # Parse record from video view which already fetched before when update video.
                    new_record = RecordNew(
                        added=get_ts_s(),
                        aid=aid,
                        bvid=video_view.bvid.lstrip('BV'),
                        view=video_view.stat.view,
                        danmaku=video_view.stat.danmaku,
                        reply=video_view.stat.reply,
                        favorite=video_view.stat.favorite,
                        coin=video_view.stat.coin,
                        share=video_view.stat.share,
                        like=video_view.stat.like,
                        dislike=video_view.stat.dislike,
                        now_rank=video_view.stat.now_rank,
                        his_rank=video_view.stat.his_rank,
                        vt=video_view.stat.vt,
                        vv=video_view.stat.vv,
                    )
                    self.record_queue.put(new_record)
                    self.logger.info(
                        f'Missing video record get. aid: {aid}, record: {new_record}')
                    self.stat.condition['missing_video_record_get'] += 1

            timer.stop()
            self.logger.debug(f'Finish check c30 need insert but not found aid. '
                              f'aid: {aid}, duration: {format_ts_ms(timer.get_duration_ms())}')
            self.stat.total_count += 1
            self.stat.total_duration_ms += timer.get_duration_ms()

    def cleanup(self):
        self.session.close()


class CheckAllZeroRecordJob(Job):
    """
    Check records, avoid all zero record got due to API error.
    Once all zero record detected, try to re-fetch video record.
    """

    def __init__(self, name: str,
                 record_queue: Queue[RecordNew], checked_record_queue: Queue[RecordNew], service: Service):
        super().__init__(name)
        self.record_queue = record_queue
        self.checked_record_queue = checked_record_queue
        self.service = service
        self._duration_limit_s = 60 * 3  # 3 minutes

    def process(self):
        # TMP duration limit
        duration_limit_due_ts = get_ts_s() + self._duration_limit_s
        self.logger.info(
            f'Duration limit due at {ts_s_to_str(duration_limit_due_ts)}.')

        while not self.record_queue.empty():
            if get_ts_s() > duration_limit_due_ts:
                self.logger.warning('Duration limit reached. Exit.')
                break

            record = self.record_queue.get()
            timer = Timer()
            timer.start()

            if is_all_zero_record(record):
                self.logger.debug(
                    f'All zero record of video aid {record.aid} detected. Try get video record again.')
                self.stat.condition['all_zero_record'] += 1

                # get video view
                try:
                    video_view = self.service.get_video_view(
                        {'aid': record.aid})
                except Exception as e:
                    self.logger.warning(
                        f'Fail to get valid video view. aid: {record.aid}, error: {e}')
                    self.stat.condition['fail_fetch_again'] += 1
                else:
                    # assemble new record
                    new_record = RecordNew(
                        added=get_ts_s(),
                        aid=video_view.aid,
                        bvid=video_view.bvid.lstrip('BV'),
                        view=video_view.stat.view,
                        danmaku=video_view.stat.danmaku,
                        reply=video_view.stat.reply,
                        favorite=video_view.stat.favorite,
                        coin=video_view.stat.coin,
                        share=video_view.stat.share,
                        like=video_view.stat.like,
                        dislike=video_view.stat.dislike,
                        now_rank=video_view.stat.now_rank,
                        his_rank=video_view.stat.his_rank,
                        vt=video_view.stat.vt,
                        vv=video_view.stat.vv
                    )

                    if is_all_zero_record(new_record):
                        self.logger.debug(
                            f'All zero record of video aid {record.aid} detected again.')
                        self.stat.condition['all_zero_record_again'] += 1
                    else:
                        # not all zero record got, use new record
                        record = new_record
                        self.logger.info(
                            f'Not all zero record {new_record} detected. Use new record instead.')
                        self.stat.condition['not_all_zero_record'] += 1

            self.checked_record_queue.put(record)

            timer.stop()
            self.stat.total_count += 1
            self.stat.total_duration_ms += timer.get_duration_ms()


# class C30NeedAddButNotFoundAidsChecker(Thread):
#     def __init__(self, need_insert_but_record_not_found_aid_list):
#         super().__init__()
#         self.need_insert_but_record_not_found_aid_list = need_insert_but_record_not_found_aid_list
#         self.logger = logging.getLogger('C30NeedAddButNotFoundAidsChecker')
#
#     def run(self):
#         # check need insert but not found aid list
#         # these aids should have record in aid_record_dict, but not found at present
#         # possible reasons:
#         # - now video tid != 30
#         # - now video code != 0
#         # - now video state = -4, forward = another video aid
#         # - ...
#         self.logger.info('Now start checking need add but not found aids...')
#         session = Session()
#         result_status_dict = defaultdict(list)
#         # self.logger.error('%s' % self.need_insert_but_record_not_found_aid_list)  # TMP
#         self.logger.error('TMP stop add affected video record, count: %d' % len(
#             self.need_insert_but_record_not_found_aid_list))  # TMP
#         sc_send('affected video found', 'send time: %s, count: %d' % (
#             get_ts_s_str(), len(self.need_insert_but_record_not_found_aid_list)))  # TMP
#         # for idx, aid in enumerate(self.need_insert_but_record_not_found_aid_list, 1):
#         #     # try update video
#         #     try:
#         #         TODO: use new update_video
#         #         tdd_video_logs = update_video(aid, bapi_with_proxy, session)
#         #     except TddCommonError as e2:
#         #         self.logger.warning('Fail to update video aid %d. Exception caught. Detail: %s' % (aid, e2))
#         #         result_status_dict['fail_aids'].append(aid)
#         #     except Exception as e2:
#         #         self.logger.error('Fail to update video aid %d. Exception caught. Detail: %s' % (aid, e2))
#         #         result_status_dict['fail_aids'].append(aid)
#         #     else:
#         #         # check update logs
#         #         for log in tdd_video_logs:
#         #             self.logger.info('Update video aid %d, attr: %s, oldval: %s, newval: %s'
#         #                              % (log.aid, log.attr, log.oldval, log.newval))
#         #         # set result status
#         #         # NOTE: here maybe code_change_aids and tid_change_aids both +1 aid
#         #         tdd_video_logs_attr_list = [ log.attr for log in tdd_video_logs]
#         #         expected_change_found = False
#         #         if 'code' in tdd_video_logs_attr_list:
#         #             result_status_dict['code_change_aids'].append(aid)
#         #             expected_change_found = True
#         #         if 'tid' in tdd_video_logs_attr_list:
#         #             result_status_dict['tid_change_aids'].append(aid)
#         #             expected_change_found = True
#         #         if 'state' in tdd_video_logs_attr_list and 'forward' in tdd_video_logs_attr_list:
#         #             result_status_dict['state_and_forward_change_aids'].append(aid)
#         #             expected_change_found = True
#         #         if not expected_change_found:
#         #             self.logger.warning('No expected change (code / tid / state & forward) found for video aid %d, need further check' % aid)
#         #             # TMP START
#         #             try:
#         #                 new_video_record = add_video_record_via_stat_api(aid, bapi_with_proxy, session)
#         #                 self.logger.warning('TMP add affected video record %s' % new_video_record)
#         #             except Exception as e3:
#         #                 self.logger.warning('TMP Fail to add video record aid %d. Exception caught. Detail: %s' % (aid, e3))
#         #             # TMP END
#         #             result_status_dict['no_expected_change_found_aids'].append(aid)
#         #     finally:
#         #         if idx % 10 == 0:
#         #             self.logger.info('%d / %d done' % (idx, len(self.need_insert_but_record_not_found_aid_list)))
#         # self.logger.info('%d / %d done' % (len(self.need_insert_but_record_not_found_aid_list),
#         #                                    len(self.need_insert_but_record_not_found_aid_list)))
#         session.close()
#         self.logger.info('Finish checking need add but not found aids! %s' %
#                          ', '.join(['%s: %d' % (k, len(v)) for (k, v) in dict(result_status_dict).items()]))
#         self.logger.warning('fail_aids: %r' % result_status_dict['fail_aids'])
#         self.logger.warning('no_change_found_aids: %r' % result_status_dict['no_change_found_aids'])


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
                self.logger.warning(
                    'Fail to add video aid %d. Exception caught. Detail: %s' % (aid, e))
            else:
                self.logger.info('Add new video %s' % new_video)
                result_status_dict['add_new_video_aids'].append(aid)

            if video_already_exist_flag:
                # try update video
                try:
                    tdd_video_logs = update_video(aid, service, session)
                except TddError as e2:
                    self.logger.warning(
                        'Fail to update video aid %d. Exception caught. Detail: %s' % (aid, e2))
                    result_status_dict['fail_aids'].append(aid)
                except Exception as e2:
                    self.logger.error(
                        'Fail to update video aid %d. Exception caught. Detail: %s' % (aid, e2))
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
                        self.logger.warning(
                            'No code change found for video aid %d, need further check' % aid)
                        result_status_dict['no_change_found_aids'].append(aid)

            if idx % 10 == 0:
                self.logger.info('%d / %d done' %
                                 (idx, len(self.no_need_insert_aid_list)))
        self.logger.info(
            '%d / %d done' % (len(self.no_need_insert_aid_list), len(self.no_need_insert_aid_list)))
        self.logger.info('Finish checking no need insert records! %s' %
                         ', '.join(['%s: %d' % (k, len(v)) for (k, v) in dict(result_status_dict).items()]))
        self.logger.warning('fail_aids: %r' % result_status_dict['fail_aids'])
        self.logger.warning('no_change_found_aids: %r' %
                            result_status_dict['no_change_found_aids'])


class DataAcquisitionJob(Job):
    def __init__(self, name: str, time_label: str, record_queue: Queue[RecordNew]):
        super().__init__(name)
        self.time_label = time_label
        self.record_queue = record_queue


class C0DataAcquisitionJob(DataAcquisitionJob):
    def __init__(self, time_label: str, record_queue: Queue[RecordNew]):
        super().__init__('c0', time_label, record_queue)

    def process(self):
        # get need insert aid list
        session = Session()
        need_insert_aid_list = get_need_insert_aid_list(
            self.time_label, False, session)
        session.close()
        self.logger.info(
            f'{len(need_insert_aid_list)} aid(s) need insert for time label {self.time_label}.')

        service = Service(mode='worker')

        # put aid into queue
        aid_queue: Queue[int] = Queue()
        for aid in need_insert_aid_list:
            aid_queue.put(aid)

        # create video record queue
        video_record_queue: Queue[TddVideoRecord] = Queue()

        # create jobs
        job_num = 10
        job_list = []
        for i in range(job_num):
            job_list.append(AddVideoRecordJob(
                f'job_{i}', aid_queue, video_record_queue, service))

        # start jobs
        for job in job_list:
            job.start()
        logger.info(f'{job_num} job(s) started.')

        # wait for jobs
        for job in job_list:
            job.join()

        # collect statistics
        job_stat_list: list[JobStat] = []
        for job in job_list:
            job_stat_list.append(job.stat)

        # merge statistics counters
        job_stat_merged = sum(job_stat_list, JobStat())

        self.logger.info('Finish add need insert aid list!')
        self.logger.info(job_stat_merged.get_summary())

        # parse tdd video record to record
        while not video_record_queue.empty():
            video_record = video_record_queue.get()
            self.record_queue.put(RecordNew(
                added=video_record.added,
                aid=video_record.aid,
                bvid=a2b(video_record.aid),
                view=video_record.view,
                danmaku=video_record.danmaku,
                reply=video_record.reply,
                favorite=video_record.favorite,
                coin=video_record.coin,
                share=video_record.share,
                like=video_record.like,
                dislike=video_record.dislike,
                now_rank=video_record.now_rank,
                his_rank=video_record.his_rank,
                vt=video_record.vt,
                vv=video_record.vv,
            ))
        self.logger.info(
            f'{self.record_queue.qsize()} record(s) parsed and returned.')


# TODO: refactor using Job, create a class DataAcquisitionJob,
#  then derive from it to create C30DataAcquisitionJob and C0DataAcquisitionJob
class C30PipelineRunner(Thread):
    def __init__(self, time_label, record_queue):
        super().__init__()
        self.time_label = time_label
        self.record_queue = record_queue
        self.logger = logging.getLogger('C30PipelineRunner')

    def get_all_c30_video_record_from_newlist_api(self, service: Service, job_num: int = 80) -> Queue[RecordNew]:
        # get newlist
        try:
            new_list = service.get_newlist({'rid': 30, 'pn': 1, 'ps': 50})
        except Exception as e:
            self.logger.error(f'Fail to get archive rank by partion for calculating page num total. '
                              f'tid: 30, pn: 1, ps: 50, error: {e}')
            raise e

        # calculate page num total
        page_num_total = math.ceil(new_list.page.count / 50)
        self.logger.info(
            f'Archive page num total calculated. page_num_total: {page_num_total}')

        # put page num into queue
        page_num_queue: Queue[int] = Queue()
        for page_num in range(1, page_num_total + 1):
            page_num_queue.put(page_num)
        self.logger.info(f'{page_num_queue.qsize()} page nums put into queue.')

        # create archive video queue
        archive_video_queue: Queue[tuple[int, NewlistArchive]] = Queue()

        # create jobs
        job_list = []
        for i in range(job_num):
            job_list.append(GetNewlistArchiveJob(
                f'job_{i}', 30, page_num_queue, archive_video_queue, service))

        # start jobs
        for job in job_list:
            job.start()
        logger.info(f'{job_num} job(s) started.')

        # wait for jobs
        for job in job_list:
            job.join()

        # collect statistics
        job_stat_list: list[JobStat] = []
        for job in job_list:
            job_stat_list.append(job.stat)

        # merge statistics counters
        job_stat_merged = sum(job_stat_list, JobStat())

        self.logger.info('Finish get archive videos!')
        self.logger.info(job_stat_merged.get_summary())

        # parse archive video to record
        record_list: list[RecordNew] = []
        while not archive_video_queue.empty():
            added, archive_video = archive_video_queue.get()
            record_list.append(RecordNew(
                added=added,
                aid=archive_video.aid,
                bvid=archive_video.bvid.lstrip('BV'),
                view=archive_video.stat.view,
                danmaku=archive_video.stat.danmaku,
                reply=archive_video.stat.reply,
                favorite=archive_video.stat.favorite,
                coin=archive_video.stat.coin,
                share=archive_video.stat.share,
                like=archive_video.stat.like,
                dislike=archive_video.stat.dislike,
                now_rank=archive_video.stat.now_rank,
                his_rank=archive_video.stat.his_rank,
                vt=archive_video.stat.vt,
                vv=archive_video.stat.vv
            ))
        self.logger.info(f'{len(record_list)} record(s) parsed.')

        # remove duplication, then record list -> aid record dict
        aid_record_dict = {}
        for record in record_list:
            aid_record_dict[record.aid] = record
        record_list_after_remove_duplication = list(aid_record_dict.values())
        self.logger.info(
            f'{len(record_list_after_remove_duplication)} record(s) left after remove duplication.')

        # build record queue and return
        record_queue: Queue[RecordNew] = Queue()
        for record in record_list_after_remove_duplication:
            record_queue.put(record)
        return record_queue

    def check_all_zero_record(
            self, record_queue: Queue[RecordNew], service: Service, job_num: int = 50
    ) -> Queue[RecordNew]:
        self.logger.info('Now start checking all zero record...')
        timer = Timer()
        timer.start()

        # write down record_queue length
        record_queue_len = record_queue.qsize()
        self.logger.info(f'Will check {record_queue_len} record(s).')

        # prepare checked record queue
        checked_record_queue: Queue[RecordNew] = Queue()

        # create jobs
        job_list = []
        for i in range(job_num):
            job_list.append(CheckAllZeroRecordJob(
                f'job_{i}', record_queue, checked_record_queue, service))

        # start jobs
        for job in job_list:
            job.start()
        logger.info(f'{job_num} job(s) started.')

        # wait for jobs
        for job in job_list:
            job.join()

        # collect statistics
        job_stat_list: list[JobStat] = []
        for job in job_list:
            job_stat_list.append(job.stat)

        # merge statistics counters with pre-initialized stat
        stat = JobStat()
        stat.condition['all_zero_record'] = 0
        stat.condition['fail_fetch_again'] = 0
        stat.condition['all_zero_record_again'] = 0
        stat.condition['not_all_zero_record'] = 0
        job_stat_merged = sum(job_stat_list, stat)

        # add remaining record to checked record queue
        while not record_queue.empty():
            checked_record_queue.put(record_queue.get())

        # write down checked_record_queue length
        checked_record_queue_len = checked_record_queue.qsize()
        self.logger.info(
            f'Got {checked_record_queue_len} record(s) after check.')
        if record_queue_len != checked_record_queue_len:
            self.logger.error(f'Records number not match after check! '
                              f'before: {record_queue_len}, after: {checked_record_queue_len}')

        # write down records number into stat condition
        job_stat_merged.condition['record_queue_len'] = record_queue_len
        job_stat_merged.condition['checked_record_queue_len'] = checked_record_queue_len

        timer.stop()

        # summary
        self.logger.info('Finish checking all zero record!')
        self.logger.info(timer.get_summary())
        self.logger.info(job_stat_merged.get_summary())
        if job_stat_merged.condition['record_queue_len'] != job_stat_merged.condition['checked_record_queue_len'] \
                or job_stat_merged.condition['not_all_zero_record'] > 0:
            sc_send_summary(
                f'{script_fullname}.check_all_zero_record', timer, job_stat_merged)
        return checked_record_queue

    def process_comprehensive(self):
        service = Service(mode='worker')

        record_queue = self.get_all_c30_video_record_from_newlist_api(service)

        # build aid record dict
        aid_record_dict: dict[int, RecordNew] = {}
        while not record_queue.empty():
            record = record_queue.get()
            aid_record_dict[record.aid] = record

        # get need insert aid list
        session = Session()
        need_insert_aid_list = get_need_insert_aid_list(
            self.time_label, True, session)
        self.logger.info(
            f'{len(need_insert_aid_list)} aid(s) need insert for time label {self.time_label}.')

        # insert records
        # TODO: extract to util funtion start
        self.logger.info('Now start inserting records...')
        # use sql directly, combine 1000 records into one sql to execute and commit
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
            sql += '(%d, %d, %d, %d, %d, %d, %d, %d, %d, %s, %s, %s, %s, %s), ' % (
                record.added, record.aid,
                record.view, record.danmaku, record.reply, record.favorite, record.coin, record.share, record.like,
                null_or_str(record.dislike), null_or_str(
                    record.now_rank), null_or_str(record.his_rank),
                null_or_str(record.vt), null_or_str(record.vv)
            )
            if idx % 1000 == 0:
                sql = sql[:-2]  # remove ending comma and space
                try:
                    session.execute(sql)
                    session.commit()
                except Exception as e:
                    self.logger.error(
                        'Fail to execute sql: %s...%s' % (sql[:100], sql[-100:]))
                    self.logger.error('Exception: %s' % str(e))
                sql = sql_prefix
                if idx % log_gap == 0:
                    self.logger.info('%d / %d done' %
                                     (idx, len(need_insert_aid_list)))
        if sql != sql_prefix:
            sql = sql[:-2]  # remove ending comma and space
            try:
                session.execute(sql)
                session.commit()
            except Exception as e:
                self.logger.error('Fail to execute sql: %s...%s' %
                                  (sql[:100], sql[-100:]))
                self.logger.error('Exception: %s' % str(e))
        self.logger.info('%d / %d done' %
                         (len(need_insert_aid_list), len(need_insert_aid_list)))
        self.logger.info('Finish inserting records! %d records added, %d aids left' % (
            len(need_insert_aid_list), len(need_insert_but_record_not_found_aid_list)))
        # TODO: extract to util funtion end

        # check need insert but not found aid list
        # # these aids should have record in aid_record_dict, but not found at present
        # # possible reasons:
        # # - now video tid != 30
        # # - now video code != 0
        # # - now video state = -4, forward = another video aid
        # # - ...
        # self.logger.info('%d c30 need add but not found aids got' % len(need_insert_but_record_not_found_aid_list))
        # self.logger.info('Now start a branch thread for checking need add but not found aids...')
        # c30_need_add_but_not_found_aids_checker = C30NeedAddButNotFoundAidsChecker(
        #     need_insert_but_record_not_found_aid_list)
        # c30_need_add_but_not_found_aids_checker.start()

        # put need insert but not found aids into queue
        need_insert_but_not_found_aid_queue: Queue[int] = Queue()
        for aid in need_insert_but_record_not_found_aid_list:
            need_insert_but_not_found_aid_queue.put(aid)
        self.logger.info(f'{len(need_insert_but_record_not_found_aid_list)} c30 need insert but not found aids '
                         f'put into queue.')

        # create missing video record queue
        missing_record_queue: Queue[RecordNew] = Queue()

        # create jobs
        check_c30_need_insert_but_not_found_aid_job_num = min(
            200, max(len(need_insert_but_record_not_found_aid_list) // 10, 1))  # [1, 200]
        check_c30_need_insert_but_not_found_aid_job_list = []
        for i in range(check_c30_need_insert_but_not_found_aid_job_num):
            check_c30_need_insert_but_not_found_aid_job_list.append(
                CheckC30NeedInsertButNotFoundAidsJob(
                    f'job_{i}', need_insert_but_not_found_aid_queue, missing_record_queue, service))

        # start jobs
        for job in check_c30_need_insert_but_not_found_aid_job_list:
            job.start()
        logger.info(
            f'{check_c30_need_insert_but_not_found_aid_job_num} job(s) started.')

        # check no need insert records
        # if time label is 04:00, we need to add all video records into tdd_video_record table,
        # therefore need_insert_aid_list contains all c30 aids in db, however, still not cover all records
        # possible reasons:
        # - some video moved into c30
        # - some video code changed to 0
        # - some video has -403 code, awesome api will also get these video, login session not required
        # - ...
        if self.time_label == '04:00':
            no_need_insert_aid_list = list(
                set(aid_record_dict.keys()) - set(need_insert_aid_list))
            self.logger.info('%d c30 no need insert records got' %
                             len(no_need_insert_aid_list))
            self.logger.info(
                'Now start a branch thread for checking need no need insert aids...')
            c30_no_need_insert_aids_checker = C30NoNeedInsertAidsChecker(
                no_need_insert_aid_list)
            c30_no_need_insert_aids_checker.start()

        # wait for jobs
        for job in check_c30_need_insert_but_not_found_aid_job_list:
            job.join()

        # collect statistics
        check_c30_need_insert_but_not_found_aid_job_stat_list: list[JobStat] = [
        ]
        for job in check_c30_need_insert_but_not_found_aid_job_list:
            check_c30_need_insert_but_not_found_aid_job_stat_list.append(
                job.stat)

        # merge statistics counters
        check_c30_need_insert_but_not_found_aid_job_stat_merged = sum(
            check_c30_need_insert_but_not_found_aid_job_stat_list, JobStat())

        self.logger.info(f'Finish check c30 need insert but not found aid!')
        self.logger.info(
            check_c30_need_insert_but_not_found_aid_job_stat_merged.get_summary())
        check_c30_need_insert_but_not_found_aid_timer = Timer()
        check_c30_need_insert_but_not_found_aid_timer.start_ts_ms \
            = check_c30_need_insert_but_not_found_aid_job_stat_merged.start_ts_ms
        check_c30_need_insert_but_not_found_aid_timer.end_ts_ms \
            = check_c30_need_insert_but_not_found_aid_job_stat_merged.end_ts_ms
        sc_send_summary(f'{script_fullname}.', check_c30_need_insert_but_not_found_aid_timer,
                        check_c30_need_insert_but_not_found_aid_job_stat_merged)

        # collect missing video records
        missing_record_list: list[RecordNew] = []
        while not missing_record_queue.empty():
            missing_record_list.append(missing_record_queue.get())

        # insert missing records
        # TODO: extract to util funtion start
        self.logger.info('Now start inserting missing records...')
        # use sql directly, combine 1000 records into one sql to execute and commit
        sql_prefix = 'insert into ' \
                     'tdd_video_record(added, aid, `view`, danmaku, reply, favorite, coin, share, `like`, ' \
                     'dislike, now_rank, his_rank, vt, vv) ' \
                     'values '
        sql = sql_prefix
        log_gap = 1000 * max(1, (len(missing_record_list) // 1000 // 10))
        for idx, record in enumerate(missing_record_list, 1):
            sql += '(%d, %d, %d, %d, %d, %d, %d, %d, %d, %s, %s, %s, %s, %s), ' % (
                record.added, record.aid,
                record.view, record.danmaku, record.reply, record.favorite, record.coin, record.share, record.like,
                null_or_str(record.dislike), null_or_str(
                    record.now_rank), null_or_str(record.his_rank),
                null_or_str(record.vt), null_or_str(record.vv)
            )
            if idx % 1000 == 0:
                sql = sql[:-2]  # remove ending comma and space
                try:
                    session.execute(sql)
                    session.commit()
                except Exception as e:
                    self.logger.error(
                        'Fail to execute sql: %s...%s' % (sql[:100], sql[-100:]))
                    self.logger.error('Exception: %s' % str(e))
                sql = sql_prefix
                if idx % log_gap == 0:
                    self.logger.info('%d / %d done' %
                                     (idx, len(missing_record_list)))
        if sql != sql_prefix:
            sql = sql[:-2]  # remove ending comma and space
            try:
                session.execute(sql)
                session.commit()
            except Exception as e:
                self.logger.error('Fail to execute sql: %s...%s' %
                                  (sql[:100], sql[-100:]))
                self.logger.error('Exception: %s' % str(e))
        self.logger.info('%d / %d done' %
                         (len(missing_record_list), len(missing_record_list)))
        self.logger.info('Finish inserting missing records! %d records added, %d aids left' % (
            len(missing_record_list), len(need_insert_but_record_not_found_aid_list)))
        # TODO: extract to util funtion end

        self.logger.info(
            'c30 video pipeline done! return %d records' % len(aid_record_dict))
        return_record_list = [record for record in aid_record_dict.values()]
        return_record_list.extend(missing_record_list)

        for record in return_record_list:
            self.record_queue.put(record)

        session.close()

    def process_simple(self):
        # get need insert aid list
        session = Session()
        need_insert_aid_list = get_need_insert_aid_list(
            self.time_label, True, session)
        session.close()
        self.logger.info(
            f'{len(need_insert_aid_list)} aid(s) need insert for time label {self.time_label}.')

        service = Service(mode='worker')

        # put aid into queue
        aid_queue: Queue[int] = Queue()
        for aid in need_insert_aid_list:
            aid_queue.put(aid)

        # create video record queue
        video_record_queue: Queue[TddVideoRecord] = Queue()

        # create jobs
        job_num = 50
        job_list = []
        for i in range(job_num):
            job_list.append(AddVideoRecordJob(
                f'job_{i}', aid_queue, video_record_queue, service,
                duration_limit_s=60 * 40  # 40 minutes
            ))

        # start jobs
        for job in job_list:
            job.start()
        logger.info(f'{job_num} job(s) started.')

        # wait for jobs
        for job in job_list:
            job.join()

        # collect statistics
        job_stat_list: list[JobStat] = []
        for job in job_list:
            job_stat_list.append(job.stat)

        # merge statistics counters
        job_stat_merged = sum(job_stat_list, JobStat())

        self.logger.info('Finish add need insert aid list!')
        self.logger.info(job_stat_merged.get_summary())

        # parse tdd video record to record
        record_cnt = 0
        while not video_record_queue.empty():
            video_record = video_record_queue.get()
            self.record_queue.put(RecordNew(
                added=video_record.added,
                aid=video_record.aid,
                bvid=a2b(video_record.aid),
                view=video_record.view,
                danmaku=video_record.danmaku,
                reply=video_record.reply,
                favorite=video_record.favorite,
                coin=video_record.coin,
                share=video_record.share,
                like=video_record.like,
                dislike=video_record.dislike,
                now_rank=video_record.now_rank,
                his_rank=video_record.his_rank,
                vt=video_record.vt,
                vv=video_record.vv,
            ))
            record_cnt += 1
        self.logger.info(f'{record_cnt} record(s) parsed and returned.')

    def run(self):
        self.logger.info('c30 video pipeline start')

        service = Service(mode='worker')

        # get newlist
        try:
            service.get_newlist({'rid': 30, 'pn': 1, 'ps': 50})

            # no error raised, api works fine, go comprehensive process
            self.process_comprehensive()
        except Exception as e:
            self.logger.error(f'Fail to get newlist, very likely api broken. '
                              f'rid: 30, pn: 1, ps: 50, error: {e}')

            # api broken, go simple process
            self.process_simple()


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
        self.logger.info('will save %d records into file %s' %
                         (len(self.records), current_filename_path))
        with open(current_filename_path, 'w') as f:
            f.write('added,aid,bvid,view,danmaku,reply,favorite,coin,share,like\n')
            for idx, record in enumerate(self.records, 1):
                f.write('%d,%d,%s,%d,%d,%d,%d,%d,%d,%d\n' % (
                    record.added, record.aid, record.bvid, record.view, record.danmaku, record.reply, record.favorite,
                    record.coin, record.share, record.like))
                if idx % 20000 == 0:
                    self.logger.info('%d / %d done' % (idx, len(self.records)))
            self.logger.info('%d / %d done' %
                             (len(self.records), len(self.records)))
        self.logger.info('Finish save %d records into file %s!' %
                         (len(self.records), current_filename_path))

        # TODO ugly design, should be separated into another class
        if self.time_label == '23:00':
            try:
                # get today filename prefix
                day_prefix = ts_s_to_str(get_ts_s())[:10]
                day_prefix_path = self.data_folder + day_prefix

                # pack today file
                self.logger.info('pack %s*.csv into %s.tar.gz' %
                                 (day_prefix_path, day_prefix_path))
                pack_result = os.popen(
                    'cd %s && mkdir %s && cp %s*.csv %s && tar -zcvf %s.tar.gz %s && rm -r %s && cd ..' % (
                        self.data_folder, day_prefix, day_prefix, day_prefix, day_prefix, day_prefix, day_prefix
                    )
                )
                for line in pack_result:
                    self.logger.info(line.rstrip('\n'))

                # get 3 day before filename prefix
                day_prefix_3d_before = ts_s_to_str(
                    get_ts_s() - 3 * 24 * 60 * 60)[:10]
                day_prefix_3d_before_path = self.data_folder + day_prefix_3d_before

                # remove 3 day before csv file
                self.logger.info('remove %s*.csv' % day_prefix_3d_before_path)
                pack_result = os.popen('rm %s*.csv' %
                                       day_prefix_3d_before_path)
                for line in pack_result:
                    self.logger.info(line.rstrip('\n'))
            except Exception as e:
                self.logger.error(
                    'Error occur when executing packing files shell scripts. Detail: %s' % e)
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
        self.logger.info('%d / %d done' %
                         (len(self.records), len(self.records)))
        session.close()
        self.logger.info('Finish save %d records into db!' % len(self.records))

        # TODO ugly design, should be separated into another class
        if self.time_label == '23:00':
            try:
                session.execute(
                    'drop table if exists tdd_video_record_hourly_4')
                self.logger.info('drop table tdd_video_record_hourly_4')

                session.execute(
                    'rename table tdd_video_record_hourly_3 to tdd_video_record_hourly_4')
                self.logger.info(
                    'rename table tdd_video_record_hourly_3 to tdd_video_record_hourly_4')

                session.execute(
                    'rename table tdd_video_record_hourly_2 to tdd_video_record_hourly_3')
                self.logger.info(
                    'rename table tdd_video_record_hourly_2 to tdd_video_record_hourly_3')

                session.execute(
                    'rename table tdd_video_record_hourly to tdd_video_record_hourly_2')
                self.logger.info(
                    'rename table tdd_video_record_hourly to tdd_video_record_hourly_2')

                session.execute(
                    'create table tdd_video_record_hourly like tdd_video_record_hourly_2')
                self.logger.info(
                    'create table tdd_video_record_hourly like tdd_video_record_hourly_2')
            except Exception as e:
                session.rollback()
                self.logger.error(
                    'Error occur when executing change tdd_video_record_hourly table. Detail: %s' % e)
            else:
                self.logger.info(
                    'Finish change tdd_video_record_hourly table!')


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
            raise ZeroDivisionError(
                'timespan between two records should not be zero')
        return RecordSpeed(
            start_ts=record_start.added,
            end_ts=record_end.added,
            timespan=timespan,
            per_seconds=per_seconds,
            view=(record_end.view - record_start.view) /
            timespan * per_seconds,
            danmaku=(record_end.danmaku - record_start.danmaku) /
            timespan * per_seconds,
            reply=(record_end.reply - record_start.reply) /
            timespan * per_seconds,
            favorite=(record_end.favorite - record_start.favorite) /
            timespan * per_seconds,
            coin=(record_end.coin - record_start.coin) /
            timespan * per_seconds,
            share=(record_end.share - record_start.share) /
            timespan * per_seconds,
            like=(record_end.like - record_start.like) / timespan * per_seconds
        )

    def _calc_record_speed_ratio(self, record_speed_start, record_speed_end, inf_magic_num=99999999):
        # record_speed_start and record_speed_end should be namedtuple RecordSpeedRatio
        return RecordSpeedRatio(
            view=(record_speed_end.view - record_speed_start.view) /
            record_speed_start.view
            if record_speed_start.view != 0 else inf_magic_num * 1
            if (record_speed_end.view - record_speed_start.view) > 0 else -1,
            danmaku=(record_speed_end.danmaku -
                     record_speed_start.danmaku) / record_speed_start.danmaku
            if record_speed_start.danmaku != 0 else inf_magic_num * 1
            if (record_speed_end.danmaku - record_speed_start.danmaku) > 0 else -1,
            reply=(record_speed_end.reply - record_speed_start.reply) /
            record_speed_start.reply
            if record_speed_start.reply != 0 else inf_magic_num * 1
            if (record_speed_end.reply - record_speed_start.reply) > 0 else -1,
            favorite=(record_speed_end.favorite -
                      record_speed_start.favorite) / record_speed_start.favorite
            if record_speed_start.favorite != 0 else inf_magic_num * 1
            if (record_speed_end.favorite - record_speed_start.favorite) > 0 else -1,
            coin=(record_speed_end.coin - record_speed_start.coin) /
            record_speed_start.coin
            if record_speed_start.coin != 0 else inf_magic_num * 1
            if (record_speed_end.coin - record_speed_start.coin) > 0 else -1,
            share=(record_speed_end.share - record_speed_start.share) /
            record_speed_start.share
            if record_speed_start.share != 0 else inf_magic_num * 1
            if (record_speed_end.share - record_speed_start.share) > 0 else -1,
            like=(record_speed_end.like - record_speed_start.like) /
            record_speed_start.like
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
            # remove current filename to avoid duplicate
            filenames.remove(self.current_filename)
        recent_records_filenames = sorted(
            list(filter(lambda file: re.search(
                r'^\d{4}-\d{2}-\d{2} \d{2}:00\.csv$', file), filenames)),
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
                                        int(line_arr[3]), int(line_arr[4]), int(
                                            line_arr[5]), int(line_arr[6]),
                                        int(line_arr[7]), int(line_arr[8]), int(line_arr[9]))
                        aid_recent_records_dict[record.aid].append(record)
                        file_records += 1
                    except Exception as e:
                        self.logger.warning('Fail to parse line %s into record, exception occurred, detail: %s' % (
                            line, e))
                self.logger.info('%d records loaded from file %s' %
                                 (file_records, filename))
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
        self.logger.info('Finish get valid pubdate from %d videos!' %
                         len(aid_pubdate_dict))

        # check recent records
        self.logger.info('Now check recent records...')
        result_status_dict = defaultdict(list)
        for idx, (aid, records) in enumerate(aid_recent_records_dict.items(), 1):
            # get pubdate from aid_pubdate_dict
            pubdate = aid_pubdate_dict.get(aid, None)
            if pubdate is None:
                self.logger.warning(
                    'Fail to get pubdate of video aid %d, continue' % aid)
                result_status_dict['no_valid_pubdate'].append(aid)
                continue
            pubdate_record = Record(
                pubdate, aid, a2b(aid), 0, 0, 0, 0, 0, 0, 0)

            records.sort(key=lambda r: r.added)  # sort by added

            # TODO should be refactored in the future to support more check logic
            # ensure at least 3 records
            if len(records) <= 2:
                # very common and not harmful to system, set to debug level is enough
                self.logger.debug(
                    'Records len of video aid %d less than 3, continue' % aid)
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
                speed_overall = self._calc_record_speed(
                    pubdate_record, records[-1])
            except ZeroDivisionError:
                self.logger.warning(
                    'Zero timespan between adjacent records of video aid %d detected, continue' % aid)
                result_status_dict['zero_timespan_between_adjacent_records'].append(
                    aid)
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
                        description='unexpected drop detected, speed now of prop %s is %.2f, < -10' % (
                            prop, value)
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
                        speed_now=speed_now[idx2 +
                                            4], speed_last=speed_last[idx2 + 4],
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
                self.logger.info('%d / %d done' %
                                 (idx, len(aid_recent_records_dict)))
        self.logger.info(
            '%d / %d done' % (len(aid_recent_records_dict), len(aid_recent_records_dict)))

        session.close()
        self.logger.info('Finish analyzing recent records! %s' %
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
            session.execute(
                'update tdd_video set recent = 0 where added < %d' % last_7d_ts)
            session.execute('update tdd_video set recent = 1 where added >= %d && added < %d' % (
                last_7d_ts, last_1d_ts))
            session.execute(
                'update tdd_video set recent = 2 where added >= %d' % last_1d_ts)
            session.commit()
            self.logger.info('Finish update recent field!')
        except Exception as e:
            self.logger.info(
                'Fail to update recent field. Exception caught. Detail: %s' % e)
            session.rollback()

    def _update_activity(self, session, active_threshold=1000, hot_threshold=5000):
        self.logger.info('Now start update activity field...')
        try:
            this_week_ts_begin = int(time.mktime(time.strptime(
                str(datetime.date.today()), '%Y-%m-%d'))) + 4 * 60 * 60
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
                    diff_records[aid] = this_week_records[aid] - \
                        last_week_records[aid]
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
                session.execute(
                    'update tdd_video set activity = 1 where aid = %d' % aid)
            for aid in hot_aids:
                session.execute(
                    'update tdd_video set activity = 2 where aid = %d' % aid)
            session.commit()

            self.logger.info('Finish update activity field! %d active videos and %d hot videos set.' % (
                len(active_aids), len(hot_aids)))
        except Exception as e:
            self.logger.info(
                'Fail to update activity field. Exception caught. Detail: %s' % e)
            session.rollback()

    def _update_freq(self, session):
        self.logger.info('Now start update freq field...')
        try:
            session.execute('update tdd_video set freq = 0')
            session.execute('update tdd_video set freq = 1 where activity = 1')
            session.execute(
                'update tdd_video set freq = 2 where activity = 2 || recent = 1')
            session.commit()
            self.logger.info('Finish update freq field!')
        except Exception as e:
            self.logger.info(
                'Fail to update freq field. Exception caught. Detail: %s' % e)
            session.rollback()

    def run(self):
        self.logger.info(
            'Now start updating recent, activity, freq fields of video...')
        session = Session()
        self._update_recent(session)
        if self.time_label == '04:00':
            self._update_activity(session)
        self._update_freq(session)
        session.close()
        self.logger.info(
            'Finish update recent, activity, freq fields of video!')


def run_hourly_video_record_add(time_task):
    time_label = time_task[-5:]  # current time, ex: 19:00
    logger.info(
        'Now start hourly video record add, time label: %s..' % time_label)

    # upstream data acquisition pipeline, c30 and c0 pipeline runner, init -> start -> join -> records
    logger.info('Now start upstream data acquisition pipelines...')

    records_queue: Queue[RecordNew] = Queue()

    c30_runner = C30PipelineRunner(time_label, records_queue)
    c0_runner = C0DataAcquisitionJob(time_label, records_queue)

    c30_runner.start()
    c0_runner.start()

    c30_runner.join()
    c0_runner.join()

    records = []
    while not records_queue.empty():
        records.append(records_queue.get())

    # remove duplicate records
    logger.info('Now check duplicate records...')
    duplicate_records_item_list = list(
        filter(lambda item: item[1] > 1, Counter(
            map(lambda r: r.bvid, records)).items())
    )  # bvid -> count of records of video with this bvid
    if len(duplicate_records_item_list) == 0:
        logger.info('No duplicate records detected!')
    else:
        logger.warning(f'Duplicate records detected! '
                       f'{len(duplicate_records_item_list)} videos with '
                       f'{sum(map(lambda item: item[1], duplicate_records_item_list))} records in total.')
        removed_records_count = 0
        for bvid, count in duplicate_records_item_list:
            logger.warning(f'Video bvid {bvid} have total {count} records.')
            for record in sorted(
                    # records from video with the same bvid
                    filter(lambda r: r.bvid == bvid, records),
                    key=lambda r: r.added  # sorted by added, asc
            )[1:]:  # remain the first one, i.e. the earliest record
                records.remove(record)
                removed_records_count += 1
        logger.warning(
            f'Finish remove duplicate records! Total {removed_records_count} duplicate records removed.')

    logger.info(
        f'Finish upstream data acquisition pipelines! {len(records)} records received')

    # downstream data analysis pipeline
    logger.info('Now start downstream data analysis pipelines...')
    data_analysis_pipeline_runner_list = [
        RecordsSaveToFileRunner(records, time_task),
        RecordsSaveToDbRunner(records, time_label),
        RecentRecordsAnalystRunner(records, time_task),
        RecentActivityFreqUpdateRunner(time_label),
    ]
    for runner in data_analysis_pipeline_runner_list:
        runner.start()
    for runner in data_analysis_pipeline_runner_list:
        runner.join()

    logger.info('Finish downstream data analysis pipelines!')
    del data_analysis_pipeline_runner_list  # release memory


def hourly_video_record_add():
    logger.info(f'Now start {script_fullname}...')
    timer = Timer()
    timer.start()

    # current time task, ex: 2013-01-31 19:00
    time_task = f'{get_ts_s_str()[:13]}:00'
    logger.info(f'Time task: {time_task}')

    try:
        run_hourly_video_record_add(time_task)
    except Exception as e:
        message = f'Exception occurred when running hourly video record add! time task: {time_task}, error: {e}'
        logger.critical(message)
        sc_send_critical(script_fullname, message,
                         __file__, get_current_line_no())
        exit(1)

    timer.stop()

    # summary
    logger.info(f'Finish {script_fullname}!')
    logger.info(f'Time task: {time_task}')
    logger.info(timer.get_summary())


def main():
    hourly_video_record_add()


if __name__ == '__main__':
    # current time task, only number, ex: 201301311900
    time_task_simple = f'{get_ts_s_str()[:13]}:00'.replace(
        '-', '').replace(' ', '').replace(':', '')
    logging_init(file_prefix=f'{script_id}_{time_task_simple}')
    main()
