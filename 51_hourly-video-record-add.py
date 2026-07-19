from db import Session, DBOperation, TddVideoRecordAbnormalChange
from threading import Thread
from queue import Queue
from util import get_ts_s, get_ts_s_str, a2b, is_all_zero_record, \
    str_to_ts_s, ts_s_to_str, logging_init, fullname, get_current_line_no, \
    SysStatLogger
import time
import datetime
import os
import re
import sys
from serverchan import sc_send_critical
from collections import namedtuple, defaultdict, Counter
from typing import Optional
from core import RecordNew
from service import Service
from job import FetchVideoRecordJob, BatchInsertVideoRecordJob, UpdateVideoJob, Job, JobPool
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


def get_need_insert_aid_list(time_label, session):
    # all in-scope videos (tdd_video, code == 0, state == 0); no tid filter --
    # c0/c30 were merged, the tid == 30 partition no longer means anything.
    if time_label == '04:00':
        # return total (daily full scan)
        return DBOperation.query_all_update_video_aids(session)

    # add 1 hour aids
    aid_list = DBOperation.query_freq_update_video_aids(2, session)  # freq = 2

    if time_label in ['00:00', '04:00', '08:00', '12:00', '16:00', '20:00']:
        # add 4 hour aids
        # freq = 1
        aid_list += DBOperation.query_freq_update_video_aids(1, session)

    return aid_list


def _stamp(time_task: str) -> str:
    # '2026-07-14 08:00' -> '202607140800'. Same convention as the log files
    # (51_202607140800_INFO.log): digits only, no '-', ' ' or ':'. Keeps the
    # date so runs on different days never append into the same file.
    return time_task.replace('-', '').replace(' ', '').replace(':', '')


def fetch_and_batch_insert_records(
        need_insert_aid_list: list, record_queue: Queue, *,
        job_num: int, fetch_label: str, writer_label: str, logger_name: str,
        duration_limit_s=60 * 40,
        fetched_queue_maxsize: int = 20000,
        recovery_path: Optional[str] = None,
        update_job_num: int = 10, update_label: str = 'video-update'):
    """
    Bulk fetch-then-batch-insert pipeline shared by the C0 and C30 (simple)
    acquisition paths:

        aid_queue -> FetchVideoRecordJob x job_num (HTTP only, ZERO DB)
                  |
                  +-- ok        -> fetched_record_queue
                  |                 -> BatchInsertVideoRecordJob x 1
                  |                    (multi-row INSERT, one commit per batch)
                  |                    -> record_queue (downstream)
                  |
                  +-- CodeError -> code_error_aid_queue
                                    -> UpdateVideoJob x update_job_num (DB)

    A single writer removes the per-record commit/fsync contention that made
    db cost ~157ms/record when 150+ workers each committed individually.

    The update pool runs CONCURRENTLY with the fetch pool (so it does not extend
    the run) and carries the SAME duration_limit_s, so the whole upstream stage
    is hard-capped. Refreshing tdd_video.code for a dead aid is what drops it out
    of future need_insert lists, so it is load-bearing -- but it is DB work, and
    doing it inline on a fetch worker let all 250 fetchers hit the DB at once,
    which deadlocked the 2026-07-15 04:00 full scan.

    Returns (fetch_stat, writer_stat, update_stat) merged JobStats.
    """
    log = logging.getLogger(logger_name)
    # pool_maxsize MUST cover job_num: every worker hits the same worker host,
    # and urllib3 caps connections per host at pool_maxsize. Exceed it and the
    # surplus connections are opened-then-discarded -- a TLS handshake per
    # request (~5KB out vs ~500B), which would multiply outbound ~10x on a box
    # whose ~3Mbps upload is already the throughput ceiling. Derive it from
    # job_num so bumping workers can't silently break keep-alive.
    service = Service(mode='worker', pool_maxsize=job_num + 32)

    # put aid into queue
    aid_queue: Queue[int] = Queue()
    for aid in need_insert_aid_list:
        aid_queue.put(aid)

    # Fetched records flow through here to the single batch writer. BOUNDED:
    # fetchers now outrun the writer (~537/s vs ~365/s), so an unbounded queue
    # just absorbs the backlog into RSS and hides it. A cap turns that into
    # backpressure -- fetchers block on put() at writer speed, which is visible
    # in the PROGRESS rate instead of a silent memory climb.
    fetched_record_queue: Queue = Queue(maxsize=fetched_queue_maxsize)

    # CodeError aids (deleted / hidden / -403) go here for the update pool.
    # Unbounded on purpose: it must never exert backpressure on a fetcher, and
    # it stays tiny (~465 aids across a 1M-aid full scan).
    code_error_aid_queue: Queue = Queue()

    # ensure_conditions: seed the keys that matter with 0 so they ALWAYS show in
    # the summary. A Counter omits a key it never saw, so without this you cannot
    # tell "nothing failed" from "the counter does not exist" -- exactly the
    # distinction you need when scanning a log for data loss.
    fetch_pool = JobPool(
        [FetchVideoRecordJob(f'job_{i}', aid_queue, fetched_record_queue, service,
                             code_error_aid_queue=code_error_aid_queue,
                             duration_limit_s=duration_limit_s)
         for i in range(job_num)],
        progress_total=len(need_insert_aid_list),
        progress_label=fetch_label,
        ensure_conditions=['success', 'code_error', 'other_exception',
                           'record_dropped_queue_full', 'duration_limit_reached'],
        logger_name=logger_name)
    writer_pool = JobPool(
        [BatchInsertVideoRecordJob('writer_0', fetched_record_queue, record_queue,
                                   recovery_path=recovery_path)],
        progress_total=len(need_insert_aid_list),
        progress_label=writer_label,
        progress_interval_s=5.0,  # writer progress is less chatty
        ensure_conditions=['batch_insert', 'batch_insert_split', 'batch_insert_split_ok',
                           'single_insert_retry_ok', 'batch_insert_fail'],
        logger_name=logger_name)
    # bounded DB concurrency: update_job_num connections, not job_num
    update_pool = JobPool(
        [UpdateVideoJob(f'updater_{i}', code_error_aid_queue, service,
                        duration_limit_s=duration_limit_s)
         for i in range(update_job_num)],
        progress_total=None,  # total is unknown until fetching is done
        progress_label=update_label,
        progress_interval_s=5.0,
        ensure_conditions=['update_exception', 'duration_limit_reached'],
        logger_name=logger_name)

    fetch_pool.start()
    writer_pool.start()
    update_pool.start()

    fetch_stat = fetch_pool.join()
    # one sentinel per consumer, AFTER all fetch workers finished (FIFO
    # guarantees each sentinel arrives behind every item that worker could take)
    fetched_record_queue.put(None)
    for _ in range(update_job_num):
        code_error_aid_queue.put(None)
    writer_stat = writer_pool.join()
    update_stat = update_pool.join()

    # label each summary: a run emits three of these and they are otherwise
    # indistinguishable in the log
    log.info(fetch_stat.get_summary(fetch_label))
    log.info(writer_stat.get_summary(writer_label))
    log.info(update_stat.get_summary(update_label))
    log.info(f'{writer_stat.total_count} record(s) fetched, batch inserted and returned.')
    log.info(f'{update_stat.total_count} code-error video(s) updated.')
    return fetch_stat, writer_stat, update_stat


class VideoRecordAcquisitionJob(Job):
    """
    Single acquisition pipeline over ALL in-scope videos (tdd_video, code == 0,
    state == 0). Formerly split into a c0 (tid != 30) and a c30 (tid == 30)
    runner, back when tid == 30 was Bilibili's VOCALOID-UTAU category and the
    c30 half could be bulk-read from the newlist api. Bilibili has retired tid
    as a user-selectable publish category (it is now AI-assigned), so the
    partition tracks nothing meaningful -- videos drift between the two buckets
    on every update_video -- and the newlist bulk path had been dead for weeks
    (see git history for process_comprehensive et al., removed here). One aid
    set, one worker pool.

    job_num=300: matches the total concurrency the old split ran (250 c30 + 50
    c0), which cleared the ~1.09M full scan in ~32 min. The first merged runs at
    250 were a consistent ~35 min (~558/s) -- outbound (~3Mbps, request headers
    + ACKs) is near saturation but not fully capped, so the extra 50 workers buy
    back those ~3 min and, more usefully, headroom under the 40-min cap. A single
    batch writer still keeps up comfortably (measured ~1000+ rec/s capacity vs
    the ~560/s network-capped arrival), so no writer parallelism is needed.
    """

    def __init__(self, time_task: str, record_queue: Queue[RecordNew]):
        super().__init__('acquisition')
        self.time_task = time_task      # full stamp, ex: '2026-07-14 08:00'
        self.time_label = time_task[-5:]  # hour only, ex: '08:00'
        self.record_queue = record_queue

    def process(self):
        session = Session()
        need_insert_aid_list = get_need_insert_aid_list(self.time_label, session)
        session.close()
        self.logger.info(
            f'{len(need_insert_aid_list)} aid(s) need insert for time label {self.time_label}.')

        fetch_and_batch_insert_records(
            need_insert_aid_list, self.record_queue,
            job_num=300,
            fetch_label='record-fetch',
            writer_label='record-db-writer',
            logger_name='VideoRecordAcquisitionJob',
            duration_limit_s=60 * 40,  # 40 minutes, caps the 04:00 full scan
            recovery_path=f'data/record_recovery_{_stamp(self.time_task)}.csv',
            update_job_num=10, update_label='record-video-update')

        self.logger.info('Finish add need insert aid list!')

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

    # upstream data acquisition: one merged pipeline over all in-scope videos
    logger.info('Now start upstream data acquisition pipeline...')

    records_queue: Queue[RecordNew] = Queue()

    acquisition_runner = VideoRecordAcquisitionJob(time_task, records_queue)
    acquisition_runner.start()
    acquisition_runner.join()

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

    # --debug: also write a {prefix}_DEBUG.log with everything (per-aid TIMING,
    # per-request REQUEST, SYSSTAT samples) for offline bottleneck analysis
    debug = '--debug' in sys.argv
    logging_init(file_prefix=f'{script_id}_{time_task_simple}', debug=debug)
    if debug:
        logger.info('Debug logging enabled, writing to '
                    f'{script_id}_{time_task_simple}_DEBUG.log')
        SysStatLogger().start()  # daemon, samples net/load/mem every 10s

    main()
