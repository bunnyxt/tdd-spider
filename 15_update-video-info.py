from db import DBOperation, Session
from service import Service
from common.error import TddError
from task import update_video
from serverchan import sc_send
from util import get_ts_s, ts_s_to_str, get_week_day, b2a, format_ts_s, get_ts_ms, format_ts_ms
from threading import Thread
from queue import Queue
from collections import defaultdict
from typing import List
from logutils import logging_init
import logging

logger = logging.getLogger('15')


class UpdateVideoServiceRunner(Thread):
    def __init__(self, name: str, bvid_queue: Queue[str], statistics: defaultdict[str, int], service: Service):
        super().__init__()
        self.name = name
        self.bvid_queue = bvid_queue
        self.statistics = statistics
        self.service = service
        self.session = Session()
        self.logger = logging.getLogger(f'UpdateVideoServiceRunner.{self.name}')

    def run(self):
        self.logger.info(f'Runner start.')
        while not self.bvid_queue.empty():
            bvid = self.bvid_queue.get()
            start_ts_ms = get_ts_ms()
            try:
                tdd_video_logs = update_video(b2a(bvid), self.service, self.session)
            except TddError as e:
                logger.warning(f'Fail to update video info. bvid: {bvid}, error: {e}')
                self.statistics['tdd_error_count'] += 1
            except Exception as e:
                logger.warning(f'Fail to update video info. bvid: {bvid}, error: {e}')
                self.statistics['other_exception_count'] += 1
            else:
                if len(tdd_video_logs) == 0:
                    self.statistics['no_update_count'] += 1
                else:
                    self.statistics['change_count'] += 1
                for log in tdd_video_logs:
                    logger.info(f'Update video info. bvid: {bvid}, attr: {log.attr}, {log.oldval} -> {log.newval}')
                    self.statistics['change_log_count'] += 1
                logger.debug(f'{tdd_video_logs} log(s) found. bvid: {bvid}')
            end_ts_ms = get_ts_ms()
            cost_ms = end_ts_ms - start_ts_ms
            self.logger.debug(f'Finish update video info. bvid: {bvid}, cost: {format_ts_ms(cost_ms)}')
            self.statistics['total_count'] += 1
            self.statistics['total_cost_ms'] += cost_ms
        self.session.close()
        self.logger.info(f'Runner end.')


def update_video_info():
    logger.info('Now start update video info...')
    start_ts = get_ts_s()  # get start ts

    session = Session()
    service = Service(mode='worker')

    # get all bvids
    all_bvids: List[str] = DBOperation.query_all_video_bvids(session)
    logger.info(f'Total {len(all_bvids)} videos got.')

    # add latest 5000 bvids first
    bvids = all_bvids[-5000:]

    # TODO: add top 1000 view bvids

    # for the rest, add 1 / 7 of them, according to the week day (0-6)
    week_day = get_week_day()
    for idx, bvid in enumerate(all_bvids[:-5000]):
        if idx % 7 == week_day:
            bvids.append(bvid)

    logger.info(f'Will update {len(bvids)} videos info.')

    # put mid into queue
    bvid_queue: Queue[str] = Queue()
    for bvid in bvids:
        bvid_queue.put(bvid)
    logger.info(f'{bvid_queue.qsize()} bvids put into queue.')

    # prepare statistics
    statistics: defaultdict[str, int] = defaultdict(int)

    # create service runner
    service_runner_num = 20
    service_runner_list = []
    for i in range(service_runner_num):
        service_runner = UpdateVideoServiceRunner(f'runner_{i}', bvid_queue, statistics, service)
        service_runner_list.append(service_runner)

    # start service runner
    for service_runner in service_runner_list:
        service_runner.start()
    logger.info(f'{service_runner_num} service runner started.')

    # wait for service runner
    for service_runner in service_runner_list:
        service_runner.join()

    # get end ts
    end_ts = get_ts_s()

    # make summary
    summary = \
        'update video info done!\n\n' \
        f'start: {ts_s_to_str(start_ts)}, ' \
        f'end: {ts_s_to_str(end_ts)}, ' \
        f'cost: {format_ts_s(end_ts - start_ts)}\n\n' \
        f'total count: {statistics["total_count"]}, ' + \
        f'average cost per service: {format_ts_ms(statistics["total_cost_ms"] // statistics["total_count"])}\n\n' \
        f'tdd error count: {statistics["tdd_error_count"]}\n\n' \
        f'other exception count: {statistics["other_exception_count"]}\n\n' \
        f'no update count: {statistics["no_update_count"]}\n\n' \
        f'change count: {statistics["change_count"]}\n\n' \
        f'change log count: {statistics["change_log_count"]}\n\n' \
        f'by.bunnyxt, {ts_s_to_str(get_ts_s())}'

    logger.info('Finish update video info!')
    logger.warning(summary)

    # send sc
    sc_send('Finish update video info!', summary)

    session.close()


def main():
    update_video_info()


if __name__ == '__main__':
    logging_init(file_prefix='15')
    main()
