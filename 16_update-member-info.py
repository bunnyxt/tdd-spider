from db import DBOperation, Session
from util import get_ts_s, ts_s_to_str, get_week_day, format_ts_s, get_ts_ms, format_ts_ms
from threading import Thread
from queue import Queue
from collections import defaultdict
from service import Service
from task import update_member
from common.error import TddError
from serverchan import sc_send
from typing import List
import logging
from logutils import logging_init

logger = logging.getLogger('16')


class UpdateMemberServiceRunner(Thread):
    def __init__(self, name: str, mid_queue: Queue[int], statistics: defaultdict[str, int], service: Service):
        super().__init__()
        self.name = name
        self.mid_queue = mid_queue
        self.statistics = statistics
        self.service = service
        self.session = Session()
        self.logger = logging.getLogger(f'UpdateMemberServiceRunner.{self.name}')

    def run(self):
        self.logger.info(f'Runner start.')
        while not self.mid_queue.empty():
            mid = self.mid_queue.get()
            start_ts_ms = get_ts_ms()
            try:
                tdd_member_logs = update_member(mid, self.service, self.session)
            except TddError as e:
                self.logger.warning(f'Fail to update member info. mid: {mid}, error: {e}')
                self.statistics['tdd_error_count'] += 1
            except Exception as e:
                self.logger.warning(f'Fail to update member info. mid: {mid}, error: {e}')
                self.statistics['other_exception_count'] += 1
            else:
                if len(tdd_member_logs) == 0:
                    self.statistics['no_update_count'] += 1
                else:
                    self.statistics['change_count'] += 1
                for log in tdd_member_logs:
                    self.logger.info(f'Update member info. mid: {mid}, attr: {log.attr}, {log.oldval} -> {log.newval}')
                    self.statistics['change_log_count'] += 1
                self.logger.debug(f'{tdd_member_logs} log(s) found. mid: {mid}')
            end_ts_ms = get_ts_ms()
            cost_ms = end_ts_ms - start_ts_ms
            self.logger.debug(f'Finish update member info. mid: {mid}, cost: {format_ts_ms(cost_ms)}')
            self.statistics['total_count'] += 1
            self.statistics['total_cost_ms'] += cost_ms
        self.session.close()
        self.logger.info(f'Runner end.')


def update_member_info():
    logger.info('Now start update member info...')
    start_ts = get_ts_s()  # get start ts

    session = Session()
    service = Service(mode='worker', retry=20)

    # get all mids
    all_mids: List[int] = DBOperation.query_all_member_mids(session)
    logger.info(f'Total {len(all_mids)} members got.')

    # add latest 1000 mids first
    mids = all_mids[-1000:]

    # TODO: add top 200 follower mids

    # for the rest, add 1 / 7 of them, according to the week day (0-6)
    week_day = get_week_day()
    for idx, mid in enumerate(all_mids[:-1000]):
        if idx % 7 == week_day:
            mids.append(mid)

    logger.info(f'Will update {len(mids)} videos info.')

    # put mid into queue
    mid_queue: Queue[int] = Queue()
    for mid in mids:
        mid_queue.put(mid)
    logger.info(f'{mid_queue.qsize()} mids put into queue.')

    # prepare statistics
    statistics: defaultdict[str, int] = defaultdict(int)

    # create service runner
    service_runner_num = 20
    service_runner_list = []
    for i in range(service_runner_num):
        service_runner = UpdateMemberServiceRunner(f'runner_{i}', mid_queue, statistics, service)
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
        'update member info done!\n\n' \
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

    logger.info('Finish update member info!')
    logger.warning(summary)

    # send sc
    sc_send('Finish update member info!', summary)

    session.close()


def main():
    update_member_info()


if __name__ == '__main__':
    logging_init(file_prefix='16')
    main()
