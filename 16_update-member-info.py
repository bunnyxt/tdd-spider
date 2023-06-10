from db import DBOperation, Session
from util import get_ts_s, ts_s_to_str, format_ts_s
from threading import Thread
from queue import Queue
from collections import defaultdict
from service import Service
from task import update_member
from common.error import TddError
from serverchan import sc_send
import logging
from logutils import logging_init

logger = logging.getLogger('16')


class UpdateMemberServiceRunner(Thread):
    def __init__(self, name, mid_queue, statistics, service):
        super().__init__()
        self.name = name
        self.mid_queue = mid_queue
        self.statistics = statistics
        self.service = service
        self.session = Session()
        self.logger = logging.getLogger('UpdateMemberServiceRunner')

    def run(self):
        self.logger.info(f'runner {self.name}, start')
        while not self.mid_queue.empty():
            mid = self.mid_queue.get()
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
                self.logger.debug(f'Finish update member info. mid: {mid}')
        self.session.close()
        self.logger.info(f'runner {self.name}, end')


def update_member_info():
    logger.info('Now start update member info...')
    start_ts = get_ts_s()  # get start ts

    service = Service(mode='worker', retry=20)
    session = Session()

    # get all mids
    mids = DBOperation.query_all_member_mids(session)
    mids = mids[0:100]
    logger.info(f'{len(mids)} mids got')

    # put mid into queue
    mid_queue = Queue()
    for mid in mids:
        mid_queue.put(mid)
    logger.info(f'{mid_queue.qsize()} mids put into queue')

    # create service runner
    statistics = defaultdict(int)
    service_runner_num = 10
    service_runner_list = []
    for i in range(service_runner_num):
        service_runner = UpdateMemberServiceRunner(f'runner_{i}', mid_queue, statistics, service)
        service_runner_list.append(service_runner)

    # start service runner
    for service_runner in service_runner_list:
        service_runner.start()
    logger.info(f'{service_runner_num} service runner started')

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
        f'total count: {len(mids)}\n\n' + \
        f'tdd error count: {statistics["tdd_error_count"]}\n\n' \
        f'other exception count: {statistics["other_exception_count"]}\n\n' \
        f'no update count: {statistics["no_update_count"]}\n\n' \
        f'change count: {statistics["change_count"]}\n\n' \
        f'change log count: {statistics["change_log_count"]}\n\n' \
        f'by.bunnyxt, {ts_s_to_str(get_ts_s())}'

    logger.info('Finish update member info!')
    logger.warning(summary)

    # send sc
    sc_result = sc_send('Finish update member info!', summary)
    if sc_result['errno'] == 0:
        logger.info('Sc summary sent: succeed!')
    else:
        logger.warning(f'Sc summary sent: failed! sc_result = {sc_result}.')

    session.close()


def main():
    update_member_info()


if __name__ == '__main__':
    logging_init(file_prefix='16')
    main()
