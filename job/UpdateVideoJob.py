from .Job import Job
from db import Session
from service import Service
from queue import Queue, Empty
from task import update_video
from timer import Timer
from util import format_ts_ms, get_ts_s, ts_s_to_str
from typing import Optional

__all__ = ['UpdateVideoJob']


class UpdateVideoJob(Job):
    """
    Refresh tdd_video metadata for aids drained from aid_queue.

    Two producers use this:
    - 15_update-video-info.py: pre-fills the queue, then puts one sentinel per
      worker.
    - 51_hourly-video-record-add.py: FetchVideoRecordJob pushes an aid here
      whenever the view api returns a CodeError (deleted / hidden / -403 video).
      Writing tdd_video.code is what drops that aid out of future need_insert
      lists (they filter code == 0), so this is load-bearing, not just
      housekeeping. Running it HERE rather than inline in the fetcher keeps
      fetchers 100% DB-free and caps DB concurrency from this path at the
      worker count instead of letting all 250 fetchers hit the DB at will --
      that unbounded coupling is what deadlocked the 2026-07-15 04:00 scan.

    Terminates on a None sentinel, so it can run CONCURRENTLY with a producer
    (an empty queue means "wait", not "done"). Producers must put one None per
    worker after they finish; being FIFO, each sentinel arrives behind every
    aid. `duration_limit_s` is a hard stop so this can never push the run past
    its window.
    """

    def __init__(self, name: str, aid_queue: 'Queue[Optional[int]]', service: Service,
                 duration_limit_s: Optional[int] = None, poll_timeout_s: float = 1.0):
        super().__init__(name)
        self.aid_queue = aid_queue
        self.service = service
        self.duration_limit_s = duration_limit_s
        self.duration_limit_due_ts_s = None
        self.poll_timeout_s = poll_timeout_s
        self.session = Session()

    def process(self):
        if self.duration_limit_s is not None:
            self.duration_limit_due_ts_s = get_ts_s() + self.duration_limit_s
            self.logger.info(f'Duration limit due at {ts_s_to_str(self.duration_limit_due_ts_s)}.')

        while True:
            if (self.duration_limit_due_ts_s is not None
                    and get_ts_s() >= self.duration_limit_due_ts_s):
                self.logger.info(f'Duration limit reached. Now exit. '
                                 f'{self.aid_queue.qsize()} aid(s) left unprocessed.')
                self.stat.condition['duration_limit_reached'] += 1
                break

            # blocking get with timeout, NOT `while not empty(): get()`: the
            # latter races -- two workers both see a non-empty queue, both call
            # get(), and the loser blocks forever on the last item. A timeout
            # also lets us re-check the duration limit while idle.
            try:
                aid = self.aid_queue.get(timeout=self.poll_timeout_s)
            except Empty:
                continue  # producer may still be running; sentinel ends us

            if aid is None:  # sentinel: producer finished
                break

            self.logger.debug(f'Now start update video info. aid: {aid}')
            timer = Timer()
            timer.start()

            try:
                tdd_video_logs = update_video(aid, self.service, self.session)
            except Exception as e:
                # roll back, else the failed transaction poisons this session
                # and every subsequent aid on this worker fails too
                try:
                    self.session.rollback()
                except Exception:
                    pass
                self.logger.error(f'Fail to update video info. aid: {aid}, error: {e}')
                self.stat.condition['update_exception'] += 1
            else:
                for log in tdd_video_logs:
                    self.logger.info(f'Update video info. aid: {aid}, '
                                     f'attr: {log.attr}, {log.oldval} -> {log.newval}')
                self.logger.debug(f'{len(tdd_video_logs)} log(s) found. aid: {aid}')
                self.stat.condition[f'{len(tdd_video_logs)}_update'] += 1

            timer.stop()
            self.logger.debug(f'Finish update video info. '
                              f'aid: {aid}, duration: {format_ts_ms(timer.get_duration_ms())}')
            self.stat.total_count += 1
            self.stat.total_duration_ms += timer.get_duration_ms()

    def cleanup(self):
        self.session.close()
