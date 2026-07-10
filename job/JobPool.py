import time
import logging
from collections import Counter
from threading import Thread, Event
from typing import Optional
from .Job import Job
from .JobStat import JobStat

__all__ = ['JobPool']


class JobPool:
    """
    Run a group of Jobs that share the same work, encapsulating the repeated
    start -> (optional progress heartbeat) -> join -> merge-stats lifecycle.

    Progress is a pool-level concern (a single Job knows neither the total work
    nor its siblings), so it lives here and is derived from each job's own
    `stat.total_count`. When enabled it emits one greppable line per interval:

        PROGRESS <label>: <done> done, <left> left (<pct>%), <rate>/s

    Use start() then join() (rather than a single blocking call) so the caller
    can run other work concurrently between them.
    """

    def __init__(self, jobs: list[Job], *,
                 progress_label: str,
                 progress_total: Optional[int] = None,
                 progress_interval_s: float = 1.0,
                 progress_show_conditions: bool = True,
                 ensure_conditions: Optional[list[str]] = None,
                 logger_name: str = 'JobPool'):
        # progress_label is required (keyword-only, no default) so every pool's
        # heartbeat is uniquely identifiable when several run concurrently.
        self.jobs = jobs
        self.progress_label = progress_label
        self.progress_total = progress_total
        self.progress_interval_s = progress_interval_s
        self.progress_show_conditions = progress_show_conditions
        # condition keys that should always appear in the merged summary (at 0
        # if never hit). Needed because Counter '+' drops zero counts, so a key
        # that no job incremented would otherwise vanish from get_summary().
        self.ensure_conditions = ensure_conditions or []
        self.logger = logging.getLogger(logger_name)
        self._stop_event = Event()
        self._progress_thread: Optional[Thread] = None

    def start(self) -> 'JobPool':
        for job in self.jobs:
            job.start()
        self.logger.info(f'{len(self.jobs)} job(s) started.')
        if self.progress_interval_s and self.progress_interval_s > 0:
            self._progress_thread = Thread(target=self._report_progress, daemon=True)
            self._progress_thread.start()
        return self

    def join(self) -> JobStat:
        for job in self.jobs:
            job.join()
        # stop and drain the heartbeat so a final line reflects the end state
        self._stop_event.set()
        if self._progress_thread is not None:
            self._progress_thread.join()
        merged = sum((job.stat for job in self.jobs), JobStat())
        # seed declared keys so they always show (direct assignment keeps a 0,
        # which Counter '+' would have dropped); never overwrites a real count.
        for key in self.ensure_conditions:
            if key not in merged.condition:
                merged.condition[key] = 0
        return merged

    def _report_progress(self):
        last_done = 0
        last_ts = time.time()
        while True:
            # wait() returns True if stopped, False on interval timeout; this
            # keeps a steady cadence and still emits one final line on stop.
            stopped = self._stop_event.wait(self.progress_interval_s)
            done = sum(job.stat.total_count for job in self.jobs)
            now = time.time()
            rate = (done - last_done) / max(0.001, now - last_ts)
            if self.progress_total:
                left = self.progress_total - done
                pct = done / self.progress_total * 100
                msg = (f'PROGRESS {self.progress_label}: {done} done, {left} left '
                       f'({pct:.1f}%), {rate:.0f}/s')
            else:
                msg = f'PROGRESS {self.progress_label}: {done} done, {rate:.0f}/s'
            if self.progress_show_conditions:
                # live breakdown of the jobs' own condition counters, e.g.
                # "missing_video_record_get: 11000, update_exception: 1300"
                conditions = sum((job.stat.condition for job in self.jobs), Counter())
                if conditions:
                    cond_str = ', '.join(
                        f'{k}: {v}' for k, v in conditions.most_common())
                    msg = f'{msg} | {cond_str}'
            self.logger.info(msg)
            last_done, last_ts = done, now
            if stopped:
                break
