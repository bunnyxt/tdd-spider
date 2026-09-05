from collections import Counter, deque
import logging
import time
from threading import Lock
from typing import Optional

logger = logging.getLogger('Service')

__all__ = ['RequestStats']

# A rate-limit storm can hit thousands of times a minute. Log the first hit of
# each (target, worker, reason) immediately, then at most one line per this
# interval, carrying the number of hits suppressed in between -- enough to see
# when it started, that it is ongoing, and how hard, without flooding the log.
RATE_LIMIT_LOG_INTERVAL_S = 60.0

# Bounded so a long run cannot grow this without limit. Only transitions and
# throttled samples are appended, not every hit.
RATE_LIMIT_EVENT_LIMIT = 500


class RequestStats:
    """
    Thread-safe request counters shared by every job through one Service.

    Two levels are counted, and they do not match one-for-one:

    - *calls*   -- one per `Service._get()`, i.e. per logical API request
    - *attempts*-- one per HTTP request actually sent, so a call that retried
                   five times contributes one call and five attempts

    Both are keyed by target (`get_member_card`, `get_video_view_trimmed`, ...)
    so a run can be read as "which API did we hit, how many times, and how did
    those attempts end".
    """

    def __init__(self, clock=time.monotonic):
        self._clock = clock
        self._lock = Lock()
        self._calls: Counter = Counter()
        self._attempts: Counter = Counter()
        self._rate_limit_events: deque = deque(maxlen=RATE_LIMIT_EVENT_LIMIT)
        self._rate_limit_log: dict = {}

    def record_call(self, target: str, outcome: str) -> None:
        with self._lock:
            self._calls[(target, outcome)] += 1

    def record_attempt(self, target: str, worker_id: Optional[str], outcome: str) -> None:
        with self._lock:
            self._attempts[(target, worker_id or 'direct', outcome)] += 1

    def record_rate_limit(self, target: str, worker_id: Optional[str], *,
                          reason: str, action: str) -> tuple[bool, int]:
        """
        Record one rate-limit hit. Returns (should_log, suppressed_since_last):
        `should_log` is True for the first hit of this (target, worker, reason)
        and then once per RATE_LIMIT_LOG_INTERVAL_S; `suppressed_since_last` is
        how many hits were swallowed since the previous logged one.
        """
        key = (target, worker_id or 'direct', reason)
        now = self._clock()
        with self._lock:
            last_logged_at, suppressed = self._rate_limit_log.get(key, (None, 0))
            should_log = (last_logged_at is None
                          or now - last_logged_at >= RATE_LIMIT_LOG_INTERVAL_S)
            if should_log:
                self._rate_limit_log[key] = (now, 0)
                self._rate_limit_events.append(
                    (time.time(), target, key[1], reason, action, suppressed))
            else:
                self._rate_limit_log[key] = (last_logged_at, suppressed + 1)
        return should_log, suppressed

    def snapshot(self) -> dict:
        with self._lock:
            return {
                'calls': dict(self._calls),
                'attempts': dict(self._attempts),
                'rate_limit_events': list(self._rate_limit_events),
            }

    def format_lines(self) -> list[str]:
        """
        Greppable one-line-per-target summary, e.g.

            REQSTAT calls get_member_card: total=1200 ok=900 rate_limited=300
            REQSTAT attempts get_member_card [gmc:0]: total=800 ok=600 rate_limited=200
        """
        snapshot = self.snapshot()
        lines = []

        by_target: dict[str, Counter] = {}
        for (target, outcome), count in snapshot['calls'].items():
            by_target.setdefault(target, Counter())[outcome] += count
        for target in sorted(by_target):
            outcomes = by_target[target]
            lines.append(f'REQSTAT calls {target}: total={sum(outcomes.values())} '
                         + ' '.join(f'{k}={v}' for k, v in sorted(outcomes.items())))

        by_worker: dict[tuple[str, str], Counter] = {}
        for (target, worker_id, outcome), count in snapshot['attempts'].items():
            by_worker.setdefault((target, worker_id), Counter())[outcome] += count
        for target, worker_id in sorted(by_worker):
            outcomes = by_worker[(target, worker_id)]
            lines.append(f'REQSTAT attempts {target} [{worker_id}]: '
                         f'total={sum(outcomes.values())} '
                         + ' '.join(f'{k}={v}' for k, v in sorted(outcomes.items())))
        return lines

    def log_summary(self, label: str = '') -> None:
        suffix = f' ({label})' if label else ''
        lines = self.format_lines()
        if not lines:
            logger.info(f'REQSTAT{suffix}: no requests recorded')
            return
        logger.info(f'REQSTAT{suffix}: {len(lines)} line(s)')
        for line in lines:
            logger.info(line)
