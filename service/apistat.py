"""
Always-on aggregation of the per-attempt outcome `Service._get` already
classifies, so `p`, retry recovery and the shape of a run are readable
without --debug (150 MB+ per hourly run in production).

Dimensions kept: (target, worker, trial, outcome), in fixed-length windows.
Deliberately not kept: aid/mid or any per-request identifier, User-Agent,
URLs, query strings, headers, bodies. Memory and log size are bounded by the
number of distinct combinations seen, never by request volume.
"""

import logging
import threading
import time
from collections import Counter
from typing import Callable, Dict, List, Optional, Tuple

from util import format_ts_s

__all__ = ['ApiStatTracker', 'NullApiStat']

logger = logging.getLogger('apistat')

DEFAULT_WINDOW_S = 5.0
OK = 'ok'

# fixed 0..1 scale, not autoscaled: two runs are only comparable if the
# vertical scale means the same thing in both
_TICKS = '▁▂▃▄▅▆▇█'
_GAP = '·'


def _sparkline(fractions: List[Optional[float]]) -> str:
    out = []
    for value in fractions:
        if value is None:
            out.append(_GAP)
        else:
            value = min(1.0, max(0.0, value))
            out.append(_TICKS[int(round(value * (len(_TICKS) - 1)))])
    return ''.join(out)


def _derive(trials: Dict[int, Counter]) -> dict:
    """p (first-attempt rejection rate) and how many calls a retry saved."""
    trial1 = trials.get(1, Counter())
    n1 = sum(trial1.values())
    ok1 = trial1.get(OK, 0)
    return {
        'p': (n1 - ok1) / n1 if n1 else None,
        'n1': n1,
        'retry_recovered': sum(c.get(OK, 0) for t, c in trials.items() if t != 1),
    }


def _exhausted(trials: Dict[int, Counter]) -> int:
    """
    How many calls ran out of retries, from the counts alone. Every call that
    fails trial N either makes a trial N+1 attempt or gives up, so
    `failures(N) - attempts(N+1)` is the number that gave up at N. That holds
    whatever `retry` each call was given, unlike reading the last trial's
    failures. Only valid per target: a retry may pick a different worker, so
    the chain does not hold within one worker's counts.
    """
    total = 0
    for trial, outcomes in trials.items():
        failures = sum(v for k, v in outcomes.items() if k != OK)
        total += failures - sum(trials.get(trial + 1, Counter()).values())
    return total


class NullApiStat:
    """Stand-in so callers never have to test `stats is not None`."""

    def record(self, target, worker, trial, outcome, now=None):
        pass

    def totals(self) -> List[dict]:
        return []

    def log_summary(self, log=None) -> None:
        pass


class ApiStatTracker:
    """
    Thread-safe; one instance lives as long as the Service that owns it and is
    shared by every thread that Service serves.
    """

    def __init__(self, window_s: float = DEFAULT_WINDOW_S,
                 clock: Callable[[], float] = time.monotonic):
        self._window_s = window_s
        self._clock = clock
        self._lock = threading.Lock()
        self._run_start = clock()
        self._index = 0
        self._window: Counter = Counter()
        self._totals: Counter = Counter()
        # (window index) -> {(target, worker): [trial-1 attempts, trial-1 ok]}
        self._series: List[Tuple[int, Dict[Tuple[str, str], List[int]]]] = []

    def record(self, target: str, worker: str, trial: int, outcome: str,
               now: Optional[float] = None) -> None:
        """One HTTP attempt. Called once per `_get` loop iteration, not once
        per logical call -- that is what makes retry recovery derivable."""
        now = self._clock() if now is None else now
        closed = None
        with self._lock:
            index = int((now - self._run_start) // self._window_s)
            if index != self._index:
                if self._window:
                    closed = self._close_locked()
                self._index = index
            self._window[(target, worker, trial, outcome)] += 1
            self._totals[(target, worker, trial, outcome)] += 1
        # logged outside the lock: a slow handler must not block record()
        if closed is not None:
            self._log_window(*closed)

    def totals(self) -> List[dict]:
        """Run totals as run-record metric rows, one per combination seen."""
        with self._lock:
            counts = dict(self._totals)
        return [{'scope': f'api:{target}:{worker}',
                 'name': f't{trial}:{outcome}',
                 'value': float(value)}
                for (target, worker, trial, outcome), value in counts.items()]

    def log_summary(self, log: Optional[logging.Logger] = None) -> None:
        """Close the last window and emit the end-of-run report."""
        with self._lock:
            closed = self._close_locked() if self._window else None
        if closed is not None:
            self._log_window(*closed)
        for line in self._report():
            (log or logger).info(line)

    def _close_locked(self) -> Tuple[int, Dict[Tuple[str, str, int, str], int]]:
        counts = dict(self._window)
        self._window = Counter()
        trial1: Dict[Tuple[str, str], List[int]] = {}
        for (target, worker, trial, outcome), n in counts.items():
            if trial != 1:
                continue
            slot = trial1.setdefault((target, worker), [0, 0])
            slot[0] += n
            if outcome == OK:
                slot[1] += n
        self._series.append((self._index, trial1))
        return self._index, counts

    def _log_window(self, index: int, counts: Dict[Tuple[str, str, int, str], int]) -> None:
        by_target: Dict[str, Counter] = {}
        for (target, _w, _t, outcome), n in counts.items():
            by_target.setdefault(target, Counter())[outcome] += n
        parts = []
        for target in sorted(by_target):
            outcomes = by_target[target]
            rejected = ' '.join(f'{k}={v}' for k, v in sorted(outcomes.items()) if k != OK)
            parts.append(f'{target}: n={sum(outcomes.values())} ok={outcomes.get(OK, 0)}'
                         + (f' {rejected}' if rejected else ''))
        elapsed = int(index * self._window_s)
        logger.info(f'API stat window #{index} (t+{format_ts_s(elapsed)}, '
                    f'{self._window_s:.0f}s): ' + '; '.join(parts))

    def _report(self) -> List[str]:
        with self._lock:
            counts = dict(self._totals)
            series = list(self._series)
        grouped: Dict[Tuple[str, str], Dict[int, Counter]] = {}
        for (target, worker, trial, outcome), n in counts.items():
            grouped.setdefault((target, worker), {}).setdefault(trial, Counter())[outcome] += n

        lines = [f'## api stat summary (window={self._window_s:.0f}s, '
                 f'windows={len(series)})']
        if not grouped:
            return lines + ['(no requests recorded)']
        for target, worker in sorted(grouped):
            trials = grouped[(target, worker)]
            lines.append(f'- {target} / {worker}')
            for trial in sorted(trials):
                outcomes = trials[trial]
                detail = ' '.join(f'{k}={v}' for k, v in sorted(outcomes.items()))
                lines.append(f'  - trial {trial}: n={sum(outcomes.values())}, {detail}')
            d = _derive(trials)
            p = f'{d["p"] * 100:.2f}%' if d['p'] is not None else 'n/a'
            lines.append(f'  - derived: p={p} (n={d["n1"]}), '
                         f'retry_recovered={d["retry_recovered"]}')

        by_target: Dict[str, Dict[int, Counter]] = {}
        for (target, _worker), trials in grouped.items():
            per_trial = by_target.setdefault(target, {})
            for trial, outcomes in trials.items():
                per_trial.setdefault(trial, Counter()).update(outcomes)
        for target in sorted(by_target):
            trials = by_target[target]
            d = _derive(trials)
            p = f'{d["p"] * 100:.2f}%' if d['p'] is not None else 'n/a'
            lines.append(f'- {target} (all workers): p={p} (n={d["n1"]}), '
                         f'retry_recovered={d["retry_recovered"]}, '
                         f'exhausted={_exhausted(trials)}')

        spark = self._sparklines(series)
        if spark:
            lines.append('- p over time, oldest -> newest window:')
            lines.extend(spark)
        return lines

    def _sparklines(self, series) -> List[str]:
        if not series:
            return []
        per_key: Dict[Tuple[str, str], Dict[int, List[int]]] = {}
        for index, trial1 in series:
            for key, (n1, ok1) in trial1.items():
                per_key.setdefault(key, {})[index] = [n1, ok1]
        lines = []
        first, last = series[0][0], series[-1][0]
        for target, worker in sorted(per_key):
            points = per_key[(target, worker)]
            fractions = []
            for index in range(first, last + 1):
                n1, ok1 = points.get(index, (0, 0))
                fractions.append(None if not n1 else (n1 - ok1) / n1)
            lines.append(f'  {target} / {worker}: {_sparkline(fractions)}')
        return lines
