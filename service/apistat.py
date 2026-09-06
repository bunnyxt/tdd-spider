"""
Always-on request/retry/rate-limit observability, without ``--debug``.

``Service._get()`` already classifies every HTTP attempt into one of a small,
fixed set of outcomes -- ``'ok'``, a rate-limit reason (``'http_412'``,
``'code_-352'``, ...), ``'http_<code>'`` for any other non-200, or one of the
attempts that never produced a parseable response (``'request_exception'``,
``'body_exception'``, ``'deadline_exceeded'``, ``'json_error'``,
``'parse_error'``). That classification is otherwise only visible as a DEBUG
line (150 MB+ per hourly run in production); this module is the always-on,
DEBUG-independent aggregation of the same classification.

Kept dimensions: ``(target, worker, trial, outcome)``, rolled into fixed
``window_s``-second windows. Deliberately NOT kept: no ``aid``/``mid`` or any
other per-request identifier, no User-Agent, no full URLs, query strings,
headers or bodies. A window's memory and log footprint is therefore bounded
by the number of distinct ``(target, worker, trial, outcome)`` combinations
actually seen, never by request volume.

``ApiStatTracker.record()`` is the hot-path call, made once per HTTP attempt
from every worker thread sharing one ``Service``. It does O(1) dict work under
one lock and returns immediately; the (at most one) window it closes is
logged AFTER the lock is released, so log I/O never happens while other
threads are blocked on the lock. ``finalize()`` closes the last partial
window and returns the whole-run report: a total table broken down by
``(target, worker, trial)``, the quantities this module exists to answer
(first-attempt rejection rate ``p``, retry-recovered count, retry-exhausted
count) derived directly from those counts, and one ASCII ``p``-over-time
sparkline per ``(target, worker)`` that saw traffic.
"""

import logging
import threading
import time
from collections import Counter
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

from util import format_ts_s

__all__ = ['ApiStatTracker', 'WindowSnapshot']

logger = logging.getLogger('apistat')

DEFAULT_WINDOW_S = 5.0

# (target, worker, trial, outcome) -> count
Key = Tuple[str, str, int, str]

OK = 'ok'

# eight shading levels (no blank level -- a real zero still gets a visible
# glyph, so it can never be confused with the separate "no data" gap glyph or
# with plain whitespace) is enough resolution to tell free / climbing /
# plateau apart by eye, and stays one character per window so a
# several-hundred-window run still fits on one terminal-friendly line.
_SPARK_TICKS = '▁▂▃▄▅▆▇█'
_GAP = '·'


def _sparkline(fractions: List[Optional[float]]) -> str:
    """
    Render a 0..1 fraction series as one glyph per point, oldest first;
    ``None`` (no trial-1 attempts that window) renders as the gap glyph.
    Deliberately a FIXED 0..1 scale (a rejection rate), unlike a min..max
    autoscaled sparkline -- two windows, or two runs, are only comparable to
    each other if the vertical scale means the same thing in both.
    """
    out = []
    for value in fractions:
        if value is None:
            out.append(_GAP)
            continue
        value = min(1.0, max(0.0, value))
        out.append(_SPARK_TICKS[int(round(value * (len(_SPARK_TICKS) - 1)))])
    return ''.join(out)


@dataclass
class WindowSnapshot:
    index: int
    start_ts: float
    end_ts: float
    counts: Dict[Key, int]


class ApiStatTracker:
    """
    Thread-safe. One instance is meant to live as long as one ``Service`` --
    i.e. for the whole run of a script, shared by every worker thread that
    Service serves (see ``Service``'s own docstring on sharing one instance).
    """

    def __init__(self, window_s: float = DEFAULT_WINDOW_S,
                 clock: Callable[[], float] = time.monotonic):
        self._window_s = window_s
        self._clock = clock
        self._lock = threading.Lock()
        self._run_start = clock()
        self._current_index = 0
        self._window_counts: Counter = Counter()
        self._totals: Counter = Counter()
        self._history: List[WindowSnapshot] = []

    def record(self, target: str, worker: str, trial: int, outcome: str,
               now: Optional[float] = None) -> None:
        """
        Count one HTTP attempt. Called once per attempt (i.e. once per
        `Service._get` loop iteration, not once per logical call) -- a
        retried call therefore contributes one row per trial, which is what
        makes retry-recovery and retry-exhaustion derivable at all (see
        ``derive``).
        """
        now = self._clock() if now is None else now
        closed = None
        with self._lock:
            idx = int((now - self._run_start) // self._window_s)
            if idx != self._current_index:
                if self._window_counts:
                    closed = self._close_window_locked(self._current_index)
                self._current_index = idx
            self._window_counts[(target, worker, trial, outcome)] += 1
            self._totals[(target, worker, trial, outcome)] += 1
        # logging (and thus any I/O) happens with the lock already released,
        # so a slow log handler never blocks other threads' record() calls
        if closed is not None:
            self._log_window(closed)

    def _close_window_locked(self, index: int) -> WindowSnapshot:
        counts = dict(self._window_counts)
        self._window_counts = Counter()
        snapshot = WindowSnapshot(
            index=index,
            start_ts=self._run_start + index * self._window_s,
            end_ts=self._run_start + (index + 1) * self._window_s,
            counts=counts)
        self._history.append(snapshot)
        return snapshot

    def _log_window(self, snapshot: WindowSnapshot) -> None:
        by_target: Dict[str, Counter] = {}
        for (target, _worker, _trial, outcome), n in snapshot.counts.items():
            by_target.setdefault(target, Counter())[outcome] += n
        elapsed_s = int(snapshot.start_ts - self._run_start)
        parts = []
        for target in sorted(by_target):
            outcomes = by_target[target]
            n = sum(outcomes.values())
            ok = outcomes.get(OK, 0)
            rejected = ' '.join(f'{k}={v}' for k, v in sorted(outcomes.items())
                                if k != OK)
            parts.append(f'{target}: n={n} ok={ok}' + (f' {rejected}' if rejected else ''))
        logger.info(
            f'API stat window #{snapshot.index} (t+{format_ts_s(elapsed_s)}, '
            f'{self._window_s:.0f}s): ' + '; '.join(parts))

    def _totals_snapshot(self) -> Dict[Key, int]:
        with self._lock:
            return dict(self._totals)

    def totals(self) -> List[dict]:
        """
        The run's total counts as ``[{'scope', 'name', 'value'}, ...]``, ready
        for ``RunRecorder.add_api_stat_metrics`` -- one row per
        ``(target, worker, trial, outcome)`` combination actually seen.
        Includes every attempt recorded so far, whether or not the window it
        happened in has been closed by ``finalize()`` yet.
        """
        rows = []
        for (target, worker, trial, outcome), value in self._totals_snapshot().items():
            rows.append({
                'scope': f'api:{target}:{worker}',
                'name': f't{trial}:{outcome}',
                'value': float(value),
            })
        return rows

    def grouped_totals(self) -> Dict[Tuple[str, str], Dict[int, Counter]]:
        """``{(target, worker): {trial: Counter(outcome -> count)}}``."""
        grouped: Dict[Tuple[str, str], Dict[int, Counter]] = {}
        for (target, worker, trial, outcome), n in self._totals_snapshot().items():
            grouped.setdefault((target, worker), {}).setdefault(trial, Counter())[outcome] += n
        return grouped

    @staticmethod
    def derive(trials: Dict[int, Counter]) -> dict:
        """
        The quantities this module exists to answer, computed directly from
        per-trial outcome counts -- no field this depends on is estimated:

        - ``p``: first-attempt rejection rate (non-``ok`` share of trial 1).
        - ``retry_recovered``: calls that failed trial 1 but eventually
          returned ``ok`` on a later trial.
        - ``exhausted``: calls that were still failing on the LAST trial any
          call in this group reached -- i.e. what ``Service._get`` raises
          ``RateLimitError``/``ResponseError`` for. Assumes ``retry`` was
          constant across the calls being summarised, which holds for one
          script run (``retry`` is a per-Service-call config, not randomised).
        """
        trial1 = trials.get(1, Counter())
        n1 = sum(trial1.values())
        ok1 = trial1.get(OK, 0)
        p = (n1 - ok1) / n1 if n1 else None
        retry_recovered = sum(counts.get(OK, 0) for t, counts in trials.items() if t != 1)
        max_trial = max(trials) if trials else None
        exhausted = None
        if max_trial is not None:
            last = trials[max_trial]
            exhausted = sum(v for k, v in last.items() if k != OK)
        return {'p': p, 'n1': n1, 'retry_recovered': retry_recovered,
                'exhausted': exhausted, 'max_trial': max_trial}

    def summary_lines(self) -> List[str]:
        """The end-of-run total table: per (target, worker), per trial, per outcome."""
        grouped = self.grouped_totals()
        lines = [f'## api stat summary (window={self._window_s:.0f}s, '
                 f'windows closed={len(self._history)})']
        if not grouped:
            lines.append('(no requests recorded)')
            return lines
        for target, worker in sorted(grouped):
            trials = grouped[(target, worker)]
            lines.append(f'- {target} / {worker}')
            for trial in sorted(trials):
                outcomes = trials[trial]
                n = sum(outcomes.values())
                detail = ' '.join(f'{k}={v}' for k, v in sorted(outcomes.items()))
                lines.append(f'  - trial {trial}: n={n}, {detail}')
            d = self.derive(trials)
            p_str = f'{d["p"] * 100:.2f}%' if d['p'] is not None else 'n/a'
            lines.append(
                f'  - derived: p={p_str} (n={d["n1"]}), '
                f'retry_recovered={d["retry_recovered"]}, '
                f'exhausted(trial {d["max_trial"]})={d["exhausted"]}')
        return lines

    def p_sparklines(self) -> List[str]:
        """
        One ``'<target> / <worker>: <sparkline>'`` line per (target, worker)
        that had trial-1 traffic, oldest -> newest closed window, gaps where a
        window had none. Empty if no window has closed yet.
        """
        if not self._history:
            return []
        per_key: Dict[Tuple[str, str], Dict[int, Tuple[int, int]]] = {}
        for snapshot in self._history:
            for (target, worker, trial, outcome), n in snapshot.counts.items():
                if trial != 1:
                    continue
                series = per_key.setdefault((target, worker), {})
                n1, ok1 = series.get(snapshot.index, (0, 0))
                n1 += n
                if outcome == OK:
                    ok1 += n
                series[snapshot.index] = (n1, ok1)
        first_index = self._history[0].index
        last_index = self._history[-1].index
        lines = []
        for target, worker in sorted(per_key):
            series = per_key[(target, worker)]
            fractions = []
            for idx in range(first_index, last_index + 1):
                if idx not in series:
                    fractions.append(None)
                else:
                    n1, ok1 = series[idx]
                    fractions.append(None if n1 == 0 else (n1 - ok1) / n1)
            lines.append(f'  {target} / {worker}: {_sparkline(fractions)}')
        return lines

    def finalize(self) -> List[str]:
        """
        Close the last partial window (if one has any counts) and return the
        full end-of-run report as a list of log lines. Safe to call more than
        once -- the second call sees an already-empty partial window and
        simply re-renders the same totals.
        """
        with self._lock:
            pending = dict(self._window_counts) if self._window_counts else None
            if pending is not None:
                snapshot = self._close_window_locked(self._current_index)
            else:
                snapshot = None
        if snapshot is not None:
            self._log_window(snapshot)
        lines = self.summary_lines()
        sparklines = self.p_sparklines()
        if sparklines:
            lines.append('- p over time (free window -> climb -> plateau should '
                         'be visible left to right), oldest -> newest window:')
            lines.extend(sparklines)
        return lines

    def log_summary(self, log: Optional[logging.Logger] = None) -> None:
        """Call ``finalize()`` and emit every line as one INFO record each."""
        target_logger = log if log is not None else logger
        for line in self.finalize():
            target_logger.info(line)
