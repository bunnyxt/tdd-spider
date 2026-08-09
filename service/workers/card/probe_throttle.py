#!/usr/bin/env python3
"""Measure where bilibili's -352 throttle starts on the member_card workers.

The point of this tool is to answer one question quickly: **will a proposed
anti-352 change let 16_update-member-info.py actually finish?** -- without
running the job for hours to find out.

Why a naive check is not enough
-------------------------------
On 2026-08-09 the card workers were verified by sending a handful of sequential
requests. All returned code 0. The job was started and collapsed within three
minutes: 288 members done, then every one of its 50 workers stuck in a loop of
-352 -> sleep 60s -> retry -> -352.

The throttle has a **burst allowance**. Low-rate and short-burst probes sail
straight through it and prove nothing. Any useful test has to run concurrently,
for long enough to exhaust the allowance, and then measure what is left.

So this reports two numbers per concurrency level, and only the second one
predicts whether a job finishes:

  burst    how many requests succeed before the first -352
  settled  successful requests/sec in the FINAL THIRD of the window, i.e.
           the sustained rate once the throttle has clamped down

A settled rate near zero means the job will never finish, no matter how good
the burst looks.

Usage
-----
    # find the ceiling: sweep concurrency against all four workers
    python service/workers/card/probe_throttle.py --sweep 1,5,10,20,50

    # is the limit per-platform or global? test each worker alone
    python service/workers/card/probe_throttle.py --per-worker --sweep 5,20

    # evaluate a change: does pacing help at the concurrency the job uses?
    python service/workers/card/probe_throttle.py --sweep 50 --duration 120

Run from the repo root with venv-3.11 active. Requests hit the real bilibili
API through the real workers, so keep durations modest.

Leave a cooldown between runs
-----------------------------
The throttle persists after a run ends, so back-to-back levels contaminate each
other. Measured 2026-08-09: a conc=10 level finished with the throttle active,
and the conc=50 level that followed immediately reported burst=0 -- its very
first request was already throttled. Wait 2-5 minutes between levels, or treat a
burst of 0 as "still throttled from last time" rather than a real reading.

Validated against production
----------------------------
2026-08-09, sweeping all four workers:

    conc   reqs    ok    -352    burst   1st352   settled/s
       1     74    67       0       67    never       2.50
      10    881   772      43      723    27.1s      25.40
      50   6192    63    6115        0     0.5s       0.00

16_update-member-info.py runs at conc=50 and stalled at 288 members with 0/s --
which is exactly what the conc=50 row predicts. The tool reproduces the real
failure in 45 seconds instead of three minutes of a doomed production job.
"""

import argparse
import json
import random
import statistics
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

# Real, long-lived accounts. Throttling can behave differently for mids that do
# not exist, so probing must not use synthetic ones.
DEFAULT_MIDS = [
    2, 208259, 546195, 1635775, 8047632, 11783021, 17706376, 37974, 703007996,
    174485983, 486906330, 3494380402, 434401755, 8964182, 22884204,
]

UA_LIST = [
    'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/72.0.3626.121 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/72.0.3626.121 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/71.0.3578.98 Safari/537.36',
]


def load_workers(repo_root: Path) -> list:
    path = repo_root / 'service' / 'endpoints.json'
    if not path.exists():
        sys.exit(f'endpoints.json not found at {path} -- run from the repo root.')
    return json.load(path.open())['get_member_card']['workers']


def worker_label(url: str) -> str:
    for needle, name in (
        ('lambda-url', 'AWS fn-url'),
        ('execute-api', 'AWS API GW'),
        ('azurewebsites', 'Azure'),
        ('run.app', 'GCP Run'),
    ):
        if needle in url:
            return name
    return url[:24]


class Probe:
    """Fires requests at a fixed concurrency for a fixed wall-clock window."""

    def __init__(self, urls, mids, timeout, pace):
        self.urls = urls
        self.mids = mids
        self.timeout = timeout
        self.pace = pace           # seconds to sleep after each request, per thread
        self.session = requests.Session()
        self.lock = threading.Lock()
        self.events = []           # (elapsed_seconds, outcome)
        self.first_352_at = None
        self.before_first_352 = 0

    def _record(self, elapsed, outcome):
        with self.lock:
            self.events.append((elapsed, outcome))
            if outcome == 'ok' and self.first_352_at is None:
                self.before_first_352 += 1
            elif outcome == 'throttled' and self.first_352_at is None:
                self.first_352_at = elapsed

    def _one(self, start):
        url = random.choice(self.urls)
        mid = random.choice(self.mids)
        try:
            r = self.session.get(
                url,
                params={'mid': mid},
                headers={'User-Agent': random.choice(UA_LIST)},
                timeout=self.timeout,
            )
            code = r.json().get('code')
            if code == 0:
                outcome = 'ok'
            elif code == -352:
                outcome = 'throttled'
            else:
                outcome = f'code{code}'
        except Exception as exc:                       # noqa: BLE001 - report, never abort
            outcome = type(exc).__name__
        self._record(time.perf_counter() - start, outcome)

    def _thread(self, start, deadline):
        while time.perf_counter() < deadline:
            self._one(start)
            if self.pace:
                time.sleep(self.pace)

    def run(self, concurrency, duration):
        start = time.perf_counter()
        deadline = start + duration
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            for _ in range(concurrency):
                pool.submit(self._thread, start, deadline)
        return self.summary(duration)

    def summary(self, duration):
        tally = Counter(o for _, o in self.events)
        total = len(self.events)
        # settled rate: successes in the final third of the window
        cutoff = duration * 2 / 3
        tail = [o for t, o in self.events if t >= cutoff]
        tail_ok = sum(1 for o in tail if o == 'ok')
        tail_secs = duration - cutoff
        return {
            'total': total,
            'ok': tally.get('ok', 0),
            'throttled': tally.get('throttled', 0),
            'other': total - tally.get('ok', 0) - tally.get('throttled', 0),
            'burst': self.before_first_352,
            'first_352_at': self.first_352_at,
            'settled_rps': tail_ok / tail_secs if tail_secs > 0 else 0.0,
            'overall_rps': tally.get('ok', 0) / duration,
            'tally': tally,
        }


def run_level(urls, mids, concurrency, duration, timeout, pace, label):
    probe = Probe(urls, mids, timeout, pace)
    s = probe.run(concurrency, duration)
    first = f"{s['first_352_at']:.1f}s" if s['first_352_at'] is not None else 'never'
    pct = (s['throttled'] / s['total'] * 100) if s['total'] else 0.0
    print(
        f"  {label:<12}{concurrency:>5}{s['total']:>8}{s['ok']:>8}{s['throttled']:>9}"
        f"{pct:>7.1f}%{s['burst']:>8}{first:>9}{s['settled_rps']:>11.2f}"
    )
    if s['other']:
        odd = {k: v for k, v in s['tally'].items() if k not in ('ok', 'throttled')}
        print(f"  {'':<12}{'':>5}  other outcomes: {odd}")
    return s


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--sweep', default='1,5,10,20,50',
                    help='comma-separated concurrency levels (default: 1,5,10,20,50)')
    ap.add_argument('--duration', type=float, default=45.0,
                    help='seconds per level; needs to exceed the burst allowance (default: 45)')
    ap.add_argument('--per-worker', action='store_true',
                    help='test each worker URL alone, to see if the limit is per-platform')
    ap.add_argument('--pace', type=float, default=0.0,
                    help='seconds each thread sleeps between requests -- use this to evaluate '
                         'a pacing fix (default: 0)')
    ap.add_argument('--timeout', type=float, default=15.0)
    ap.add_argument('--mids', help='file with one mid per line; defaults to a built-in real set')
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[3]
    workers = load_workers(repo_root)
    mids = ([int(x) for x in Path(args.mids).read_text().split()] if args.mids else DEFAULT_MIDS)
    levels = [int(x) for x in args.sweep.split(',')]

    print(f"member_card throttle probe -- {len(workers)} workers, {len(mids)} mids, "
          f"{args.duration:.0f}s per level, pace={args.pace}s")
    print("\n  settled_rps is the number that matters: sustained successful req/s in the final")
    print("  third of the window, after the burst allowance is spent. Near zero => job stalls.\n")
    header = (f"  {'target':<12}{'conc':>5}{'reqs':>8}{'ok':>8}{'-352':>9}{'352%':>8}"
              f"{'burst':>8}{'1st352':>9}{'settled/s':>11}")
    print(header)
    print('  ' + '-' * (len(header) - 2))

    targets = ([(worker_label(u), [u]) for u in workers] if args.per_worker
               else [('all workers', workers)])
    results = {}
    for label, urls in targets:
        for c in levels:
            results[(label, c)] = run_level(urls, mids, c, args.duration,
                                            args.timeout, args.pace, label)
        if args.per_worker:
            print()

    print("\n  verdict:")
    for (label, c), s in results.items():
        need = 24931 / s['settled_rps'] / 3600 if s['settled_rps'] > 0.01 else None
        eta = f"{need:.1f}h to do 24,931 members" if need else "job would NEVER finish"
        print(f"    {label:<12} conc={c:<4} settled {s['settled_rps']:>6.2f}/s  ->  {eta}")


if __name__ == '__main__':
    main()
