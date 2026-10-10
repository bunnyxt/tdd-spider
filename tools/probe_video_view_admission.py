"""Read-only, rate-limited comparison of two trimmed video-view workers.

Input URLs and aids stay in ignored/private files. Output contains aggregate
outcomes only. The production endpoint configuration is never modified.
"""

import argparse
from collections import Counter, defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import csv
import gzip
import json
from pathlib import Path
import random
import sys
import time
from urllib.parse import urlparse

from service import Service
from service.apistat import ApiStatTracker


ENDPOINT = 'get_video_view_trimmed'
ARMS = ('baseline', 'candidate')


def sample_aids(path: Path, count: int, seed: int) -> list[int]:
    """Uniform sample from a 51_ CSV without retaining its full contents."""
    rng = random.Random(seed)
    sample = []
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt', newline='') as source:
        rows = csv.DictReader(source)
        if 'aid' not in (rows.fieldnames or ()):
            raise ValueError('input CSV has no aid column')
        for index, row in enumerate(rows):
            aid = int(row['aid'])
            if index < count:
                sample.append(aid)
            else:
                slot = rng.randrange(index + 1)
                if slot < count:
                    sample[slot] = aid
    if len(sample) != count:
        raise ValueError(f'input CSV has {len(sample)} rows, need {count}')
    rng.shuffle(sample)
    return sample


def summarize_trials(tracker: ApiStatTracker, worker_ids: dict[str, str]) -> dict:
    counts = defaultdict(Counter)
    id_to_arm = {value: key for key, value in worker_ids.items()}
    for row in tracker.totals():
        scope = row['scope'].split(':')
        if len(scope) != 3 or scope[1] != ENDPOINT:
            continue
        arm = id_to_arm.get(scope[2])
        if arm is not None:
            counts[arm][row['name']] += int(row['value'])
    result = {}
    for arm in ARMS:
        outcomes = counts[arm]
        trials = {}
        for name, value in sorted(outcomes.items()):
            trial, outcome = name.split(':', 1)
            trials.setdefault(trial, {})[outcome] = value
        n1 = sum(trials.get('t1', {}).values())
        failed1 = n1 - trials.get('t1', {}).get('ok', 0)
        conditional_failure = {}
        for trial, values in trials.items():
            n = sum(values.values())
            conditional_failure[trial] = ((n - values.get('ok', 0)) / n
                                          if n else None)
        result[arm] = {
            'n1': n1,
            'p': failed1 / n1 if n1 else None,
            'conditional_failure': conditional_failure,
            'retry_recovered': sum(values.get('ok', 0)
                                   for trial, values in trials.items()
                                   if trial != 't1'),
            'trials': trials,
        }
    return result


def percentile(values: list[int], fraction: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(fraction * (len(ordered) - 1)))]


def run_probe(args, services: dict[str, Service], aids: list[int],
              worker_ids: dict[str, str], tracker: ApiStatTracker) -> dict:
    outcomes = {arm: Counter() for arm in ARMS}
    durations = {arm: [] for arm in ARMS}
    pending = set()
    stop_reason = None
    scheduled = 0
    started = time.monotonic()

    def request(arm, aid):
        began = time.monotonic()
        try:
            # Let Service select a UA on every attempt, as it does in 51_.
            services[arm].get_video_view_trimmed({'aid': aid})
            outcome = 'ok'
        except Exception as error:
            outcome = type(error).__name__
        return arm, outcome, int((time.monotonic() - began) * 1000)

    def collect(done):
        for future in done:
            arm, outcome, elapsed_ms = future.result()
            outcomes[arm][outcome] += 1
            durations[arm].append(elapsed_ms)

    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        for index, aid in enumerate(aids):
            if stop_reason:
                break
            target = started + index / args.rate
            delay = target - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            arm = ARMS[index % 2]
            pending.add(pool.submit(request, arm, aid))
            scheduled += 1
            if len(pending) >= args.concurrency * 2:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                collect(done)
            # An early stop protects the candidate from an Azure-like result.
            if index % 100 == 99:
                done = {future for future in pending if future.done()}
                pending -= done
                collect(done)
                trial = summarize_trials(tracker, worker_ids)
                candidate = trial['candidate']
                baseline = trial['baseline']
                if candidate['n1'] >= args.stop_after:
                    if candidate['p'] >= args.stop_p:
                        stop_reason = 'candidate_p_stop'
                    elif (baseline['n1'] >= args.stop_after
                          and candidate['p'] - baseline['p'] >= args.stop_gap):
                        stop_reason = 'candidate_gap_stop'
        if pending:
            done, _ = wait(pending)
            collect(done)

    elapsed_s = time.monotonic() - started
    trials = summarize_trials(tracker, worker_ids)
    for arm in ARMS:
        trials[arm]['calls'] = dict(sorted(outcomes[arm].items()))
        trials[arm]['final_failure_count'] = sum(
            count for outcome, count in outcomes[arm].items() if outcome != 'ok')
        trials[arm]['latency_ms'] = {
            'p50': percentile(durations[arm], 0.50),
            'p95': percentile(durations[arm], 0.95),
            'p99': percentile(durations[arm], 0.99),
        }
    return {
        'scheduled': scheduled,
        'elapsed_s': round(elapsed_s, 3),
        'realized_calls_per_s': round(scheduled / elapsed_s, 3),
        'target_calls_per_s': args.rate,
        'rate_ratio': round(scheduled / elapsed_s / args.rate, 3),
        'stop_reason': stop_reason,
        'arms': trials,
    }


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--endpoints', required=True, type=Path)
    parser.add_argument('--baseline-id', required=True)
    parser.add_argument('--candidate-url-file', required=True, type=Path)
    parser.add_argument('--aid-csv', required=True, type=Path)
    parser.add_argument('--per-arm', type=int, required=True)
    parser.add_argument('--rate', type=float, required=True,
                        help='total logical calls per second across both arms')
    parser.add_argument('--concurrency', type=int, default=150)
    parser.add_argument('--seed', type=int, default=1)
    parser.add_argument('--stop-after', type=int, default=500)
    parser.add_argument('--stop-p', type=float, default=0.20)
    parser.add_argument('--stop-gap', type=float, default=0.10)
    args = parser.parse_args(argv)
    if args.per_arm <= 0 or args.rate <= 0 or args.concurrency <= 0:
        parser.error('per-arm, rate, and concurrency must be positive')
    if args.stop_after <= 0 or not 0 < args.stop_p <= 1 or not 0 < args.stop_gap <= 1:
        parser.error('invalid early-stop thresholds')
    return args


def main(argv=None):
    args = parse_args(argv)
    config = json.loads(args.endpoints.read_text())
    endpoint = config[ENDPOINT]
    matches = [worker for worker in endpoint['workers']
               if isinstance(worker, dict) and worker.get('id') == args.baseline_id]
    if len(matches) != 1 or not matches[0].get('enabled'):
        raise ValueError('baseline worker must exist exactly once and be enabled')
    candidate_url = args.candidate_url_file.read_text().strip()
    parsed_url = urlparse(candidate_url)
    if parsed_url.scheme != 'https' and not (
            parsed_url.scheme == 'http'
            and parsed_url.hostname in ('127.0.0.1', 'localhost')):
        raise ValueError('candidate URL must be HTTPS (or loopback HTTP for tests)')
    worker_ids = {'baseline': args.baseline_id,
                  'candidate': 'probe-candidate'}
    tracker = ApiStatTracker()
    ua_pool = endpoint.get('user_agents') or []
    if not ua_pool:
        raise ValueError('endpoint must provide the production User-Agent pool')
    services = {}
    for arm, url in [('baseline', matches[0]['url']),
                     ('candidate', candidate_url)]:
        worker = {'id': worker_ids[arm], 'url': url,
                  'platform': arm, 'weight': 1, 'enabled': True}
        services[arm] = Service(
            mode='worker', pool_maxsize=args.concurrency,
            endpoints={ENDPOINT: {'workers': [worker], 'user_agents': ua_pool}},
            stats=tracker)
    aids = sample_aids(args.aid_csv, args.per_arm * 2, args.seed)
    result = run_probe(args, services, aids, worker_ids, tracker)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result['stop_reason'] is None else 2


if __name__ == '__main__':
    sys.exit(main())
