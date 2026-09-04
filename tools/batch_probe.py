"""
Conformance + performance probe for the batched trimmed video_view worker
(``service/workers/video_view/aws_lambda_batch.mjs``, contract v1).

Status: the batch rollout was found NO-GO during calibration -- driving batches
of concurrent fetches through one worker at a sustained rate trips bilibili's
anti-crawler on the view endpoint (sticky HTTP 412), and a batch that fails
that way routes every item to single-aid fallback, amplifying upstream load
rather than reducing it. The batch client path stays default-off. This probe is
kept because its ``conformance`` command is still the right per-deploy contract
gate for the batch worker if it is ever revisited, and ``harvest`` / ``perf`` /
``parity`` / ``summarize`` are the tools that produced the evidence.

Run ``conformance`` after every deploy of the batch worker. It is deliberately
kept in the repo so the exact checks that gate a deploy are versioned alongside
the contract they check.

    python -m tools.batch_probe conformance
    python -m tools.batch_probe harvest --out aids.txt --count 5000
    python -m tools.batch_probe perf --aids aids.txt --n 10 --batches 200 \
        --concurrency 30 --out run.jsonl
    python -m tools.batch_probe parity --aids aids.txt --count 1000 --out parity.json
    python -m tools.batch_probe summarize run.jsonl

--------------------------------------------------------------------------------
URL handling -- the worker URL is NEVER embedded in this file.
It is resolved, in order, from:
    1. --url <value>                         (explicit, ad-hoc runs)
    2. $TDD_BATCH_URL                        (environment)
    3. --endpoints <path> (default service/endpoints.json)
       -> get_video_view_trimmed_batch.workers[0]
The single-aid trimmed URL (for `parity`) and the newlist URL (for `harvest`)
come from the same endpoints file only -- never a flag, never hard-coded.

Database safety -- this module imports ``service`` only. It never imports
``job`` / ``task`` / ``db`` / ``Session``, never constructs a Job, never opens
a DB connection, and issues no write of any kind. Every call it makes is a
read-only HTTP GET against a worker URL. `Service.get_video_view_trimmed_batch`
and `Service.get_video_view_trimmed` are pure HTTP + parse.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from service import Service, CodeError, MisalignmentError, ResponseError  # noqa: E402
from service.response import VideoViewTrimmed  # noqa: E402
from util import a2b  # noqa: E402

DEFAULT_ENDPOINTS = _REPO_ROOT / "service" / "endpoints.json"
STAT_KEYS = ["aid", "view", "danmaku", "reply", "favorite", "coin", "share",
             "now_rank", "his_rank", "like", "dislike"]  # vt/vv optional


# --------------------------------------------------------------------------
# URL / endpoint resolution
# --------------------------------------------------------------------------

def _load_endpoints(path):
    p = Path(path)
    if not p.is_file():
        raise SystemExit(f"endpoints file not found: {p} (pass --endpoints or set $TDD_BATCH_URL)")
    return json.loads(p.read_text())


def resolve_batch_url(args):
    if getattr(args, "url", None):
        return args.url
    env = os.environ.get("TDD_BATCH_URL")
    if env:
        return env
    eps = _load_endpoints(args.endpoints)
    workers = (eps.get("get_video_view_trimmed_batch") or {}).get("workers") or []
    if not workers:
        raise SystemExit("no get_video_view_trimmed_batch.workers in endpoints file")
    return workers[0]


def _endpoints_for_service(args, *, batch_url):
    """Build a minimal endpoints dict for an injected Service: the batch URL
    from resolve_batch_url plus, when available, the single-aid trimmed and
    newlist URLs from the endpoints file (used by `parity` / `harvest`)."""
    ep = {"get_video_view_trimmed_batch": {"direct": "invalid://x", "workers": [batch_url]}}
    try:
        disk = _load_endpoints(args.endpoints)
    except SystemExit:
        disk = {}
    for key in ("get_video_view_trimmed", "get_newlist", "get_video_view"):
        if disk.get(key):
            ep[key] = disk[key]
    return ep


def make_service(args, *, batch_url):
    return Service(mode="worker", retry=1, timeout=8.0,
                   endpoints=_endpoints_for_service(args, batch_url=batch_url))


# --------------------------------------------------------------------------
# raw HTTP (for the request-shape checks the client never issues)
# --------------------------------------------------------------------------

def _raw_get(url, aids_param):
    params = {} if aids_param is None else {"aids": aids_param}
    r = requests.get(url, params=params, timeout=15,
                     headers={"User-Agent": "tdd-batch-probe"})
    body = None
    try:
        body = r.json()
    except ValueError:
        pass
    return r.status_code, body, r.text


# --------------------------------------------------------------------------
# aid harvesting (real, recent tid=30 aids via the newlist worker)
# --------------------------------------------------------------------------

def cmd_harvest(args):
    batch_url = resolve_batch_url(args)
    svc = make_service(args, batch_url=batch_url)
    if "get_newlist" not in svc.endpoints:
        raise SystemExit("harvest needs get_newlist in the endpoints file")
    seen, out = set(), []
    pn = 1
    while len(out) < args.count and pn <= args.max_pages:
        try:
            nl = svc.get_newlist({"tid": 30, "pn": pn, "ps": 50}, retry=2, timeout=15.0)
        except Exception as e:  # noqa: BLE001
            print(f"  pn={pn} failed: {e}", file=sys.stderr)
            pn += 1
            continue
        if not nl.archives:
            print(f"  pn={pn}: empty archives, stopping", file=sys.stderr)
            break
        for arc in nl.archives:
            if arc.aid not in seen:
                seen.add(arc.aid)
                out.append(arc.aid)
        pn += 1
        time.sleep(args.sleep)
    out = out[:args.count]
    Path(args.out).write_text("\n".join(str(a) for a in out) + "\n")
    print(f"wrote {len(out)} aids -> {args.out}")


def _load_aids(path):
    return [int(x) for x in Path(path).read_text().split() if x.strip()]


# --------------------------------------------------------------------------
# conformance
# --------------------------------------------------------------------------

class Check:
    def __init__(self):
        self.rows = []

    def record(self, name, ok, detail=""):
        self.rows.append((name, bool(ok), detail))
        mark = "PASS" if ok else "FAIL"
        print(f"  [{mark}] {name}" + (f"  -- {detail}" if detail else ""))

    @property
    def ok(self):
        return all(ok for _, ok, _ in self.rows)


def _is_trimmed_envelope(body):
    return (isinstance(body, dict)
            and set(body) == {"code", "message", "ttl", "data"}
            and isinstance(body.get("data"), dict)
            and set(body["data"]) == {"bvid", "aid", "stat"})


def cmd_conformance(args):
    batch_url = resolve_batch_url(args)
    svc = make_service(args, batch_url=batch_url)
    chk = Check()

    # good aids: from --aids, else harvest a few from newlist
    if args.aids:
        good = _load_aids(args.aids)[:args.good_count]
    elif "get_newlist" in svc.endpoints:
        nl = svc.get_newlist({"tid": 30, "pn": 1, "ps": 50}, retry=2, timeout=15.0)
        good = [arc.aid for arc in nl.archives][:args.good_count]
    else:
        raise SystemExit("conformance needs --aids or get_newlist in endpoints")
    if len(good) < 3:
        raise SystemExit("need at least 3 good aids")
    print(f"batch worker: {batch_url.split('//')[-1].split('.')[0]}...(redacted)")
    print(f"good aids: {good}")

    # --- C1: envelope shape, order, echo, v/requested ---
    items = svc.get_video_view_trimmed_batch(good)
    chk.record("C1 client returns one item per aid, in order",
               [i.aid for i in items] == good)

    # raw view of the same call for envelope-level assertions
    st, body, _ = _raw_get(batch_url, ",".join(str(a) for a in good))
    chk.record("C1 raw: HTTP 200 on a valid batch", st == 200, f"status={st}")
    chk.record("C1 raw: v == 1 (int)",
               isinstance(body, dict) and body.get("v") == 1 and isinstance(body["v"], int))
    chk.record("C1 raw: requested == len(aids)",
               isinstance(body, dict) and body.get("requested") == len(good))
    results = body.get("results") if isinstance(body, dict) else None
    chk.record("C1 raw: results is a list, same length",
               isinstance(results, list) and len(results) == len(good))
    if isinstance(results, list) and len(results) == len(good):
        chk.record("C1 raw: each item echoes its raw aid token in order",
                   all(r.get("aid") == str(a) for r, a in zip(results, good)))
        chk.record("C1 raw: every item carries numeric ms",
                   all(isinstance(r.get("ms"), (int, float)) for r in results))

    # --- C2: healthy item -> trimmed envelope + identity ---
    ok_items = [i for i in items if i.view is not None]
    chk.record("C2 at least one healthy item among good aids", len(ok_items) >= 1,
               f"{len(ok_items)}/{len(good)} healthy")
    if isinstance(results, list):
        healthy_raw = [r for r in results if r.get("kind") == "json"
                       and r.get("status") == 200
                       and isinstance(r.get("body"), dict)
                       and r["body"].get("code") == 0]
        chk.record("C2 raw: healthy body is the trimmed envelope (exact key set)",
                   healthy_raw and all(_is_trimmed_envelope(r["body"]) for r in healthy_raw))
        chk.record("C2 raw: trimmed stat has all required keys",
                   healthy_raw and all(
                       all(k in r["body"]["data"]["stat"] for k in STAT_KEYS)
                       for r in healthy_raw))
    for it in ok_items:
        assert isinstance(it.view, VideoViewTrimmed)
    chk.record("C2 client: bvid == 'BV'+a2b(aid) for every healthy item",
               all(it.view.bvid == "BV" + a2b(it.aid) for it in ok_items))
    chk.record("C2 client: data.aid == requested aid",
               all(it.view.aid == it.aid for it in ok_items))
    chk.record("C2 client: stat.aid in {aid, 0}",
               all(it.view.stat.aid in (it.aid, 0) for it in ok_items))

    # --- C3: upstream error code passes through UNMODIFIED (not trimmed) ---
    dead_aid = 999999999999999
    st, body, _ = _raw_get(batch_url, str(dead_aid))
    r0 = (body or {}).get("results", [{}])[0]
    chk.record("C3 nonexistent aid -> kind json, code != 0",
               r0.get("kind") == "json" and isinstance(r0.get("body"), dict)
               and r0["body"].get("code") != 0,
               f"code={((r0.get('body') or {}).get('code'))}")
    chk.record("C3 error body is NOT trimmed (passed through unmodified)",
               isinstance(r0.get("body"), dict) and not _is_trimmed_envelope(r0["body"]))
    # client routes it as CodeError, same as the single path
    citems = svc.get_video_view_trimmed_batch([dead_aid])
    chk.record("C3 client routes nonexistent aid as CodeError",
               isinstance(citems[0].error, CodeError) and citems[0].view is None)

    # --- C4: non-numeric token forwarded as-is, upstream verdict passes through ---
    st, body, _ = _raw_get(batch_url, "notanumber")
    r0 = (body or {}).get("results", [{}])[0]
    chk.record("C4 non-numeric token echoed verbatim", r0.get("aid") == "notanumber")
    chk.record("C4 non-numeric token: worker still 200, item resolved per-item",
               st == 200 and r0.get("kind") in ("json", "non_json"))

    # --- C5: duplicate tokens fetched independently, echoed 1:1 ---
    dup = good[0]
    st, body, _ = _raw_get(batch_url, f"{dup},{dup},{dup}")
    rs = (body or {}).get("results", [])
    chk.record("C5 duplicate tokens -> 3 items, all echo the same aid",
               len(rs) == 3 and all(r.get("aid") == str(dup) for r in rs))

    # --- C6/C7: missing / empty aids -> HTTP 400 ---
    st, body, _ = _raw_get(batch_url, None)
    chk.record("C6 missing aids -> HTTP 400 {v:1, error:/aids/}",
               st == 400 and isinstance(body, dict) and body.get("v") == 1
               and "aids" in str(body.get("error", "")), f"status={st}")
    st, body, _ = _raw_get(batch_url, "")
    chk.record("C7 empty aids -> HTTP 400", st == 400, f"status={st}")
    # client turns a whole-batch 400 into a ResponseError (kill-switch food)
    try:
        svc.get_video_view_trimmed_batch([])
        chk.record("C7 client: empty aid list -> ResponseError", False, "no error raised")
    except ResponseError:
        chk.record("C7 client: empty aid list -> ResponseError", True)
    except Exception as e:  # noqa: BLE001
        chk.record("C7 client: empty aid list -> ResponseError", False, f"got {type(e).__name__}")

    # --- C8: > MAX_AIDS -> HTTP 400, and it tells us the deployed MAX_AIDS ---
    probe_n = args.max_aids_probe
    st, body, _ = _raw_get(batch_url, ",".join(str(a) for a in range(probe_n + 1)))
    err = str((body or {}).get("error", ""))
    chk.record(f"C8 {probe_n + 1} tokens -> HTTP 400 'too many aids'",
               st == 400 and "too many aids" in err, f"status={st} error={err!r}")
    # --- C9: exactly MAX_AIDS -> HTTP 200 ---
    st9, body9, _ = _raw_get(batch_url, ",".join(str(a) for a in good[:1] * probe_n))
    chk.record(f"C9 exactly {probe_n} tokens -> HTTP 200",
               st9 == 200 and isinstance(body9, dict) and body9.get("requested") == probe_n,
               f"status={st9}")

    # --- C10: single-aid worker answering ?aids= is caught as MisalignmentError ---
    if "get_video_view_trimmed" in svc.endpoints:
        single_url = svc.endpoints["get_video_view_trimmed"]["workers"][0]
        misaligned_ep = dict(svc.endpoints)
        misaligned_ep["get_video_view_trimmed_batch"] = {"direct": "invalid://x",
                                                         "workers": [single_url]}
        mis_svc = Service(mode="worker", retry=1, timeout=8.0, endpoints=misaligned_ep)
        try:
            mis_svc.get_video_view_trimmed_batch(good[:2])
            chk.record("C10 batch key pointed at single-aid worker -> Misalignment/Response",
                       False, "no error raised")
        except (MisalignmentError, ResponseError) as e:
            chk.record("C10 batch key pointed at single-aid worker -> Misalignment/Response",
                       True, type(e).__name__)
        except Exception as e:  # noqa: BLE001
            chk.record("C10 batch key pointed at single-aid worker -> Misalignment/Response",
                       False, f"got {type(e).__name__}")

    print()
    print(f"CONFORMANCE: {'ALL PASS' if chk.ok else 'FAILURES'} "
          f"({sum(1 for _, ok, _ in chk.rows if ok)}/{len(chk.rows)})")
    if args.json_out:
        Path(args.json_out).write_text(json.dumps(
            {"ok": chk.ok, "checks": [{"name": n, "pass": o, "detail": d}
                                      for n, o, d in chk.rows]}, indent=2))
    return 0 if chk.ok else 1


# --------------------------------------------------------------------------
# perf driver
# --------------------------------------------------------------------------

def _one_batch(svc, aids):
    t0 = time.perf_counter()
    rec = {"n": len(aids), "t_start": time.time()}
    try:
        items = svc.get_video_view_trimmed_batch(aids)
        rec["wall_ms"] = int((time.perf_counter() - t0) * 1000)
        rec["outcome"] = "ok"
        kinds = {}
        item_ms = []
        for it in items:
            if it.view is not None:
                kinds["ok"] = kinds.get("ok", 0) + 1
            elif isinstance(it.error, CodeError):
                kinds["code_error"] = kinds.get("code_error", 0) + 1
            else:
                kinds["transient"] = kinds.get("transient", 0) + 1
        rec["item_kinds"] = kinds
    except MisalignmentError as e:
        rec["wall_ms"] = int((time.perf_counter() - t0) * 1000)
        rec["outcome"] = "misalignment"
        rec["error"] = str(e)[:300]
    except Exception as e:  # noqa: BLE001
        rec["wall_ms"] = int((time.perf_counter() - t0) * 1000)
        rec["outcome"] = "whole_batch_failure"
        rec["error"] = f"{type(e).__name__}: {str(e)[:200]}"
    return rec


def _one_batch_raw(url, aids):
    """Raw call so we can keep the per-item `ms` the worker reports (the client
    discards it). This is the E[max_N] / tail signal."""
    t0 = time.perf_counter()
    rec = {"n": len(aids), "t_start": time.time()}
    try:
        r = requests.get(url, params={"aids": ",".join(str(a) for a in aids)},
                         timeout=20, headers={"User-Agent": "tdd-batch-probe"})
        rec["wall_ms"] = int((time.perf_counter() - t0) * 1000)
        rec["http_status"] = r.status_code
        body = r.json()
        rec["v"] = body.get("v")
        rec["requested"] = body.get("requested")
        results = body.get("results", [])
        rec["item_ms"] = [it.get("ms") for it in results]
        rec["kinds"] = [it.get("kind") for it in results]
        rec["item_status"] = [it.get("status") for it in results]
        rec["max_item_ms"] = max((m for m in rec["item_ms"] if isinstance(m, (int, float))),
                                 default=None)
        rec["outcome"] = "ok" if r.status_code == 200 else "http_error"
    except Exception as e:  # noqa: BLE001
        rec["wall_ms"] = int((time.perf_counter() - t0) * 1000)
        rec["outcome"] = "exception"
        rec["error"] = f"{type(e).__name__}: {str(e)[:200]}"
    return rec


class _RateLimiter:
    def __init__(self, per_sec):
        self.min_gap = 1.0 / per_sec if per_sec and per_sec > 0 else 0.0
        self._lock = threading.Lock()
        self._next = 0.0

    def wait(self):
        if not self.min_gap:
            return
        with self._lock:
            now = time.perf_counter()
            if now < self._next:
                time.sleep(self._next - now)
                now = time.perf_counter()
            self._next = max(now, self._next) + self.min_gap


def cmd_perf(args):
    batch_url = resolve_batch_url(args)
    aids_pool = _load_aids(args.aids)
    if len(aids_pool) < args.n:
        raise SystemExit(f"aid pool ({len(aids_pool)}) smaller than --n ({args.n})")
    svc = make_service(args, batch_url=batch_url) if args.mode == "client" else None
    limiter = _RateLimiter(args.rate)

    batches = []
    for _ in range(args.batches):
        batches.append(random.sample(aids_pool, args.n))

    out = open(args.out, "w") if args.out else None
    recs = []
    stop = threading.Event()
    fail_count = [0]
    lock = threading.Lock()

    def work(aids):
        if stop.is_set():
            return None
        limiter.wait()
        if args.mode == "client":
            rec = _one_batch(svc, aids)
            bad = rec["outcome"] != "ok"
        else:
            rec = _one_batch_raw(batch_url, aids)
            bad = rec["outcome"] != "ok"
        with lock:
            if bad:
                fail_count[0] += 1
                if args.max_failures and fail_count[0] >= args.max_failures:
                    stop.set()
        return rec

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        futs = [ex.submit(work, b) for b in batches]
        for i, f in enumerate(as_completed(futs)):
            rec = f.result()
            if rec is None:
                continue
            recs.append(rec)
            if out:
                out.write(json.dumps(rec) + "\n")
                out.flush()
            if (i + 1) % max(1, args.batches // 20) == 0:
                print(f"  {i + 1}/{args.batches} batches, {fail_count[0]} failed")
    if out:
        out.close()
    elapsed = time.time() - t0
    print(f"\ndone: {len(recs)} batches in {elapsed:.1f}s "
          f"({len(recs) / elapsed:.1f} batch/s), {fail_count[0]} failed"
          + ("  [STOPPED: max-failures hit]" if stop.is_set() else ""))
    if args.out:
        _summarize(recs, args)
    return 0


# --------------------------------------------------------------------------
# parity: batch path vs single-aid path, field by field
# --------------------------------------------------------------------------

def cmd_parity(args):
    batch_url = resolve_batch_url(args)
    svc = make_service(args, batch_url=batch_url)
    if "get_video_view_trimmed" not in svc.endpoints:
        raise SystemExit("parity needs get_video_view_trimmed in the endpoints file")
    aids = _load_aids(args.aids)
    random.shuffle(aids)
    aids = aids[:args.count]
    n = args.n

    # --- batch path ---
    batch_view = {}
    batch_err = {}
    for i in range(0, len(aids), n):
        chunk = aids[i:i + n]
        try:
            for it in svc.get_video_view_trimmed_batch(chunk):
                if it.view is not None:
                    batch_view[it.aid] = it.view
                else:
                    batch_err[it.aid] = type(it.error).__name__
        except Exception as e:  # noqa: BLE001
            for a in chunk:
                batch_err[a] = f"batch:{type(e).__name__}"
        time.sleep(args.sleep)

    # --- single path (verbatim what fetch_video_record_via_video_view calls) ---
    single_view = {}
    single_err = {}
    for a in aids:
        try:
            single_view[a] = svc.get_video_view_trimmed({"aid": a}, retry=2, timeout=10.0)
        except CodeError as e:
            single_err[a] = f"CodeError({e.code})"
        except Exception as e:  # noqa: BLE001
            single_err[a] = type(e).__name__
        time.sleep(args.sleep)

    # --- compare ---
    volatile = {"view", "danmaku", "reply", "favorite", "coin", "share",
                "like", "dislike", "now_rank", "his_rank", "vt", "vv"}
    stable = {"bvid", "aid"}
    both_ok = sorted(set(batch_view) & set(single_view))
    mism_stable, mism_volatile = [], []
    for a in both_ok:
        b, s = batch_view[a], single_view[a]
        for f in stable:
            if getattr(b, f) != getattr(s, f):
                mism_stable.append({"aid": a, "field": f,
                                    "batch": getattr(b, f), "single": getattr(s, f)})
        for f in volatile:
            bv, sv = getattr(b.stat, f), getattr(s.stat, f)
            if bv != sv:
                mism_volatile.append({"aid": a, "field": f, "batch": bv, "single": sv})

    outcome_disagree = []
    for a in aids:
        bo = "view" if a in batch_view else batch_err.get(a, "missing")
        so = "view" if a in single_view else single_err.get(a, "missing")
        b_ok, s_ok = a in batch_view, a in single_view
        if b_ok != s_ok:
            outcome_disagree.append({"aid": a, "batch": bo, "single": so})

    report = {
        "aids_tested": len(aids), "batch_size": n,
        "both_view_ok": len(both_ok),
        "batch_only_view": sorted(set(batch_view) - set(single_view)),
        "single_only_view": sorted(set(single_view) - set(batch_view)),
        "stable_field_mismatches": mism_stable,
        "volatile_field_mismatches_count": len(mism_volatile),
        "volatile_field_mismatch_sample": mism_volatile[:20],
        "outcome_disagreements": outcome_disagree,
    }
    print(json.dumps({k: v for k, v in report.items()
                      if k not in ("volatile_field_mismatch_sample",)}, indent=2, default=str))
    if args.out:
        Path(args.out).write_text(json.dumps(report, indent=2, default=str))
    verdict_ok = not mism_stable and not outcome_disagree
    print(f"\nPARITY: {'OK' if verdict_ok else 'DISCREPANCY'} "
          f"(stable mismatches={len(mism_stable)}, outcome disagreements={len(outcome_disagree)}, "
          f"volatile diffs={len(mism_volatile)}/{len(both_ok)} — expected, counts move between calls)")
    return 0 if verdict_ok else 1


# --------------------------------------------------------------------------
# summarize a perf jsonl
# --------------------------------------------------------------------------

def _pctl(xs, q):
    if not xs:
        return None
    xs = sorted(xs)
    k = (len(xs) - 1) * q
    lo = int(k)
    hi = min(lo + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def _summarize(recs, args=None):
    by_n = {}
    for r in recs:
        by_n.setdefault(r.get("n"), []).append(r)
    print("\n=== summary ===")
    for nval in sorted(k for k in by_n if k is not None):
        rs = by_n[nval]
        walls = [r["wall_ms"] for r in rs if "wall_ms" in r]
        max_items = [r["max_item_ms"] for r in rs
                     if isinstance(r.get("max_item_ms"), (int, float))]
        all_items = [m for r in rs for m in r.get("item_ms", [])
                     if isinstance(m, (int, float))]
        oc = {}
        kinds = {}
        for r in rs:
            oc[r.get("outcome")] = oc.get(r.get("outcome"), 0) + 1
            for k in r.get("kinds", []):
                kinds[k] = kinds.get(k, 0) + 1
        print(f"\n N={nval}  batches={len(rs)}  outcomes={oc}")
        if kinds:
            print(f"   item kinds: {kinds}")
        if max_items:
            print(f"   E[max_N] (mean of per-batch max item ms): {statistics.mean(max_items):.0f}")
            print(f"   max item ms  p50={_pctl(max_items,.5):.0f}  p90={_pctl(max_items,.9):.0f}  "
                  f"p99={_pctl(max_items,.99):.0f}  p99.9={_pctl(max_items,.999):.0f}  "
                  f"max={max(max_items):.0f}")
        if all_items:
            print(f"   single item ms  p50={_pctl(all_items,.5):.0f}  p99={_pctl(all_items,.99):.0f}  "
                  f"p99.9={_pctl(all_items,.999):.0f}  max={max(all_items):.0f}  (n={len(all_items)})")
        if walls:
            print(f"   client wall ms  p50={_pctl(walls,.5):.0f}  p99={_pctl(walls,.99):.0f}  "
                  f"max={max(walls):.0f}")


def cmd_summarize(args):
    recs = [json.loads(l) for l in Path(args.jsonl).read_text().splitlines() if l.strip()]
    _summarize(recs)
    return 0


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def _add_url_args(p):
    p.add_argument("--url", help="explicit batch worker URL (overrides env + endpoints)")
    p.add_argument("--endpoints", default=str(DEFAULT_ENDPOINTS),
                   help="path to endpoints.json (default: %(default)s)")


def main(argv=None):
    ap = argparse.ArgumentParser(prog="tools.batch_probe", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    pc = sub.add_parser("conformance", help="contract checks against the deployed worker")
    _add_url_args(pc)
    pc.add_argument("--aids", help="file of known-good aids (else harvested from newlist)")
    pc.add_argument("--good-count", type=int, default=8)
    pc.add_argument("--max-aids-probe", type=int, default=20,
                    help="deployed MAX_AIDS (C8/C9 boundary). default 20")
    pc.add_argument("--json-out")
    pc.set_defaults(func=cmd_conformance)

    ph = sub.add_parser("harvest", help="collect real tid=30 aids via the newlist worker")
    _add_url_args(ph)
    ph.add_argument("--out", required=True)
    ph.add_argument("--count", type=int, default=5000)
    ph.add_argument("--max-pages", type=int, default=400)
    ph.add_argument("--sleep", type=float, default=0.3)
    ph.set_defaults(func=cmd_harvest)

    pp = sub.add_parser("perf", help="drive N-aid batches at controlled concurrency")
    _add_url_args(pp)
    pp.add_argument("--aids", required=True)
    pp.add_argument("--n", type=int, required=True)
    pp.add_argument("--batches", type=int, required=True)
    pp.add_argument("--concurrency", type=int, default=10)
    pp.add_argument("--rate", type=float, default=0.0, help="max batches/sec (0=unlimited)")
    pp.add_argument("--mode", choices=["raw", "client"], default="raw",
                    help="raw keeps per-item ms; client exercises the real parser")
    pp.add_argument("--max-failures", type=int, default=0, help="stop after this many bad batches")
    pp.add_argument("--out")
    pp.set_defaults(func=cmd_perf)

    pr = sub.add_parser("parity", help="batch path vs single-aid path, field by field")
    _add_url_args(pr)
    pr.add_argument("--aids", required=True)
    pr.add_argument("--count", type=int, default=1000)
    pr.add_argument("--n", type=int, default=20)
    pr.add_argument("--sleep", type=float, default=0.05)
    pr.add_argument("--out")
    pr.set_defaults(func=cmd_parity)

    ps = sub.add_parser("summarize", help="summarize a perf .jsonl")
    ps.add_argument("jsonl")
    ps.set_defaults(func=cmd_summarize)

    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
