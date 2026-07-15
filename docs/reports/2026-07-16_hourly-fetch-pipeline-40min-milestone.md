# 2026-07-16 — Hourly video-record pipeline: the 40-minute full-scan milestone

**Goal achieved:** the 04:00 daily full scan (~1.09M videos) now completes in **32m 47s**, inside the 40-minute window, with zero data loss. It previously took over an hour and often did not finish at all.

Work spanned 2026-07-10 → 2026-07-16 on `51_hourly-video-record-add.py` and the shared `job/` + `service/` layers, in two phases: a **foundation** that made the per-video path correct, observable, and lean (PRs #18–#28), and a **performance push** that took it to the milestone (PRs #29–#35). This is the archival summary; per-change detail lives in the PRs.

---

## Where we started

### The original mechanism: records as a side effect of the "comprehensive" crawl

Originally, C30 records were **not fetched directly**. They came out as a *byproduct* of the `process_comprehensive` path, which paged the entire tid=30 category listing via the bulk **newlist API** (one request returned 50 videos' stats), built a big in-memory dict of every archive it saw, and then joined that dict against the set of videos due for an update. A video's hourly record was whatever stat happened to appear in that crawl; videos the crawl didn't surface fell to a secondary check job.

This had structural problems that made it "way slower" and unreliable as the video count grew toward 1M:

- **It was only as good as the bulk API.** The newlist endpoint degraded over time — incomplete pages, a documented **~20% of videos missed**, and eventually returning empty (`count = 0`) altogether. Record collection was hostage to an API we didn't control and that was decaying.
- **Coupled discovery to recording.** Because records rode along with the listing crawl, you couldn't record a known video without re-crawling the whole category — and anything the crawl dropped simply got no record that hour.
- **Whole-listing work regardless of need.** It paged and materialized the entire category every run, then filtered, rather than fetching exactly the videos that were due.

When the bulk API finally broke for good, the pipeline fell back to `process_simple` — a direct, per-video fetch of the known set from our own DB. That fallback is the honest baseline this work started from, and even it was slow and unstable:

- **~200 videos/sec that collapsed to 15–40/sec** partway through most runs.
- **The full scan did not finish.** A pre-optimization 04:00 run took **~1h 1m and got through only 434k of ~999k videos (43%)**; later runs hit the 40-minute cap having fetched ~615k. The comprehensive era never reliably completed a full scan either.
- **Silent data loss:** a failed batch insert dropped up to 1,000 records with only a log line.

### The pivot

The whole effort is a move **from "discover-and-record via a decaying bulk API" to "directly fetch exactly the videos we need, fast."** The comprehensive path was deleted; the per-video path was made fast enough (via everything below) that its 50-videos-per-request efficiency is no longer missed — one trimmed request is ~250 bytes, and 250 of them run in parallel.

The end state: **~560 videos/sec sustained, no mid-run collapse, no data loss, a full scan that finishes in 33 min, and a positively-clean run summary** (every failure counter explicitly reads 0).

| | Comprehensive era (bulk newlist) | Per-aid fallback (baseline) | Now |
|---|---|---|---|
| mechanism | crawl whole category, record as byproduct | direct per-video fetch, unoptimized | direct per-video fetch, optimized |
| depends on | the (decaying) bulk newlist API | — | trimmed worker + own DB |
| completeness | ~20% of videos missed | truncated (43–62% at 04:00) | 100% |
| full scan | never reliably finished | 1h+, did not finish | **32m 47s, complete** |
| throughput | (bottlenecked on one bulk endpoint) | ~200/s → collapses to 15–40/s | ~560/s sustained |

---

## Foundation: the groundwork that made the push possible (PRs #18–#28)

Before any speed work, the per-video path had to become **correct, observable, and lean**. When the bulk newlist API broke, `run()` still fell into `process_comprehensive`, which funneled every video through an edge-case recovery job — so the first job was to make the direct path trustworthy and cheap. These changes, roughly in order:

### Correctness first — the pipeline was lying about failure

- **#18 — stop the DB session cascade.** The C30 phase was logging a **~92% failure rate**, which looked like the API was dead. It wasn't. Each worker reused one DB session for its whole loop; when a DB op hit a transient "too many connections", the session was left in an invalid-transaction state and **never rolled back**, so every subsequent video on that worker failed. One blip poisoned a worker for the rest of the run. **Before: 60,745 `update_exception`/hour (57,952 of them — 95% — were cascading `InvalidRequestError`, not real failures). After: rollback-on-failure → near-zero.** The failures had also been mislabeled "video not in table," hiding the real cause. This is the fix that made every later measurement trustworthy.
- **#23 — return a lightweight record, confine the ORM to the INSERT.** The 02:00 run had been writing **empty CSVs**: ORM row objects were crossing the worker→writer thread boundary and dying with `DetachedInstanceError`. Fixed by having the fetch return a plain immutable record (`RecordNew`) and keeping ORM objects inside the persistence call only. Prerequisite for the fetch/write split (#28).

### The strategic pivot — leave the comprehensive crawl behind

- **#21 — view-only per-video fetch when the bulk API is empty (Phase 0).** The bulk newlist returned `count: 0` **without raising**, so `run()` always took `process_comprehensive`, which then funneled **every** video through the recovery job — fetching **view + tags + staff metadata** per video. That path threw **~5,000 `video_tags` errors/hour** (a flaky endpoint) which *aborted records whose stats had already been fetched*, and cost a second HTTP round-trip per video for data the record never uses. **After: route to `process_simple` (one view call per video, no tags/metadata) when bulk is empty → tags errors ~5k/hr → ~0, and one API call instead of two.** This is the moment the pipeline started fetching directly instead of as a crawl byproduct.

### Observability — you can't optimize what you can't see

- **#19 / #20 — `JobPool` with a per-second progress heartbeat**, unified across all pool sites; C30 worker cap raised to 150. Gave every run a live `PROGRESS <label>: N done, M/s` line and merged per-pool stats.
- **#27 — per-video stage-timing instrumentation.** An opt-in DEBUG log recording per-request timing, plus live per-stage averages in the INFO log. **This is what made the entire performance push possible** — it turned "it feels slow" into "db insert is 157ms/record, p50 17ms / p90 458ms," which pointed straight at commit contention rather than row cost.

### Leanness — remove per-request waste and dead weight

- **#24 — HTTP keep-alive via a pooled session.** `_get` did a **fresh TCP+TLS handshake on every request** — ~100 handshakes/sec across the workers, adding latency and outbound bytes on the very link that's the throughput ceiling. **After: one pooled, thread-safe session; handshakes → near-zero.**
- **#28 — fetch/write split with batched inserts.** Profiling (via #27) showed **db insert+commit at 157ms/record — 21% of per-video time — but p50 17ms vs p90 458ms**: the cost was commit/fsync **contention** from 150+ workers each committing individually, not the row itself. Split into fetch-only workers feeding **one batched writer** (multi-row INSERT, one commit per 1000). **Before: ~200/s, ~200 concurrent MySQL sessions. After: ~250/s, and concurrent sessions dropped to a couple** (fetch workers no longer held connections). This split is the backbone the entire performance push builds on — and, later, the thing whose one remaining DB-touching corner caused the deadlock.
- **#25 — cap C0 fetch at 40 min.** A prior 04:00 had overrun to **1h 52m** because the C0 pipeline had no time limit.
- **#26 — remove `RecordsSaveToDbRunner`.** A dead feature writing a since-dropped hourly table via a single-threaded second copy (~25 min, silently failing mid-save). Removing it reclaimed disk and deleted a whole silent-failure surface.

**Net effect of the foundation:** the direct per-video path went from a 92%-failing, empty-CSV-producing, tags-error-storming byproduct of a broken crawl, to a correct, instrumented, ~250/s pipeline holding ~2 DB connections — the platform the milestone push then optimized.

---

## The performance push (PRs #29–#35)

Each item is a merged PR. They build on the fetch/write-split foundation (#28) that moved persistence off the fetch workers into a batched writer.

### 1. Wall-clock request deadline (#29)
**Problem:** runs degraded 10–17× within minutes while the *median* request stayed fast. Root cause: the HTTP client's `timeout` bounds the gap between socket reads, not the total request. A response that trickled bytes never timed out — single requests ran 100+ seconds, each parking one of the fetch workers. A handful of these progressively ate the whole pool (a few 60–120s requests occupy a worker 100× longer than a normal one).
**Fix:** stream the body and enforce a real wall-clock budget, scaled by the declared response size (so genuinely large payloads aren't killed mid-transfer). Abandon and retry anything over budget.
**Result:** the tail-latency collapse disappeared. First full scan with the fix went from 1h+ to 43m.

### 2. Trimmed worker endpoint (#30)
**Problem:** the record path needs ~250 bytes per video (id + ~13 stat counters), but the upstream view API returns 200KB–2.8MB for season/multi-part videos. That bloat saturated the server's constrained *inbound* link and caused most of the deadline-tail requests.
**Fix:** a dedicated worker that fetches the full payload upstream (on a fat pipe) and returns only the fields the record path consumes. A 1.47MB response trims to 236 bytes. Error semantics pass through unchanged.
**Result:** per-video HTTP time dropped ~30%, inbound fell from 22–45 Mbps to 5–10 Mbps, and the oversized-payload tail ceased to exist for this path.

### 3. More fetch workers, and finding the real ceiling (#31)
**Change:** 150 → 250 fetch workers. Also fixed a latent footgun — the HTTP connection-pool size was fixed while every worker hits one host, so past 256 workers keep-alive would silently break (a TLS handshake per request). Pool size now derives from the worker count.
**Result:** ~335/sec → ~537/sec — and it revealed the hard ceiling: the server's **~3 Mbps outbound** saturates at 250 workers (request headers + ACKs, ~500 B/request). More workers past this point do not help; they only deepen the queue and inflate latency. This is now the binding constraint.

### 4. Writer durability + backpressure (#32)
**Problem:** the higher fetch rate made the single batch writer the bottleneck. Under load a 1000-row insert hit the DB timeout, the retry failed, and the whole batch was dropped — silent DB data loss.
**Fix:** a failing batch is **split and retried recursively** (1000 → 500 → …) so one bad row costs one record, not a thousand. Anything still failing at a single row is written to a dated recovery CSV instead of vanishing. The fetch→write queue is bounded, converting hidden memory growth into visible backpressure.
**Result:** verified in production — a batch failed, split cleanly, and lost nothing.

### 5. The deadlock — and the structural fix (#33, #34)
**Incident (2026-07-15 04:00):** the full scan hung for 90+ minutes at 72.6% and had to be killed. This was a self-inflicted regression: #31 and #32 combined into a circular wait.

- Fetch workers opened a DB connection on the rare "video deleted/hidden" path and **held it for the worker's whole life.** The full scan hit ~2,200 of these spread across all 250 workers, so nearly every worker pinned a connection — 250 workers against a 200-connection pool.
- The pool exhausted → the batch writer couldn't get a connection to drain the queue → the (now bounded) queue filled → every fetcher blocked forever trying to enqueue, **while holding the connections the writer needed.** Deadlock.
- The previously-unbounded queue had been *masking* the connection hoarding; bounding it closed the loop.

**Fix, in two parts:**
- **#33 (stop the bleeding):** the deleted-video path uses a short-lived session, closed immediately; enqueue operations can no longer block forever.
- **#34 (remove the cause):** fetch workers now do **zero DB work** — they don't even import the session type. A deleted/hidden video is handed to a small, bounded pool of update workers that run concurrently and carry the same 40-minute cap. DB concurrency from the fetch tier went from "up to 250" to exactly zero.

**Result:** the 2026-07-16 full scan processed **2,230 deleted-video events** — the exact traffic that deadlocked the box the day before — as a complete non-event. All resolved, zero exceptions, no impact on fetch throughput.

### 6. Observability cleanup (#34, #35)
- Each run emits three job summaries (fetch / writer / updater); they're now **labeled** and **seed their failure counters to 0**, so a clean run *asserts* it was clean rather than just omitting the counter. Added `duration_limit_reached` to the summary — the one-line verdict on whether a scan finished in-window.
- **#35:** CJK titles/descriptions were being logged as `\uXXXX` escapes. A log formatter misnamed "unescape" was actually escaping all non-ASCII. Now it collapses only line-breaking characters (keeping one record per line for grep) and leaves Chinese/Japanese text readable; log files write as UTF-8.

---

## The milestone run (2026-07-16 04:00)

```
997,715 C30 videos fetched in ~29 min (~560/s)   duration_limit_reached: 0
  995,500 records written        batch_insert_fail: 0    record_dropped_queue_full: 0
  2,215 deleted-video events  →  2,215 updated           update_exception: 0
   90,474 C0 videos            +  15 updated              all failure counters 0
total run: 32m 47s   (40-min cap, ~7 min headroom)
```

---

## Where the ceiling is now

Throughput is **network-bound on ~3 Mbps outbound**, saturated at 250 workers (measured: tx pinned 3.0–3.35 Mbps for a full fetch phase). More workers will not help — the pool is already ~100% busy (250 ÷ 0.44s/fetch ≈ 567/s, matching the ~560/s measured). If more throughput is ever needed, the levers are **fewer bytes per request** (trim the User-Agent / hostname) or **a fatter uplink** — not more threads.

## Remaining / parked

- **C0 + C30 pipeline merge:** the two pipelines are now identical except for a `tid == 30` vs `tid != 30` filter — a partition that Bilibili's retirement of the tid=30 category has made meaningless. A plan exists to merge them into one pool (recovers the ~16 min C0's workers currently sit idle at 04:00). Not yet implemented.
- **Discovery for non-tid-30 videos:** as Bilibili AI-assigns categories, new virtual-singer videos increasingly won't be tid=30. The routine discovery script still only polls the tid=30 list. This is the real long-term problem and is partly a product question (how to decide a video is in-scope). The evocalrank-based discovery script is an existing tid-free template.
- **DB writer parallelism:** a single batch writer is adequate now (writer time is ~0/record at quiet hours) but was the bottleneck under the 08:00 load spike; the merge plan folds in multiple writers.

---

## Method notes (reusable)

The diagnostic loop that drove every fix: pull the run log (gzip + copy down), grep the per-pool stage-timing summaries and per-minute progress, and — when a latency mystery appeared — turn on the opt-in per-request DEBUG log and compute **percentiles**, not averages. Every real finding this week (median fast while p99 exploded; 1,974 of 1,975 deadline hits being oversized payloads; keep-alive holding) was invisible in averages and obvious in percentiles. System-stat sampling (network tx/rx, load, memory) correlated throughput dips to the outbound saturation that turned out to be the ceiling.

Deployment is manual copy-to-server + blob-hash verification (the prod host can't reach the git remote), run per-file after each merge.
