# card workers

Purpose-built workers for `get_member_card`. **Do not deploy the generic `../template/` files to
this endpoint** — these differ from the templates in two ways that matter.

## Why this endpoint is special

`member_card` is the only endpoint bilibili rate-limits. It is the only one that returns
`code: -352`, and it is the only endpoint deployed to more than one platform:

| Endpoint | Workers |
|---|---|
| `get_member_card` | **4** — AWS Function URL, AWS API Gateway, GCP Cloud Run, Azure Functions |
| everything else | 1 — AWS only, which is sufficient |

The multi-platform spread exists to spread requests across source IP pools. Note that the two AWS
URLs are one Lambda behind two front doors, so AWS carries ~50% of the traffic, not 25%.

## How these differ from `../template/`

1. **`baseUrl` is pinned to `http://api.bilibili.com/x/web-interface/card`.** The templates ship a
   placeholder meant to be edited per deployment; these are not templates and should not be edited.
2. **The caller's `User-Agent` is forwarded upstream.** The generic templates call
   `fetch(url.href)` with no headers, so bilibili sees a bare runtime UA.

## Deploy targets

| File | Deploys to |
|---|---|
| `aws_lambda.mjs` | Lambda `bilibili-member-card` (serves **both** the Function URL and the `tdd-worker-API` HTTP API) |
| `gcp_cloud_run.js` | Cloud Run `bilibili-member-card` |
| `azure_function_app.js` | Azure Functions `bilibili-member-card` |

There is no Cloudflare deployment.

## Verify after deploying

A worker pointed at the wrong upstream still returns HTTP 200 with `code: 0` — it just returns the
wrong payload, and **cloud monitoring cannot see this**. Lambda's `Errors` metric counts crashes and
timeouts, not wrong answers. Check the payload, not the dashboard:

```bash
curl -s 'https://<worker-url>?mid=1635775' \
  -H 'User-Agent: Mozilla/5.0' | python3 -m json.tool | head -20
```

Expect `code: 0` and `data.card.name == "牛奶源"`. If you see `data: {archives, page}`, the
`baseUrl` is pointing at the newlist endpoint.

## Known limitation: `-352` is not solved

Forwarding the User-Agent does **not** solve it. Measured 2026-08-09 — sequential requests succeed
regardless of UA (18 requests across modern, legacy, and mobile UAs, all `code: 0`), while
`16_update-member-info.py` at 50 concurrent workers hits `-352` on essentially every request.
**The throttle is rate-based, not fingerprint-based.**

## Testing a proposed fix — `probe_throttle.py`

Use it before touching the production job. It reproduces the real failure in 45 seconds.

```bash
python service/workers/card/probe_throttle.py --sweep 1,5,10,20,50   # find the ceiling
python service/workers/card/probe_throttle.py --per-worker --sweep 20 # per-platform or global?
python service/workers/card/probe_throttle.py --sweep 50 --pace 0.5   # does pacing help?
```

The number that matters is **`settled/s`** — sustained successful requests/sec in the final third
of the window, once the burst allowance is spent. The throttle lets a burst through before
clamping, so short or sequential probes look perfect and prove nothing. That is precisely how the
2026-08-09 deploy was verified as "all four workers correct" minutes before the job collapsed.

Baseline, all four workers:

| conc | reqs | ok | −352 | burst | 1st −352 | settled/s | verdict |
|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 74 | 67 | 0 | 67 | never | 2.50 | 2.8 h for 24,931 members |
| 10 | 881 | 772 | 43 | 723 | 27.1 s | 25.40 | 0.3 h |
| 50 | 6192 | 63 | 6115 | 0 | 0.5 s | **0.00** | **never finishes** |

conc=50 is what the job uses, and 0.00/s is exactly what it did: stalled at 288 members.

**Leave 2–5 minutes between runs.** The throttle persists after a run ends. The conc=50 row above
shows `burst=0` because it followed conc=10 immediately — treat a burst of 0 as leftover throttling,
not a reading.
