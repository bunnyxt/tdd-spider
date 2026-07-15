# Agent guide

Guidance for AI agents (Claude Code and others) working in this repository.

## Reports / knowledge base

`docs/reports/` holds dated, point-in-time work summaries — the project's
running memory of *why* things changed, not just what.

- **Before significant work:** skim recent reports in `docs/reports/` for
  context on how the current state came to be.
- **After significant work** — a milestone, a multi-PR arc, or an incident
  worth remembering — write a new report there. Name it
  `YYYY-MM-DD_short-title.md` (date first, so the folder sorts chronologically).
- Reports are **immutable snapshots.** Don't edit an old report to reflect new
  developments; write a new one. Point-in-time is the whole value.
- Keep the bar **high.** A report per milestone/incident is a knowledge base;
  a report per session is noise.
- **No secrets in reports:** no server IPs, worker/endpoint URLs, credentials,
  or tokens. Describe infrastructure generically ("the prod server", "the
  worker Lambda").

## Repository notes

- Scripts are numbered by role (`12_`, `51_`, …) and run from cron on the prod
  server. `51_hourly-video-record-add.py` is the hourly stats pipeline.
- Shared code lives in `job/`, `service/`, `task/`, `db/`, `util/`, `core/`.
- The prod server **cannot reach GitHub** — deployment is manual copy + blob-hash
  verification, not `git pull`. Its git history is intentionally diverged.
- `service/endpoints.json` (worker URLs) and `conf/conf.ini` (DB credentials)
  hold environment secrets and are git-ignored (see `.gitignore`) — never commit
  them. Their `.example` siblings are the tracked templates.
