# Agent guide

Guidance for AI agents (Claude Code and others) working in this repository.

## Work summaries

Dated work summaries and incident write-ups are **not kept in this repo.** They
live in a private knowledge base outside it, because in practice they accumulate
infrastructure detail — account IDs, endpoint URLs, cost figures, hostnames —
that shouldn't sit in a public repository.

Don't create `docs/reports/`. If a milestone or incident is worth recording, ask
the user where to write it.

## Repository notes

- Scripts are numbered by role (`12_`, `51_`, …) and run from cron on the prod
  server. `51_hourly-video-record-add.py` is the hourly stats pipeline.
- Shared code lives in `job/`, `service/`, `task/`, `db/`, `util/`, `core/`.
- The prod server **cannot reach GitHub** — deployment is manual copy + blob-hash
  verification, not `git pull`. Its git history is intentionally diverged.
- `service/endpoints.json` (worker URLs) and `conf/conf.ini` (DB credentials)
  hold environment secrets and are git-ignored (see `.gitignore`) — never commit
  them. Their `.example` siblings are the tracked templates.
