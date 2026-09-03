# Agent guide

Guidance for AI agents (Claude Code and others) working in this repository.

## Work summaries

Dated work summaries and incident write-ups are **not kept in this repo.** They
live in a private knowledge base outside it, because in practice they accumulate
infrastructure detail — account IDs, endpoint URLs, cost figures, hostnames —
that shouldn't sit in a public repository.

Don't create `docs/reports/`. If a milestone or incident is worth recording, ask
the user where to write it.

## Public metadata hygiene

Treat this repository and its GitHub activity as public. Branch names, commit
messages, pull-request titles and bodies, review comments, and issue text must
stand on repository-visible evidence alone. Do not mention private planning
systems, local absolute paths, private session links, server aliases, or
internal deployment records. Refer to private context only in the private
system where it lives.

This restriction also applies to every tracked file, including source comments,
docstrings, tests, fixtures, examples, and repository documentation. Never add
private ticket IDs (such as `BL-...`), knowledge-base paths, local absolute
paths, agent/session links, or internal planning notes to tracked content.

Before committing or opening a pull request, inspect the added lines in the
public diff for private-context leaks. Use an equivalent check when the base is
not `origin/master`:

```bash
git diff --unified=0 origin/master...HEAD \
  | rg '^\+' \
  | rg 'BL-[0-9]+|/Users/|Projects/kb|Projects/bl|claude\.ai|private (backlog|ticket)|session link'
```

Any match must be reviewed and removed unless the matched text is itself part
of this public-hygiene instruction. Existing historical matches do not justify
adding new ones; keep checks scoped to the current diff rather than treating
the whole repository as clean.

## Repository notes

- Scripts are numbered by role (`12_`, `51_`, …) and run from cron on the prod
  server. `51_hourly-video-record-add.py` is the hourly stats pipeline.
- Shared code lives in `job/`, `service/`, `task/`, `db/`, `util/`, `core/`.
- The prod server **cannot reach GitHub** — deployment is manual copy + blob-hash
  verification, not `git pull`. Its git history is intentionally diverged.
- `service/endpoints.json` (worker URLs) and `conf/conf.ini` (DB credentials)
  hold environment secrets and are git-ignored (see `.gitignore`) — never commit
  them. Their `.example` siblings are the tracked templates.

## Tests

- Committed tests live in `tests/`. Run them from the repo root:
  `venv-3.11/bin/python -m unittest discover -s tests` (stdlib `unittest`, no
  extra deps; a plain `python` works too where `venv-3.11` isn't set up).
- Worker (`.mjs`) tests are Node-based and live in `tests/` too. Run them from
  the repo root with `node --test tests/` (built-in `node:test`, no deps,
  Node ≥ 18; upstream traffic is mocked, no network needed).
- Root-level `test-*.py` / `test-*.ipynb` / `test.py` are throwaway scratch and
  are git-ignored — don't put committed tests there, and don't rely on them.
