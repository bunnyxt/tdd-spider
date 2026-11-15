# PostgreSQL maintenance jobs

Run these independently from the collectors with Bash and PostgreSQL 18's
`psql`. They do not install functions or create roles. The schema installation
must already provide:

- `public.tdd_ensure_history_partitions(regclass,timestamptz,timestamptz)`:
  create and validate the whitelisted history partitions; never delete history.
- `public.tdd_refresh_random_pool(text)`: stage eligible candidates, validate
  contiguous IDs, then publish with transactional `DELETE` + `INSERT`. Its
  arguments are `member` and `video`. Concurrent refresh of one pool must fail;
  an empty eligible set is published with a warning.
- `tdd_ingest.control`, configured with the same mode/run as the collectors.

Both functions must run with invoker permissions. Use a **separate maintenance
connection**, selected by a libpq service (`PGSERVICE`), with credentials stored
in a protected service/passfile. Do not elevate the collector role. The
maintenance role needs `USAGE` on `public` and `tdd_ingest`, `SELECT` on control,
and `EXECUTE` on the two functions. Pool refresh additionally needs `SELECT` on
video/member and their relevant latest tables, plus `SELECT`, `DELETE`, and
`INSERT` on the two pools and database `TEMPORARY` permission. Partition creation
requires ownership of the three partitioned parents and `CREATE` on `public`;
use an owner/maintenance role for that job, never grant this to the collector.
An administrator can use separate libpq services for pool and partition jobs.
These jobs need no permission to change control.

From the checkout root (the wrapper resolves its SQL independently of the
working directory):

```sh
PGSERVICE=tdd-maintenance bash scripts/postgresql/run-maintenance.sh refresh-member-pool
```

During a trial also set `TDD_TRIAL_RUN_ID` to the active UUID. Each job takes the
shared migration lock before reading control in READ COMMITTED, holds it through
commit, and rejects a missing/stopped control, a mismatched trial, or a trial ID
in formal mode. It does not silently initialize or change control. Migration
replacement must hold the matching exclusive lock `(141376009,169)`.

Each invocation logs UTC `START`, SQL results, and `SUCCESS` only after commit.
A SQL, connection, or configuration error logs `ERROR` and exits nonzero; a
failed refresh rolls back and keeps the old pool. Configure restricted log
directories and rotation outside the script. There is no polling, dependency
DAG, database scheduler extension, or automatic retry.

The example crontab schedules member refresh at 05:45 and video refresh at
09:45 Asia/Shanghai. Partition precreation is at the initial maintenance slot
02:35, subject to runtime load verification. It keeps the current plus next
three UTC months and the current plus next UTC year writable. Configure and
verify the cron scheduler timezone before installing; `TZ=` alone is not a
portable scheduling setting. Run each job manually first and verify a natural
scheduled run before treating deployment as complete. This directory does not
install or enable any cron entry automatically.
