# PostgreSQL collector runtime

Use Python 3.12 and the locked dependencies in `requirements.txt`:

```sh
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
cp conf/conf.ini.example conf/conf.ini
```

Configure `[db_postgresql]` in `conf/conf.ini`. `TDD_CONFIG_PATH` selects a
different INI file; that selected file is the only source of Python collector
database settings. `dbname`, `user` and `password` must be nonempty. The defaults
are host `127.0.0.1`, port `5432`, SSL mode `prefer`, no separate `hostaddr`, and
connection timeout 10 seconds (configurable as `connect_timeout`, 1–300 seconds).
SSL settings and connection `options` also belong in this section. Protect the
file because it contains credentials.

`PGHOST`, `PGHOSTADDR`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`, `PGOPTIONS` and the
other environment values do not override configured/defaulted collector
settings. If an additional libpq environment variable would supply an
unspecified setting, configuration fails with its name; unset it or configure
that setting in the INI. Named libpq services are not used by the collector:
unset `PGSERVICE` and do not add `service` to this section. This restriction is
for the Python collector; the separate `psql` maintenance commands retain
their documented authentication setup.

Connections open only when needed; all collector threads share a pool capped
at four database connections. HTTP fetchers keep their existing worker
configuration in `service/endpoints.json`.

The database must already contain the compatible business tables, partitions
and maintenance triggers. Install `db/ingest.sql` as the database owner before
starting collectors. Grant the collector role `USAGE` on `tdd_ingest`,
`SELECT` on `tdd_ingest.control`, `SELECT, INSERT, UPDATE` on
`tdd_ingest.profile_observation`, and `INSERT` on
`tdd_ingest.manifest`, alongside its business-table and identity-sequence
permissions. Do not grant collectors `INSERT`, `UPDATE`, `DELETE` or `TRUNCATE`
on `tdd_ingest.control`. The application does not create or drop business tables.

Application timestamps remain integer Unix seconds. PostgreSQL stores `added`
and diagnostic snapshot timestamps as whole-second `timestamptz`. Only actual
U+0000 is removed from text; SQL NULL, empty strings and literal escape text
remain distinct. The database maintains the three statistics latest tables and
`member.last_video_aid` in the same transaction as their source changes.

Database connections apply bounded startup deadlines to ordinary reads as well
as writes: lock wait 10,000 ms, statement 60,000 ms, transaction 120,000 ms, and
idle time within a transaction 60,000 ms. Configure the first three as
`lock_timeout_ms`, `statement_timeout_ms` and `transaction_timeout_ms` in the
selected `[db_postgresql]` section; each must be an integer from 1 through
300,000. Zero does not disable a deadline. Connection startup and write
transactions use the same parser, defaults and validation. Application limits
are appended after the INI's connection `options`, so an earlier zero cannot
override them. Write transactions also apply the first three locally on each
transaction; those settings do not replace the connection bounds used by other
queries. The former `TDD_DB_*_TIMEOUT_MS` environment overrides are ignored.

Tasks fetch HTTP input before calling named database functions with explicit
arguments. `db.write.insert_video_records()` and
`insert_member_follower_records()` accept the collected lightweight records and
return complete keys only after commit. `db.profiles.update_video_profile()`
and `update_member_profile()` lock and read stored snapshots, compare incoming
values, and commit profile fields, staff, observation watermarks and logs
together. Their SQL stays in the database layer. Standalone queries borrow a
pooled connection and release it before returning their result. Aid lookups
return a snapshot or `None`; query errors propagate. An insert reports whether
it inserted a row or found the expected business key already present, including
a competing insert after a precheck. Tasks interpret these outcomes as success,
`AlreadyExistError` or `NotExistError`. Unrelated constraint violations and
connection errors remain database errors.

The shared `execute_write()` helper calls a Python function inside one guarded
transaction; it does not dispatch operation strings or import task code.
Database-internal helpers receive the same psycopg connection, including the
trial manifest writer. Tasks, jobs and numbered scripts do not own database
connections or transactions. Batch writers configure batch size and queue
polling. A failed transaction is reported to its caller and is not automatically
retried.

## Failed writes

Successful observations write directly to PostgreSQL in one guarded business
transaction. There is no input file, JSON digest, receipt or automatic replay
on the successful path. Independent observations remain independent, including
multiple records for the same entity in the same second.

After a write raises an exception, the application attempts to save the input
as a restrictive JSON file under `data/failed-writes/`; `TDD_FAILURE_DIR`
selects another directory. The error log identifies the file for manual
inspection. Failure to save the file is also logged, and the original write
error still propagates. Files are diagnostic inputs, not an automatically
processed queue. They are not fsynced and do not guarantee crash recovery.

A lost connection during COMMIT may mean the data was committed even though
the caller received an error. Before manually submitting a failed input,
compare it with the database and its trial manifest where applicable. Do not
blindly repeat or split a transaction whose commit outcome is unknown: there
is no receipt-based deduplication. Repeated submissions can create additional
valid same-second records. Confirmed batches alone continue to downstream
processing; manual database repair does not recreate an interrupted run's CSV,
snapshots or derived analysis.

Earlier versions used `data/replay/`, `TDD_REPLAY_DIR` and `python -m db.replay`.
That replay command is no longer provided. Stop old collectors before upgrading
and inspect any old files manually; do not automatically delete, execute or
convert them. Rerunning `db/ingest.sql` removes the manifest's old receipt
foreign key, but leaves any legacy receipt table and its rows untouched. The
new collector neither reads nor writes those receipts.

## Profile observation order

Mutable video and member profiles also record their most recent observed
Unix second in `tdd_ingest.profile_observation`. A profile transaction locks
the business row and advances this watermark atomically with profile fields,
status, staff relationships and change logs. An older observation is treated
as a no-op and logs a warning; it cannot undo a newer successful observation.
Independent observations in the same second retain last-write-wins behavior.
Video view/status time is captured before slower member/tag enrichment. The
business row's `added` remains its original creation time. An ignored `-403`
does not advance the watermark, and immutable statistics appends remain
independent of profile ordering.

When upgrading an existing collector database, rerun `db/ingest.sql` and grant
the collector `SELECT, INSERT, UPDATE` on the new watermark table before
starting this code. For an existing baseline, initialize each video's and
member's watermark with the baseline's source cutoff second before accepting
profile updates. During a baseline replacement, stop collectors and drain
transactions, hold the exclusive maintenance lock, retire failed inputs for
the old run, then replace the profiles and their watermark entries in the
same maintenance step. Seed `('video', aid, source_cutoff_second)` for every
replaced video and `('member', mid, source_cutoff_second)` for every replaced
member. Do not infer the cutoff from a business row's creation time or the
later import completion time. The cutoff is an ordering fence for the
installed baseline, not a claim that every source profile was fetched at that
second. Do not submit pre-upgrade profile inputs whose ordering relative to
that baseline is unknown.
Keep watermark rows with their matching profiles when backing up or restoring
a live deployment. Preserving watermarks from a different baseline can reject
valid observations; clearing them while retaining failed old observations can
allow stale state to return. Keep these watermarks when accepting a trial's
profiles into formal mode; changing the run ID alone does not reset ordering.

Set `TDD_TRIAL_RUN_ID` to the active run UUID when collecting a trial run. This
additionally records the complete primary keys of the seven immutable append
tables in `tdd_ingest.manifest`, in the same transaction as the business rows.
Its batch UUID and ordinal group manifest entries; they are not deduplication
keys. Formal collection must not set a trial run ID and adds no trial manifest
entries. Sprint daily summaries and details remain separately committed
observations.

## Migration-period deployment and run control

This guard is temporary migration tooling. Retain the per-transaction check
through trials, cutover and the migration wrap-up protection window, including
formal collection during that window. Setting `mode='formal'` does not close
the window or remove this protection. The deployment owner must explicitly
close the window before following the retirement checklist below.

The owner or a separate maintenance role configures the single row in
`tdd_ingest.control`; collectors can only read it. `singleton` must be true,
`deployment_id` identifies the database deployment, and the accepted modes are:

| Mode | Accepted observation run ID |
| --- | --- |
| `stopped` | None: all writes are rejected. |
| `trial` | Exactly `active_run_id`, which must be a non-NULL UUID. |
| `formal` | SQL NULL only; `active_run_id` must also be NULL. |

Each observation, append, and hourly `recent`/`activity`/`freq` write transaction
takes transaction-level shared advisory lock
`(141376009, 169)` before reading control or performing
its business callback. The maintenance connection must hold the matching
session-level exclusive advisory lock for its entire maintenance operation,
including control changes. The control read uses a separate READ COMMITTED
statement after acquiring the lock, so it sees a completed maintenance change
even if it waited for the lock. The shared lock remains held through commit or
rollback. Each hourly derived phase checks control even if fetching produced
no candidates, so an empty upstream result cannot bypass a stopped or obsolete
run. Activity input files are checked before opening its write transaction;
the relevant database state is read after the transaction's gate. An old run
cannot write after a new run is activated or after the deployment enters formal
mode.

This is an application ingest guard, not a prohibition on arbitrary direct SQL.
Stop collector processes and scheduled starts, drain their transactions, and
only then maintain or replace data. Releasing the maintenance lock does not
restart collectors automatically.

Installation leaves control empty and ingestion fails closed. A missing
control table also fails normally as a database error. Only a disposable,
one-off verification database may opt into empty-control compatibility with
`TDD_ALLOW_UNMANAGED_INGEST=1`; this does not ignore a missing table or override
any existing control row, including `stopped`. Do not use this compatibility
setting for a managed trial or formal deployment.

Trial manifests are retained for the migration tools to reconcile or reset a
trial. Those tools own cleanup and the protection that prevents an accepted
trial from being deleted after formalization. The collector does not delete
manifests or reset data. Restore profiles, their observation watermarks and
trial metadata as a matching set.

The input queue between HTTP fetchers and the batch writer remains in memory.
A process or machine failure can lose fetched results that have not committed.
Failure files are a best-effort aid for errors the process can catch, not an
exactly-once or lossless collection guarantee.

## Scheduled maintenance

The independent [maintenance jobs](../scripts/postgresql/README.md) provide
explicit `psql` entrypoints and a [cron example](../scripts/postgresql/crontab.example):
member random-pool refresh at 05:45, video refresh at 09:45 Asia/Shanghai, plus
future-partition precreation at an initial 02:35 slot. They use a separate
maintenance connection, the same run control and shared migration lock, and
log completion only after commit. Install the required database functions and
verify the scheduler timezone and a natural run before enabling the jobs.
Collectors do not create partitions or refresh the random pools themselves.

## Retiring migration tooling

The long-term collector uses named database operations, explicit transactions
and ordinary PostgreSQL concurrency controls. It must eventually run without
consulting `tdd_ingest.control` on every write. Migration completion includes
removing the temporary implementation, not leaving a permanently enabled
bypass flag or an empty control table.

| Keep for normal operation | Remove after the migration wrap-up window closes |
| --- | --- |
| Connection pool, selected INI settings, timeout validation and transaction handling | Deployment/run control lookup, trial UUID configuration and unmanaged-test bypass |
| Direct batch inserts and explicit generated-key returns | Trial-only manifest insertion, complete-key cleanup tables and migration-specific tests |
| Profile snapshots, explicit field differences, row locks and observation watermarks | Baseline reset/import tools and the migration-wide shared/exclusive advisory-lock protocol |
| Best-effort failure diagnostics and manual reconciliation of uncertain commits | Obsolete receipt/pending replay compatibility and migration-only installers |
| Latest/last-video triggers, random-pool refresh and partition maintenance | Maintenance jobs' `require-active-run.sql` include and trial/control bindings |

Retirement is a separate reviewed change, after the operator records that the
wrap-up window has closed. No automatic transition is tied to elapsed days or
the `formal` flag:

1. Record the accepted data baseline, backups and deployed versions. Permanently
   revoke trial reset/import authority; remove scheduled starts and access for
   obsolete collectors and migration tools. An old restored configuration must
   not regain cleanup authority.
2. Stop and drain all current writers and maintenance jobs. Remove run-control
   queries, trial-manifest writes, trial arguments/environment settings and the
   migration-only advisory-lock protocol from every active path, including
   hourly derived updates and the maintenance wrapper. Keep normal business
   transactions, profile row locks and maintenance functions' own locks.
3. Remove the temporary code, tests, grants and install/upgrade instructions.
   Retire `tdd_ingest.control` and `tdd_ingest.manifest` only after no live caller
   needs them; archive required migration evidence before deleting temporary
   migration database objects and runtime files. Do not `DROP SCHEMA ... CASCADE`:
   `tdd_ingest.profile_observation` and its permissions are permanent state.
4. Verify the resulting collector against an isolated database with no control
   or manifest tables: profile refresh, video/follower batches, hourly derived
   writes and scheduled maintenance must succeed. Check failure propagation,
   uncertain-commit handling and watermark ordering still work. Confirm no
   active code/configuration references the retired objects or trial flags.
5. Deploy the reviewed steady-state version, resume schedules and verify actual
   writes and logs. If recovery uses an old backup, keep writers disabled until
   current code and permanent cleanup revocation are reapplied; do not revive
   obsolete trial tools from that backup.

The current implementation still requires the migration guard. The checklist
does not authorize dropping its tables while these collectors are running.

## Checks

```sh
.venv/bin/python -m unittest discover -s tests
```

PostgreSQL integration tests require an explicitly selected disposable database
with the same schema and writer permissions. Put its connection settings in a
separate restricted INI file and select it with `TDD_CONFIG_PATH`. They append
synthetic fixtures and never call an HTTP endpoint:

```sh
TDD_CONFIG_PATH=/path/to/disposable.ini TDD_ALLOW_UNMANAGED_INGEST=1 TDD_PG_INTEGRATION=1 .venv/bin/python -m unittest discover -s tests -p 'test_pg_*.py'
```

The command above explicitly selects empty-control compatibility because these
legacy fixtures use a new trial UUID per test. Run it only against a disposable
database with `db/ingest.sql` installed and no control row. Tests of managed
deployments must configure control under the maintenance lock instead.

`TDD_PG_EVIDENCE_FILE` can select a JSON-lines file containing the expected
trial manifest counts for an independent observer. The collector writer does
not need permission to read the manifest.
