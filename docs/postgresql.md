# PostgreSQL collector runtime

## Permanent setup

These requirements remain after migration tooling is retired.

Use Python 3.12 and the locked dependencies in `requirements.txt`:

```sh
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
cp conf/conf.ini.example conf/conf.ini
cp service/endpoints.json.example service/endpoints.json
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

Connections open only when needed; all threads in one collector process share
a pool. Set `pool_max_size` in the selected `[db_postgresql]` section to a positive
integer (default `4`). This is a per-process connection limit; choose the deployed
value after testing representative workloads and concurrent scripts. HTTP fetchers keep their existing worker
configuration in `service/endpoints.json`.

Install compatible business tables, partitions and maintenance triggers outside
this repository. Grant the collector role the business-table and identity-sequence
permissions its operations require. The application does not create or drop
business tables.

Prepare a restricted directory for best-effort failed-write files (by default
`data/failed-writes/`, configurable with `TDD_FAILURE_DIR`). Reconciliation of
uncertain commits and any resubmission are manual; no automatic replay or
resubmission command is provided. Never replay an earlier trial's failure files
into a new baseline. See [Failed writes](#failed-writes).

## Migration-only setup

The following requirements are temporary, but remain mandatory in the current
version through trials, cutover and the migration wrap-up window, including
formal collection. Installing the SQL alone does not enable writes.

1. With collectors stopped, install `db/ingest.sql` as the database owner. The
   entire `tdd_ingest` schema (`control`, `manifest`) is migration-only. Grant
   the collector role `USAGE` on `tdd_ingest`, `SELECT` on `tdd_ingest.control`
   and `INSERT` on `tdd_ingest.manifest`. Do not grant collectors `INSERT`,
   `UPDATE`, `DELETE` or `TRUNCATE` on control, or cleanup rights on manifest.
   Remove these objects, grants and installation instructions through
   [Retiring migration tooling](#retiring-migration-tooling).
2. **Configure control before starting writers.** Installation leaves it empty
   and writes fail closed. The owner or maintenance role must configure `formal`
   with `active_run_id=NULL` and no process `TDD_TRIAL_RUN_ID`, or `trial` with
   a non-NULL `active_run_id` and the identical process `TDD_TRIAL_RUN_ID`.
   Follow [deployment and run control](#migration-period-deployment-and-run-control),
   including the maintenance lock. Remove control and trial environment settings
   through [Retiring migration tooling](#retiring-migration-tooling).
3. `TDD_ALLOW_UNMANAGED_INGEST=1` is only for disposable, one-off test databases
   with an installed but empty control table. It cannot bypass a missing table
   or an existing row, including `stopped`, and is not a managed-deployment setup
   option. Remove this compatibility flag through
   [Retiring migration tooling](#retiring-migration-tooling).
4. Keep `db/migration.py` and the two hooks: `transaction.write` calls
   `before_write`; `append_rows` calls `record_keys`. They provide run control,
   the maintenance lock and trial manifests. Remove the module and hooks through
   [Retiring migration tooling](#retiring-migration-tooling), together with their
   database dependencies.

## Database writes and data semantics

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
300,000. Zero does not disable a deadline. Application limits
are appended after the INI's connection `options`, so an earlier zero cannot
override them. Writes use these connection limits without repeating per-transaction
timeout SQL. The former `TDD_DB_*_TIMEOUT_MS` environment overrides are ignored.

Tasks fetch HTTP input before calling named database functions with explicit
arguments. The full-view endpoint requires the title key and raises `FormatError`
when it is missing. Its response contract declares `title: str`, but scalar type
validation is deferred to a unified response-validation change. Existing key and
container checks remain; the database still rejects NULL titles. Empty strings
remain valid. `db.write.insert_video_records()` and
`insert_member_follower_records()` accept the collected lightweight records and
return complete keys only after commit. `db.profiles.update_video_profile()`
and `update_member_profile()` lock and read stored snapshots, compare incoming
values, and commit profile fields, staff and logs
together. Their SQL stays in the database layer. Standalone queries borrow a
pooled connection and release it before returning their result. Aid lookups
return a snapshot or `None`; query errors propagate. An insert reports whether
it inserted a row or found the expected business key already present, including
a competing insert after a precheck. Tasks interpret these outcomes as success,
`AlreadyExistError` or `NotExistError`. Unrelated constraint violations and
connection errors remain database errors.

Profile differences use `NewVideoLog` / `NewMemberLog` write inputs without
generated IDs. Their inserts return the complete stored rows; only then does
the database layer construct `TddVideoLog` / `TddMemberLog` snapshots. Inputs
are never patched with generated IDs. Log inserts, trial keys and profile/staff
changes remain in the same transaction, and the public return types stay the
same.

`db.profiles.create_video()` accepts a `NewVideo`, prepared `NewMember` inputs,
staff relationships and an optional first `VideoRecord`. Creation inputs carry
no generated IDs. Database-defaulted columns are omitted; business values such
as `recent=2` remain explicit. All related writes share one transaction, and the
function returns a separate `TddVideo` snapshot from the stored row only after
commit. An existing video aid returns `None` before any related writes; the
task translates that outcome to `AlreadyExistError`.

Creation input types follow the API response contracts, which are stricter than
nullable columns retained for old data. `NewVideo` permits `None` only for
`attribute` and `forward`; `NewMember` has no nullable fields. Required response
fields have no placeholder defaults in these inputs. This is a typing contract;
comprehensive runtime response-type validation remains separate work.

The active `task.add_video()` always includes the first collected statistics in
that transaction. Member creation is part of video creation or refresh; there
is no separate member-creation task. Sprint tasks pass aid, collection time and
the API statistics directly to their SQL writer, which returns a stored snapshot
and commits any million-view status transition in the same transaction. Abnormal
statistics writes bind the analyst's values and two original collected records
directly; callers do not construct or mutate database row objects.

Prepared members are candidates found missing during an earlier lookup. If a
member mid now exists, keep its stored profile and continue creating the video,
other missing members, staff and first record. This expected member conflict
does not abort the transaction.

The shared `db.transaction.write()` calls a Python function inside one guarded
transaction; it does not dispatch operation strings or import task code.
Database-internal helpers receive the same psycopg connection, including the
trial manifest writer. Tasks, jobs and numbered scripts do not own database
connections or transactions. Batch writers configure batch size and queue
polling. A failed transaction is reported to its caller and is not automatically
retried.

The write path separates normal persistence from temporary migration controls:

| Module | Responsibility |
| --- | --- |
| `db/basic.py` | Connection pool; connection options apply the selected INI timeouts. |
| `db/transaction.py` | One transaction, commit, best-effort failure input, error propagation. |
| `db/profiles.py`, `db/write.py`, `db/sprint.py`, `db/hourly.py` | Named business SQL operations; no trial run parameter. |
| `db/ingest.py` | Pipelined append batches and explicit returned keys or stored snapshots. |
| `db/migration.py` | Temporary run control, shared maintenance lock and trial manifest. |

There are two migration hooks: `before_write(connection)` at the transaction
boundary and `record_keys(connection, table, keys)` after an append. The latter
also covers sprint records; their returned snapshots and status updates stay
in the same transaction. Formal appends do not build manifest keys or generate
batch UUIDs. The guard checks READ COMMITTED once per transaction; writes do
not reapply timeout settings already installed on the connection.

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

During migration, a successful write gate supplies the run label for failure
diagnostics. If connection acquisition or the gate itself fails, no business
callback has run and that label is absent. It is never passed into business SQL.

A lost connection during COMMIT may mean the data was committed even though
the caller received an error. Reconciliation and any resubmission are manual;
there is no resubmission command. Compare the input with the database and its
trial manifest where applicable. Isolate failure files from earlier trial
rounds; never replay them into a new baseline. Do not
blindly repeat or split a transaction whose commit outcome is unknown: there
is no receipt-based deduplication. Repeated submissions can create additional
valid same-second records. Confirmed batches alone continue to downstream
processing; manual database repair does not recreate an interrupted run's CSV,
snapshots or derived analysis.

In `51_`, a failed batch never blocks confirmed batches from reaching CSV,
the daily snapshot and analysis. Before downstream processing, keep the earliest
`added` record per `bvid`, as before; this does not remove database history.
Write failures and queue drops are counted and reported as a failed run only
after all downstream stages finish. Each derived-field update still uses its
own guarded transaction, and failure of one update does not prevent the next.
A missing seven-day snapshot skips activity with a warning; malformed rows
are counted and skipped while good rows remain usable. Neither case prevents
frequency updates or fails the run. A wrong snapshot header fails activity
without reading its rows or writing activity values; frequency updates still
run. A wrong header in any selected recent CSV skips the whole recent-record
analysis before checking videos or writing abnormal changes. Empty or failed
pubdate reads also fail that analysis step. Per-video history-read and
abnormal-change write failures warn with the aid and continue to the next video.
These failures appear in logs and run metrics and fail the run only after all
downstream stages finish.

At 23:00, daily packing runs even if the hourly CSV write fails. A failed CSV
write skips its dependent 04:00 snapshot. Packing checks command success and a
nonempty archive; three-day-old CSVs are deleted only when their own day's
archive exists and is nonempty. Packing and cleanup failures are counted,
unarchived CSVs are kept, and independent downstream work continues. Archive names,
the date-directory layout and the three-day retention are unchanged.

Earlier versions used `data/replay/`, `TDD_REPLAY_DIR` and `python -m db.replay`.
That replay command is no longer provided. Stop old collectors before upgrading
and inspect any old files manually; do not automatically delete, execute or
convert them. Rerunning `db/ingest.sql` removes the manifest's old receipt
foreign key, but leaves any legacy receipt table and its rows untouched. The
new collector neither reads nor writes those receipts.

## Concurrent profile updates

Video and member updates lock the business row with `FOR UPDATE` before
comparing fields. Profile changes, staff relationships and change logs share
one transaction. A waiting updater compares against the preceding committed
values, and its changes take effect when it commits, even if its HTTP input
was observed earlier. Profile updates do not reject inputs by observation time.
This may temporarily restore an older value. A later successful refresh may
correct it, depending on collection coverage and request results; there is no
one-hour recovery guarantee.

The update's `added` remains the business time of its change logs; video
view/status time is captured before slower member/tag enrichment. It does not
change the profile's original creation time. An ignored `-403` leaves the
profile unchanged. Statistics retain their collection timestamps and their
independent latest-table rules.

During baseline replacement, stop collectors, drain transactions and hold the
exclusive maintenance lock. Isolate failed inputs from the previous run for
inspection; never replay them into the new baseline. There is no database
ordering check that makes such a replay safe.

Set `TDD_TRIAL_RUN_ID` to the active run UUID when collecting a trial run. This
additionally records the complete primary keys of the seven immutable append
tables in `tdd_ingest.manifest`, in the same transaction as the business rows.
Its batch UUID and ordinal group manifest entries; they are not deduplication
keys. Formal collection must not set a trial run ID and adds no trial manifest
entries. Sprint daily summaries and details remain separately committed
observations.

The trial environment is fixed for each collector process. Stop and drain
collectors before changing the active run or switching to formal mode, then
restart with the matching configuration.

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

For a new installation with an empty control table, the owner can initialize
formal mode in a single `psql` session as follows. Stop and drain collectors
and scheduled maintenance first, supply a deployment UUID with the `psql`
option `--set=deployment_id=YOUR_DEPLOYMENT_UUID` using the maintenance connection's normal
authentication, and leave `TDD_TRIAL_RUN_ID` unset in collector processes:

```sql
\set ON_ERROR_STOP on
SELECT pg_advisory_lock(141376009, 169);
BEGIN;
INSERT INTO tdd_ingest.control (singleton, deployment_id, mode, active_run_id)
VALUES (true, :'deployment_id'::uuid, 'formal', NULL);
COMMIT;
SELECT pg_advisory_unlock(141376009, 169);
```

For a new trial, use `'trial', :'trial_run_id'::uuid` in place of `'formal', NULL`,
supply `trial_run_id` as a `psql` variable, and start collectors with exactly that
UUID as `TDD_TRIAL_RUN_ID`. These examples deliberately reject an existing
control row; changing an existing deployment requires the maintenance procedure
below, not rerunning initialization. If an error interrupts an interactive
session, roll back and disconnect to release its session lock. Control and its
environment settings are removed through
[Retiring migration tooling](#retiring-migration-tooling).

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
manifests or reset data. Restore business data and trial metadata as a matching
set, and keep obsolete failed inputs isolated from the restored baseline.

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
| Profile snapshots, explicit field differences and row locks | Baseline reset/import tools and the migration-wide shared/exclusive advisory-lock protocol |
| Best-effort failure diagnostics and manual reconciliation of uncertain commits | Obsolete receipt/pending replay compatibility and migration-only installers |
| Latest/last-video triggers, random-pool refresh and partition maintenance | Maintenance jobs' `require-active-run.sql` include and trial/control bindings |

Retirement is a separate reviewed change, after the operator records that the
wrap-up window has closed. No automatic transition is tied to elapsed days or
the `formal` flag:

1. Record the accepted data baseline, backups and deployed versions. Permanently
   revoke trial reset/import authority; remove scheduled starts and access for
   obsolete collectors and migration tools. An old restored configuration must
   not regain cleanup authority.
2. Stop and drain all current writers and maintenance jobs. Delete
   `db/migration.py` and its imports and two hooks in `db/transaction.py` and
   `db/ingest.py`. No business function signatures need to change. Remove trial
   environment settings and the maintenance wrapper's temporary control/lock
   include as well. Keep normal transactions, failure diagnostics, profile row
   locks and maintenance functions' own locks.
3. Remove the temporary code, tests, grants and install/upgrade instructions.
   Retire `tdd_ingest.control` and `tdd_ingest.manifest` only after no live caller
   needs them; archive required migration evidence before deleting temporary
   migration database objects and runtime files. Remove the migration schema
   only after checking its remaining objects and dependencies.
4. Verify the resulting collector against an isolated database with no control
   or manifest tables: profile refresh, video/follower batches, hourly derived
   writes and scheduled maintenance must succeed. Check failure propagation,
   uncertain-commit handling and row-locked profile updates still work. Confirm no
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
