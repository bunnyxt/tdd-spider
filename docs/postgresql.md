# PostgreSQL collector runtime

Use Python 3.12 and the locked dependencies in `requirements.txt`:

```sh
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
cp conf/conf.ini.example conf/conf.ini
```

Configure `[db_postgresql]` in `conf/conf.ini`, or use `PGHOST`, `PGPORT`,
`PGDATABASE`, `PGUSER`, `PGPASSWORD` and `PGSSLMODE`. `TDD_CONFIG_PATH` selects
another configuration file. Connections open only when needed; all collector
threads share a pool capped at four database connections. HTTP fetchers keep
their existing worker configuration in `service/endpoints.json`.

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
idle time within a transaction 60,000 ms. Override the first three with
`TDD_DB_LOCK_TIMEOUT_MS`, `TDD_DB_STATEMENT_TIMEOUT_MS` and
`TDD_DB_TRANSACTION_TIMEOUT_MS`; each must be an integer from 1 through
300,000. Zero does not disable a deadline. Application limits are appended after
existing connection `options` or `PGOPTIONS`, so an earlier zero cannot override
them. Observation transactions also set the first three with `SET LOCAL` on
each transaction; those transaction-local settings do not replace the connection
bounds used by other queries.

Observation entry points, including the video/follower batch writers, call
`observe()` to acquire their connection and own the complete write transaction.
They do not accept a caller's `Session`. Sessions passed to profile-preparation
queries or internal transactional helpers remain scoped to those operations.
Batch writers configure batch size and queue polling. A failed transaction is
reported to its caller and is not automatically retried.

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

## Deployment and run control

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

## Checks

```sh
.venv/bin/python -m unittest discover -s tests
```

PostgreSQL integration tests require an explicitly selected disposable database
with the same schema and writer permissions. They append synthetic fixtures
and never call an HTTP endpoint:

```sh
TDD_ALLOW_UNMANAGED_INGEST=1 TDD_PG_INTEGRATION=1 .venv/bin/python -m unittest discover -s tests -p 'test_pg_*.py'
```

The command above explicitly selects empty-control compatibility because these
legacy fixtures use a new trial UUID per test. Run it only against a disposable
database with `db/ingest.sql` installed and no control row. Tests of managed
deployments must configure control under the maintenance lock instead.

`TDD_PG_EVIDENCE_FILE` can select a JSON-lines file containing the expected
trial manifest counts for an independent observer. The collector writer does
not need permission to read the manifest.
