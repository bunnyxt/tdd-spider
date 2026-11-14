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
`SELECT, INSERT, UPDATE` on `tdd_ingest.batch`, and `INSERT` on
`tdd_ingest.manifest`, alongside its business-table and identity-sequence
permissions. The application does not create or drop business tables.

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
each attempt; those transaction-local settings do not replace the connection
bounds used by other queries. Retryable observation failures use the same UUID
for at most three attempts.

## Pending observations

Each business observation gets a UUID and a durable JSON input before its
transaction starts. The input includes fetched profile responses, so replay
does not send another worker request. Receipts and appended primary-key
manifests commit with the business changes. An independent observation gets a
new UUID, including when the entity and second match an earlier observation.

Pending inputs live in `data/replay/` by default; `TDD_REPLAY_DIR` selects
another directory. A confirmed transaction removes its input. On an error or
interrupted commit, keep the file and replay that exact observation:

```sh
.venv/bin/python -m db.replay data/replay/<observation-uuid>.json
```

Replay checks the receipt under the original UUID and run identifier before
writing. An already committed observation returns its original result. A
confirmed rollback can execute the saved input. Reusing a UUID with changed
input is rejected. Do not replace a pending file with a newly fetched input.

Set `TDD_TRIAL_RUN_ID` to a UUID when collecting a trial run. This additionally
records the complete primary keys of the seven immutable append tables in
`tdd_ingest.manifest`. Without it, receipts still prevent duplicate retries.
Sprint daily summaries and details remain separately committed observations.

Database receipts and trial manifests are currently retained. The application
does not delete them when a pending input is cleared. Owner-managed retention,
cleanup and capacity validation are required before long-running production
collection; the disposable-database tests do not establish that capacity.

The input queue between HTTP fetchers and the batch writer remains in memory.
Durable replay covers observations published as pending inputs; stopping the
process before that point requires repeating the unfinished acquisition.

## Checks

```sh
.venv/bin/python -m unittest discover -s tests
```

PostgreSQL integration tests require an explicitly selected disposable database
with the same schema and writer permissions. They append synthetic fixtures
and never call an HTTP endpoint:

```sh
TDD_PG_INTEGRATION=1 .venv/bin/python -m unittest discover -s tests -p test_pg_application.py
```

`TDD_PG_EVIDENCE_FILE` can select a JSON-lines file containing the expected
trial manifest counts for an independent observer. The collector writer does
not need permission to read the manifest.
