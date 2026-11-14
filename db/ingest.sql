-- Install as the database owner before enabling collectors. Auxiliary objects
-- do not alter business tables. Grant the collector role USAGE on this schema,
-- SELECT/INSERT/UPDATE on batch, and INSERT on manifest (no DELETE or TRUNCATE).
BEGIN;
CREATE SCHEMA IF NOT EXISTS tdd_ingest;
REVOKE ALL ON SCHEMA tdd_ingest FROM PUBLIC;
CREATE TABLE IF NOT EXISTS tdd_ingest.batch (
    batch_id uuid PRIMARY KEY,
    kind text NOT NULL,
    payload_sha256 text NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    run_id uuid,
    result jsonb,
    committed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS tdd_ingest.manifest (
    batch_id uuid NOT NULL REFERENCES tdd_ingest.batch(batch_id),
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    run_id uuid NOT NULL,
    table_name text NOT NULL CHECK (table_name IN (
        'tdd_video_record', 'tdd_member_follower_record',
        'tdd_member_total_stat_record', 'tdd_video_log', 'tdd_member_log',
        'tdd_video_record_abnormal_change', 'tdd_sprint_video_record')),
    key jsonb NOT NULL,
    PRIMARY KEY (batch_id, ordinal)
);
CREATE INDEX IF NOT EXISTS manifest_run_table_idx
    ON tdd_ingest.manifest (run_id, table_name);
REVOKE ALL ON ALL TABLES IN SCHEMA tdd_ingest FROM PUBLIC;
COMMIT;
