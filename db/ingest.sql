-- Install as the database owner with collectors stopped. Auxiliary objects
-- do not alter business tables. Grant the collector role USAGE on this schema,
-- SELECT on control, SELECT/INSERT/UPDATE on profile_observation,
-- and INSERT on manifest.
-- Collectors must not be able to change control or DELETE/TRUNCATE manifests.
BEGIN;
CREATE SCHEMA IF NOT EXISTS tdd_ingest;
REVOKE ALL ON SCHEMA tdd_ingest FROM PUBLIC;
CREATE TABLE IF NOT EXISTS tdd_ingest.control (
    singleton boolean PRIMARY KEY CHECK (singleton IS TRUE),
    deployment_id uuid NOT NULL,
    mode text NOT NULL CHECK (mode IN ('stopped', 'trial', 'formal')),
    active_run_id uuid,
    CHECK ((mode = 'trial') = (active_run_id IS NOT NULL))
);
-- Intentionally no initial row: an unconfigured deployment fails closed.
-- Mutable profiles have immutable creation times in their business tables.
-- Keep the most recent observed second separately so delayed observations
-- cannot replace a newer code, profile field, or staff relationship.
CREATE TABLE IF NOT EXISTS tdd_ingest.profile_observation (
    entity_kind text NOT NULL CHECK (entity_kind IN ('video', 'member')),
    entity_id bigint NOT NULL,
    observed_at bigint NOT NULL,
    PRIMARY KEY (entity_kind, entity_id)
);
CREATE TABLE IF NOT EXISTS tdd_ingest.manifest (
    batch_id uuid NOT NULL,
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    run_id uuid NOT NULL,
    table_name text NOT NULL CHECK (table_name IN (
        'tdd_video_record', 'tdd_member_follower_record',
        'tdd_member_total_stat_record', 'tdd_video_log', 'tdd_member_log',
        'tdd_video_record_abnormal_change', 'tdd_sprint_video_record')),
    key jsonb NOT NULL,
    PRIMARY KEY (batch_id, ordinal)
);
-- Earlier installs linked each manifest to a receipt. Detach that legacy
-- constraint without deleting any existing receipt or pending-input evidence.
ALTER TABLE tdd_ingest.manifest DROP CONSTRAINT IF EXISTS manifest_batch_id_fkey;
CREATE INDEX IF NOT EXISTS manifest_run_table_idx
    ON tdd_ingest.manifest (run_id, table_name);
REVOKE ALL ON ALL TABLES IN SCHEMA tdd_ingest FROM PUBLIC;
COMMIT;
