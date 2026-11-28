-- Include only inside a READ COMMITTED transaction, before reading business data.
-- Match collector control without granting the maintenance role control writes.
-- Migration replacement holds the exclusive version of this lock throughout.
SELECT pg_advisory_xact_lock_shared(141376009,169);
SELECT set_config('tdd.maintenance_run_id', :'trial_run_id', true);
DO $guard$
DECLARE
    expected_run uuid := nullif(current_setting('tdd.maintenance_run_id'),'')::uuid;
    current_mode text;
    current_run uuid;
BEGIN
    SELECT mode,active_run_id INTO current_mode,current_run
    FROM tdd_ingest.control WHERE singleton;
    IF NOT FOUND OR NOT (
        (current_mode='trial' AND expected_run IS NOT NULL AND expected_run=current_run)
        OR (current_mode='formal' AND expected_run IS NULL)
    ) THEN
        RAISE EXCEPTION 'maintenance control does not permit this run'
            USING ERRCODE='55000';
    END IF;
END;
$guard$;
