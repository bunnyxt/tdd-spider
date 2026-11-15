-- Publish the complete video pool atomically; the function logs candidate count.
BEGIN ISOLATION LEVEL READ COMMITTED;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='5min';
\ir require-active-run.sql
SELECT public.tdd_refresh_random_pool('video') AS candidate_count;
COMMIT;
