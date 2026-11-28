-- Publish the complete member pool atomically; the function logs candidate count.
BEGIN ISOLATION LEVEL READ COMMITTED;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='5min';
\ir require-active-run.sql
SELECT public.tdd_refresh_random_pool('member') AS candidate_count;
COMMIT;
