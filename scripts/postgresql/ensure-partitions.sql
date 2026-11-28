-- Current + next three UTC months; current + next UTC year. Never delete history.
BEGIN ISOLATION LEVEL READ COMMITTED;
SET LOCAL TimeZone='UTC';
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='30s';
\ir require-active-run.sql
SELECT * FROM public.tdd_ensure_history_partitions(
    'public.tdd_video_record'::regclass,
    date_trunc('month',current_timestamp,'UTC'),
    date_trunc('month',current_timestamp,'UTC')+interval '4 months');
SELECT * FROM public.tdd_ensure_history_partitions(
    'public.tdd_member_follower_record'::regclass,
    date_trunc('year',current_timestamp,'UTC'),
    date_trunc('year',current_timestamp,'UTC')+interval '2 years');
SELECT * FROM public.tdd_ensure_history_partitions(
    'public.tdd_member_total_stat_record'::regclass,
    date_trunc('year',current_timestamp,'UTC'),
    date_trunc('year',current_timestamp,'UTC')+interval '2 years');
COMMIT;
