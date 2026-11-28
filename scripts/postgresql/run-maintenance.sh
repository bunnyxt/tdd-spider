#!/usr/bin/env bash
# Run from cron with a dedicated libpq maintenance service; no password in argv.
set -euo pipefail

job=${1:-}
case "$job" in
  ensure-partitions|refresh-video-pool|refresh-member-pool) ;;
  *) echo 'ERROR usage: run-maintenance.sh ensure-partitions|refresh-video-pool|refresh-member-pool' >&2; exit 64 ;;
esac

started=$SECONDS
trap 'rc=$?; if (( rc != 0 )); then printf "%s ERROR job=%s elapsed_s=%s exit=%s\n" "$(date -u +%FT%TZ)" "$job" "$((SECONDS-started))" "$rc" >&2; fi' EXIT
printf '%s INFO START job=%s\n' "$(date -u +%FT%TZ)" "$job"
: "${PGSERVICE:?set PGSERVICE for the dedicated maintenance connection}"
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
export PGCONNECT_TIMEOUT=10
psql -X --no-password --set=ON_ERROR_STOP=1 \
  --set=trial_run_id="${TDD_TRIAL_RUN_ID:-}" \
  --file="$script_dir/$job.sql"
printf '%s INFO SUCCESS job=%s elapsed_s=%s\n' "$(date -u +%FT%TZ)" "$job" "$((SECONDS-started))"
