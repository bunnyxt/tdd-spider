#!/usr/bin/env bash
set -euo pipefail

# Usage: ./log_size_stats.sh <log_dir> [days]
#  - First argument: directory containing log files
#  - Second argument (optional): number of days for average calculation
#    If omitted, AVG/DAY is not shown.

DIR="${1:-.}"
DAYS="${2:-}"

# Print an explanation if days is provided
if [[ -n "$DAYS" ]]; then
  echo "Note: AVG/DAY = SIZE divided by $DAYS days (user-provided)."
fi

# Print table header
if [[ -n "$DAYS" ]]; then
  printf "%-30s  %15s  %15s\n" "FILE" "SIZE(KB)" "AVG/DAY(KB)"
  printf "%-30s  %15s  %15s\n" "----" "--------" "-----------"
else
  printf "%-30s  %15s\n" "FILE" "SIZE(KB)"
  printf "%-30s  %15s\n" "----" "--------"
fi

# Find only files strictly matching "<digits>_<UPPER>.log"
while IFS= read -r -d '' f; do
  size_bytes=$(stat -c '%s' "$f")
  size_kb=$(( size_bytes / 1024 ))

  if [[ -n "$DAYS" ]]; then
    avg_kb=$(( size_kb / DAYS ))
    printf "%-30s  %'15d  %'15d\n" "$(basename "$f")" "$size_kb" "$avg_kb"
  else
    printf "%-30s  %'15d\n" "$(basename "$f")" "$size_kb"
  fi
done < <(
  find "$DIR" -maxdepth 1 -type f -regextype posix-extended \
    -regex '.*/[0-9]+_[A-Z]+\.log$' -print0 \
  | sort -z
)
