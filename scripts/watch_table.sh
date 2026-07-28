#!/usr/bin/env bash
# Re-run a SQL query against the running `postgres` container every N seconds
# and pretty-print the result -- a live view of a table as events flow
# through the system.
#
# Implemented as a plain bash loop instead of psql's own `\watch`
# meta-command: chaining `\watch` through docker exec + shell + tasks.json
# quoting layers is fragile. A loop with the query passed as a single,
# normally-quoted argument is far more robust and portable.
#
# Usage: watch_table.sh "<SQL QUERY>" [interval_seconds]
set -euo pipefail

cd "$(dirname "$0")/.."

QUERY="${1:?Usage: watch_table.sh \"<SQL QUERY>\" [interval_seconds]}"
INTERVAL="${2:-15}"

trap 'echo; echo "👋 Stopped watching."; exit 0' INT TERM

while true; do
  clear
  echo "── $(date '+%H:%M:%S') ── refresh every ${INTERVAL}s ── Ctrl+C to stop ──"
  echo
  # -P border=2 draws a clean boxed grid (easier to scan than the plain
  # default), -T disables pseudo-tty allocation for predictable piping.
  docker compose exec -T postgres psql -U datamesh -d datamesh -P border=2 -c "$QUERY" \
    || echo "⚠️  Query failed (is the stack still starting up?). Retrying..."
  sleep "$INTERVAL"
done