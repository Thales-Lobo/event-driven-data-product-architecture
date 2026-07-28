#!/usr/bin/env bash
# Blocks until the API's /health endpoint responds, or a timeout is reached.
#
# Why this exists: `docker compose up -d` returns as soon as containers are
# *created*, not once they're actually ready to serve traffic. Kafka in
# particular can take a few extra seconds after its healthcheck passes before
# it reliably accepts producer/consumer connections. Polling /health (which
# only succeeds once FastAPI's lifespan has fully started, tables are
# created, and the Kafka producer is connected) is a much more honest signal
# than a fixed `sleep N`.
#
# Used by scripts/start_stack.sh, which is what the VS Code "Start Full
# Stack" task (see .vscode/tasks.json) actually runs.
set -euo pipefail

MAX_WAIT="${1:-60}"
ELAPSED=0

echo "⏳ Waiting for API to become healthy (timeout: ${MAX_WAIT}s)..."

until curl --silent --fail http://localhost:8000/health > /dev/null 2>&1; do
  sleep 2
  ELAPSED=$((ELAPSED + 2))
  if [ "$ELAPSED" -ge "$MAX_WAIT" ]; then
    echo "❌ Timed out after ${MAX_WAIT}s waiting for the API to become healthy."
    echo "   Run 'docker compose logs' (or the individual service logs) to investigate."
    exit 1
  fi
done

echo "✅ API is healthy after ${ELAPSED}s. Stack is ready."