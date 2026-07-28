#!/usr/bin/env bash
# Build (if needed) and start every container, then block until the stack is
# genuinely ready to be exercised. This is the single entrypoint the VS Code
# "Start Full Stack" task calls -- see .vscode/tasks.json.
set -euo pipefail

# Always run from the repository root, regardless of the caller's cwd.
cd "$(dirname "$0")/.."

echo "🐳 Building and starting containers (postgres, kafka, api, orchestrator)..."
docker compose up --build -d

bash scripts/wait_for_stack.sh 60