#!/usr/bin/env bash
# One command: bring up Postgres 16 if needed, then SIGKILL workers at every step.
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -z "${DATABASE_URL:-}" ]; then
  docker compose up -d --wait postgres
fi
[ -d node_modules ] || npm ci
npx vitest run test/chaos.test.ts test/property.test.ts test/broken.test.ts
