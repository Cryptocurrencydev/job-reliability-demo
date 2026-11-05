#!/usr/bin/env bash
# One command: bring up Postgres 16 if needed, then SIGKILL workers at every step.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -z "${DATABASE_URL:-}" ]; then
  if docker compose version >/dev/null 2>&1; then
    docker compose up -d --wait postgres
  else
    # No Compose plugin: same container, started directly.
    docker start job-reliability-pg >/dev/null 2>&1 || docker run -d --name job-reliability-pg \
      -e POSTGRES_USER=tunesmith -e POSTGRES_PASSWORD=tunesmith_local -e POSTGRES_DB=tunesmith \
      -p 127.0.0.1:55433:5432 public.ecr.aws/docker/library/postgres:16-alpine \
      postgres -c max_connections=100 >/dev/null
    until docker exec job-reliability-pg pg_isready -U tunesmith -d tunesmith >/dev/null 2>&1; do sleep 1; done
    sleep 2 # the image restarts Postgres once after first-time init
    until docker exec job-reliability-pg pg_isready -U tunesmith -d tunesmith >/dev/null 2>&1; do sleep 1; done
  fi
fi

[ -d node_modules ] || npm ci
npx vitest run test/chaos.test.ts test/property.test.ts test/broken.test.ts
