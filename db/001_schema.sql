-- Tunesmith job ledger. This table, not the queue, is the source of truth.
CREATE TABLE IF NOT EXISTS gen_jobs (
  id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  client_request_id   text NOT NULL UNIQUE,          -- a double-click cannot create two jobs
  batch               text NOT NULL DEFAULT 'default',
  prompt              text NOT NULL,
  state               text NOT NULL DEFAULT 'queued'
    CHECK (state IN ('queued','submitting','submitted','completed','failed','unknown')),
  idempotency_key     text NOT NULL UNIQUE,          -- deterministic, fixed at creation
  provider_request_id text,
  submit_attempts     int  NOT NULL DEFAULT 0,
  last_submit_at      timestamptz,
  lease_owner         text,
  lease_version       bigint NOT NULL DEFAULT 0,     -- the fencing token
  lease_expires_at    timestamptz,
  next_check_at       timestamptz,
  needs_review        boolean NOT NULL DEFAULT false,-- the explicit "a human must look" flag
  review_reason       text,
  failure_reason      text,
  created_at          timestamptz NOT NULL DEFAULT now(),
  updated_at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS gen_jobs_sweep_idx ON gen_jobs (lease_expires_at, next_check_at)
  WHERE state NOT IN ('completed','failed');
CREATE INDEX IF NOT EXISTS gen_jobs_batch_idx ON gen_jobs (batch);

CREATE TABLE IF NOT EXISTS tracks (
  job_id      uuid PRIMARY KEY REFERENCES gen_jobs(id),
  audio_url   text NOT NULL,
  duration_ms int  NOT NULL,
  written_by_lease bigint NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now()
);

-- Every state change, with the lease that made it. Written by trigger, so no code path can skip it.
CREATE TABLE IF NOT EXISTS gen_job_events (
  id            bigserial PRIMARY KEY,
  job_id        uuid NOT NULL REFERENCES gen_jobs(id),
  from_state    text,
  to_state      text NOT NULL,
  lease_version bigint NOT NULL,
  lease_owner   text,
  at            timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS gen_job_events_job_idx ON gen_job_events (job_id, id);

-- The state machine lives in the database, so a bug in any worker cannot make an illegal move.
CREATE OR REPLACE FUNCTION gen_jobs_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := clock_timestamp();
  IF NEW.lease_version < OLD.lease_version THEN
    RAISE EXCEPTION 'lease_version may not go backwards (% -> %)', OLD.lease_version, NEW.lease_version
      USING ERRCODE = 'check_violation';
  END IF;
  IF NEW.idempotency_key <> OLD.idempotency_key THEN
    RAISE EXCEPTION 'idempotency_key is immutable' USING ERRCODE = 'check_violation';
  END IF;
  IF NEW.state = OLD.state THEN
    RETURN NEW;
  END IF;
  IF NOT (
       (OLD.state = 'queued'     AND NEW.state IN ('submitting'))
    OR (OLD.state = 'submitting' AND NEW.state IN ('submitted','unknown','failed'))
    OR (OLD.state = 'unknown'    AND NEW.state IN ('submitted','submitting','failed'))
    OR (OLD.state = 'submitted'  AND NEW.state IN ('completed','failed','unknown'))
  ) THEN
    RAISE EXCEPTION 'illegal job transition % -> %', OLD.state, NEW.state
      USING ERRCODE = 'check_violation';
  END IF;
  INSERT INTO gen_job_events (job_id, from_state, to_state, lease_version, lease_owner)
  VALUES (NEW.id, OLD.state, NEW.state, NEW.lease_version, NEW.lease_owner);
  RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS gen_jobs_guard_trg ON gen_jobs;
CREATE TRIGGER gen_jobs_guard_trg BEFORE UPDATE ON gen_jobs
  FOR EACH ROW EXECUTE FUNCTION gen_jobs_guard();

-- Test-only: which job should die at which step. Empty in any real deployment.
CREATE TABLE IF NOT EXISTS chaos_plan (
  job_id uuid NOT NULL,
  step   text NOT NULL,
  action text NOT NULL DEFAULT 'kill' CHECK (action IN ('kill','stall')),
  PRIMARY KEY (job_id, step)
);

-- Used by the incident scripts: one hot row, like a shared credits balance.
CREATE TABLE IF NOT EXISTS incident_credits (
  account_id int PRIMARY KEY,
  balance    int NOT NULL
);
INSERT INTO incident_credits VALUES (1, 1000000) ON CONFLICT DO NOTHING;
