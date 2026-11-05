import { createHash } from 'node:crypto';
import type pg from 'pg';

export type JobState = 'queued' | 'submitting' | 'submitted' | 'completed' | 'failed' | 'unknown';

export interface JobRow {
  id: string;
  client_request_id: string;
  batch: string;
  prompt: string;
  state: JobState;
  idempotency_key: string;
  provider_request_id: string | null;
  submit_attempts: number;
  lease_owner: string | null;
  lease_version: string; // bigint arrives as a string
  needs_review: boolean;
  review_reason: string | null;
  failure_reason: string | null;
}

export interface Lease {
  jobId: string;
  version: string;
  owner: string;
}

/** Thrown when a newer lease has taken the job. The old worker must stop, not retry. */
export class Fenced extends Error {
  constructor(jobId: string, version: string) {
    super(`lease ${version} on job ${jobId} was superseded`);
    this.name = 'Fenced';
  }
}

/** Same request in, same key out. A restart can never mint a fresh key for the same job. */
export function idempotencyKey(clientRequestId: string): string {
  return 'tsm_' + createHash('sha256').update(`tunesmith:v1:${clientRequestId}`).digest('hex').slice(0, 40);
}

export async function insertJob(
  db: Pick<pg.Pool, 'query'>,
  j: { clientRequestId: string; prompt: string; batch?: string },
): Promise<string | null> {
  const r = await db.query(
    `INSERT INTO gen_jobs (client_request_id, prompt, batch, idempotency_key)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (client_request_id) DO NOTHING
     RETURNING id`,
    [j.clientRequestId, j.prompt, j.batch ?? 'default', idempotencyKey(j.clientRequestId)],
  );
  return r.rows[0]?.id ?? null;
}

/** One autocommit statement: bumps the fencing token only if nobody holds a live lease. */
export async function claim(pool: pg.Pool, jobId: string, owner: string, ttlMs: number): Promise<JobRow | null> {
  const r = await pool.query<JobRow>(
    `UPDATE gen_jobs
        SET lease_owner = $2,
            lease_version = lease_version + 1,
            lease_expires_at = now() + ($3 * interval '1 millisecond'),
            next_check_at = NULL
      WHERE id = $1
        AND state NOT IN ('completed','failed')
        AND (lease_expires_at IS NULL OR lease_expires_at <= now())
        AND (next_check_at IS NULL OR next_check_at <= now())
      RETURNING *`,
    [jobId, owner, ttlMs],
  );
  return r.rows[0] ?? null;
}

/**
 * Every worker write goes through here. `set` is a SET clause using $3 onward.
 * Zero rows updated means another worker owns the job now, so we throw and stop.
 */
export async function fencedUpdate(pool: pg.Pool, lease: Lease, set: string, params: unknown[] = []): Promise<JobRow> {
  const r = await pool.query<JobRow>(
    `UPDATE gen_jobs SET ${set} WHERE id = $1 AND lease_version = $2 RETURNING *`,
    [lease.jobId, lease.version, ...params],
  );
  if (r.rowCount === 0) throw new Fenced(lease.jobId, lease.version);
  return r.rows[0]!;
}

/** Returns false if the lease is gone. */
export async function renewLease(pool: pg.Pool, lease: Lease, ttlMs: number): Promise<boolean> {
  const r = await pool.query(
    `UPDATE gen_jobs SET lease_expires_at = now() + ($3 * interval '1 millisecond')
      WHERE id = $1 AND lease_version = $2 AND state NOT IN ('completed','failed')`,
    [lease.jobId, lease.version, ttlMs],
  );
  return r.rowCount === 1;
}

/** Give the job back, to be picked up again no sooner than `afterMs` from now. */
export function release(pool: pg.Pool, lease: Lease, afterMs: number): Promise<JobRow> {
  return fencedUpdate(
    pool,
    lease,
    `lease_owner = NULL, lease_expires_at = NULL, next_check_at = now() + ($3 * interval '1 millisecond')`,
    [afterMs],
  );
}

export async function msSinceLastSubmit(pool: pg.Pool, jobId: string): Promise<number> {
  const r = await pool.query<{ age: string | null }>(
    `SELECT (extract(epoch FROM now() - last_submit_at) * 1000)::bigint AS age FROM gen_jobs WHERE id = $1`,
    [jobId],
  );
  return Number(r.rows[0]?.age ?? Number.MAX_SAFE_INTEGER);
}

/** The result row and the fence check are one statement, so a stale worker cannot write a track. */
export async function writeTrackFenced(pool: pg.Pool, lease: Lease, audioUrl: string, durationMs: number): Promise<void> {
  const r = await pool.query(
    `WITH owner AS (
       SELECT id FROM gen_jobs WHERE id = $1 AND lease_version = $2 FOR UPDATE
     ), ins AS (
       INSERT INTO tracks (job_id, audio_url, duration_ms, written_by_lease)
       SELECT id, $3, $4, $2 FROM owner
       ON CONFLICT (job_id) DO NOTHING
     )
     SELECT count(*)::int AS n FROM owner`,
    [lease.jobId, lease.version, audioUrl, durationMs],
  );
  if (r.rows[0].n === 0) throw new Fenced(lease.jobId, lease.version);
}

/** Jobs nobody is working on that should be: dead lease, due re-check, or lost queue message. */
export async function findStale(pool: pg.Pool, graceMs: number, limit = 100): Promise<string[]> {
  const r = await pool.query<{ id: string }>(
    `SELECT id FROM gen_jobs
      WHERE state NOT IN ('completed','failed')
        AND (lease_expires_at IS NULL OR lease_expires_at <= now())
        AND (next_check_at IS NULL OR next_check_at <= now())
        AND updated_at <= now() - ($1 * interval '1 millisecond')
        AND NOT (needs_review AND state = 'queued')
      ORDER BY updated_at
      LIMIT $2`,
    [graceMs, limit],
  );
  return r.rows.map((x) => x.id);
}
