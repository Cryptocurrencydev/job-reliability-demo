import { performance } from 'node:perf_hooks';
import type pg from 'pg';
import type { ReliabilityConfig } from '../config.ts';
import type { ProviderClient } from '../provider/client.ts';
import {
  claim,
  Fenced,
  fencedUpdate,
  msSinceLastSubmit,
  release,
  renewLease,
  writeTrackFenced,
  type JobRow,
  type JobState,
  type Lease,
} from './ledger.ts';

export type Step = 'before_intent' | 'after_intent' | 'after_provider' | 'during_result' | 'after_result_write';
export const STEPS: Step[] = ['before_intent', 'after_intent', 'after_provider', 'during_result', 'after_result_write'];

export type Failpoint = (step: Step, jobId: string) => Promise<void>;

export interface Deps {
  pool: pg.Pool;
  provider: ProviderClient;
  cfg: ReliabilityConfig;
  owner: string;
  failpoint?: Failpoint;
}

export type Outcome = 'skipped' | 'fenced' | 'released' | 'flagged' | 'completed' | 'failed';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * Keeps the lease alive while we wait on the network, and tracks a local deadline
 * from the monotonic clock. A paused process cannot heartbeat, so on waking it sees
 * the deadline has passed and refuses to spend money, even before the DB tells it so.
 */
class LeaseKeeper {
  private timer: NodeJS.Timeout;
  private deadline: number;
  private ctrl = new AbortController();
  readonly signal = this.ctrl.signal;

  constructor(private d: Deps, private lease: Lease, startedAt: number) {
    this.deadline = startedAt + d.cfg.leaseTtlMs;
    this.timer = setInterval(() => void this.beat(), d.cfg.heartbeatMs);
  }

  private async beat() {
    const sentAt = performance.now();
    try {
      if (await renewLease(this.d.pool, this.lease, this.d.cfg.leaseTtlMs)) {
        this.deadline = sentAt + this.d.cfg.leaseTtlMs;
      } else {
        this.ctrl.abort();
      }
    } catch {
      // DB unreachable. The local deadline will run out and stop us.
    }
  }

  assertHeldLocally() {
    if (this.signal.aborted || performance.now() >= this.deadline) {
      throw new Fenced(this.lease.jobId, this.lease.version);
    }
  }

  stop() {
    clearInterval(this.timer);
  }
}

export async function processJob(d: Deps, jobId: string): Promise<Outcome> {
  const startedAt = performance.now();
  const row = await claim(d.pool, jobId, d.owner, d.cfg.leaseTtlMs);
  if (!row) return 'skipped'; // someone holds it, it is finished, or it is not due yet

  const lease: Lease = { jobId, version: row.lease_version, owner: d.owner };
  const keeper = new LeaseKeeper(d, lease, startedAt);
  try {
    return await advance(d, lease, keeper, row);
  } catch (err) {
    if (err instanceof Fenced) return 'fenced';
    throw err;
  } finally {
    keeper.stop();
  }
}

async function advance(d: Deps, lease: Lease, keeper: LeaseKeeper, first: JobRow): Promise<Outcome> {
  await d.failpoint?.('before_intent', lease.jobId);

  if (Number(first.lease_version) > d.cfg.maxClaims && !first.needs_review) {
    await fencedUpdate(d.pool, lease, `needs_review = true, review_reason = 'too_many_claims'`);
    await release(d.pool, lease, d.cfg.settleMs * 5);
    return 'flagged';
  }
  if (first.needs_review && first.state === 'queued') {
    await release(d.pool, lease, d.cfg.settleMs * 5);
    return 'flagged';
  }

  let row = first;
  let state: JobState = row.state;
  for (;;) {
    switch (state) {
      case 'queued':
        state = await submit(d, lease, keeper, row);
        break;
      case 'submitting':
        // The previous owner died between the intent write and recording the answer.
        // Whether the provider charged is unknown. Never assume it did not.
        row = await fencedUpdate(d.pool, lease, `state = 'unknown'`);
        state = 'unknown';
        break;
      case 'unknown': {
        const next = await reconcile(d, lease, row);
        if (next === 'resubmit') {
          state = await submit(d, lease, keeper, row);
          break;
        }
        if (next === 'submitted') {
          state = 'submitted';
          break;
        }
        return next;
      }
      case 'submitted':
        return await finish(d, lease, row);
      default:
        return 'skipped';
    }
  }
}

/** The only path to the paid call. Intent is durable first; no connection is held during the call. */
async function submit(d: Deps, lease: Lease, keeper: LeaseKeeper, row: JobRow): Promise<JobState> {
  await fencedUpdate(
    d.pool,
    lease,
    `state = 'submitting', submit_attempts = submit_attempts + 1, last_submit_at = now()`,
  );
  await d.failpoint?.('after_intent', lease.jobId);

  keeper.assertHeldLocally();
  const res = await d.provider.submit(row.idempotency_key, row.prompt, keeper.signal);
  await d.failpoint?.('after_provider', lease.jobId);

  if (res.kind === 'accepted') {
    await fencedUpdate(d.pool, lease, `state = 'submitted', provider_request_id = $3`, [res.requestId]);
    return 'submitted';
  }
  if (res.kind === 'rejected') {
    await fencedUpdate(d.pool, lease, `state = 'failed', failure_reason = $3, lease_owner = NULL, lease_expires_at = NULL`, [
      `provider_rejected: ${res.reason}`,
    ]);
    return 'failed';
  }
  await fencedUpdate(d.pool, lease, `state = 'unknown', failure_reason = $3`, [`submit_outcome_unknown: ${res.reason}`]);
  return 'unknown';
}

/** Ask the provider what happened. Only a confirmed absence, after the settle window, allows a resend. */
async function reconcile(d: Deps, lease: Lease, row: JobRow): Promise<'submitted' | 'resubmit' | 'released' | 'flagged'> {
  const found = await d.provider.lookup(row.idempotency_key);

  if (found.kind === 'found') {
    await fencedUpdate(
      d.pool,
      lease,
      `state = 'submitted', provider_request_id = $3, needs_review = false, review_reason = NULL`,
      [found.requestId],
    );
    return 'submitted';
  }
  if (found.kind === 'unavailable') {
    await release(d.pool, lease, d.cfg.settleMs / 2);
    return 'released';
  }

  const age = await msSinceLastSubmit(d.pool, lease.jobId);
  if (age < d.cfg.settleMs) {
    await release(d.pool, lease, d.cfg.settleMs - age + 10);
    return 'released';
  }
  if (row.needs_review) {
    await release(d.pool, lease, d.cfg.settleMs * 5); // flagged jobs are only ever adopted, never resent
    return 'flagged';
  }

  const latest = await fencedUpdate(d.pool, lease, `updated_at = updated_at`);
  if (d.cfg.resubmitPolicy === 'resubmit' && latest.submit_attempts < d.cfg.maxSubmitAttempts) return 'resubmit';

  await fencedUpdate(d.pool, lease, `needs_review = true, review_reason = $3`, [
    `no provider record after ${latest.submit_attempts} attempt(s); outcome unconfirmed`,
  ]);
  await release(d.pool, lease, d.cfg.settleMs * 5);
  return 'flagged';
}

/** Poll for a bounded time, then hand the job back instead of squatting on a worker slot. */
async function finish(d: Deps, lease: Lease, row: JobRow): Promise<Outcome> {
  const stopAt = performance.now() + d.cfg.pollBudgetMs;
  while (performance.now() < stopAt) {
    const s = await d.provider.lookup(row.idempotency_key);
    if (s.kind === 'found' && s.status === 'succeeded') {
      await d.failpoint?.('during_result', lease.jobId);
      await writeTrackFenced(d.pool, lease, s.audioUrl!, s.durationMs!);
      await d.failpoint?.('after_result_write', lease.jobId);
      await fencedUpdate(
        d.pool,
        lease,
        `state = 'completed', failure_reason = NULL, lease_owner = NULL, lease_expires_at = NULL`,
      );
      return 'completed';
    }
    if (s.kind === 'found' && s.status === 'failed') {
      await fencedUpdate(
        d.pool,
        lease,
        `state = 'failed', failure_reason = 'generation_failed', lease_owner = NULL, lease_expires_at = NULL`,
      );
      return 'failed';
    }
    await sleep(d.cfg.pollMs);
  }
  await release(d.pool, lease, d.cfg.pollMs);
  return 'released';
}
