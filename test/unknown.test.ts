import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type pg from 'pg';
import { makePool, migrate } from '../src/db.ts';
import { startMockProvider, type MockProvider } from '../src/provider/server.ts';
import { processJob } from '../src/worker/pipeline.ts';
import { addJobs, deps, drive, jobs, resetDb, sleep } from './helpers.ts';

let pool: pg.Pool;
let provider: MockProvider;

beforeAll(async () => {
  pool = makePool({ max: 4, application_name: 'test-harness' });
  await migrate(pool);
});
afterAll(() => pool.end());
beforeEach(async () => {
  await resetDb(pool);
  provider = await startMockProvider();
});
afterEach(() => provider.close());

async function one(batch: string) {
  return (await addJobs(pool, batch, 1))[0]!;
}

describe('unknown outcomes are looked up, never blindly resent', () => {
  it('timeout after the charge: adopts the existing generation, no second POST', async () => {
    const j = await one('blackhole');
    provider.setBehaviour(j.key, { mode: 'blackhole' });
    const row = await drive(deps(pool, provider.url), j.id);
    expect(row.state).toBe('completed');
    expect(provider.arrivals(j.key)).toBe(1);
    expect(provider.charges(j.key)).toBe(1);
  });

  it('5xx after the charge: same, a 502 is not proof of failure', async () => {
    const j = await one('502');
    provider.setBehaviour(j.key, { mode: 'error_charged' });
    const row = await drive(deps(pool, provider.url), j.id);
    expect(row.state).toBe('completed');
    expect(provider.arrivals(j.key)).toBe(1);
    expect(provider.charges(j.key)).toBe(1);
  });

  it('the job is in state unknown between the timeout and the lookup', async () => {
    const j = await one('unknown-state');
    provider.setBehaviour(j.key, { mode: 'blackhole' });
    await processJob(deps(pool, provider.url), j.id);
    const ev = await pool.query('SELECT to_state FROM gen_job_events WHERE job_id = $1 ORDER BY id', [j.id]);
    expect(ev.rows.map((r) => r.to_state)).toContain('unknown');
  });

  it('request never arrived: waits out the settle window, then resends with the SAME key', async () => {
    const j = await one('drop');
    provider.setBehaviour(j.key, { mode: 'drop', onlyFirst: true });
    const d = deps(pool, provider.url);
    await processJob(d, j.id);
    await processJob(d, j.id); // inside the window: must not POST again
    expect(provider.arrivals(j.key)).toBe(1);

    const row = await drive(d, j.id);
    expect(row.state).toBe('completed');
    expect(row.submit_attempts).toBe(2);
    expect(provider.arrivals(j.key)).toBe(2);
    expect(provider.charges(j.key)).toBe(1);
  });

  it('a late arrival inside the settle window is found, not duplicated', async () => {
    const j = await one('late');
    provider.setBehaviour(j.key, { ingestDelayMs: 400 }); // past the 250ms client timeout, inside settle
    const row = await drive(deps(pool, provider.url), j.id);
    expect(row.state).toBe('completed');
    expect(provider.charges(j.key)).toBe(1);
    expect(provider.arrivals(j.key)).toBe(1);
  });

  it('policy=flag: never resends, ends explicitly flagged with zero charges', async () => {
    const j = await one('flag');
    provider.setBehaviour(j.key, { mode: 'drop' });
    const row = await drive(deps(pool, provider.url, { resubmitPolicy: 'flag' }), j.id);
    expect(row.needs_review).toBe(true);
    expect(row.state).toBe('unknown');
    expect(provider.arrivals(j.key)).toBe(1);
  });

  it('resend attempts are capped, then the job is flagged', async () => {
    const j = await one('cap');
    provider.setBehaviour(j.key, { mode: 'drop' });
    const row = await drive(deps(pool, provider.url, { maxSubmitAttempts: 2 }), j.id);
    expect(row.needs_review).toBe(true);
    expect(provider.arrivals(j.key)).toBe(2);
    expect(provider.charges(j.key)).toBe(0);
  });

  it('provider lookup down: the job waits in unknown, does not resend, then recovers', async () => {
    const j = await one('down');
    provider.setBehaviour(j.key, { mode: 'blackhole' });
    provider.setLookupDown(true);
    const d = deps(pool, provider.url);
    await processJob(d, j.id);
    await sleep(600); // past the settle window: "can't ask" must not be treated as "not there"
    await pool.query('UPDATE gen_jobs SET next_check_at = NULL WHERE id = $1', [j.id]);
    await processJob(d, j.id);

    const [row] = await jobs(pool, 'down');
    expect(row!.state).toBe('unknown');
    expect(row!.needs_review).toBe(false);
    expect(provider.arrivals(j.key)).toBe(1);

    provider.setLookupDown(false);
    await pool.query('UPDATE gen_jobs SET next_check_at = NULL WHERE id = $1', [j.id]);
    const done = await drive(d, j.id);
    expect(done.state).toBe('completed');
    expect(provider.charges(j.key)).toBe(1);
  });
});

describe('the honest limit of the settle window', () => {
  // The provider takes longer to record the request than the settle window allows.
  const tooLate = { ingestDelayMs: 900 };

  it('KNOWN LIMIT: resubmit policy + provider without dedupe + arrival after the window = 2 charges', async () => {
    const j = await one('limit');
    provider.setBehaviour(j.key, tooLate);
    await drive(deps(pool, provider.url), j.id);
    await sleep(1000);
    expect(provider.charges(j.key)).toBe(2); // documented in the README; this test keeps the claim honest
  });

  it('fix 1: a provider that dedupes on the key closes it completely', async () => {
    const j = await one('dedupe');
    provider.setDedupe(true);
    provider.setBehaviour(j.key, tooLate);
    await drive(deps(pool, provider.url), j.id);
    await sleep(1000);
    expect(provider.charges(j.key)).toBe(1);
  });

  it('fix 2: policy=flag holds at one charge, and adopts the late arrival when it appears', async () => {
    const j = await one('adopt');
    provider.setBehaviour(j.key, tooLate);
    const d = deps(pool, provider.url, { resubmitPolicy: 'flag' });
    const flagged = await drive(d, j.id);
    expect(flagged.needs_review).toBe(true);

    await sleep(600);
    await pool.query('UPDATE gen_jobs SET next_check_at = NULL WHERE id = $1', [j.id]);
    await processJob(d, j.id);
    const [row] = await jobs(pool, 'adopt');
    expect(row!.state).toBe('completed');
    expect(row!.needs_review).toBe(false);
    expect(provider.charges(j.key)).toBe(1);
  });
});

describe('definite answers end the job', () => {
  it('a 4xx rejection fails explicitly, no charge, no retry', async () => {
    const j = await one('reject');
    provider.setBehaviour(j.key, { mode: 'reject' });
    const row = await drive(deps(pool, provider.url), j.id);
    expect(row.state).toBe('failed');
    expect(row.failure_reason).toContain('provider_rejected');
    expect(provider.arrivals(j.key)).toBe(1);
    expect(provider.charges(j.key)).toBe(0);
  });

  it('a generation that fails at the provider is failed once, not re-bought', async () => {
    const j = await one('genfail');
    provider.setBehaviour(j.key, { mode: 'fail_generation' });
    const row = await drive(deps(pool, provider.url), j.id);
    expect(row.state).toBe('failed');
    expect(row.failure_reason).toBe('generation_failed');
    expect(provider.charges(j.key)).toBe(1);
  });

  it('a poison job that keeps being claimed is flagged, not looped forever', async () => {
    const j = await one('poison');
    await pool.query('UPDATE gen_jobs SET lease_version = 100 WHERE id = $1', [j.id]);
    const row = await drive(deps(pool, provider.url), j.id);
    expect(row.needs_review).toBe(true);
    expect(row.review_reason).toBe('too_many_claims');
    expect(provider.arrivals(j.key)).toBe(0);
  });
});
