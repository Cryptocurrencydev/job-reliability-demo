import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { ChildProcess } from 'node:child_process';
import type pg from 'pg';
import { makePool, migrate } from '../src/db.ts';
import { startMockProvider, type MockProvider } from '../src/provider/server.ts';
import { STEPS } from '../src/worker/pipeline.ts';
import { addJobs, checkInvariants, CLEAN, Cluster, jobs, resetDb, sleep, spawnWorker, waitSettled } from './helpers.ts';

let pool: pg.Pool;
let provider: MockProvider;
let cluster: Cluster | undefined;
const paused = new Set<ChildProcess>(); // cleaned up even when an assertion fails

beforeAll(async () => {
  pool = makePool({ max: 4, application_name: 'test-harness' });
  await migrate(pool);
});
afterAll(async () => {
  await pool.end();
});
beforeEach(async () => {
  await resetDb(pool);
  provider = await startMockProvider();
});
afterEach(async () => {
  for (const c of paused) c.kill('SIGKILL');
  paused.clear();
  await cluster?.stop();
  cluster = undefined;
  await provider.close();
});

describe('SIGKILL at every step, restart, one paid submission', () => {
  it.each(STEPS)('killed at %s', async (step) => {
    const batch = `kill-${step}`;
    const [job, other] = await addJobs(pool, batch, 2);
    await pool.query('INSERT INTO chaos_plan (job_id, step) VALUES ($1, $2)', [job!.id, step]);

    cluster = new Cluster({ providerUrl: provider.url });
    await cluster.start();
    const rows = await waitSettled(pool, batch);

    expect(cluster.deaths).toBe(1); // the kill really happened
    expect(await checkInvariants(pool, batch, provider)).toEqual(CLEAN);
    expect(rows.map((r) => r.state)).toEqual(['completed', 'completed']);
    expect(provider.charges(job!.key)).toBe(1);
    expect(provider.charges(other!.key)).toBe(1);
  });

  it('survives being killed at every step in turn on the same job', async () => {
    const batch = 'kill-all';
    const [job] = await addJobs(pool, batch, 1);
    for (const step of STEPS) await pool.query('INSERT INTO chaos_plan (job_id, step) VALUES ($1, $2)', [job!.id, step]);

    cluster = new Cluster({ providerUrl: provider.url });
    await cluster.start();
    const rows = await waitSettled(pool, batch, 45_000);

    expect(cluster.deaths).toBe(STEPS.length);
    expect(rows[0]!.state).toBe('completed');
    expect(provider.charges(job!.key)).toBe(1);
    expect(await checkInvariants(pool, batch, provider)).toEqual(CLEAN);
  });

  it('killed while the provider call is in flight, charge lands after the worker is dead', async () => {
    const batch = 'kill-inflight';
    const [job] = await addJobs(pool, batch, 1);
    provider.setBehaviour(job!.key, { ingestDelayMs: 200 });
    provider.onSubmit = () => {
      for (const c of cluster?.children ?? []) c.kill('SIGKILL');
      provider.onSubmit = undefined;
    };

    cluster = new Cluster({ providerUrl: provider.url });
    await cluster.start();
    const rows = await waitSettled(pool, batch);

    expect(cluster.deaths).toBe(1);
    expect(rows[0]!.state).toBe('completed');
    expect(provider.charges(job!.key)).toBe(1);
  });
});

describe('production default policy (flag): never resends automatically', () => {
  const cfg = { resubmitPolicy: 'flag' };

  it('killed after paying: the charge is found, the job completes', async () => {
    const batch = 'flag-paid';
    const [job] = await addJobs(pool, batch, 1);
    await pool.query(`INSERT INTO chaos_plan (job_id, step) VALUES ($1, 'after_provider')`, [job!.id]);
    cluster = new Cluster({ providerUrl: provider.url, cfg });
    await cluster.start();
    const rows = await waitSettled(pool, batch);
    expect(cluster.deaths).toBe(1);
    expect(rows[0]!.state).toBe('completed');
    expect(provider.charges(job!.key)).toBe(1);
  });

  it('killed before paying: no record at the provider, so it is flagged for a human, zero charges', async () => {
    const batch = 'flag-unpaid';
    const [job] = await addJobs(pool, batch, 1);
    await pool.query(`INSERT INTO chaos_plan (job_id, step) VALUES ($1, 'after_intent')`, [job!.id]);
    cluster = new Cluster({ providerUrl: provider.url, cfg });
    await cluster.start();
    const rows = await waitSettled(pool, batch);
    expect(cluster.deaths).toBe(1);
    expect(rows[0]!.needs_review).toBe(true);
    expect(rows[0]!.state).toBe('unknown');
    expect(provider.arrivals(job!.key)).toBe(0);
    expect(await checkInvariants(pool, batch, provider)).toEqual(CLEAN);
  });
});

describe('fencing: a paused worker wakes up after being replaced', () => {
  it('paused before the paid call: wakes, sees its lease lapsed, does not spend', async () => {
    const batch = 'stall-before';
    const [job] = await addJobs(pool, batch, 1);
    await pool.query(`INSERT INTO chaos_plan (job_id, step, action) VALUES ($1, 'after_intent', 'stall')`, [job!.id]);

    const a = await spawnWorker({ providerUrl: provider.url });
    paused.add(a);
    await untilPlanConsumed(job!.id);
    await sleep(150); // A is now frozen by SIGSTOP

    cluster = new Cluster({ providerUrl: provider.url });
    await cluster.start();
    const rows = await waitSettled(pool, batch);
    expect(rows[0]!.state).toBe('completed');

    a.kill('SIGCONT');
    await sleep(800);
    expect(provider.charges(job!.key)).toBe(1);
  });

  it('paused after the paid call: its late writes are refused, the new owner finishes', async () => {
    const batch = 'stall-after';
    const [job] = await addJobs(pool, batch, 1);
    await pool.query(`INSERT INTO chaos_plan (job_id, step, action) VALUES ($1, 'after_provider', 'stall')`, [job!.id]);

    const a = await spawnWorker({ providerUrl: provider.url });
    paused.add(a);
    await untilPlanConsumed(job!.id);
    await sleep(150);

    cluster = new Cluster({ providerUrl: provider.url });
    await cluster.start();
    const rows = await waitSettled(pool, batch);
    expect(rows[0]!.state).toBe('completed');
    const finalVersion = rows[0]!.lease_version;

    a.kill('SIGCONT');
    await sleep(800);

    const after = (await jobs(pool, batch))[0]!;
    expect(after.state).toBe('completed');
    expect(after.lease_version).toBe(finalVersion);
    expect(provider.charges(job!.key)).toBe(1);
    const ev = await pool.query('SELECT lease_version FROM gen_job_events WHERE job_id = $1 ORDER BY id', [job!.id]);
    const versions = ev.rows.map((r) => Number(r.lease_version));
    expect([...versions].sort((x, y) => x - y)).toEqual(versions); // no stale write in the history
  });
});

async function untilPlanConsumed(jobId: string) {
  for (let i = 0; i < 200; i++) {
    const r = await pool.query('SELECT 1 FROM chaos_plan WHERE job_id = $1', [jobId]);
    if (r.rowCount === 0) return;
    await sleep(50);
  }
  throw new Error('chaos plan was not consumed');
}
