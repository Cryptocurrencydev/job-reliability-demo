import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import pg from 'pg';
import { dbUrl } from '../src/config.ts';
import { makePool, migrate } from '../src/db.ts';
import { startMockProvider, type MockProvider } from '../src/provider/server.ts';
import { processJobBroken } from '../src/worker/broken.ts';
import { processJob, type Deps } from '../src/worker/pipeline.ts';
import { addJobs, deps, jobs, record, resetDb, sleep } from './helpers.ts';

const APP = 'pool-under-test';
const POOL_MAX = 4;
const CONCURRENCY = 20;
const JOBS = 60;

let admin: pg.Client;
let harness: pg.Pool;
let provider: MockProvider;

beforeAll(async () => {
  harness = makePool({ max: 2, application_name: 'test-harness' });
  await migrate(harness);
  admin = new pg.Client({ connectionString: dbUrl(), application_name: 'pool-observer' });
  await admin.connect();
});
afterAll(async () => {
  await admin.end();
  await harness.end();
});
beforeEach(async () => {
  await resetDb(harness);
  provider = await startMockProvider();
});
afterEach(() => provider.close());

async function load(run: (d: Deps, id: string) => Promise<unknown>, connectionTimeoutMillis: number) {
  const pool = makePool({ max: POOL_MAX, application_name: APP, connectionTimeoutMillis });
  const batch = `load-${Math.random()}`;
  const list = await addJobs(harness, batch, JOBS);
  provider.setDefault({ ackDelayMs: 150, generationMs: 10 });
  const d = deps(pool, provider.url, { providerTimeoutMs: 2000, settleMs: 3000, leaseTtlMs: 4000, heartbeatMs: 1000, pollBudgetMs: 3000 });

  let peakWaiting = 0;
  let waitingSum = 0;
  let samples = 0;
  const sampler = setInterval(() => {
    peakWaiting = Math.max(peakWaiting, pool.waitingCount);
    waitingSum += pool.waitingCount;
    samples++;
  }, 5);
  let peakIdleInTx = 0;
  let observing = true;
  const observer = (async () => {
    while (observing) {
      const r = await admin.query(
        `SELECT count(*)::int AS n FROM pg_stat_activity WHERE application_name = $1 AND state = 'idle in transaction'`,
        [APP],
      );
      peakIdleInTx = Math.max(peakIdleInTx, r.rows[0].n);
      await sleep(15);
    }
  })();

  let failed = 0;
  const queue = list.map((j) => j.id);
  await Promise.all(
    Array.from({ length: CONCURRENCY }, async () => {
      for (let id = queue.shift(); id; id = queue.shift()) await run(d, id).catch(() => failed++);
    }),
  );
  clearInterval(sampler);
  observing = false;
  await observer;
  const completed = (await jobs(harness, batch)).filter((j) => j.state === 'completed').length;
  await pool.end();
  const meanWaiting = +(waitingSum / Math.max(1, samples)).toFixed(2);
  return { peakWaiting, meanWaiting, peakIdleInTx, peakProviderInFlight: provider.peakInFlight(), failed, completed, poolMax: POOL_MAX, concurrency: CONCURRENCY, jobs: JOBS };
}

describe('pool behaviour under load', () => {
  it('safe: provider calls outnumber connections, nothing waits on them, nothing times out', async () => {
    const m = await load(processJob, 3000);
    record('pool', 'safe', m);
    expect(m.completed).toBe(JOBS);
    expect(m.failed).toBe(0);
    expect(m.peakIdleInTx).toBe(0);
    // More calls in flight than connections exist: none of them can be holding one.
    expect(m.peakProviderInFlight).toBeGreaterThan(POOL_MAX);
    // Each slot has at most one short query queued, so this cannot grow with job count.
    expect(m.peakWaiting).toBeLessThanOrEqual(CONCURRENCY);
  });

  it('broken: the same load is capped by the pool, and work times out', async () => {
    const m = await load(processJobBroken, 300);
    record('pool', 'broken', m);
    expect(m.peakProviderInFlight).toBeLessThanOrEqual(POOL_MAX);
    expect(m.peakIdleInTx).toBeGreaterThan(0);
    expect(m.failed).toBeGreaterThan(0);
    expect(m.completed).toBeLessThan(JOBS);
  });

  it('no connection or transaction exists at the moment of the paid call', async () => {
    const pool = makePool({ max: POOL_MAX, application_name: APP });
    const [j] = await addJobs(harness, 'moment', 1);
    let seen: { checkedOut: number; inTx: number } | undefined;
    provider.onSubmit = async () => {
      const r = await admin.query(
        `SELECT count(*)::int AS n FROM pg_stat_activity WHERE application_name = $1 AND state <> 'idle'`,
        [APP],
      );
      seen = { checkedOut: pool.totalCount - pool.idleCount, inTx: r.rows[0].n };
    };
    // A long heartbeat so a renewal cannot legitimately overlap the sample.
    await processJob(deps(pool, provider.url, { leaseTtlMs: 20_000, heartbeatMs: 9000 }), j!.id);
    await pool.end();
    expect(seen).toEqual({ checkedOut: 0, inTx: 0 });
  });

  it('the same moment in broken mode shows the held connection (the check can fail)', async () => {
    const pool = makePool({ max: POOL_MAX, application_name: APP });
    const [j] = await addJobs(harness, 'moment-broken', 1);
    let seen: { checkedOut: number; inTx: number } | undefined;
    provider.onSubmit = async () => {
      const r = await admin.query(
        `SELECT count(*)::int AS n FROM pg_stat_activity WHERE application_name = $1 AND state <> 'idle'`,
        [APP],
      );
      seen = { checkedOut: pool.totalCount - pool.idleCount, inTx: r.rows[0].n };
    };
    await processJobBroken(deps(pool, provider.url), j!.id);
    await pool.end();
    expect(seen).toEqual({ checkedOut: 1, inTx: 1 });
  });
});
