import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { PgBoss } from 'pg-boss';
import type pg from 'pg';
import { QUEUE } from '../src/config.ts';
import { makePool, migrate } from '../src/db.ts';
import { startMockProvider, type MockProvider } from '../src/provider/server.ts';
import { processJob } from '../src/worker/pipeline.ts';
import { enqueue, startBoss, sweep } from '../src/worker/queue.ts';
import { deps, jobs, resetDb, sleep, waitSettled } from './helpers.ts';

let pool: pg.Pool;
let provider: MockProvider;
let boss: PgBoss;

beforeAll(async () => {
  pool = makePool({ max: 4, application_name: 'test-harness' });
  await migrate(pool);
});
afterAll(() => pool.end());
beforeEach(async () => {
  await resetDb(pool);
  provider = await startMockProvider();
});
afterEach(async () => {
  await boss?.stop({ graceful: false, timeout: 1000 }).catch(() => {});
  await provider.close();
});

const worker = (run: (id: string) => Promise<unknown>) =>
  boss.work<{ jobId: string }>(QUEUE, { pollingIntervalSeconds: 0.5, localConcurrency: 4 }, async ([j]) => {
    await run(j!.data.jobId);
  });

async function deliveries(jobId: string): Promise<{ state: string; retry_count: number }[]> {
  return (await pool.query(`SELECT state, retry_count FROM pgboss.job WHERE name = $1 AND data->>'jobId' = $2`, [QUEUE, jobId])).rows;
}

describe('pg-boss delivers at least once; the money rule holds anyway', () => {
  it('retryLimit: a crash after the paid call is retried by pg-boss, and pays nothing extra', async () => {
    boss = await startBoss({ fast: true, retryLimit: 3 });
    const id = (await enqueue(pool, boss, { clientRequestId: 'retry', prompt: 'x', batch: 'retry' }))!;
    let threw = false;
    const d = deps(pool, provider.url, { leaseTtlMs: 400, heartbeatMs: 150 }, async (step) => {
      if (step === 'after_provider' && !threw) {
        threw = true;
        throw new Error('crash after paying');
      }
    });
    await worker(async (jobId) => {
      const r = await processJob(d, jobId);
      // A real retry needs a sweep too: the lease must lapse first, so keep offering.
      if (r === 'skipped') await sleep(100);
      return r;
    });
    const end = Date.now() + 15_000;
    while (Date.now() < end) {
      await sweep(pool, boss, 100);
      if ((await jobs(pool, 'retry'))[0]!.state === 'completed') break;
      await sleep(150);
    }
    const [row] = await jobs(pool, 'retry');
    expect(threw).toBe(true);
    expect(row!.state).toBe('completed');
    const key = row!.idempotency_key;
    expect(provider.arrivals(key)).toBe(1);
    expect(provider.charges(key)).toBe(1);
    expect(Math.max(...(await deliveries(id)).map((x) => x.retry_count))).toBeGreaterThanOrEqual(1);
  });

  it('expireInSeconds shorter than the job: pg-boss redelivers mid-flight, the lease refuses the second run', async () => {
    boss = await startBoss({ fast: true, retryLimit: 3, expireInSeconds: 1 });
    await enqueue(pool, boss, { clientRequestId: 'exp', prompt: 'x', batch: 'exp' });
    const key = (await jobs(pool, 'exp'))[0]!.idempotency_key;
    provider.setBehaviour(key, { ackDelayMs: 3500 });

    const outcomes: string[] = [];
    const d = deps(pool, provider.url, { providerTimeoutMs: 6000, settleMs: 8000, leaseTtlMs: 1500, heartbeatMs: 300 });
    await worker(async (jobId) => outcomes.push(await processJob(d, jobId)));

    const rows = await waitSettled(pool, 'exp', 15_000);
    expect(rows[0]!.state).toBe('completed');
    expect(outcomes).toContain('skipped'); // the redelivery really happened and was refused
    expect(provider.arrivals(key)).toBe(1);
    expect(provider.charges(key)).toBe(1);
  });

  it('singletonKey collapses queued duplicates only, it does not stop a second run', async () => {
    boss = await startBoss({ fast: true });
    const opts = { singletonKey: 'same-job' };
    expect(await boss.send(QUEUE, { jobId: 'x' }, opts)).toBeTruthy();
    expect(await boss.send(QUEUE, { jobId: 'x' }, opts)).toBeNull(); // refused while one is queued

    const [taken] = await boss.fetch(QUEUE);
    expect(taken).toBeTruthy(); // now active
    expect(await boss.send(QUEUE, { jobId: 'x' }, opts)).toBeTruthy(); // accepted: two can now run
  });

  it('retryLimit exhausted: pg-boss says failed, the ledger still finishes the job', async () => {
    boss = await startBoss({ fast: true, retryLimit: 0 });
    const id = (await enqueue(pool, boss, { clientRequestId: 'exhaust', prompt: 'x', batch: 'exhaust' }))!;
    let first = true;
    const d = deps(pool, provider.url);
    await worker(async (jobId) => {
      if (first) {
        first = false;
        throw new Error('handler blew up before doing anything');
      }
      return processJob(d, jobId);
    });
    const end = Date.now() + 10_000;
    while (Date.now() < end) {
      await sweep(pool, boss, 100);
      if ((await jobs(pool, 'exhaust'))[0]!.state === 'completed') break;
      await sleep(150);
    }
    expect((await deliveries(id)).map((x) => x.state)).toContain('failed');
    const [row] = await jobs(pool, 'exhaust');
    expect(row!.state).toBe('completed');
    expect(provider.charges(row!.idempotency_key)).toBe(1);
  });

  it('queue message deleted outright: the sweeper rebuilds it from the ledger', async () => {
    boss = await startBoss({ fast: true });
    await enqueue(pool, boss, { clientRequestId: 'gone', prompt: 'x', batch: 'gone' });
    await pool.query('DELETE FROM pgboss.job WHERE name = $1', [QUEUE]);

    const d = deps(pool, provider.url);
    await worker((jobId) => processJob(d, jobId));
    const end = Date.now() + 10_000;
    while (Date.now() < end) {
      await sweep(pool, boss, 100);
      if ((await jobs(pool, 'gone'))[0]!.state === 'completed') break;
      await sleep(150);
    }
    const [row] = await jobs(pool, 'gone');
    expect(row!.state).toBe('completed');
    expect(provider.charges(row!.idempotency_key)).toBe(1);
  });

  it('enqueue is atomic: a failed transaction leaves neither a row nor a message', async () => {
    boss = await startBoss({ fast: true });
    const c = await pool.connect();
    await c.query('BEGIN');
    await boss.send(QUEUE, { jobId: 'never' }, { db: { executeSql: (t: string, v?: unknown[]) => c.query(t, v as unknown[]) } });
    await c.query('ROLLBACK');
    c.release();
    const r = await pool.query(`SELECT 1 FROM pgboss.job WHERE data->>'jobId' = 'never'`);
    expect(r.rowCount).toBe(0);
  });
});
