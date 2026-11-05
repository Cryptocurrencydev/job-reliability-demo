import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import fc from 'fast-check';
import type pg from 'pg';
import { makePool, migrate } from '../src/db.ts';
import { startMockProvider, type Behaviour, type MockProvider } from '../src/provider/server.ts';
import { STEPS } from '../src/worker/pipeline.ts';
import { addJobs, checkInvariants, CLEAN, Cluster, record, resetDb, waitSettled } from './helpers.ts';

const RUNS = Number(process.env.PROPERTY_RUNS ?? 15);

let pool: pg.Pool;
let provider: MockProvider;
let cluster: Cluster;

beforeAll(async () => {
  pool = makePool({ max: 4, application_name: 'test-harness' });
  await migrate(pool);
  await resetDb(pool);
  provider = await startMockProvider();
  cluster = new Cluster({ providerUrl: provider.url }, 2);
  await cluster.start();
});
afterAll(async () => {
  await cluster.stop();
  await provider.close();
  await pool.end();
});

// Every provider behaviour, with delays kept inside the settle window (the documented assumption).
const behaviour: fc.Arbitrary<Behaviour> = fc.oneof(
  fc.constant<Behaviour>({}),
  fc.integer({ min: 0, max: 400 }).map((ackDelayMs) => ({ ackDelayMs })), // may exceed the 250ms client timeout
  fc.integer({ min: 0, max: 200 }).map((ingestDelayMs) => ({ ingestDelayMs })),
  fc.integer({ min: 10, max: 500 }).map((generationMs) => ({ generationMs })),
  fc.constant<Behaviour>({ mode: 'blackhole' }),
  fc.constant<Behaviour>({ mode: 'error_charged' }),
  fc.constant<Behaviour>({ mode: 'error', onlyFirst: true }),
  fc.constant<Behaviour>({ mode: 'drop', onlyFirst: true }),
  fc.constant<Behaviour>({ mode: 'drop' }), // never arrives: must end flagged, not lost
  fc.constant<Behaviour>({ mode: 'reject' }),
  fc.constant<Behaviour>({ mode: 'fail_generation' }),
);

const plan = fc.record({
  behaviour,
  kills: fc.subarray(STEPS, { maxLength: 2 }),
});

describe('random kill points x random provider behaviour', () => {
  it(`holds the invariants over ${RUNS} generated scenarios`, { timeout: 15 * 60_000 }, async () => {
    let batchNo = 0;
    let totalJobs = 0;
    let totalKillsPlanned = 0;
    const seed = Number(process.env.PROPERTY_SEED ?? Date.now() % 2_147_483_647);

    await fc.assert(
      fc.asyncProperty(fc.array(plan, { minLength: 4, maxLength: 8 }), async (plans) => {
        const batch = `prop-${batchNo++}`;
        const list = await addJobs(pool, batch, plans.length);
        for (const [i, p] of plans.entries()) {
          provider.setBehaviour(list[i]!.key, p.behaviour);
          for (const step of p.kills) {
            await pool.query('INSERT INTO chaos_plan (job_id, step) VALUES ($1, $2)', [list[i]!.id, step]);
            totalKillsPlanned++;
          }
        }
        totalJobs += plans.length;
        const rows = await waitSettled(pool, batch, 60_000);
        expect(await checkInvariants(pool, batch, provider)).toEqual(CLEAN);
        for (const j of rows) expect(provider.charges(j.idempotency_key)).toBeLessThanOrEqual(1);
        return true;
      }),
      { numRuns: RUNS, seed, endOnFailure: true, verbose: 1 },
    );

    record('property', 'latest', {
      scenarios: RUNS,
      jobs: totalJobs,
      killsPlanned: totalKillsPlanned,
      realSigkills: cluster.deaths,
      paidSubmissions: provider.charges(),
      seed,
    });
    expect(cluster.deaths).toBeGreaterThan(0); // the run must really have killed things
  });
});
