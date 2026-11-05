import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { PgBoss } from 'pg-boss';
import type pg from 'pg';
import { makePool, migrate } from '../src/db.ts';
import { startMockProvider, type MockProvider } from '../src/provider/server.ts';
import { enqueue, startBoss } from '../src/worker/queue.ts';
import { checkInvariants, CLEAN, Cluster, jobs, record, resetDb, sleep, waitSettled } from './helpers.ts';

/**
 * The same scenario through both code paths, judged by the same invariant function.
 * Broken mode must FAIL the invariants, or the safe design has proved nothing.
 */
let pool: pg.Pool;
let provider: MockProvider;
let boss: PgBoss | undefined;
let cluster: Cluster | undefined;

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
  await cluster?.stop();
  await boss?.stop({ graceful: false, timeout: 1000 }).catch(() => {});
  cluster = undefined;
  boss = undefined;
  await provider.close();
});

async function scenario(o: {
  mode: 'safe' | 'broken';
  batch: string;
  retryLimit: number;
  killAt?: string;
  behaviour?: Parameters<MockProvider['setBehaviour']>[1];
  settleMs: number;
}) {
  boss = await startBoss({ fast: true, retryLimit: o.retryLimit, expireInSeconds: 2 });
  const id = (await enqueue(pool, boss, { clientRequestId: o.batch, prompt: 'x', batch: o.batch }))!;
  const key = (await jobs(pool, o.batch))[0]!.idempotency_key;
  if (o.behaviour) provider.setBehaviour(key, o.behaviour);
  if (o.killAt) await pool.query('INSERT INTO chaos_plan (job_id, step) VALUES ($1, $2)', [id, o.killAt]);

  cluster = new Cluster({ mode: o.mode, providerUrl: provider.url, retryLimit: o.retryLimit, expireSeconds: 2 });
  await cluster.start();
  const rows = await waitSettled(pool, o.batch, o.settleMs);
  await sleep(300);
  const v = await checkInvariants(pool, o.batch, provider);
  return { state: rows[0]!.state, charges: provider.charges(key), violations: v, deaths: cluster.deaths };
}

describe('crash right after the provider call, pg-boss retries on', () => {
  it('broken: the retry pays again', async () => {
    const r = await scenario({ mode: 'broken', batch: 'dup-b', retryLimit: 3, killAt: 'after_provider', settleMs: 15_000 });
    record('comparison', 'crash_after_paying', { ...(readPrev('crash_after_paying')), broken: pick(r) });
    expect(r.deaths).toBe(1);
    expect(r.charges).toBe(2);
    expect(r.violations.duplicates).toHaveLength(1);
  });

  it('safe: same crash, one charge', async () => {
    const r = await scenario({ mode: 'safe', batch: 'dup-s', retryLimit: 3, killAt: 'after_provider', settleMs: 15_000 });
    record('comparison', 'crash_after_paying', { ...(readPrev('crash_after_paying')), safe: pick(r) });
    expect(r.deaths).toBe(1);
    expect(r.charges).toBe(1);
    expect(r.violations).toEqual(CLEAN);
  });
});

describe('crash right after the provider call, pg-boss retries off', () => {
  it('broken: customer paid for, job stuck forever, nothing reports it', async () => {
    const r = await scenario({ mode: 'broken', batch: 'lost-b', retryLimit: 0, killAt: 'after_provider', settleMs: 7000 });
    record('comparison', 'crash_no_retries', { ...(readPrev('crash_no_retries')), broken: pick(r) });
    expect(r.deaths).toBe(1);
    expect(r.state).toBe('queued');
    expect(r.charges).toBe(1);
    expect(r.violations.lost).toHaveLength(1);
  });

  it('safe: same crash, the ledger recovers it without pg-boss', async () => {
    const r = await scenario({ mode: 'safe', batch: 'lost-s', retryLimit: 0, killAt: 'after_provider', settleMs: 15_000 });
    record('comparison', 'crash_no_retries', { ...(readPrev('crash_no_retries')), safe: pick(r) });
    expect(r.state).toBe('completed');
    expect(r.charges).toBe(1);
    expect(r.violations).toEqual(CLEAN);
  });
});

describe('provider times out after charging', () => {
  it('broken: every retry is another charge', async () => {
    const r = await scenario({ mode: 'broken', batch: 'to-b', retryLimit: 3, behaviour: { mode: 'blackhole' }, settleMs: 6000 });
    record('comparison', 'provider_timeout', { ...(readPrev('provider_timeout')), broken: pick(r) });
    expect(r.charges).toBeGreaterThanOrEqual(3);
    expect(r.violations.duplicates).toHaveLength(1);
  });

  it('safe: looks the charge up, one charge, job completes', async () => {
    const r = await scenario({ mode: 'safe', batch: 'to-s', retryLimit: 3, behaviour: { mode: 'blackhole' }, settleMs: 15_000 });
    record('comparison', 'provider_timeout', { ...(readPrev('provider_timeout')), safe: pick(r) });
    expect(r.state).toBe('completed');
    expect(r.charges).toBe(1);
    expect(r.violations).toEqual(CLEAN);
  });
});

const pick = (r: { state: string; charges: number; violations: { lost: string[] } }) => ({
  state: r.state,
  charges: r.charges,
  lost: r.violations.lost.length,
});

import { existsSync, readFileSync } from 'node:fs';
function readPrev(key: string) {
  return existsSync('results/comparison.json') ? JSON.parse(readFileSync('results/comparison.json', 'utf8'))[key] : {};
}
