import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import fc from 'fast-check';
import type pg from 'pg';
import { DEFAULTS, FAST, validate } from '../src/config.ts';
import { makePool, migrate } from '../src/db.ts';
import { claim, Fenced, fencedUpdate, findStale, idempotencyKey, insertJob, renewLease, type Lease } from '../src/worker/ledger.ts';
import { addJobs, resetDb, sleep } from './helpers.ts';

let pool: pg.Pool;
beforeAll(async () => {
  pool = makePool({ max: 4, application_name: 'test-harness' });
  await migrate(pool);
});
afterAll(() => pool.end());
beforeEach(() => resetDb(pool));

const STATES = ['queued', 'submitting', 'submitted', 'completed', 'failed', 'unknown'];
const LEGAL = new Set([
  'queued>submitting',
  'submitting>submitted', 'submitting>unknown', 'submitting>failed',
  'unknown>submitted', 'unknown>submitting', 'unknown>failed',
  'submitted>completed', 'submitted>failed', 'submitted>unknown',
]);

describe('state machine is enforced by the database', () => {
  const pairs = STATES.flatMap((a) => STATES.filter((b) => a !== b).map((b) => [a, b] as const));
  it.each(pairs)('%s -> %s', async (from, to) => {
    const [j] = await addJobs(pool, `t-${from}-${to}`, 1);
    await pool.query(`ALTER TABLE gen_jobs DISABLE TRIGGER gen_jobs_guard_trg`);
    await pool.query('UPDATE gen_jobs SET state = $2 WHERE id = $1', [j!.id, from]);
    await pool.query(`ALTER TABLE gen_jobs ENABLE TRIGGER gen_jobs_guard_trg`);
    const attempt = pool.query('UPDATE gen_jobs SET state = $2 WHERE id = $1', [j!.id, to]);
    if (LEGAL.has(`${from}>${to}`)) await expect(attempt).resolves.toBeTruthy();
    else await expect(attempt).rejects.toThrow(/illegal job transition/);
  });

  it('the idempotency key cannot be changed', async () => {
    const [j] = await addJobs(pool, 'immutable', 1);
    await expect(pool.query(`UPDATE gen_jobs SET idempotency_key = 'other' WHERE id = $1`, [j!.id])).rejects.toThrow(/immutable/);
  });

  it('the fencing token cannot go backwards', async () => {
    const [j] = await addJobs(pool, 'backwards', 1);
    await claim(pool, j!.id, 'a', 1000);
    await expect(pool.query('UPDATE gen_jobs SET lease_version = 0 WHERE id = $1', [j!.id])).rejects.toThrow(/backwards/);
  });
});

describe('idempotency keys', () => {
  it('are deterministic and collision-free over random inputs', () => {
    fc.assert(
      fc.property(fc.string(), fc.string(), (a, b) => {
        expect(idempotencyKey(a)).toBe(idempotencyKey(a));
        if (a !== b) expect(idempotencyKey(a)).not.toBe(idempotencyKey(b));
      }),
    );
  });

  it('a repeated client request does not create a second job', async () => {
    const first = await insertJob(pool, { clientRequestId: 'dbl', prompt: 'x' });
    const second = await insertJob(pool, { clientRequestId: 'dbl', prompt: 'x' });
    expect(first).toBeTruthy();
    expect(second).toBeNull();
  });
});

describe('leases and fencing', () => {
  it('a live lease cannot be claimed; an expired one can, with a higher token', async () => {
    const [j] = await addJobs(pool, 'lease', 1);
    const a = await claim(pool, j!.id, 'a', 80);
    expect(await claim(pool, j!.id, 'b', 80)).toBeNull();
    await sleep(120);
    const b = await claim(pool, j!.id, 'b', 80);
    expect(Number(b!.lease_version)).toBe(Number(a!.lease_version) + 1);
  });

  it('only one of many simultaneous claimers wins', async () => {
    const [j] = await addJobs(pool, 'race', 1);
    const wins = await Promise.all(Array.from({ length: 12 }, (_, i) => claim(pool, j!.id, `w${i}`, 5000)));
    expect(wins.filter(Boolean)).toHaveLength(1);
  });

  it('a superseded worker cannot write or renew', async () => {
    const [j] = await addJobs(pool, 'stale', 1);
    const a = (await claim(pool, j!.id, 'a', 1))!;
    await sleep(10);
    await claim(pool, j!.id, 'b', 5000);
    const old: Lease = { jobId: j!.id, version: a.lease_version, owner: 'a' };
    await expect(fencedUpdate(pool, old, `state = 'submitting'`)).rejects.toBeInstanceOf(Fenced);
    expect(await renewLease(pool, old, 5000)).toBe(false);
  });

  it('property: over any interleaving, only the newest lease can write', async () => {
    await fc.assert(
      fc.asyncProperty(fc.array(fc.nat({ max: 3 }), { minLength: 2, maxLength: 12 }), async (ops) => {
        await resetDb(pool);
        const [j] = await addJobs(pool, 'interleave', 1);
        const leases: Lease[] = [];
        for (const pick of ops) {
          await pool.query('UPDATE gen_jobs SET lease_expires_at = NULL WHERE id = $1', [j!.id]);
          const r = await claim(pool, j!.id, `w${leases.length}`, 1000);
          leases.push({ jobId: j!.id, version: r!.lease_version, owner: r!.lease_owner! });
          const target = leases[Math.min(pick, leases.length - 1)]!;
          const newest = target === leases[leases.length - 1];
          const write = fencedUpdate(pool, target, `failure_reason = 'x'`);
          if (newest) await expect(write).resolves.toBeTruthy();
          else await expect(write).rejects.toBeInstanceOf(Fenced);
        }
      }),
      { numRuns: 25 },
    );
  });

  it('stale detection finds a dead lease and a job whose queue message never existed', async () => {
    const [dead, orphan, live] = await addJobs(pool, 'sweep', 3);
    await claim(pool, dead!.id, 'gone', 1);
    await claim(pool, live!.id, 'alive', 60_000);
    await sleep(30);
    const stale = await findStale(pool, 0);
    expect(stale).toContain(dead!.id);
    expect(stale).toContain(orphan!.id);
    expect(stale).not.toContain(live!.id);
  });
});

describe('config guards', () => {
  it('ships valid defaults', () => {
    expect(validate(DEFAULTS)).toBeTruthy();
    expect(validate(FAST)).toBeTruthy();
  });
  it('refuses a settle window shorter than the provider timeout', () => {
    expect(() => validate({ ...DEFAULTS, settleMs: DEFAULTS.providerTimeoutMs })).toThrow(/settleMs/);
  });
  it('refuses a lease a single missed heartbeat would lose', () => {
    expect(() => validate({ ...DEFAULTS, heartbeatMs: DEFAULTS.leaseTtlMs })).toThrow(/leaseTtlMs/);
  });
});
