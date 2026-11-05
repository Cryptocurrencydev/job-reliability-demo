import { PgBoss } from 'pg-boss';
import type pg from 'pg';
import { dbUrl, QUEUE } from '../config.ts';
import { findStale, insertJob } from './ledger.ts';

export interface BossOptions {
  retryLimit?: number;
  retryDelay?: number;
  expireInSeconds?: number;
  fast?: boolean;
}

/**
 * pg-boss is the doorbell, not the ledger. It delivers at least once, so everything here
 * assumes a job can be delivered twice, delivered late, or not at all.
 *
 * Policy `short` with singletonKey = job id: at most one QUEUED message per job, while an
 * active (possibly dead) delivery does not block the sweeper from queuing a replacement.
 */
export async function startBoss(o: BossOptions = {}): Promise<PgBoss> {
  const boss = new PgBoss({
    connectionString: dbUrl(),
    schema: 'pgboss',
    max: 3,
    application_name: 'tunesmith-boss',
    ...(o.fast ? { monitorIntervalSeconds: 1, superviseIntervalSeconds: 1 } : {}),
  });
  boss.on('error', () => {}); // a dropped connection surfaces here; the sweeper covers the gap
  await boss.start();
  await ensureQueue(boss, o);
  return boss;
}

export async function ensureQueue(boss: PgBoss, o: BossOptions = {}): Promise<void> {
  const options = {
    policy: 'short' as const,
    retryLimit: o.retryLimit ?? 3,
    retryDelay: o.retryDelay ?? 0,
    expireInSeconds: o.expireInSeconds ?? 900,
  };
  const existing = await boss.getQueue(QUEUE);
  if (existing) await boss.updateQueue(QUEUE, { retryLimit: options.retryLimit, retryDelay: options.retryDelay, expireInSeconds: options.expireInSeconds });
  else await boss.createQueue(QUEUE, options);
}

/** The ledger row and the queue message commit together, or neither does. */
export async function enqueue(
  pool: pg.Pool,
  boss: PgBoss,
  j: { clientRequestId: string; prompt: string; batch?: string },
): Promise<string | null> {
  const c = await pool.connect();
  try {
    await c.query('BEGIN');
    const id = await insertJob(c, j);
    if (id) {
      await boss.send(QUEUE, { jobId: id }, {
        singletonKey: id,
        db: { executeSql: (text: string, values?: unknown[]) => c.query(text, values as unknown[]) },
      });
    }
    await c.query('COMMIT');
    return id;
  } catch (err) {
    await c.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    c.release();
  }
}

/** Re-rings the doorbell for any job the ledger says nobody is working on. */
export async function sweep(pool: pg.Pool, boss: PgBoss, graceMs: number): Promise<number> {
  const ids = await findStale(pool, graceMs);
  for (const id of ids) await boss.send(QUEUE, { jobId: id }, { singletonKey: id }).catch(() => null);
  return ids.length;
}
