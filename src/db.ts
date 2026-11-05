import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import pg from 'pg';
import { dbUrl } from './config.ts';

export function makePool(opts: Partial<pg.PoolConfig> = {}): pg.Pool {
  const pool = new pg.Pool({
    connectionString: dbUrl(),
    max: 5,
    connectionTimeoutMillis: 3000,
    idleTimeoutMillis: 10_000,
    application_name: 'tunesmith-worker',
    ...opts,
  });
  pool.on('error', () => {}); // a killed backend must not crash the process
  return pool;
}

export async function migrate(pool: pg.Pool): Promise<void> {
  const path = fileURLToPath(new URL('../db/001_schema.sql', import.meta.url));
  // A transaction-scoped lock, so this also works through a transaction-mode pooler.
  const c = await pool.connect();
  try {
    await c.query('BEGIN');
    await c.query('SELECT pg_advisory_xact_lock(827361)');
    await c.query(readFileSync(path, 'utf8'));
    await c.query('COMMIT');
  } catch (err) {
    await c.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    c.release();
  }
}
