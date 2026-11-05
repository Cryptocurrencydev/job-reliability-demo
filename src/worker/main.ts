import { randomUUID } from 'node:crypto';
import { DEFAULTS, FAST, QUEUE, validate, type ReliabilityConfig } from '../config.ts';
import { makePool } from '../db.ts';
import { ProviderClient } from '../provider/client.ts';
import { processJobBroken } from './broken.ts';
import { processJob, type Deps, type Failpoint } from './pipeline.ts';
import { startBoss, sweep } from './queue.ts';

/**
 * Worker process. WORKER_MODE=safe (default) or broken.
 * CHAOS=1 enables failpoints driven by the chaos_plan table, so the kill is a real SIGKILL
 * at an exact line. The plan row is deleted first, so the restarted worker does not die again.
 */
const mode = process.env.WORKER_MODE === 'broken' ? 'broken' : 'safe';
const fast = process.env.FAST_TIMINGS === '1';
const cfg: ReliabilityConfig = validate({
  ...(fast ? FAST : DEFAULTS),
  ...(process.env.CFG_JSON ? JSON.parse(process.env.CFG_JSON) : {}),
});

const pool = makePool({ max: Number(process.env.POOL_MAX ?? 5) });
const provider = new ProviderClient(process.env.PROVIDER_URL ?? 'http://127.0.0.1:4010', cfg.providerTimeoutMs);
const owner = `${process.pid}-${randomUUID().slice(0, 8)}`;

const chaos: Failpoint = async (step, jobId) => {
  const r = await pool.query('DELETE FROM chaos_plan WHERE job_id = $1 AND step = $2 RETURNING action', [jobId, step]);
  const action = r.rows[0]?.action;
  if (action === 'kill') {
    process.kill(process.pid, 'SIGKILL');
    await new Promise(() => {}); // SIGKILL is delivered; never run another line
  }
  if (action === 'stall') process.kill(process.pid, 'SIGSTOP'); // resumes here on SIGCONT
};

const deps: Deps = { pool, provider, cfg, owner, ...(process.env.CHAOS === '1' ? { failpoint: chaos } : {}) };

const boss = await startBoss({
  fast,
  retryLimit: Number(process.env.BOSS_RETRY_LIMIT ?? 3),
  retryDelay: 0,
  expireInSeconds: Number(process.env.BOSS_EXPIRE_SECONDS ?? (fast ? 2 : 900)),
});

const run = mode === 'broken' ? processJobBroken : processJob;
await boss.work<{ jobId: string }>(
  QUEUE,
  { pollingIntervalSeconds: 0.5, localConcurrency: Number(process.env.CONCURRENCY ?? 4), batchSize: 1 },
  async ([job]) => {
    await run(deps, job!.data.jobId);
  },
);

let timer: NodeJS.Timeout | undefined;
if (mode === 'safe') {
  timer = setInterval(() => void sweep(pool, boss, cfg.sweepMs).catch(() => {}), cfg.sweepMs);
}

const shutdown = async () => {
  clearInterval(timer);
  await boss.stop({ graceful: true, timeout: 5000 }).catch(() => {});
  await pool.end().catch(() => {});
  process.exit(0);
};
process.on('SIGTERM', () => void shutdown());
process.on('SIGINT', () => void shutdown());

process.stdout.write('ready\n');
