import { spawn, type ChildProcess } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import type pg from 'pg';
import type { MockProvider } from '../src/provider/server.ts';
import type { JobRow } from '../src/worker/ledger.ts';
import { insertJob } from '../src/worker/ledger.ts';
import { mkdirSync, readFileSync, writeFileSync, existsSync } from 'node:fs';
import { FAST, type ReliabilityConfig } from '../src/config.ts';
import { ProviderClient } from '../src/provider/client.ts';
import { processJob, type Deps, type Failpoint } from '../src/worker/pipeline.ts';

const MAIN = fileURLToPath(new URL('../src/worker/main.ts', import.meta.url));
const ROOT = fileURLToPath(new URL('..', import.meta.url));

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export async function resetDb(pool: pg.Pool): Promise<void> {
  await pool.query('TRUNCATE gen_job_events, tracks, gen_jobs, chaos_plan CASCADE');
  const q = await pool.query(`SELECT to_regclass('pgboss.job') AS t`);
  if (q.rows[0].t) await pool.query(`DELETE FROM pgboss.job`);
}

export interface WorkerEnv {
  mode?: 'safe' | 'broken';
  providerUrl: string;
  cfg?: Record<string, unknown>;
  retryLimit?: number;
  expireSeconds?: number;
  concurrency?: number;
}

export function spawnWorker(e: WorkerEnv): Promise<ChildProcess> {
  const child = spawn(process.execPath, ['--import', 'tsx', MAIN], {
    cwd: ROOT,
    stdio: ['ignore', 'pipe', 'inherit'],
    env: {
      ...process.env,
      WORKER_MODE: e.mode ?? 'safe',
      PROVIDER_URL: e.providerUrl,
      FAST_TIMINGS: '1',
      CHAOS: '1',
      CFG_JSON: JSON.stringify(e.cfg ?? {}),
      BOSS_RETRY_LIMIT: String(e.retryLimit ?? 3),
      BOSS_EXPIRE_SECONDS: String(e.expireSeconds ?? 2),
      CONCURRENCY: String(e.concurrency ?? 4),
    },
  });
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('worker did not start in 20s')), 20_000);
    child.stdout!.on('data', (d) => {
      if (String(d).includes('ready')) {
        clearTimeout(t);
        resolve(child);
      }
    });
    child.once('exit', (code, sig) => {
      clearTimeout(t);
      reject(new Error(`worker exited during start: ${code ?? sig}`));
    });
  });
}

/** Keeps N workers running, restarting any that die, like a process manager would. */
export class Cluster {
  children = new Set<ChildProcess>();
  deaths = 0;
  private stopping = false;
  constructor(private env: WorkerEnv, private size = 1) {}

  async start() {
    await Promise.all(Array.from({ length: this.size }, () => this.add()));
  }

  private async add(): Promise<void> {
    if (this.stopping) return;
    const c = await spawnWorker(this.env);
    this.children.add(c);
    c.removeAllListeners('exit');
    c.once('exit', () => {
      this.children.delete(c);
      if (this.stopping) return;
      this.deaths += 1;
      void this.add().catch(() => {});
    });
  }

  async stop() {
    this.stopping = true;
    const exits = [...this.children].map((c) => new Promise((r) => c.once('exit', r)));
    for (const c of this.children) c.kill('SIGKILL');
    await Promise.all(exits);
  }
}

export const isSettled = (j: JobRow) => j.state === 'completed' || j.state === 'failed' || j.needs_review;

export async function jobs(pool: pg.Pool, batch: string): Promise<JobRow[]> {
  return (await pool.query<JobRow>('SELECT * FROM gen_jobs WHERE batch = $1', [batch])).rows;
}

export async function waitSettled(pool: pg.Pool, batch: string, timeoutMs = 30_000): Promise<JobRow[]> {
  const end = Date.now() + timeoutMs;
  for (;;) {
    const rows = await jobs(pool, batch);
    if (rows.every(isSettled) || Date.now() > end) return rows;
    await sleep(60);
  }
}

export async function addJobs(pool: pg.Pool, batch: string, n: number): Promise<{ id: string; key: string }[]> {
  const out: { id: string; key: string }[] = [];
  for (let i = 0; i < n; i++) {
    const id = (await insertJob(pool, { clientRequestId: `${batch}-${i}`, prompt: `song ${i}`, batch }))!;
    const r = await pool.query('SELECT idempotency_key FROM gen_jobs WHERE id = $1', [id]);
    out.push({ id, key: r.rows[0].idempotency_key });
  }
  return out;
}

export interface Violations {
  lost: string[]; // not terminal and not flagged
  duplicates: string[]; // charged more than once
  completedUnpaid: string[]; // completed with no charge on record
  completedNoTrack: string[]; // completed with no result row
  rejectedButCharged: string[];
}

/** The invariants, as one function, used on the safe design AND the broken one. */
export async function checkInvariants(pool: pg.Pool, batch: string, provider: MockProvider): Promise<Violations> {
  const rows = await jobs(pool, batch);
  const tracks = new Set(
    (await pool.query('SELECT t.job_id FROM tracks t JOIN gen_jobs g ON g.id = t.job_id WHERE g.batch = $1', [batch])).rows.map(
      (r) => r.job_id,
    ),
  );
  const v: Violations = { lost: [], duplicates: [], completedUnpaid: [], completedNoTrack: [], rejectedButCharged: [] };
  for (const j of rows) {
    const charges = provider.charges(j.idempotency_key);
    if (!isSettled(j)) v.lost.push(j.id);
    if (charges > 1) v.duplicates.push(j.id);
    if (j.state === 'completed' && charges !== 1) v.completedUnpaid.push(j.id);
    if (j.state === 'completed' && !tracks.has(j.id)) v.completedNoTrack.push(j.id);
    if (j.failure_reason?.startsWith('provider_rejected') && charges !== 0) v.rejectedButCharged.push(j.id);
  }
  return v;
}

export const CLEAN: Violations = { lost: [], duplicates: [], completedUnpaid: [], completedNoTrack: [], rejectedButCharged: [] };

/** Merge a measured number into a results file that the results page is built from. */
export function record(file: string, key: string, value: unknown) {
  if (process.env.NO_RECORD) return; // mutation runs must not overwrite real results
  mkdirSync('results', { recursive: true });
  const path = `results/${file}.json`;
  const cur = existsSync(path) ? JSON.parse(readFileSync(path, 'utf8')) : {};
  cur[key] = value;
  writeFileSync(path, JSON.stringify(cur, null, 2) + '\n');
}

export function deps(pool: pg.Pool, url: string, cfg: Partial<ReliabilityConfig> = {}, failpoint?: Failpoint): Deps {
  const c = { ...FAST, ...cfg };
  return { pool, provider: new ProviderClient(url, c.providerTimeoutMs), cfg: c, owner: 'test', ...(failpoint ? { failpoint } : {}) };
}

/** Plays the part of the sweeper in-process: keep offering the job until it settles. */
export async function drive(d: Deps, jobId: string, timeoutMs = 8000): Promise<JobRow> {
  const end = Date.now() + timeoutMs;
  for (;;) {
    await processJob(d, jobId);
    const j = (await d.pool.query<JobRow>('SELECT * FROM gen_jobs WHERE id = $1', [jobId])).rows[0]!;
    if (isSettled(j) || Date.now() > end) return j;
    await sleep(30);
  }
}
