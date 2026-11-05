import { mkdirSync, writeFileSync } from 'node:fs';
import { monitorEventLoopDelay, performance } from 'node:perf_hooks';
import pg from 'pg';
import { dbUrl } from '../../src/config.ts';
import { makePool, migrate } from '../../src/db.ts';
import { ProviderClient } from '../../src/provider/client.ts';
import { startMockProvider, type MockProvider } from '../../src/provider/server.ts';

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export interface Ctx {
  pool: pg.Pool;
  mock: MockProvider;
  client: (timeoutMs: number) => ProviderClient;
  tally: { ok: number; poolTimeouts: number; queryTimeouts: number; otherErrors: number; attempts: number; jobs: number };
  /** Sorts an error into the tally the way you would sort log lines. */
  classify: (e: unknown) => void;
}

export interface Evidence {
  mechanism: string;
  title: string;
  poolMax: number;
  durationMs: number;
  outcomes: Ctx['tally'] & { paidSubmissions: number; paidPerJob: number; retryAmplification: number };
  pool: { peakWaiting: number; peakCheckedOut: number; samples: number; afterLoad: { total: number; idle: number; waiting: number } };
  eventLoop: { p50ms: number; p99ms: number; maxMs: number; heartbeatsLate: number; heartbeatsTotal: number };
  pgStatActivity: { peakIdleInTransaction: number; peakIdle: number; peakActive: number; peakLockWaits: number; oldestXactMs: number; snapshots: number };
  recovered: boolean;
  timeline: { t: number; total: number; idle: number; waiting: number }[];
}

export async function run(o: {
  mechanism: string;
  title: string;
  pool?: Partial<pg.PoolConfig>;
  load: (c: Ctx) => Promise<void>;
}): Promise<Evidence> {
  const app = `incident-${o.mechanism}`;
  const poolMax = 5;
  const pool = makePool({ max: poolMax, connectionTimeoutMillis: 800, application_name: app, ...o.pool });
  await migrate(pool);

  const admin = new pg.Client({ connectionString: dbUrl(), application_name: 'incident-observer' });
  await admin.connect();
  const mock = await startMockProvider();

  const tally: Ctx['tally'] = { ok: 0, poolTimeouts: 0, queryTimeouts: 0, otherErrors: 0, attempts: 0, jobs: 0 };
  const classify = (e: unknown) => {
    const m = String((e as Error)?.message ?? e);
    if (/timeout exceeded when trying to connect/i.test(m)) tally.poolTimeouts++;
    else if (/query read timeout|statement timeout|canceling statement/i.test(m)) tally.queryTimeouts++;
    else tally.otherErrors++;
  };

  // Pool gauges, the numbers most apps never export.
  const timeline: Evidence['timeline'] = [];
  const t0 = performance.now();
  const poolTimer = setInterval(() => {
    timeline.push({ t: Math.round(performance.now() - t0), total: pool.totalCount, idle: pool.idleCount, waiting: pool.waitingCount });
  }, 100);

  // Event loop delay, plus a stand-in for a job heartbeat that should tick every 200ms.
  const eld = monitorEventLoopDelay({ resolution: 10 });
  eld.enable();
  let hbLate = 0;
  let hbTotal = 0;
  let hbLast = performance.now();
  const hbTimer = setInterval(() => {
    const now = performance.now();
    hbTotal++;
    if (now - hbLast > 500) hbLate++;
    hbLast = now;
  }, 200);

  // The database's own view, over a connection outside the pool under test.
  const act = { peakIdleInTransaction: 0, peakIdle: 0, peakActive: 0, peakLockWaits: 0, oldestXactMs: 0, snapshots: 0 };
  let observing = true;
  const observer = (async () => {
    while (observing) {
      const r = await admin.query(
        `SELECT count(*) FILTER (WHERE state = 'idle in transaction')::int AS iit,
                count(*) FILTER (WHERE state = 'idle')::int AS idle,
                count(*) FILTER (WHERE state = 'active')::int AS active,
                count(*) FILTER (WHERE wait_event_type = 'Lock')::int AS locks,
                coalesce(max(extract(epoch FROM now() - xact_start) * 1000), 0)::int AS oldest
           FROM pg_stat_activity WHERE application_name = $1`,
        [app],
      );
      const s = r.rows[0];
      act.snapshots++;
      act.peakIdleInTransaction = Math.max(act.peakIdleInTransaction, s.iit);
      act.peakIdle = Math.max(act.peakIdle, s.idle);
      act.peakActive = Math.max(act.peakActive, s.active);
      act.peakLockWaits = Math.max(act.peakLockWaits, s.locks);
      act.oldestXactMs = Math.max(act.oldestXactMs, s.oldest);
      await sleep(100);
    }
  })();

  await o.load({ pool, mock, client: (ms) => new ProviderClient(mock.url, ms), tally, classify });
  const durationMs = Math.round(performance.now() - t0);

  // Load has stopped. Does the system heal by itself? This separates a leak from congestion.
  await sleep(1200);
  const recovered = await pool.query('SELECT 1').then(() => true, () => false);
  const afterLoad = { total: pool.totalCount, idle: pool.idleCount, waiting: pool.waitingCount };

  observing = false;
  await observer;
  clearInterval(poolTimer);
  clearInterval(hbTimer);
  eld.disable();

  const paid = mock.charges();
  const ev: Evidence = {
    mechanism: o.mechanism,
    title: o.title,
    poolMax,
    durationMs,
    outcomes: {
      ...tally,
      paidSubmissions: paid,
      paidPerJob: tally.jobs ? +(paid / tally.jobs).toFixed(2) : 0,
      retryAmplification: tally.jobs ? +(tally.attempts / tally.jobs).toFixed(2) : 0,
    },
    pool: {
      peakWaiting: Math.max(0, ...timeline.map((x) => x.waiting)),
      peakCheckedOut: Math.max(0, ...timeline.map((x) => x.total - x.idle)),
      samples: timeline.length,
      afterLoad,
    },
    eventLoop: {
      p50ms: +(eld.percentile(50) / 1e6).toFixed(1),
      p99ms: +(eld.percentile(99) / 1e6).toFixed(1),
      maxMs: +(eld.max / 1e6).toFixed(1),
      heartbeatsLate: hbLate,
      heartbeatsTotal: hbTotal,
    },
    pgStatActivity: act,
    recovered,
    timeline,
  };

  mkdirSync('evidence', { recursive: true });
  writeFileSync(`evidence/${o.mechanism}.json`, JSON.stringify(ev, null, 2) + '\n');
  print(ev);

  await admin.end();
  await mock.close();
  // A leaked client would keep pool.end() waiting forever, which is rather the point.
  await Promise.race([pool.end(), sleep(300)]);
  process.exit(0);
}

function print(e: Evidence) {
  const line = (k: string, v: unknown) => console.log(`  ${k.padEnd(34)} ${v}`);
  console.log(`\n[${e.mechanism}] ${e.title}`);
  line('jobs / attempts', `${e.outcomes.jobs} / ${e.outcomes.attempts} (x${e.outcomes.retryAmplification})`);
  line('ok / pool timeouts / query timeouts', `${e.outcomes.ok} / ${e.outcomes.poolTimeouts} / ${e.outcomes.queryTimeouts}`);
  line('paid submissions', `${e.outcomes.paidSubmissions} (${e.outcomes.paidPerJob} per job)`);
  line('pool peak waiting / checked out', `${e.pool.peakWaiting} / ${e.pool.peakCheckedOut} of ${e.poolMax}`);
  line('pool after load (total/idle/wait)', `${e.pool.afterLoad.total} / ${e.pool.afterLoad.idle} / ${e.pool.afterLoad.waiting}`);
  line('recovered without a restart', e.recovered ? 'yes' : 'NO');
  line('event loop p99 / max', `${e.eventLoop.p99ms}ms / ${e.eventLoop.maxMs}ms`);
  line('heartbeats late', `${e.eventLoop.heartbeatsLate} of ${e.eventLoop.heartbeatsTotal}`);
  line('peak idle in transaction', e.pgStatActivity.peakIdleInTransaction);
  line('peak lock waits', e.pgStatActivity.peakLockWaits);
  line('oldest transaction', `${e.pgStatActivity.oldestXactMs}ms`);
}
