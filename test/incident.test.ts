import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import type { Evidence } from '../scripts/incident/harness.ts';

function reproduce(name: string): Evidence {
  const r = spawnSync(process.execPath, ['--import', 'tsx', `scripts/incident/${name}.ts`], { encoding: 'utf8' });
  expect(r.status, r.stderr).toBe(0);
  return JSON.parse(readFileSync(`evidence/${name}.json`, 'utf8'));
}

// The whole point of the playbook is that these four look different. If they stop being
// distinguishable, the diagnosis table is wrong, so the signatures are tested.
describe('each mechanism leaves its own fingerprint', () => {
  it('A held transaction: long xacts and lock waits, loop healthy, heals afterwards', () => {
    const e = reproduce('a-hold-tx');
    expect(e.outcomes.poolTimeouts).toBeGreaterThan(0);
    expect(e.pool.peakWaiting).toBeGreaterThan(0);
    expect(e.pgStatActivity.peakIdleInTransaction).toBeGreaterThan(0);
    expect(e.pgStatActivity.peakLockWaits).toBeGreaterThan(0);
    expect(e.pgStatActivity.oldestXactMs).toBeGreaterThan(1000);
    expect(e.eventLoop.p99ms).toBeLessThan(150);
    expect(e.recovered).toBe(true);
  });

  it('B leak: pool stays full with no transactions open and never heals', () => {
    const e = reproduce('b-leak');
    expect(e.outcomes.poolTimeouts).toBeGreaterThan(0);
    expect(e.pool.afterLoad.idle).toBe(0);
    expect(e.pgStatActivity.peakIdleInTransaction).toBe(0);
    expect(e.eventLoop.p99ms).toBeLessThan(150);
    expect(e.recovered).toBe(false);
  });

  it('C CPU block: huge loop delay, late heartbeats, database quiet', () => {
    const e = reproduce('c-cpu-block');
    expect(e.outcomes.poolTimeouts + e.outcomes.queryTimeouts).toBeGreaterThan(0);
    expect(e.eventLoop.p99ms).toBeGreaterThan(500);
    expect(e.eventLoop.heartbeatsLate).toBeGreaterThan(0);
    expect(e.pgStatActivity.peakIdleInTransaction).toBe(0);
    expect(e.pgStatActivity.peakLockWaits).toBe(0);
    expect(e.recovered).toBe(true);
  });

  it('D retry storm: attempts multiply and paid submissions exceed one per job', () => {
    const e = reproduce('d-retry-storm');
    expect(e.outcomes.retryAmplification).toBeGreaterThan(2);
    expect(e.outcomes.paidPerJob).toBeGreaterThan(1);
    expect(e.pool.peakWaiting).toBeGreaterThan(0);
    expect(e.eventLoop.p99ms).toBeLessThan(150);
  });
});
