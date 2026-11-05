import { spawnSync } from 'node:child_process';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';

/**
 * Do the tests actually protect anything? Remove each safeguard, one at a time,
 * and require the suite to go red. A mutant that survives means a claim is unproven.
 */
interface Mutant {
  name: string;
  file: string;
  find: string;
  replace: string;
  tests: string[];
  filter?: string;
}

const P = 'src/worker/pipeline.ts';
const L = 'src/worker/ledger.ts';
const C = 'src/provider/client.ts';

const mutants: Mutant[] = [
  {
    name: 'Fencing removed from worker writes',
    file: L,
    find: '`UPDATE gen_jobs SET ${set} WHERE id = $1 AND lease_version = $2 RETURNING *`',
    replace: '`UPDATE gen_jobs SET ${set} WHERE id = $1 AND $2::bigint IS NOT NULL RETURNING *`',
    tests: ['test/ledger.test.ts'],
    filter: 'leases and fencing',
  },
  {
    name: 'Claim ignores a live lease',
    file: L,
    find: "AND (lease_expires_at IS NULL OR lease_expires_at <= now())\n        AND (next_check_at IS NULL OR next_check_at <= now())\n      RETURNING *",
    replace: 'AND (next_check_at IS NULL OR next_check_at <= now())\n      RETURNING *',
    tests: ['test/ledger.test.ts'],
    filter: 'leases and fencing',
  },
  {
    name: 'Local lease deadline check removed before the paid call',
    file: P,
    find: '  keeper.assertHeldLocally();\n',
    replace: '',
    tests: ['test/chaos.test.ts'],
    filter: 'paused before the paid call',
  },
  {
    name: 'Crash recovery resends instead of looking up',
    file: P,
    find: "        row = await fencedUpdate(d.pool, lease, `state = 'unknown'`);\n        state = 'unknown';\n        break;",
    replace: "        row = await fencedUpdate(d.pool, lease, `state = 'unknown'`);\n        state = await submit(d, lease, keeper, row);\n        break;",
    tests: ['test/chaos.test.ts'],
    filter: 'killed at after_provider',
  },
  {
    name: 'Settle window removed',
    file: P,
    find: 'if (age < d.cfg.settleMs) {',
    replace: 'if (age < 0) {',
    tests: ['test/unknown.test.ts'],
    filter: 'late arrival inside the settle window',
  },
  {
    name: 'Lookup outage treated as "not found"',
    file: P,
    find: "if (found.kind === 'unavailable') {\n    await release(d.pool, lease, d.cfg.settleMs / 2);\n    return 'released';\n  }\n",
    replace: '',
    tests: ['test/unknown.test.ts'],
    filter: 'provider lookup down',
  },
  {
    name: '5xx treated as a definite rejection',
    file: C,
    find: 'if (res.status >= 400 && res.status < 500)',
    replace: 'if (res.status >= 400)',
    tests: ['test/unknown.test.ts'],
    filter: 'a 502 is not proof',
  },
  {
    name: 'Resend attempts uncapped',
    file: P,
    find: "d.cfg.resubmitPolicy === 'resubmit' && latest.submit_attempts < d.cfg.maxSubmitAttempts",
    replace: "d.cfg.resubmitPolicy === 'resubmit'",
    tests: ['test/unknown.test.ts'],
    filter: 'resend attempts are capped',
  },
  {
    name: 'Sweeper stops re-dispatching stale jobs',
    file: 'src/worker/queue.ts',
    find: 'const ids = await findStale(pool, graceMs);',
    replace: 'const ids: string[] = [];',
    tests: ['test/pgboss.test.ts'],
    filter: 'queue message deleted outright',
  },
  {
    name: 'Connection held across the paid call',
    file: P,
    find: '  const res = await d.provider.submit(row.idempotency_key, row.prompt, keeper.signal);\n',
    replace:
      "  const held = await d.pool.connect();\n  await held.query('BEGIN');\n  const res = await d.provider.submit(row.idempotency_key, row.prompt, keeper.signal);\n  await held.query('COMMIT');\n  held.release();\n",
    tests: ['test/pool.test.ts'],
    filter: 'no connection or transaction exists',
  },
];

const vitest = (m: Mutant) =>
  spawnSync('npx', ['vitest', 'run', ...m.tests, '--reporter=dot', ...(m.filter ? ['-t', m.filter] : [])], {
    encoding: 'utf8',
    env: { ...process.env, NO_RECORD: '1' },
  });

const results: { name: string; killed: boolean }[] = [];
for (const m of mutants) {
  // A red test only means something if the same test was green a moment ago.
  const baseline = vitest(m);
  if (baseline.status !== 0 || !/\b[1-9]\d* passed/.test(baseline.stdout)) {
    console.error(baseline.stdout.slice(-1500));
    throw new Error(`baseline not green, result would be meaningless: ${m.name}`);
  }
  const original = readFileSync(m.file, 'utf8');
  if (!original.includes(m.find)) throw new Error(`mutant no longer applies: ${m.name}`);
  writeFileSync(m.file, original.replace(m.find, m.replace));
  try {
    const r = vitest(m);
    // Must fail on an assertion in a test, not by failing to compile or load.
    const killed = r.status !== 0 && /\b[1-9]\d* failed/.test(r.stdout.replace(/Test Files.*\n/, ''));
    results.push({ name: m.name, killed });
    console.log(`${killed ? 'caught  ' : 'SURVIVED'}  ${m.name}`);
  } finally {
    writeFileSync(m.file, original);
  }
}

mkdirSync('results', { recursive: true });
const caught = results.filter((r) => r.killed).length;
writeFileSync('results/mutants.json', JSON.stringify({ total: results.length, caught, results }, null, 2) + '\n');
console.log(`\n${caught}/${results.length} safeguards proven by a failing test when removed`);
if (caught !== results.length) process.exit(1);
