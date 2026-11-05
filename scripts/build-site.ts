import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import type { Evidence } from './incident/harness.ts';

/** Renders site/index.html from the raw output of the latest test run. No numbers are typed by hand. */
const read = <T,>(p: string): T | null => (existsSync(p) ? (JSON.parse(readFileSync(p, 'utf8')) as T) : null);

interface VitestReport {
  numTotalTests: number;
  numPassedTests: number;
  numFailedTests: number;
  testResults: { name: string; assertionResults: { status: string; title: string; fullName: string; duration?: number }[] }[];
}
const vt = read<VitestReport>('results/vitest.json');
if (!vt) throw new Error('results/vitest.json missing: run npm test first');

type Side = { state: string; charges: number; lost: number };
const comparison = read<Record<string, { safe: Side; broken: Side }>>('results/comparison.json') ?? {};
const property = read<{ latest: { scenarios: number; jobs: number; killsPlanned: number; realSigkills: number; paidSubmissions: number; seed: number } }>('results/property.json')?.latest;
const pool = read<Record<'safe' | 'broken', { peakWaiting: number; meanWaiting: number; peakIdleInTx: number; peakProviderInFlight: number; failed: number; completed: number; poolMax: number; concurrency: number; jobs: number }>>('results/pool.json');
const mutants = read<{ total: number; caught: number; results: { name: string; killed: boolean }[] }>('results/mutants.json');
const names = ['a-hold-tx', 'b-leak', 'c-cpu-block', 'd-retry-storm'];
const evidence = names.map((n) => read<Evidence>(`evidence/${n}.json`)).filter((e): e is Evidence => !!e);

const esc = (s: unknown) => String(s).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c]!);
const suiteLabels: Record<string, string> = {
  'chaos.test.ts': 'Chaos: SIGKILL and SIGSTOP on real worker processes',
  'property.test.ts': 'Property: random kills and provider behaviour',
  'broken.test.ts': 'Broken mode against safe mode',
  'pool.test.ts': 'Connection pool under load',
  'pgboss.test.ts': 'pg-boss retries, expiry and singletonKey',
  'unknown.test.ts': 'Unknown outcomes and reconciliation',
  'ledger.test.ts': 'State machine, leases and fencing',
  'incident.test.ts': 'Incident fingerprints A to D',
};
const suites = vt.testResults
  .map((f) => {
    const base = f.name.split('/').pop()!;
    return {
      base,
      label: suiteLabels[base] ?? base,
      total: f.assertionResults.length,
      passed: f.assertionResults.filter((a) => a.status === 'passed').length,
    };
  })
  .sort((a, b) => Object.keys(suiteLabels).indexOf(a.base) - Object.keys(suiteLabels).indexOf(b.base));

const scenarioLabels: Record<string, string> = {
  crash_after_paying: 'Worker killed right after paying, pg-boss retries on',
  crash_no_retries: 'Worker killed right after paying, pg-boss retries off',
  provider_timeout: 'Provider charges, then never answers',
};
const stateLabel = (s: Side) => (s.lost ? `stuck in ${s.state}` : s.state);

function spark(e: Evidence): string {
  const w = 220;
  const h = 44;
  const t = e.timeline;
  if (!t.length) return '';
  const maxT = t[t.length - 1]!.t || 1;
  const maxY = Math.max(15, ...t.map((p) => p.waiting));
  const pts = t.map((p) => `${((p.t / maxT) * w).toFixed(1)},${(h - (p.waiting / maxY) * (h - 2) - 1).toFixed(1)}`).join(' ');
  return `<svg viewBox="0 0 ${w} ${h}" role="img" aria-label="Pool waiting count over ${Math.round(maxT / 1000)} seconds, peak ${e.pool.peakWaiting}"><polyline points="${pts}" fill="none" stroke="currentColor" stroke-width="1.5"/></svg>`;
}

const allGreen = vt.numFailedTests === 0 && vt.numPassedTests > 0;
const when = new Date().toISOString().slice(0, 16).replace('T', ' ') + ' UTC';
const sha = (process.env.GITHUB_SHA ?? '').slice(0, 7);
const runUrl = process.env.RUN_URL;

const html = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Tunesmith job reliability results</title>
<meta name="description" content="Test evidence that paid music generation jobs are never lost and never charged twice, across real process kills.">
<style>
:root{--ink:#17181c;--mute:#5d616b;--line:#dcdde1;--paper:#fbfaf7;--card:#fff;--good:#0b6b4f;--bad:#b3261e;--accent:#3d3a8c}
@media (prefers-color-scheme:dark){:root{--ink:#ecebe6;--mute:#a2a5ae;--line:#33353c;--paper:#141519;--card:#1b1c21;--good:#55c9a0;--bad:#f2857c;--accent:#a9a6f2}}
*{box-sizing:border-box}
body{margin:0;background:var(--paper);color:var(--ink);font:16px/1.55 ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif}
main{max-width:920px;margin:0 auto;padding:48px 16px 32px}
h1{font:600 clamp(28px,5vw,40px)/1.15 ui-serif,Georgia,serif;margin:8px 0 14px;letter-spacing:-.01em}
h2{font:600 22px/1.25 ui-serif,Georgia,serif;margin:52px 0 6px}
p{margin:0 0 12px;max-width:68ch}
.eyebrow{font:600 12px/1 ui-monospace,Menlo,monospace;letter-spacing:.09em;text-transform:uppercase;color:var(--accent)}
.mute{color:var(--mute)}
.small{font-size:14px}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:1px;background:var(--line);border:1px solid var(--line);margin:28px 0 0}
.stat{background:var(--card);padding:16px}
.stat b{display:block;font:600 30px/1.1 ui-monospace,Menlo,monospace;font-variant-numeric:tabular-nums}
.stat span{font-size:14px;color:var(--mute)}
.scroll{overflow-x:auto;margin-top:14px}
table{border-collapse:collapse;width:100%;font-size:15px;min-width:560px}
th,td{text-align:left;padding:9px 12px 9px 0;border-bottom:1px solid var(--line);vertical-align:top}
th{font-size:13px;font-weight:600;color:var(--mute)}
td.n,th.n{text-align:right;font-variant-numeric:tabular-nums;font-family:ui-monospace,Menlo,monospace;padding-left:12px;padding-right:28px;white-space:nowrap}
td.n:last-child,th.n:last-child{padding-right:0}
.good{color:var(--good);font-weight:600}.bad{color:var(--bad);font-weight:600}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,380px),1fr));gap:14px;margin-top:14px}
.card{background:var(--card);border:1px solid var(--line);padding:14px 16px}
.card h3{margin:0 0 6px;font-size:15px;line-height:1.35}
.card svg{display:block;width:100%;height:44px;color:var(--accent);margin:8px 0}
.card dl{display:grid;grid-template-columns:1fr auto;gap:2px 12px;margin:0;font-size:14px}
.card dt{color:var(--mute)}.card dd{margin:0;font-family:ui-monospace,Menlo,monospace;font-variant-numeric:tabular-nums}
ul.plain{list-style:none;padding:0;margin:12px 0 0;font-size:15px}
ul.plain li{padding:6px 0;border-bottom:1px solid var(--line);display:flex;justify-content:space-between;gap:16px}
a{color:var(--accent)}
footer{max-width:920px;margin:0 auto;padding:24px 16px 48px;font-size:14px;color:var(--mute);border-top:1px solid var(--line)}
</style>
</head>
<body>
<main>
<div class="eyebrow">Tunesmith, a fictional AI music app</div>
<h1>${allGreen ? 'No job lost. No job paid for twice.' : 'The latest run has failures.'}</h1>
<p>Workers were killed at every step of a paid music generation job, then restarted. This page is generated from the raw output of the latest test run, including the runs where the protections were deliberately switched off.</p>
<p class="mute small">Run ${esc(when)}${sha ? `, commit ${esc(sha)}` : ''}${runUrl ? `. <a href="${esc(runUrl)}">See the CI run</a>` : ''}. <a href="https://github.com/Cryptocurrencydev/job-reliability-demo">Source and playbook</a>.</p>

<div class="stats">
  <div class="stat"><b class="${allGreen ? 'good' : 'bad'}">${vt.numPassedTests}/${vt.numTotalTests}</b><span>tests passed</span></div>
  ${property ? `<div class="stat"><b>${property.realSigkills}</b><span>real SIGKILLs in the property test</span></div>
  <div class="stat"><b>${property.jobs}</b><span>random jobs, ${property.scenarios} scenarios</span></div>` : ''}
  ${mutants ? `<div class="stat"><b>${mutants.caught}/${mutants.total}</b><span>safeguards removed and caught</span></div>` : ''}
</div>

<h2>The same failure, with and without the design</h2>
<p class="mute">Both columns ran the same scenario and were judged by the same check.</p>
<div class="scroll"><table>
<thead><tr><th>Scenario</th><th>Broken mode</th><th class="n">Charges</th><th>Safe design</th><th class="n">Charges</th></tr></thead>
<tbody>${Object.entries(comparison)
  .map(
    ([k, v]) => `<tr><td>${esc(scenarioLabels[k] ?? k)}</td><td class="${v.broken.lost ? 'bad' : ''}">${esc(stateLabel(v.broken))}</td><td class="n ${v.broken.charges > 1 ? 'bad' : ''}">${v.broken.charges}</td><td class="good">${esc(stateLabel(v.safe))}</td><td class="n good">${v.safe.charges}</td></tr>`,
  )
  .join('')}</tbody>
</table></div>

${pool ? `<h2>Connection pool under load</h2>
<p class="mute">${pool.safe.jobs} jobs, ${pool.safe.concurrency} at a time, on a pool of ${pool.safe.poolMax} connections.</p>
<div class="scroll"><table>
<thead><tr><th>Measure</th><th class="n">Broken</th><th class="n">Safe</th></tr></thead>
<tbody>
<tr><td>Jobs completed</td><td class="n bad">${pool.broken.completed}</td><td class="n good">${pool.safe.completed}</td></tr>
<tr><td>Jobs that hit a connection timeout</td><td class="n bad">${pool.broken.failed}</td><td class="n good">${pool.safe.failed}</td></tr>
<tr><td>Provider calls in flight at once, peak</td><td class="n">${pool.broken.peakProviderInFlight}</td><td class="n">${pool.safe.peakProviderInFlight}</td></tr>
<tr><td>Connections idle in transaction, peak</td><td class="n bad">${pool.broken.peakIdleInTx}</td><td class="n good">${pool.safe.peakIdleInTx}</td></tr>
<tr><td>Pool waiting count, average</td><td class="n">${pool.broken.meanWaiting}</td><td class="n">${pool.safe.meanWaiting}</td></tr>
<tr><td>Pool waiting count, peak</td><td class="n">${pool.broken.peakWaiting}</td><td class="n">${pool.safe.peakWaiting}</td></tr>
</tbody></table></div>
<p class="mute small" style="margin-top:12px">Safe mode has more provider calls in flight than connections exist, so none of those calls can be holding one.</p>` : ''}

<h2>Four ways to get the same symptoms</h2>
<p class="mute">Each mechanism reproduced on purpose. The line is the number of requests waiting for a connection. The differences between these cards are how you tell them apart in a real incident.</p>
<div class="grid">${evidence
  .map(
    (e) => `<div class="card"><h3>${esc(e.title)}</h3>${spark(e)}<dl>
<dt>Timeouts</dt><dd>${e.outcomes.poolTimeouts + e.outcomes.queryTimeouts}</dd>
<dt>Healed without restart</dt><dd class="${e.recovered ? '' : 'bad'}">${e.recovered ? 'yes' : 'no'}</dd>
<dt>Idle in transaction, peak</dt><dd>${e.pgStatActivity.peakIdleInTransaction}</dd>
<dt>Lock waits, peak</dt><dd>${e.pgStatActivity.peakLockWaits}</dd>
<dt>Event loop p99</dt><dd>${e.eventLoop.p99ms}ms</dd>
<dt>Paid per job</dt><dd>${e.outcomes.paidPerJob}</dd>
</dl></div>`,
  )
  .join('')}</div>

${mutants ? `<h2>Do the tests protect anything?</h2>
<p class="mute">Each safeguard was removed in turn. Its test had to pass first, then fail with the safeguard gone.</p>
<ul class="plain">${mutants.results.map((m) => `<li><span>${esc(m.name)}</span><span class="${m.killed ? 'good' : 'bad'}">${m.killed ? 'caught' : 'survived'}</span></li>`).join('')}</ul>` : ''}

<h2>Test suites</h2>
<div class="scroll"><table>
<thead><tr><th>Suite</th><th class="n">Passed</th></tr></thead>
<tbody>${suites.map((s) => `<tr><td>${esc(s.label)}</td><td class="n ${s.passed === s.total ? 'good' : 'bad'}">${s.passed}/${s.total}</td></tr>`).join('')}</tbody>
</table></div>

<h2>What this does not prove</h2>
<p>Everything here ran against a mock provider and a test database, not a real provider or a real incident. If a provider records a request later than the settle window and also does not dedupe on the idempotency key, an automatic resend can still charge twice. A test pins that case, and the readme explains the two ways to close it.</p>
</main>
<footer>Demo by <a href="https://dctechltd.com">DC Tech</a></footer>
</body>
</html>
`;

mkdirSync('site', { recursive: true });
writeFileSync('site/index.html', html);
console.log(`site/index.html: ${vt.numPassedTests}/${vt.numTotalTests} tests`);
