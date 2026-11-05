import { performance } from 'node:perf_hooks';
import { run, sleep } from './harness.ts';

// Mechanism C: synchronous work on the event loop. The database is fine; Node cannot hear it.
const big = Array.from({ length: 40_000 }, (_, i) => ({ id: i, waveform: 'x'.repeat(40), tags: ['a', 'b', 'c'] }));

function processResultSync(ms: number) {
  const end = performance.now() + ms;
  while (performance.now() < end) JSON.parse(JSON.stringify(big)); // the bug: big JSON on the main thread
}

await run({
  mechanism: 'c-cpu-block',
  title: 'Synchronous CPU work blocks the event loop',
  pool: { query_timeout: 300 } as object,
  load: async ({ pool, tally, classify }) => {
    const queries: Promise<void>[] = [];
    let running = true;
    const traffic = (async () => {
      while (running) {
        tally.jobs++;
        tally.attempts++;
        queries.push(pool.query('SELECT 1').then(() => void tally.ok++, classify));
        await sleep(40);
      }
    })();
    for (let i = 0; i < 6; i++) {
      await sleep(250);
      // Real traffic does not pause for the block: these are in flight when the loop freezes.
      // Postgres answers within a millisecond, but Node's timers run first when the loop wakes,
      // so healthy queries are reported as timeouts.
      for (let q = 0; q < 10; q++) {
        tally.jobs++;
        tally.attempts++;
        queries.push(pool.query('SELECT 1').then(() => void tally.ok++, classify));
      }
      processResultSync(900);
    }
    running = false;
    await traffic;
    await Promise.all(queries);
  },
});
