import { run, sleep } from './harness.ts';

// Mechanism D: the recovery logic is the amplifier. Every timeout immediately tries again,
// taking another connection and, because nothing checks first, paying the provider again.
await run({
  mechanism: 'd-retry-storm',
  title: 'Timeouts trigger immediate retries that take more connections',
  pool: { connectionTimeoutMillis: 400 },
  load: async ({ pool, mock, client, tally, classify }) => {
    mock.setDefault({ ackDelayMs: 1200 }); // the provider is slow; it charges on arrival
    const provider = client(500); // the client gives up before it answers
    const job = async (i: number) => {
      tally.jobs++;
      for (let attempt = 0; attempt < 6; attempt++) {
        tally.attempts++;
        try {
          const c = await pool.connect();
          try {
            await c.query('BEGIN');
            const res = await provider.submit(`d-${i}`, 'song');
            if (res.kind === 'accepted') {
              await c.query('COMMIT');
              tally.ok++;
              return;
            }
            await c.query('ROLLBACK');
          } finally {
            c.release();
          }
        } catch (e) {
          classify(e);
        }
        // the bug: no backoff, no cap on concurrency, no lookup before paying again
      }
    };
    const all: Promise<void>[] = [];
    for (let i = 0; i < 20; i++) {
      all.push(job(i));
      await sleep(50);
    }
    await Promise.all(all);
  },
});
