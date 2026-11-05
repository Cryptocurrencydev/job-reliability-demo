import { run, sleep } from './harness.ts';

// Mechanism A: a pooled client and an open transaction stay held across the provider call.
await run({
  mechanism: 'a-hold-tx',
  title: 'Transaction held open across the provider HTTP call',
  load: async ({ pool, mock, client, tally, classify }) => {
    mock.setDefault({ ackDelayMs: 1500 });
    const provider = client(5000);
    const job = async (i: number) => {
      tally.jobs++;
      tally.attempts++;
      try {
        const c = await pool.connect();
        try {
          await c.query('BEGIN');
          await c.query('UPDATE incident_credits SET balance = balance - 1 WHERE account_id = 1');
          await provider.submit(`a-${i}`, 'song'); // the bug: 1.5s inside the transaction
          await c.query('COMMIT');
          tally.ok++;
        } finally {
          c.release();
        }
      } catch (e) {
        classify(e);
      }
    };
    const all: Promise<void>[] = [];
    for (let i = 0; i < 30; i++) {
      all.push(job(i));
      await sleep(60);
    }
    await Promise.all(all);
  },
});
