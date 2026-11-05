import { run, sleep } from './harness.ts';

// Mechanism B: an error path returns without releasing the client.
await run({
  mechanism: 'b-leak',
  title: 'Connection leak on an error path',
  load: async ({ pool, mock, client, tally, classify }) => {
    const provider = client(1000);
    const job = async (i: number) => {
      tally.jobs++;
      tally.attempts++;
      if (i % 3 === 0) mock.setBehaviour(`b-${i}`, { mode: 'reject' });
      try {
        const c = await pool.connect();
        await c.query('SELECT 1');
        const res = await provider.submit(`b-${i}`, 'song');
        if (res.kind === 'rejected') return; // the bug: no c.release() on this path
        await c.query('SELECT 1');
        c.release();
        tally.ok++;
      } catch (e) {
        classify(e);
      }
    };
    const all: Promise<void>[] = [];
    for (let i = 0; i < 36; i++) {
      all.push(job(i));
      await sleep(60);
    }
    await Promise.all(all);
  },
});
