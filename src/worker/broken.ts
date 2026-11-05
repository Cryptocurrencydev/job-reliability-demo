import type { Deps, Outcome } from './pipeline.ts';

/**
 * BROKEN MODE. The common shape of this code in the wild, kept so the tests can prove what
 * the safe design prevents. Do not copy.
 *
 * - holds a pooled client and an open transaction across the paid call
 * - writes no intent, so a crash leaves no trace that money may have been spent
 * - no lease, no fencing, no reconciliation: pg-boss retries simply call the provider again
 */
export async function processJobBroken(d: Deps, jobId: string): Promise<Outcome> {
  const client = await d.pool.connect();
  try {
    await client.query('BEGIN');
    const { rows } = await client.query('SELECT * FROM gen_jobs WHERE id = $1 FOR UPDATE', [jobId]);
    const row = rows[0];
    if (!row || row.state === 'completed' || row.state === 'failed') {
      await client.query('COMMIT');
      return 'skipped';
    }
    await d.failpoint?.('before_intent', jobId);
    await d.failpoint?.('after_intent', jobId);

    const res = await d.provider.submit(row.idempotency_key, row.prompt);
    await d.failpoint?.('after_provider', jobId);

    if (res.kind === 'rejected') {
      await client.query(`UPDATE gen_jobs SET state = 'submitting' WHERE id = $1 AND state = 'queued'`, [jobId]);
      await client.query(`UPDATE gen_jobs SET state = 'failed', failure_reason = $2 WHERE id = $1`, [jobId, res.reason]);
      await client.query('COMMIT');
      return 'failed';
    }
    if (res.kind === 'unknown') throw new Error(`provider call failed: ${res.reason}`); // pg-boss will "just retry"

    let result = await d.provider.lookup(row.idempotency_key);
    while (result.kind !== 'found' || result.status === 'processing') {
      await new Promise((r) => setTimeout(r, d.cfg.pollMs));
      result = await d.provider.lookup(row.idempotency_key);
    }
    await d.failpoint?.('during_result', jobId);
    await client.query(`UPDATE gen_jobs SET state = 'submitting' WHERE id = $1 AND state = 'queued'`, [jobId]);
    await client.query(`UPDATE gen_jobs SET state = 'submitted', provider_request_id = $2 WHERE id = $1`, [jobId, res.requestId]);
    await client.query(
      `INSERT INTO tracks (job_id, audio_url, duration_ms, written_by_lease) VALUES ($1, $2, $3, 0) ON CONFLICT DO NOTHING`,
      [jobId, result.audioUrl ?? 'none', result.durationMs ?? 0],
    );
    await d.failpoint?.('after_result_write', jobId);
    await client.query(`UPDATE gen_jobs SET state = 'completed' WHERE id = $1`, [jobId]);
    await client.query('COMMIT');
    return 'completed';
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}
