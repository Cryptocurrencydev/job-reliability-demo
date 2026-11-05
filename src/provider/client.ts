export type SubmitResult =
  | { kind: 'accepted'; requestId: string }
  | { kind: 'rejected'; reason: string } // the provider definitely refused: no charge
  | { kind: 'unknown'; reason: string }; // timeout, 5xx, network: it may or may not have charged

export type LookupResult =
  | { kind: 'found'; requestId: string; status: string; audioUrl: string | null; durationMs: number | null }
  | { kind: 'not_found' }
  | { kind: 'unavailable'; reason: string };

export class ProviderClient {
  constructor(private baseUrl: string, private timeoutMs: number) {}

  /** The only call that costs money. */
  async submit(key: string, prompt: string, signal?: AbortSignal): Promise<SubmitResult> {
    try {
      const res = await fetch(`${this.baseUrl}/v1/generations`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'idempotency-key': key },
        body: JSON.stringify({ prompt }),
        signal: this.signal(signal),
      });
      const body = (await res.json().catch(() => ({}))) as { id?: string; error?: string };
      if (res.ok && body.id) return { kind: 'accepted', requestId: body.id };
      if (res.status >= 400 && res.status < 500) return { kind: 'rejected', reason: body.error ?? `http ${res.status}` };
      return { kind: 'unknown', reason: `http ${res.status}` };
    } catch (err) {
      return { kind: 'unknown', reason: (err as Error).name };
    }
  }

  async lookup(key: string, signal?: AbortSignal): Promise<LookupResult> {
    try {
      const res = await fetch(`${this.baseUrl}/v1/generations?idempotency_key=${encodeURIComponent(key)}`, {
        signal: this.signal(signal),
      });
      if (res.status === 404) return { kind: 'not_found' };
      if (!res.ok) return { kind: 'unavailable', reason: `http ${res.status}` };
      const b = (await res.json()) as { id: string; status: string; audio_url: string | null; duration_ms: number | null };
      return { kind: 'found', requestId: b.id, status: b.status, audioUrl: b.audio_url, durationMs: b.duration_ms };
    } catch (err) {
      return { kind: 'unavailable', reason: (err as Error).name };
    }
  }

  private signal(extra?: AbortSignal) {
    const t = AbortSignal.timeout(this.timeoutMs);
    return extra ? AbortSignal.any([t, extra]) : t;
  }
}
