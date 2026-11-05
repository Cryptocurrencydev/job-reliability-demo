import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import { randomUUID } from 'node:crypto';
import type { AddressInfo } from 'node:net';

/**
 * Mock paid music provider. It charges for EVERY accepted submission unless `dedupe` is on.
 * That is deliberately harsher than a good real provider: the client must not rely on the
 * provider collapsing duplicates to meet the "one paid generation" rule.
 */
export type Behaviour = {
  /** ok: normal. reject: 400, no charge. error: 500, no charge. error_charged: charge, then 500.
   *  blackhole: charge, never answer. drop: never charge, never answer. fail_generation: charged, generation fails. */
  mode?: 'ok' | 'reject' | 'error' | 'error_charged' | 'blackhole' | 'drop' | 'fail_generation';
  /** Charge immediately, answer this much later (a slow response). */
  ackDelayMs?: number;
  /** The request is in flight this long before it is charged or visible to lookup (a late arrival). */
  ingestDelayMs?: number;
  /** How long the generation takes after acceptance. */
  generationMs?: number;
  /** Apply the failure to the first submission only; later ones behave normally. */
  onlyFirst?: boolean;
};

export interface Submission {
  seq: number;
  key: string;
  charged: boolean;
  requestId: string | null;
  at: number;
}

interface Generation {
  id: string;
  key: string;
  acceptedAt: number;
  generationMs: number;
  fails: boolean;
}

export interface MockProvider {
  url: string;
  server: Server;
  submissions: Submission[];
  /** Most POSTs open at the same moment. */
  peakInFlight(): number;
  charges(key?: string): number;
  submissionsFor(key: string): number;
  /** POSTs received for a key, including ones the provider never recorded. */
  arrivals(key: string): number;
  setBehaviour(key: string, b: Behaviour): void;
  setDefault(b: Behaviour): void;
  setDedupe(on: boolean): void;
  /** Status lookups answer 503 while on. */
  setLookupDown(on: boolean): void;
  /** Called synchronously when a POST arrives. Used to sample pool state during the call. */
  onSubmit?: (key: string) => void | Promise<void>;
  close(): Promise<void>;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export async function startMockProvider(port = 0): Promise<MockProvider> {
  const submissions: Submission[] = [];
  const byKey = new Map<string, Generation>();
  const byId = new Map<string, Generation>();
  const behaviours = new Map<string, Behaviour>();
  let fallback: Behaviour = {};
  let dedupe = false;
  let lookupDown = false;
  const arrivals = new Map<string, number>();
  let seq = 0;
  let inFlight = 0;
  let peak = 0;

  function accept(key: string, b: Behaviour): Generation {
    const g: Generation = {
      id: `gen_${randomUUID()}`,
      key,
      acceptedAt: Date.now(),
      generationMs: b.generationMs ?? 30,
      fails: b.mode === 'fail_generation',
    };
    byKey.set(key, g); // the newest wins; duplicates stay visible in `submissions`
    byId.set(g.id, g);
    return g;
  }

  function view(g: Generation) {
    const done = Date.now() - g.acceptedAt >= g.generationMs;
    return {
      id: g.id,
      idempotency_key: g.key,
      status: !done ? 'processing' : g.fails ? 'failed' : 'succeeded',
      audio_url: done && !g.fails ? `https://cdn.tunesmith.example/${g.id}.mp3` : null,
      duration_ms: done && !g.fails ? 180_000 : null,
    };
  }

  const json = (res: ServerResponse, status: number, body: unknown) => {
    if (res.destroyed) return;
    res.writeHead(status, { 'content-type': 'application/json' });
    res.end(JSON.stringify(body));
  };

  async function submit(req: IncomingMessage, res: ServerResponse) {
    const key = req.headers['idempotency-key'];
    if (typeof key !== 'string' || !key) return json(res, 400, { error: 'idempotency-key required' });
    req.resume();
    const b = { ...fallback, ...(behaviours.get(key) ?? {}) };
    const arrival = (arrivals.get(key) ?? 0) + 1;
    arrivals.set(key, arrival);
    const mode = b.onlyFirst && arrival > 1 ? 'ok' : (b.mode ?? 'ok');
    await mock.onSubmit?.(key);

    // A client that has disconnected does not stop a request already in flight.
    if (b.ingestDelayMs) await sleep(b.ingestDelayMs);

    const record = (charged: boolean, requestId: string | null) =>
      submissions.push({ seq: ++seq, key, charged, requestId, at: Date.now() });

    if (mode === 'drop') return; // never reached the provider's ledger
    if (mode === 'reject') {
      record(false, null);
      return json(res, 400, { error: 'prompt rejected' });
    }
    if (mode === 'error') {
      record(false, null);
      return json(res, 500, { error: 'internal' });
    }
    if (dedupe && byKey.has(key)) {
      record(false, byKey.get(key)!.id);
      return json(res, 200, view(byKey.get(key)!));
    }
    const g = accept(key, b);
    record(true, g.id);
    if (mode === 'blackhole') return;
    if (mode === 'error_charged') return json(res, 502, { error: 'bad gateway' });
    if (b.ackDelayMs) await sleep(b.ackDelayMs);
    return json(res, 200, view(g));
  }

  const server = createServer((req, res) => {
    const url = new URL(req.url ?? '/', 'http://x');
    if (req.method === 'POST' && url.pathname === '/v1/generations') {
      peak = Math.max(peak, ++inFlight);
      void submit(req, res).finally(() => inFlight--);
      return;
    }
    if (req.method === 'GET' && lookupDown) return json(res, 503, { error: 'unavailable' });
    if (req.method === 'GET' && url.pathname === '/v1/generations') {
      const g = byKey.get(url.searchParams.get('idempotency_key') ?? '');
      return g ? json(res, 200, view(g)) : json(res, 404, { error: 'not found' });
    }
    if (req.method === 'GET' && url.pathname.startsWith('/v1/generations/')) {
      const g = byId.get(url.pathname.split('/').pop() ?? '');
      return g ? json(res, 200, view(g)) : json(res, 404, { error: 'not found' });
    }
    json(res, 404, { error: 'no route' });
  });

  await new Promise<void>((r) => server.listen(port, '127.0.0.1', r));
  const addr = server.address() as AddressInfo;

  const mock: MockProvider = {
    url: `http://127.0.0.1:${addr.port}`,
    server,
    submissions,
    peakInFlight: () => peak,
    charges: (key) => submissions.filter((s) => s.charged && (!key || s.key === key)).length,
    submissionsFor: (key) => submissions.filter((s) => s.key === key).length,
    arrivals: (key) => arrivals.get(key) ?? 0,
    setBehaviour: (key, b) => void behaviours.set(key, b),
    setDefault: (b) => void (fallback = b),
    setDedupe: (on) => void (dedupe = on),
    setLookupDown: (on) => void (lookupDown = on),
    close: () =>
      new Promise((r) => {
        server.closeAllConnections();
        server.close(() => r());
      }),
  };
  return mock;
}
