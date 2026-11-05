# job-reliability-demo

[![ci](https://github.com/Cryptocurrencydev/job-reliability-demo/actions/workflows/ci.yml/badge.svg)](https://github.com/Cryptocurrencydev/job-reliability-demo/actions/workflows/ci.yml)

Tunesmith is a fictional AI music app whose background jobs call a paid provider.
This repo reproduces four ways such an app gets database timeouts, a full connection pool and event-loop delay.
It then fixes the real risk: a crash or retry that loses a customer's job or pays for it twice.
The fix is tested by killing real worker processes at every step, and each safeguard is proven by removing it.
[`DIAGNOSIS-PLAYBOOK.md`](DIAGNOSIS-PLAYBOOK.md) explains how to work out which mechanism hit a real system.

**Results page:** https://job-reliability-demo.vercel.app

## Invariants proven

| Invariant | Proven by |
| --- | --- |
| Every job ends `completed`, `failed`, or explicitly flagged for review. None is lost or stuck | Chaos and property tests |
| One paid submission per job, across SIGKILL at every step | Chaos and property tests, against a provider that charges for every POST |
| An unknown outcome is looked up by idempotency key, never blindly resent | `test/unknown.test.ts` |
| A replaced worker cannot write, even after waking from a pause | SIGSTOP/SIGCONT tests, fencing property test |
| No connection or transaction is held during the provider call | `test/pool.test.ts` |
| pg-boss retries, expiry and redelivery cannot cause a second charge | `test/pgboss.test.ts` |
| The tests themselves can fail | Broken mode, and `npm run mutants` |

The one case these invariants do not cover is under [Known limit](#known-limit).

## Run the chaos suite

```bash
npm run chaos
```

This starts Postgres 16 in Docker and installs dependencies if needed, then kills real worker processes. It needs Node 22 and Docker. To use your own database, set `DATABASE_URL`.

| Command | What it does |
| --- | --- |
| `npm test` | Everything |
| `npm run incident` | Reproduces mechanisms A to D and writes [`evidence/SUMMARY.md`](evidence/SUMMARY.md) |
| `npm run incident:a` (`b`, `c`, `d`) | One mechanism |
| `npm run mutants` | Removes each safeguard and requires a test to fail |

## Part 1: the four mechanisms

Each script records pool gauges over time, an event-loop delay histogram and `pg_stat_activity` snapshots, then prints a summary.

| Script | Mechanism | Telltale sign |
| --- | --- | --- |
| `a-hold-tx.ts` | Transaction held across the provider call | Long transactions and lock waits, event loop healthy |
| `b-leak.ts` | Checkout without release on an error path | Never heals without a restart |
| `c-cpu-block.ts` | Synchronous work blocks the event loop | Huge loop delay, and timeouts while the database is quiet |
| `d-retry-storm.ts` | Timeouts trigger instant retries | Attempts multiply, more than one payment per job |

`test/incident.test.ts` asserts these signatures, so the playbook's diagnosis table cannot silently go stale.

## Part 2: the safe design

```
queued -> submitting -> submitted(provider_request_id) -> completed
              |              |                        -> failed
              v              v
           unknown  <--------+   looked up by idempotency key, then adopted, resent or flagged
```

- **The ledger is the truth, and the queue is only a doorbell.** `gen_jobs` records each job's state. If a pg-boss message is lost, the sweeper rebuilds it from the ledger.
- **Intent comes first.** `submitting` and a deterministic idempotency key are committed before the provider call, so a crash always leaves a trace that money may have been spent.
- **No connection is held across the call.** Workers use short autocommit statements through `pool.query()`.
- **Leases with fencing.** Each claim increments `lease_version`, and every worker write includes it in the `WHERE` clause. If zero rows update, the worker has been replaced and it stops.
- **A paused worker checks itself.** The provider cannot check a fencing token, so the worker compares a monotonic-clock deadline immediately before paying.
- **Unknown stays unknown.** A timeout or 5xx is never treated as failure. Only a 4xx counts as a definite refusal.
- **The database enforces the rules.** A trigger rejects illegal state moves, keeps the idempotency key immutable, and logs every transition.

### After a timeout or crash

| Provider lookup says | Action |
| --- | --- |
| Found | Adopt that generation. Nothing is resent |
| Lookup unavailable | Wait. "Can't ask" is never treated as "not there" |
| Not found, inside the settle window | Wait. It may still be in flight |
| Not found, after the settle window | `resubmit` policy: resend with the same key, up to a cap. `flag` policy: flag for a human |

Flagged jobs keep being checked, so a late arrival is adopted rather than left flagged.

### Known limit

A not-found answer can only be trusted once anything in flight has had time to land. If a provider records a request **later than the settle window** and **does not dedupe on the key**, the `resubmit` policy charges twice. `test/unknown.test.ts` pins this behaviour under `KNOWN LIMIT`.

There are two ways to close it, and both are tested:

1. Use a provider that dedupes on the idempotency key. This is the only complete fix.
2. Use `resubmitPolicy: 'flag'`, which never resends automatically. The cost is that requests which truly never arrived wait for a human.

A related window: a worker that freezes between its deadline check and the socket write could still send after being replaced. That gap is microseconds wide and provider-side dedupe closes it too. It is not covered by a test, because it cannot be hit deterministically.

**The production default is `flag`.** Switch a provider to `resubmit` only once its dedupe is confirmed, or its worst-case ingest delay is documented and well inside the settle window. The kill tests run `resubmit` as the harder path, and also cover `flag`.

### How pg-boss settings interact

Checked against pg-boss 12.38.1 and [its docs](https://github.com/timgit/pg-boss/tree/master/docs) on 2026-11-05. The docs say plainly that [jobs are delivered at least once](https://github.com/timgit/pg-boss/blob/master/docs/introduction.md): "a handler can run more than once for the same job. Write handlers so that running one twice does no harm."

| Setting | What it does | Why it cannot protect the money by itself | Test |
| --- | --- | --- | --- |
| `retryLimit` (default 2) | Retries a failed or expired job | A retry after the paid call is exactly the double charge. Here it only re-rings the doorbell | `retryLimit: a crash after the paid call...` |
| `expireInSeconds` (default 15 min, max 24 h) | Fails an attempt that stays active too long | [The old handler keeps running](https://github.com/timgit/pg-boss/blob/master/docs/api/queues.md#heartbeat-vs-expiration), so two can overlap. The lease refuses the second | `expireInSeconds shorter than the job...` |
| `heartbeatSeconds` (minimum 10, off by default) | Detects dead workers | Detection takes `heartbeatSeconds` plus up to `monitorIntervalSeconds` (default 60). A worker that only looked dead keeps running. Our lease is separate, so recovery speed does not depend on it | Lease tests |
| `singletonKey` with policy `short` | One *queued* job per key | Says nothing about active jobs, so a second can start while the first runs. Used only to collapse sweeper duplicates | `singletonKey collapses queued duplicates only...` |
| `policy: exclusive` | One queued or active job per key | Tempting, but a dead worker's active job blocks the replacement until it expires. That is why `short` is used | Design choice, not tested |
| `retentionSeconds`, `deleteAfterSeconds` | Delete old rows (14 days queued, 7 days completed) | Queue rows expire, which is one more reason the ledger owns job state | Not separately tested |
| `transactional: true` | Runs the handler in one transaction | [Holds a pool connection for the whole handler](https://github.com/timgit/pg-boss/blob/master/docs/api/workers.md), which is mechanism A. Never use it around a provider call | Not used |

pg-boss does settle attempts safely: a superseded worker's completion is [discarded by `retryCount`](https://github.com/timgit/pg-boss/blob/master/docs/api/queues.md#heartbeat-vs-expiration). That protects the queue's records, though. It cannot un-send an HTTP request, which is why provider idempotency has to hold whatever pg-boss does.

### Transaction-mode poolers (PgBouncer style)

A transaction-mode pooler hands your connection to someone else after every transaction, so anything tied to a session breaks. [PgBouncer's feature table](https://www.pgbouncer.org/features.html) lists `SET`/`RESET`, `LISTEN`, `WITH HOLD` cursors, SQL `PREPARE`/`DEALLOCATE`, session advisory locks and `LOAD` as never working in that mode, along with `PRESERVE`/`DELETE ROWS` temp tables.

| Feature | Risk here |
| --- | --- |
| `LISTEN/NOTIFY` (`useListenNotify`) | [Does not work through a transaction pooler](https://github.com/timgit/pg-boss/blob/master/docs/api/constructor.md#uselistennotify). Off by default, and polling remains as the fallback. Leave it off, or give pg-boss a direct connection |
| pg-boss schema migrations | [Use `pg_advisory_xact_lock()`](https://github.com/timgit/pg-boss/blob/master/docs/ops.md), which is transaction-scoped and compatible |
| Session advisory locks in your own code | Silently stop protecting anything. `src/db.ts` uses the transaction-scoped form for that reason |
| Pooler `query_wait_timeout` | Adds a second place for connection timeouts to appear, which muddies diagnosis |

**Recommendation:** connect pg-boss and migrations directly, and use the pooled URL only for short app queries if you need it. This repo's tests connect directly. **I have not run pg-boss through a real PgBouncer**, so test it before relying on it.

## Replit Postgres: what is verified and what is not

Read from Replit's docs on 2026-11-05. I have not tested any of this on a live Replit deployment.

| Claim | Status | Source |
| --- | --- | --- |
| New development databases run on Helium, Replit's PostgreSQL 16 service (since 4 Dec 2025) | Verified in docs | [Dev and production](https://docs.replit.com/features/data-and-storage/development-and-production.md) |
| Production databases are provided by Neon | Verified in docs. One page hints at non-Neon production databases, so check yours | [Add a database](https://docs.replit.com/build/add-database.md), [Connection details](https://docs.replit.com/features/data-and-storage/connection-details.md) |
| Production connections are **not pooled by default**. Add `-pooler` to the host to pool | Verified in docs | [Dev and production](https://docs.replit.com/features/data-and-storage/development-and-production.md) |
| Dev and production have separate databases, each with its own `DATABASE_URL` | Verified in docs | [Dev and production](https://docs.replit.com/features/data-and-storage/development-and-production.md) |
| An idle production database is suspended after 5 minutes | Verified in docs, stated in a billing context | [Usage billing](https://docs.replit.com/billing/about-usage-based-billing.md) |
| Neon's pooler runs in transaction mode, with `default_pool_size` at 0.9 x `max_connections` | Verified for Neon. **Unverified** that Replit keeps these defaults | [Neon pooling docs](https://neon.com/docs/connect/connection-pooling) (read via its GitHub source) |
| Neon's `max_connections` is 104 at 0.25 CU, 419 at 1 CU | Verified for Neon. **Unverified** which size Replit uses | Same |
| Connection limits for Replit databases | **Unverified.** Not found in Replit's docs. Ask support | Playbook section 5 |
| Whether Helium has a pooler | **Unverified.** None documented | |
| Autoscale scales to zero when idle and restarts regularly by design | Verified in docs | [Deployment types](https://docs.replit.com/features/publishing/deployment-types.md), [Deployment help](https://docs.replit.com/help/deployment-and-publishing) |
| Reserved VM never sleeps and can run a background worker | Verified in docs | [Deployment types](https://docs.replit.com/features/publishing/deployment-types.md) |
| Autoscale stops background workers between requests | **Unverified.** My inference from the above. No explicit statement found | |

Neon's docs list `NOTIFY` as unsupported when pooled, while PgBouncer's list it as working. Treat it as unavailable.

**What this suggests, pending confirmation:** run workers on a Reserved VM rather than Autoscale, and connect pg-boss directly. Note that a polling worker also keeps the database from ever idling, which may affect compute billing.

## Layout

```
db/001_schema.sql        ledger, state-machine trigger, audit log
src/worker/pipeline.ts   the safe path
src/worker/broken.ts     the naive path, for comparison
src/worker/ledger.ts     claims, fenced writes, stale detection
src/worker/queue.ts      pg-boss setup, atomic enqueue, sweeper
src/provider/            mock paid provider and its client
scripts/incident/        mechanisms A to D
scripts/mutants.ts       removes each safeguard, requires a failure
test/                    the proof
```

Timings in tests are scaled down (700ms leases) so a kill-and-recover cycle takes about a second. `DEFAULTS` in `src/config.ts` has production-shaped values, and these are starting points to tune per provider.

Everything here runs against a mock provider and a test database. It proves the design, not any particular production system.

---

Demo by [DC Tech](https://dctechltd.com)
