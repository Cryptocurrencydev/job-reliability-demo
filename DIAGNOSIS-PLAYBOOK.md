# Diagnosis playbook

How I would find out what actually happened in a production incident of this kind, and how I would decide when the app is safe to release again.

The symptoms in this kind of incident are database connection timeouts, a saturated connection pool and a large Node event-loop delay. Those three symptoms sound like one problem, but at least four different bugs produce them, and the fix for one does nothing for the others. So the first job is to prove which one you have.

Nothing in sections 1 to 3 changes production. Everything there is read-only.

**Words used here**

| Term | Plain meaning |
| --- | --- |
| Connection pool | A small fixed set of database connections your app shares, like a limited number of phone lines |
| Pool saturation | All lines are busy, so new work queues up and eventually gives up |
| Event loop | The single thread Node uses to do everything. If it is busy, nothing else happens, including replying to the database |
| Event-loop delay | How long work sat waiting for that thread |
| `idle in transaction` | A database connection that started a piece of work and is now just sitting there, holding its locks, doing nothing |
| Paid submission | A request to the music provider that costs money |

---

## 1. Rebuild the timeline, and keep facts apart from guesses

Start one table and keep it honest. A row is **Confirmed** only when you can point at evidence. Anything else stays a hypothesis, however likely it feels. The common failure in incident reviews is a plausible story being repeated until people treat it as fact.

**Timeline**

| Time (UTC) | What happened | Source | Status |
| --- | --- | --- | --- |
| _14:02:10_ | _First `timeout exceeded when trying to connect` in worker log_ | _Log line, link_ | Confirmed |
| _14:01-14:03_ | _Deploy of commit abc123_ | _Replit deploy history_ | Confirmed |
| _14:02_ | _Database was restarted by the platform_ | _Nobody has checked_ | Hypothesis, ask Replit |
| _14:05_ | _Provider was slow_ | _Provider status page shows nothing_ | Hypothesis, weak |

**Hypotheses**

| # | Hypothesis | Evidence for | Evidence against | What would settle it | Status |
| --- | --- | --- | --- | --- | --- |
| A | Connection held across the provider call | | | `pg_stat_activity` from the incident, code read of the job handler | Open |
| B | Connection leak on an error path | | | Did it heal without a restart? | Open |
| C | Synchronous CPU work blocking the event loop | | | Event-loop delay vs DB load at the same moment | Open |
| D | Retry logic amplified a smaller problem | | | Attempts per job, provider invoices | Open |
| E | Platform event: restart, DB suspend or maintenance, CPU throttling | | | Replit support answers, section 5 | Open |

Rules that keep this table useful:

- Put every timestamp in UTC. Mixed time zones have wrecked more timelines than anything else.
- Record what you looked at and found nothing in. "No restarts in the deploy log" is a real result.
- Order matters more than coincidence. Did the event-loop delay start **before** the pool filled, or after? The first thing to go wrong is usually nearest the cause.
- More than one can be true. D often rides on top of A, B or C, so a "yes" on one does not close the others.

---

## 2. What to collect, read-only

Collect this before anyone restarts, redeploys or cleans anything up. Logs on hosted platforms rotate, so do this first.

| Evidence | Why it matters | How |
| --- | --- | --- |
| Application logs with timestamps, 1 hour before to 1 hour after | The order of events | Replit deployment logs. Export them rather than scrolling |
| Exact deployed commit and config at the time | What code was really running | Deploy history, plus env var names and values for pool size, timeouts and pg-boss settings. Do not paste secrets into shared documents |
| Recent changes | Most incidents follow a change | `git log` around that window, including dependency bumps |
| Pool numbers (`totalCount`, `idleCount`, `waitingCount`) | Shows saturation and whether it healed | From logs if you already emit them. If not, add this first (section 4) |
| Event-loop delay | Separates "the DB is slow" from "Node is stuck" | `perf_hooks.monitorEventLoopDelay`, same note |
| `pg_stat_activity` | The database's own view of every connection | Query below |
| pg-boss job states | What the queue believed | Query below |
| Provider invoice or usage export | The ground truth on how much was paid for | Provider dashboard. Compare with your job count for the window |
| Customer reports | Jobs that were lost or stuck | Support inbox, with timestamps |

**Read-only queries.** These only read. Run them on a read-only user if you can.

```sql
-- Who is connected and what are they doing right now?
SELECT state, wait_event_type, count(*),
       max(now() - state_change) AS longest_in_state,
       max(now() - xact_start)   AS oldest_transaction
FROM pg_stat_activity
WHERE datname = current_database()
GROUP BY 1, 2
ORDER BY 3 DESC;

-- Connections sitting in an open transaction, with the last thing they ran.
SELECT pid, application_name, now() - xact_start AS open_for, left(query, 100) AS last_query
FROM pg_stat_activity
WHERE state = 'idle in transaction'
ORDER BY xact_start;

-- Who is blocked, and by whom.
SELECT waiting.pid AS blocked_pid, left(waiting.query, 80) AS blocked_query,
       blocker.pid AS blocking_pid, blocker.state AS blocker_state, left(blocker.query, 80) AS blocker_query
FROM pg_stat_activity waiting
JOIN LATERAL unnest(pg_blocking_pids(waiting.pid)) AS b(pid) ON true
JOIN pg_stat_activity blocker ON blocker.pid = b.pid;

-- How close are we to the server limit?
SELECT count(*) AS in_use, current_setting('max_connections') AS server_limit FROM pg_stat_activity;

-- pg-boss: job states by queue. The schema is "pgboss" unless configured otherwise.
SELECT name, state, count(*), max(retry_count) AS most_retries,
       min(created_on) AS oldest
FROM pgboss.job
GROUP BY 1, 2
ORDER BY 1, 2;

-- Jobs that ran more than once in the window: each one may be a double charge.
SELECT id, name, state, retry_count, created_on, started_on, completed_on
FROM pgboss.job
WHERE retry_count > 0
  AND created_on BETWEEN '<incident start>' AND '<incident end>'
ORDER BY retry_count DESC;
```

**A limitation to be honest about.** `pg_stat_activity` shows only the present. If nobody captured it during the incident, that evidence is gone and the matching rows stay hypotheses. That alone is a good reason to do section 4 before anything else, so the next occurrence is fully recorded. pg-boss rows also get removed by its retention settings, so export them soon.

---

## 3. Telling mechanisms A to D apart

Each mechanism is reproduced in this repo. Run `npm run incident` to see them side by side, and read the measured numbers in [`evidence/SUMMARY.md`](evidence/SUMMARY.md). The table below is what those runs show, and a test keeps it true.

| What you see | A. Held transaction | B. Leak | C. CPU block | D. Retry storm |
| --- | --- | --- | --- | --- |
| Connection timeouts | Yes | Yes | Yes | Yes, the most |
| `waitingCount` | High | High | Brief spikes | High |
| **Heals once load drops?** | Yes | **No, needs a restart** | Yes | Yes |
| `idle in transaction` | **Yes, long-lived** | No | No | Yes, short-lived |
| Lock waits | **Yes, if rows are shared** | No | No | Rarely |
| Connections `idle` in Postgres while the app says all are busy | No | **Yes** | No | No |
| Event-loop delay | Normal | Normal | **Huge** | Normal |
| Heartbeats late | No | No | **Yes** | No |
| Database CPU and activity | Moderate | Low | **Low, the DB is fine** | High |
| Attempts per job | 1 | 1 | 1 | **Many** |
| Paid submissions per job | 1 or fewer | 1 or fewer | 1 or fewer | **More than 1** |

**The four questions, in order**

1. **Did it heal without a restart?** If it stayed broken until someone restarted, suspect **B**.
2. **Was the event-loop delay high while the database was quiet?** Then the database is innocent and it is **C**. Timeouts here are false alarms: Postgres answered quickly, but Node never got to read the answer.
3. **Were there long `idle in transaction` connections or lock waits?** That points to **A**.
4. **Did attempts or provider charges climb faster than real jobs?** That is **D**, and D is often a second layer on another cause. Look for what started it.

**About the event-loop delay in your incident.** A and B leave the loop healthy, so if yours truly had a large delay, either C is involved or something else was burning CPU. That includes CPU throttling by the platform, which is why section 5 matters. The other direction also counts: a blocked loop can create the pool symptoms by itself, so a saturated pool does not prove the pool is the root cause.

**E. Also rule out the platform.** Replit's documentation says a production database idle for 5 minutes is suspended, and Autoscale deployments restart regularly by design and can scale to zero. A worker hosted on Autoscale could plausibly be stopped mid-job, and a first query against a suspended database could be slow. I have not verified either of those against a real Replit app, so treat them as questions, not findings. See the [Replit notes](README.md#replit-postgres-what-is-verified-and-what-is-not).

---

## 4. Smallest safe fix per mechanism

**Do this first, whichever mechanism wins: add the gauges.** Log `totalCount`, `idleCount`, `waitingCount` and event-loop delay every 10 seconds, and set `application_name` on each pool. It takes about 20 lines, cannot change job behaviour, and the next incident answers itself.

| | Smallest safe fix | Test it in isolation | Production acceptance check |
| --- | --- | --- | --- |
| **A** | Commit before the provider call and open a new short transaction after it. The pattern is in `src/worker/pipeline.ts`. Set `idle_in_transaction_session_timeout` as a backstop | `npm run incident:a`, then the pool tests: provider calls in flight exceed connections, and no connection is checked out during the call | Over 24 hours at normal load, no `idle in transaction` older than 5 seconds. `waitingCount` returns to 0 between bursts |
| **B** | Use one wrapper that always releases in `finally`, or use `pool.query()`, which cannot leak. Search for every `pool.connect()` | `npm run incident:b`, then force the error path and confirm `idleCount` returns to its starting value | After a forced provider error in staging, the pool returns to baseline with no restart. Alert if checked-out connections stay above 80% for 5 minutes |
| **C** | Move the heavy step off the main thread with `worker_threads`, stream it, or split it into chunks. Find it first with `node --cpu-prof` in staging | `npm run incident:c`, then process the largest real payload and assert event-loop p99 stays under 100ms | Event-loop p99 under 100ms over 24 hours including the largest real jobs. No heartbeat gaps |
| **D** | Exponential backoff with jitter, a retry cap and a concurrency limit, and **a provider lookup before any resend** | `npm run incident:d`, then a test with a slow provider asserting one paid submission per job | Attempts per job under 1.2 in normal running. Provider invoice count matches completed jobs for a full week |
| **E** | Depends on Replit's answers. A long-running worker most likely belongs on a Reserved VM rather than Autoscale | Restart the worker mid-job in staging and confirm the job recovers | A planned restart during real traffic loses no jobs and creates no extra charges |

**Whichever mechanism it was**, the money rule needs the safe design in this repo, because any crash can cause a double charge or a stuck job regardless of what caused the crash. Roll it out in this order, since each step is safe alone:

1. Add the gauges.
2. Add the job ledger table and write to it alongside the existing flow. Behaviour does not change.
3. Write the intent row and a stable idempotency key before each provider call.
4. Add the lookup-before-resend logic.
5. Add leases, fencing and the sweeper.

**Before step 3, get one fact from each provider in writing:** does it dedupe on an idempotency key, and can you look up a request by your own key? The whole design depends on the second one. If a provider offers no lookup, a timed-out job can never be resolved automatically, so those jobs have to be flagged for a human.

---

## 5. Questions for Replit support

Only they hold this evidence. Send it with the exact UTC window, the deployment name and the database identifier. Ask for yes or no plus a timestamp, not a general explanation.

**Deployment and compute**

1. Did this deployment restart, redeploy, migrate host or get killed for any reason during `<window>`? Exact timestamps and reason, please.
2. Did it autoscale up or down, or scale to zero, during that window? How many instances ran at each point?
3. Was the deployment CPU-throttled or near its CPU or memory limit? Can you share CPU, memory and throttling metrics for that window?
4. Were there any out-of-memory kills?
5. What signal and grace period does a process receive before being stopped, and is that the same for Autoscale and Reserved VM?
6. Is an Autoscale process guaranteed to keep running background work when no HTTP requests are arriving?

**Database**

7. Was there any maintenance, failover, restart, compute resize or storage event on this database during `<window>`?
8. Was the database compute suspended and resumed during that window? How long does a resume usually take?
9. What is the `max_connections` for this database, and what compute size is it on?
10. Were connections refused or terminated by the platform at that time? Are server-side Postgres logs available for that window?
11. Is the app connecting directly or through a pooler? If pooled, which pool mode and what are the limits?

**Network and history**

12. Was there any network incident between the deployment and the database in that window?
13. Were there any open platform incidents affecting either service, whether or not they appeared on the status page?
14. How long are deployment logs and metrics retained, and can you preserve this window now?

---

## 6. Release recommendation template

Copy this for the decision. Each box needs evidence linked next to it. "I think so" counts as unchecked.

```markdown
# Release recommendation: <version>, <date>

**Decision: READY / CONDITIONALLY READY / BLOCKED**
**Decided by:** <name>   **Evidence reviewed through:** <date, time UTC>

## Root cause status
- Confirmed cause: <mechanism and evidence link, or "not proven">
- Still open: <hypotheses, and what would close them>

## Must be true before release
- [ ] No job can be lost: chaos suite green on this commit <link>
- [ ] One paid submission per job: chaos and property tests green <link>
- [ ] Unknown outcomes are looked up, never blindly resent <link>
- [ ] Safeguards proven by removing them (mutation check) <link>
- [ ] Provider lookup-by-key and dedupe behaviour confirmed in writing, per provider
- [ ] Settle window set above the provider's documented worst-case delay
- [ ] Pool and event-loop gauges live, with alerts
- [ ] Rollback tested

## Reasons for this decision
<plain sentences>

## Conditions (if conditionally ready)
| Condition | Owner | Due | What happens if it is not met |
| --- | --- | --- | --- |

## Watch after release
| Signal | Healthy | Roll back if |
| --- | --- | --- |
| Paid submissions / completed jobs | 1.00 | Above 1.02 over 1 hour |
| Jobs flagged for review | Under 0.5% | Above 2% |
| Oldest non-final job | Under 15 min | Over 1 hour |
| Pool `waitingCount` | Back to 0 between bursts | Above 0 for 5 min |
| Event-loop p99 | Under 100ms | Over 500ms for 5 min |
```

**How to choose**

| Decision | Means | Typical example |
| --- | --- | --- |
| **Ready** | Every box is checked, and the root cause is confirmed or the design is proven safe whichever cause it was | Everything green, gauges live, provider behaviour confirmed in writing |
| **Conditionally ready** | The money and job-loss rules are proven, and what remains is limited and has an owner and a date | Root cause still open, but gauges are live and a limited rollout will capture it. Replit's answers are pending and nothing depends on them |
| **Blocked** | Anything can still lose a job or charge twice, or nobody can tell whether it can | Provider has no lookup and no flagging path exists. Chaos suite red. Gauges missing, so a repeat would again be unprovable |

The figures in the template, such as the 1.02 and 2% thresholds, are starting points to tune against real traffic. They are not measured from any production system.
