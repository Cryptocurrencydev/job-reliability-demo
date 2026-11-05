export type ResubmitPolicy = 'resubmit' | 'flag';

export interface ReliabilityConfig {
  leaseTtlMs: number;
  heartbeatMs: number;
  /** Abort the provider call after this long. The outcome is then unknown, not failed. */
  providerTimeoutMs: number;
  /**
   * How long after a submit attempt a "not found" from the provider may be believed.
   * Must exceed the provider timeout plus the provider's worst-case ingest delay.
   */
  settleMs: number;
  /** What to do when the settle window passes and the provider still has no record. */
  resubmitPolicy: ResubmitPolicy;
  maxSubmitAttempts: number;
  maxClaims: number;
  sweepMs: number;
  pollMs: number;
  pollBudgetMs: number;
}

export const DEFAULTS: ReliabilityConfig = {
  leaseTtlMs: 30_000,
  heartbeatMs: 10_000,
  providerTimeoutMs: 30_000,
  settleMs: 120_000,
  // Conservative default: switch to 'resubmit' only for a provider with confirmed dedupe,
  // or a documented worst-case ingest delay safely inside settleMs.
  resubmitPolicy: 'flag',
  maxSubmitAttempts: 3,
  maxClaims: 25,
  sweepMs: 15_000,
  pollMs: 5_000,
  pollBudgetMs: 20_000,
};

/** Scaled-down timings so a kill-and-recover cycle takes about a second in tests. */
export const FAST: ReliabilityConfig = {
  leaseTtlMs: 700,
  heartbeatMs: 200,
  providerTimeoutMs: 250,
  settleMs: 500,
  resubmitPolicy: 'resubmit',
  maxSubmitAttempts: 3,
  maxClaims: 40,
  sweepMs: 150,
  pollMs: 40,
  pollBudgetMs: 300,
};

export function validate(cfg: ReliabilityConfig): ReliabilityConfig {
  if (cfg.heartbeatMs * 2 > cfg.leaseTtlMs) throw new Error('leaseTtlMs must be at least 2x heartbeatMs');
  if (cfg.settleMs <= cfg.providerTimeoutMs) throw new Error('settleMs must exceed providerTimeoutMs');
  return cfg;
}

export const QUEUE = 'generate';
export const DEFAULT_DB_URL = 'postgres://tunesmith:tunesmith_local@127.0.0.1:55433/tunesmith';
export const dbUrl = () => process.env.DATABASE_URL ?? DEFAULT_DB_URL;
