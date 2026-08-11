# Source Quota Governance

All external market-data requests use `SourceQuotaStore`. A source window is
declared with its fixed capacity, then every real transport call, including a
retry, follows this order:

1. Persist a quota lease and `pending` attempt.
2. Mark the attempt dispatched immediately before the provider call.
3. Commit that call's unit as `success` or `failure`, then release it.

A restarted process never re-dispatches a persisted pending attempt. Recovery
commits it as `unknown`, preserving the quota charge for audit and preventing a
possible double request. Auction recovery is protected by its cross-process
capture lock. Market-minute, reference, and daily-close recovery use a minimum
monotonic age; a different boot identity is always stale. Daily-close scans all
pending attempts for the source, including attempts from prior quota windows.

`release()` rejects a lease linked to a pending or dispatched attempt. Only
`commit_attempt()` or `recover_attempt()` may finish such a lease. Attempt
timestamps are clamped to lifecycle order when wall time moves backwards, and
the ledger records boot identity, monotonic observation, lifecycle sequence,
and rollback count. The ledger schema is migrated additively from version 2 to
version 3 under a bounded cross-process lock and `BEGIN EXCLUSIVE`. Each column
is rechecked inside the transaction before alteration; an incompatible,
partial, or lock-starved schema fails closed.

`daily-close.source.v1` config exposes only operational quota fields:

```json
{
  "quota_path": "/absolute/runtime/live/daily-close/quota.sqlite3",
  "quota_units_per_window": 20,
  "quota_accounting_mode": "transport",
  "quota_cost_per_request": null,
  "pending_recovery_min_age_seconds": 300
}
```

The production profile binds this path to the daily-close spool and validates
transport accounting. No source credential is stored in the profile or quota
ledger.

The default daily-close fetcher returns a typed `DailyCloseFetchResult` receipt
aggregating transport-call receipts. A successful no-retry capture currently
uses 11 calls: `daily`, `daily_basic`, `adj_factor`, five separate
`index_daily` calls, `stock_basic`, `stock_st`, and `suspend_d`. This is a
runtime observation, not a fixed reservation. Every retry gets another durable
attempt and receipt before transport; exhaustion prevents that transport call.
The gateway verifies every receipt against the authoritative ledger. Missing,
forged, or mismatched receipts fail closed.

The production reference source uses the same `transport` accounting mode.
Its no-retry baseline is six calls, while retries increase the typed aggregate
and quota consumption one call at a time. Request-level fixed costs remain only
as an explicit compatibility mode for controlled custom adapters.

Before deployment, run the read-only check against the intended runtime root:

```bash
uv run rquant preflight --profile production --runtime-root /absolute/runtime
```

An uninitialized production ledger for auction, market-minute, reference, or
daily-close is a failure and produces a nonzero exit. The research/candidate
profile reports the same first-run state as a warning. The checks never create
the ledgers and do not require or print a provider token.

Research adapters must publish a `ResearchAdapterSourceUsage` Pydantic contract.
An external adapter declares its provider source, estimated and actual calls,
and a source-matching quota lease. An immutable local snapshot adapter declares
`external=false`, `immutable_snapshot=true`, and zero calls/zero quota. Missing
or inconsistent external usage is rejected before execution.

The current strategy adapters are immutable local snapshot adapters, so they
remain zero-quota. A future adapter that performs external calls must bind its
execution-time actual usage and attempt completion at the worker invocation
boundary; that worker integration is intentionally outside this change.
