# Paper and Research Cost Alignment

## Contract

`ExecutionCostSpec` v3 is the only paper-alignment contract. Its identity is
`cost_spec_id = sha256(canonical_json)`, where the canonical payload includes
`schema_version: 3` and `cost_engine_version`. It also carries exact instrument
selectors, component rules, executed-notional/per-fill semantics, shared-engine
slippage, price tick, and money rounding.

Paper quote contexts require an attested `A_SHARE` classification from the
`security_listing_status` reference chain, including its record and generation
provenance. Symbol suffixes and six-digit code shapes are never classification
authority: missing, fund, bond, ETF, or otherwise non-A-share context rejects
before ledger mutation.

The research target notional is replay topology, not a fee-contract field. A
strict comparison therefore checks the resolved fill inputs and calculations in
addition to the v3 spec ID. V1 and v2 specs remain readable for historical
research arithmetic only; they are never upgraded implicitly and cannot be
paper-comparable.

`calculate_execution_costs(spec, order_input, instrument_context)` is the one
pure calculator. It resolves one exact selector, applies shared slippage to the
executed price, assesses each fee component on executed notional for each
persisted fill, and emits rule IDs plus a context fingerprint. Empty, no-match,
or ambiguous selector evidence is invalid.

## Paper Ledger

Fresh paper ledgers use schema v5/internal migration v4. A new account is bound
immutably to one persisted canonical `paper_cost_spec`. Every new fill and
execution receipt is `KNOWN_V3` and stores transfer fee, total fees, spec ID,
schema version, context fingerprint, and the resolved receipt evidence.

Accounting uses the result once:

- BUY cash and FIFO basis add `total_fees` to executed notional.
- SELL cash and realized P&L deduct `total_fees` from executed notional.
- Slippage is already in executed price/notional and is never added to
  `total_fees`.

The execution request fingerprint includes the v3 spec, engine, normalized
context, and resolved calculation. Replaying identical evidence returns the
original receipt; changed evidence conflicts before ledger mutation.

Pure cost-math matching is non-authoritative. The only exact comparison API is
`PaperBrokerStore.compare_research_execution_costs(...)`; callers provide only
research evidence, the bound account ID, and ordered persisted execution IDs.
Within one read transaction the store reads and reconciles the account, cost
spec, orders, fills, receipts, authority snapshot, and ledger head, then replays
each fill through the shared engine. No export token, issuer, cache,
caller-supplied paper fact, or alternate exact-v3 path exists.

Exact comparison also requires an external
`rquant-paper-ledger-head/v1` Ed25519 checkpoint naming the configured ledger,
current v5 head, and migration-attestation digest. Runtime wiring contains only
a pinned public key, allowed ledger ID, positive
`ledger_anchor_max_age_seconds`, non-negative
`ledger_anchor_future_skew_seconds`, and the runtime builder's trusted clock.
The verifier normalizes that construction-time clock to UTC and accepts
`issued_at` only when it is no older than the configured maximum age and no
further in the future than the configured skew; both boundaries are inclusive.
The comparison caller cannot provide or override the clock. Missing, invalid,
stale, future, or different-head evidence returns `CURRENT_HEAD_UNANCHORED`;
local diagnostics remain available, but exact comparability is false. This
patch does not contain a production private key or issue a production anchor.

## Offline Migration and Cutover

Opening a v4 file through `PaperBrokerStore` fails closed with `offline
migration required`; it never mutates the source. Stop the writer, checkpoint
the SQLite source (no active WAL, SHM, or journal sidecar), and use
`migrate_paper_ledger_v4_offline_copy(source_path, candidate_path)`. The API
first verifies the closed regular source using the frozen
schema-v4/internal-2 reconciler. That independent replay validates cash, FIFO
lots and consumptions, realized P&L, receipts, and the predecessor
attestation/head using observed v4 commission-and-tax accounting. Every BUY lot
must exactly match its source fill's account, instrument, signal, quantity,
basis, executed/persisted timestamps, sequence, and acquisition trade date. Its
availability date must exactly match the persisted execution request's
authoritative acquisition-availability fact and must be later than the
acquisition trade date. Consumption rows must map only to SELL fills and pass
FIFO, quantity, unit-cost, ordering, and persisted-time checks. Expected and
persisted allocations use the same canonical tuple: availability date,
acquisition trade date, BUY execution time, BUY persistence time, fill
sequence, then lot ID. The persisted allocation query explicitly joins each
consumption to its lot, BUY fill, and BUY order; lexical lot ID order has no
authority. Multi-fill receipt order snapshots are validated at their own
cumulative fill sequence rather than against the order's later final state.
Direct migration tests corrupt each lot provenance field, consumption mapping,
realized P&L, and receipt payload; every case rejects before a candidate exists
while preserving the corrupt source's pre-attempt hash. The migrator then
copies a verified source to a distinct temporary path, transforms only that
copy in an explicit transaction, verifies it, and atomically publishes only
the requested offline candidate. Every injected or ordinary phase failure
leaves the source bytes and SHA-256 unchanged and publishes no candidate.

The committed v4 fixture is built only by running exact parent
`c088774c3199c02edf203a3af758452eb38a5118` with parent `src` first. Its seed
uses the parent's public partial/incremental execution APIs to create a
500-share BUY as 200- and 300-share fills, then a 300-share SELL across both
lots. The seed fixes explicit parent execution IDs so chronological lot IDs are
intentionally inverse to lexical order. The closed binary SHA-256 is
`be1497e0725f6427ff5c61db64b79fdd504a9968b547fd69effc5f55882a0822`;
the manifest independently freezes schema 4/internal migration 2, schema
objects, triggers, predecessor identities, seed identity, and business rows.

Migration deliberately leaves historical cash, P&L, lots, receipt JSON, and
legacy fee fields unchanged. It sets the new authority and fee fields to
`NULL`, marks prior account/fill/receipt evidence `LEGACY_UNKNOWN`, and attests
`unknown_cost_provenance_count`. The original v4 schema, attestation, head, and
tamper-marker facts are retained as canonical primary-key-ordered rows in
immutable archive tables. A non-self-referential archive binding and digest are
recorded in the immutable `rquant-paper-ledger-migration/v2` report; its digest
is carried by revision 1, every later v5 head, and the external head anchor.
Coordinated archive-plus-binding tampering therefore fails migration-attestation
validation, while a forged internal head still fails external verification.

Legacy accounts are audit-only. Create a new v5 account bound to the active
explicit v3 spec before performing aligned executions. A fresh account can be
reconciled and read beside quarantined legacy evidence; the legacy account
cannot submit executions. To roll back a failed or unsuitable migration,
discard the candidate and retain the verified pre-migration source. The result
derives reconciliation state from the independent v4 report and permits live
promotion only with a valid current-head anchor; the local migrator returns an
unanchored audit candidate. Do not run this migration against production as
part of routine deployment.

## Verification

Run the focused local checks in the project virtual environment:

```sh
cd /Users/roxor/brain/30-projects/rQuant/.worktrees/stage8-paper-cost-alignment
RQUANT_DISABLE_DOTENV=1 TUSHARE_TOKEN_MAIN=00000000000000000000000000000000 \
DATA_DIR=/private/tmp/rquant-stage8-paper-cost-alignment-tests/data \
DUCKDB_PATH=/private/tmp/rquant-stage8-paper-cost-alignment-tests/data/rquant.duckdb \
PARQUET_DIR=/private/tmp/rquant-stage8-paper-cost-alignment-tests/parquet \
LOG_DIR=/private/tmp/rquant-stage8-paper-cost-alignment-tests/log \
.venv/bin/python -m pytest -q \
  tests/unit/test_paper_cost_alignment.py \
  tests/unit/test_paper_broker.py \
  tests/unit/test_paper_ledger_v4_migration.py \
  tests/unit/test_paper_ledger_anchor.py \
  tests/unit/test_strategy_paper_lifecycle.py \
  tests/unit/test_runtime_builder_paper.py \
  tests/unit/test_runtime_paper_quote.py
```

An exact `KNOWN_V3` comparison is true only when spec ID, engine, normalized
instrument context, selected rules, fill topology/notional, slippage, tick,
quantum, rounding, and resolved calculations all agree. Any missing, legacy,
rate-only, unbound, or mismatched evidence is false with a machine reason.
