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

Fresh paper ledgers use schema v5/internal migration v3. A new account is bound
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

The strict paper/research comparator accepts only an opaque
`PaperExecutionCostBindingExport` issued by
`PaperBrokerStore.export_reconciled_execution_cost_binding(...)`. The broker
issues it only after reconciling persisted account, fill, receipt, cost-spec,
runtime-generation, and attestation-head evidence. Constructed caller objects,
including lookalike v3 evidence, are untrusted and compare false.

## Offline Migration and Cutover

Opening a v4 file through `PaperBrokerStore` fails closed with `offline
migration required`; it never mutates the source. Stop the writer, checkpoint
the SQLite source (no active WAL, SHM, or journal sidecar), and use
`migrate_paper_ledger_v4_offline_copy(source_path, candidate_path)`. The API
copies the source to a candidate, migrates and verifies that candidate in a
single SQLite transaction, checks integrity/trust/reconciliation, and promotes
only the verified candidate atomically. Every injected or ordinary failure
leaves the source bytes and hash unchanged.

Migration deliberately leaves historical cash, P&L, lots, receipt JSON, and
legacy fee fields unchanged. It sets the new authority and fee fields to
`NULL`, marks prior account/fill/receipt evidence `LEGACY_UNKNOWN`, and attests
`unknown_cost_provenance_count`. The original v4 schema, attestation, head, and
tamper-marker facts are retained in immutable archive tables. Their content and
the recorded predecessor schema/attestation/head fingerprints are part of the
v5 integrity chain; archive tampering quarantines the candidate.

Legacy accounts are audit-only. Create a new v5 account bound to the active
explicit v3 spec before performing aligned executions. A fresh account can be
reconciled and read beside quarantined legacy evidence; the legacy account
cannot submit executions. To roll back a failed or unsuitable migration,
replace the working offline copy with the verified pre-migration copy. Do not
run this migration against production as part of routine deployment.

## Verification

Run the focused local checks in the project virtual environment:

```sh
cd /Users/roxor/brain/30-projects/rQuant/.worktrees/stage8-paper-cost-alignment
RQUANT_DISABLE_DOTENV=1 TUSHARE_TOKEN_MAIN=00000000000000000000000000000000 \
DATA_DIR=/private/tmp/rquant-stage8-paper-cost-alignment-tests/data \
DUCKDB_PATH=/private/tmp/rquant-stage8-paper-cost-alignment-tests/data/rquant.duckdb \
PARQUET_DIR=/private/tmp/rquant-stage8-paper-cost-alignment-tests/parquet \
LOG_DIR=/private/tmp/rquant-stage8-paper-cost-alignment-tests/log \
.venv/bin/python -m pytest tests/unit/test_paper_cost_alignment.py tests/unit/test_paper_broker.py -q
```

An exact `KNOWN_V3` comparison is true only when spec ID, engine, normalized
instrument context, selected rules, fill topology/notional, slippage, tick,
quantum, rounding, and resolved calculations all agree. Any missing, legacy,
rate-only, unbound, or mismatched evidence is false with a machine reason.
