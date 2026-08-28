# PAPER_COST_FULL_ALIGNMENT Implementation Plan

> **For Codex:** Execute this plan in the frozen worktree with strict RED-GREEN-REFACTOR cycles. Do not use network, `.env`, credentials, production data, production services, or another worktree.

**Goal:** Remediate `RQS8-P1-001` by making eligible research and paper executions use one attested v3 cost contract and accounting result.

**Architecture:** Add one Pydantic v3 `ExecutionCostSpec` contract with canonical JSON identity and one pure shared calculator. Preserve v1/v2 parsing and historical arithmetic, but quarantine them from paper alignment. Bind every fresh v5 paper account/fill/receipt to a v3 spec and context; migrate historical v4 evidence to explicit `LEGACY_UNKNOWN` without inventing fees.

**Tech Stack:** Python 3.11, Pydantic v2, SQLite, Decimal, pytest.

---

## Frozen Boundary And Threat Model

- Snapshot commit: `c088774c3199c02edf203a3af758452eb38a5118`.
- Worktree: `/Users/roxor/brain/30-projects/rQuant/.worktrees/stage8-paper-cost-alignment`.
- Branch: `cdx/stage8-paper-cost-alignment`.
- Scope: local financial-simulation consistency and reliability only. No production migration, data access, services, credentials, or network activity.

Assets and trust boundaries:

- Account cash, FIFO lot basis, realized P&L, immutable fills/receipts, and research comparability claims are authoritative simulation records.
- The configured v3 cost spec and normalized instrument context cross into the shared calculator; its resolved result crosses into paper persistence and reconciliation.
- Historical v1/v2 research and v4 paper rows remain readable evidence, but are not trusted alignment inputs.

Failure paths to block:

- Divergent research and paper fee/slippage calculations, including paper transfer fee omission.
- No-match, overlapping, or ambiguous instrument selectors becoming a zero-fee fallback.
- A fee minimum being assessed per order instead of per persisted fill.
- Duplicate execution IDs accepting changed cost/context evidence after ledger mutation.
- A v4 migration synthesizing zero transfer fees or allowing legacy accounts to mix with v3 fills.
- Tampered fee, identity, context, or receipt data reconciling as valid.

Invariants:

- Only one v3 pure calculation establishes executed price/notional and every fee component.
- `KNOWN_V3` uses fees once: buy cash/basis add total fees; sell cash/P&L deduct total fees; slippage stays in price/notional.
- Fresh paper accounts have one immutable v3 binding, and fresh fills/receipts carry complete matching provenance.
- Legacy unknown evidence is read/audit-only and makes comparability false.
- Rejected unsupported context and conflicting retry evidence occur before mutation; transaction failures roll back all new ledger facts.

Excluded:

- Real-world fee-rate defaults, order placement, production SQLite migration/promotion, and release metadata/changelog updates.

## TDD Tasks

### Task 1: Define RED coverage for the shared v3 contract and calculator

**Files:**
- Modify: `tests/unit/test_research_run_spec.py`
- Modify: `tests/unit/test_order_execution_costs.py`
- Create: `tests/unit/test_paper_cost_alignment.py`

1. Add minimal v3 fixtures with explicit non-production values and normalized SH A-share context.
2. Add failing tests for canonical v3 IDs, selector validation, HALF_UP rounding, side applicability, nonzero and zero transfer minimums, and shared research-paper BUY/SELL golden calculation equality.
3. Run the focused tests and retain the expected missing-v3 failures.

### Task 2: Implement the shared v3 contract and pure calculator

**Files:**
- Modify: `src/rquant/research_run_spec.py`
- Modify: `src/rquant/order_execution_costs.py`
- Modify: `src/rquant/strategy_execution_costs.py`

1. Add typed v3 selectors, rule, slippage, money, order-input/context/result models, canonical JSON identity, and alignment eligibility.
2. Implement `calculate_execution_costs` with exact component rounding and fingerprinting; retain the loose legacy calculator only as a deliberate compatibility wrapper.
3. Make research v3 notional replay use the shared calculator and report false `UNBOUND_RESEARCH_COST` until a strict binding is supplied.
4. Re-run Task 1 tests green before refactoring.

### Task 3: Define RED coverage for v5 paper authority and accounting

**Files:**
- Modify: `tests/unit/test_paper_broker.py`
- Modify: `tests/unit/test_paper_contracts.py`
- Modify: `tests/unit/test_runtime_builder_paper.py`
- Modify: `tests/unit/test_runtime_production_profile.py`

1. Add failing account/fill/receipt provenance, cash/basis/P&L, T+1, partial-fill minimum, retry/conflict, rollback, reconciliation-tamper, v4 migration, and runtime-config tests.
2. Run the focused tests and retain the expected schema/API failures.

### Task 4: Implement v5 paper persistence and strict comparison

**Files:**
- Modify: `src/rquant/paper_contracts.py`
- Modify: `src/rquant/paper_broker.py`
- Modify: `src/rquant/runtime_builder_paper.py`
- Modify: `src/rquant/runtime_production_profile.py`
- Modify: affected manifest fixtures and legacy scalar-policy callers

1. Derive `BrokerCostPolicy` only from v3, bind v5 accounts to immutable canonical specs, and persist the shared result once per fill.
2. Add transactional v4-to-v5 quarantine migration and schema attestations with unknown-cost evidence.
3. Update reconciliation, receipts, idempotency evidence, and strict v3 comparability predicate.
4. Re-run the Task 3 tests green, then targeted paper/runtime regressions.

### Task 5: Document and verify

**Files:**
- Create: `docs/architecture/paper-research-cost-alignment.md`

1. Document v3 identity, selector/context requirements, legacy quarantine, offline migration verification/recovery, and fresh-account cutover.
2. Run the RED/GREEN matrix, affected test modules, type/lint checks available in the repository, and a final `git diff --check`.
3. Self-review every PC-01 through PC-11 requirement, make only verified conventional commits, and report exact evidence.
