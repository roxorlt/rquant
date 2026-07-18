# Stage 1 Bounded Repair And Formal Smoke Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Bound historical minute repair memory to one trading day and add a fail-closed formal smoke replay command for all three Stage 1 strategies.

**Architecture:** Minute repair becomes a two-pass process: the first pass binds per-day content hashes into the immutable plan, while apply reconstructs and verifies one day at a time into a shared staging generation before one atomic publication. A separate formal smoke module executes versioned fixed strategy specs only through `ResearchExecutionSession` and persists normal Strategy Lab evidence.

**Tech Stack:** Python 3.11+, pandas, DuckDB, Pydantic, pytest, argparse, existing research lake/catalog/gate and Strategy Lab result APIs.

---

### Task 1: Bound minute-session completeness checks

**Files:**
- Modify: `tests/unit/test_backfill_planner.py`
- Modify: `src/rquant/backfill_manifest.py`

**Step 1: Write failing equivalence tests**

Cover exact 241-minute sessions, one missing minute, an off-grid minute, duplicate rows, unrelated
codes, wrong source and targets split across batch boundaries. Assert the checker returns only completed
`(ts_code, trade_date)` keys and scalar counts, never a 241-value time list.

**Step 2: Run RED**

Run the new tests directly. Expected: current list-aggregation implementation violates the bounded
result contract.

**Step 3: Implement bounded scalar aggregation**

Batch exact target keys, join rather than scan unrelated sessions, and compute count/min/max plus
grid-validity aggregates in DuckDB. Group batches by trading date, push an explicit half-open
`trade_time` range into operational queries, and pass only that date's immutable Parquet artifact
to lake queries. Keep `read_only=True` and `temp_directory=''`.

**Step 4: Run GREEN**

```bash
PYTHONPATH=src /Users/roxor/brain/30-projects/rQuant/.venv/bin/pytest \
  tests/unit/test_backfill_planner.py tests/unit/test_backfill_manifest.py -q
```

**Step 5: Commit**

```bash
git add src/rquant/backfill_manifest.py tests/unit/test_backfill_planner.py
git commit -m "fix(backfill): bound minute completeness checks"
```

### Task 2: Prove planning does not retain minute frames

**Files:**
- Modify: `tests/unit/test_research_minute_repair.py`
- Modify: `src/rquant/research_minute_repair.py`

**Step 1: Write the failing test**

Add a test that builds a multi-day preview, captures `_PreparedMinuteRepair`, and asserts it contains
the immutable plan and existing manifests but no `merged_by_date` or other `DataFrame` collection.

**Step 2: Run test to verify it fails**

Run:

```bash
PYTHONPATH=src /Users/roxor/brain/30-projects/rQuant/.venv/bin/pytest \
  tests/unit/test_research_minute_repair.py::test_preview_does_not_retain_merged_day_frames -q
```

Expected: FAIL because `_PreparedMinuteRepair` still exposes `merged_by_date`.

**Step 3: Write minimal implementation**

Remove `merged_by_date` from `_PreparedMinuteRepair`; keep computing each `ResearchMinuteRepairDayPlan`
in `_build_prepared_minute_repair`, but discard the returned merged frame after hashing.

**Step 4: Run targeted and module tests**

Run the new test, then:

```bash
PYTHONPATH=src /Users/roxor/brain/30-projects/rQuant/.venv/bin/pytest \
  tests/unit/test_research_minute_repair.py -q
```

Expected: new test PASS; apply tests fail only where staging still expects retained frames.

**Step 5: Commit**

```bash
git add src/rquant/research_minute_repair.py tests/unit/test_research_minute_repair.py
git commit -m "refactor(research): release planned minute frames by day"
```

### Task 3: Rebuild and verify one day at a time during apply

**Files:**
- Modify: `tests/unit/test_research_minute_repair.py`
- Modify: `src/rquant/research_minute_repair.py`

**Step 1: Write failing bounded-export tests**

Add tests proving:

- apply invokes the staged export with `start_date == end_date` once per changed day;
- no export source contains rows from another day;
- a source-row change between the plan pass and staging pass raises a content-drift error before live
  publication;
- the second pass checks existing manifest hash, target sessions, row counts, source hash, merged hash
  and expected staged manifest hash.

**Step 2: Run tests to verify RED**

Run each new test directly. Expected failures: missing per-day reconstruction and current global concat.

**Step 3: Implement per-day staging**

Add a helper that reconstructs one `ResearchMinuteRepairDayPlan` from the operational source and live
research partition, compares it to the bound day plan, and returns the verified merged frame. Change
`_prepare_repair_generation` to accept `source_database`, loop over `prepared.plan.days`, build a
single-day in-memory export source, call `export_research_dataset(..., start_date=day,
end_date=day)`, collect its manifest, close the source, and release the frame before the next day.
Delete the global `pd.concat` path.

**Step 4: Vectorize merge and stream row hashing**

Add golden equivalence tests before changing the implementation. Replace the per-row
`Series.to_dict()` merge with vectorized key reconciliation while preserving old `created_at` for
null-safe business-column equality. Feed canonical rows incrementally into SHA256 rather than building
a whole CSV string and encoded copy.

**Step 5: Verify**

Run:

```bash
PYTHONPATH=src /Users/roxor/brain/30-projects/rQuant/.venv/bin/pytest \
  tests/unit/test_research_minute_repair.py -q
```

Expected: all tests PASS, including journal rollback and zero-write preview cases.

Also run the subprocess RSS probe with a common maximum-sized warm-up day followed by either one or
ten measured days. Each day contains 512 complete sessions. Bound the one-to-ten-day delta as
DuckDB/Parquet allocation overhead and separately require the day-five-to-day-ten peak range to stay
within 48 MiB; a linear total-row slope fails even when the absolute process baseline is high.

**Step 6: Commit**

```bash
git add src/rquant/research_minute_repair.py tests/unit/test_research_minute_repair.py
git commit -m "fix(research): stage minute repairs with bounded memory"
```

### Task 4: Define fixed formal smoke specifications

**Files:**
- Create: `tests/unit/test_formal_smoke_replay.py`
- Create: `src/rquant/formal_smoke_replay.py`

**Step 1: Write failing specification tests**

Test that the registry contains exactly `n_shape`, `growth_board_surge`, and `auction_gap`; every spec
has a stable version and canonical payload; changing any parameter changes its spec hash; unsupported
strategies fail.

**Step 2: Run RED**

```bash
PYTHONPATH=src /Users/roxor/brain/30-projects/rQuant/.venv/bin/pytest \
  tests/unit/test_formal_smoke_replay.py -q
```

Expected: import failure because the module does not exist.

**Step 3: Implement minimal typed specs**

Create frozen Pydantic models and a registry for the three v1 fixed parameter sets documented in the
design. Reuse existing Strategy Lab canonical hashing rather than adding a second hash algorithm.

**Step 4: Run GREEN**

Run the target test and Ruff on the new module.

**Step 5: Commit**

```bash
git add src/rquant/formal_smoke_replay.py tests/unit/test_formal_smoke_replay.py
git commit -m "feat(research): define fixed Stage 1 smoke specs"
```

### Task 5: Execute and persist formal smoke runs

**Files:**
- Modify: `tests/unit/test_formal_smoke_replay.py`
- Modify: `src/rquant/formal_smoke_replay.py`

**Step 1: Write failing execution tests**

Test that execution:

- constructs `ResearchGateRequest(mode="formal")` with exact strategy, range, code commit and explicit
  audit/snapshot/binding evidence;
- opens only `open_gated_research_store`;
- rejects mismatched selected evidence and never calls rolling `open_readonly_store`;
- calls the existing fixed strategy engine;
- persists a comparable Strategy Lab v2 run;
- emits run ID, audit, snapshot, binding, spec hash, result hash, metrics and no missing reason.

**Step 2: Run RED**

Run each new test directly. Expected: missing executor behavior.

**Step 3: Implement strategy adapters**

Add small adapters for the three existing engines and metric builders. Build a gate research manifest,
then use `build_strategy_lab_run` and `save_strategy_lab_run`. Require the gate decision evidence to
match the command request before compute and preserve one execution session for the whole run.

**Step 4: Run GREEN**

Run:

```bash
PYTHONPATH=src /Users/roxor/brain/30-projects/rQuant/.venv/bin/pytest \
  tests/unit/test_formal_smoke_replay.py \
  tests/unit/test_strategy_lab_runs.py \
  tests/unit/test_strategy_research_gate.py -q
```

**Step 5: Commit**

```bash
git add src/rquant/formal_smoke_replay.py tests/unit/test_formal_smoke_replay.py
git commit -m "feat(research): execute bound formal smoke replays"
```

### Task 6: Add the non-interactive CLI

**Files:**
- Modify: `tests/unit/test_cli.py`
- Modify: `src/rquant/cli.py`

**Step 1: Write failing CLI tests**

Test parser and command behavior for required `--strategy`, `--start-date`, `--end-date`,
`--audit-run-id`, `--snapshot-id`, `--binding-hash`, optional `--output-dir`, JSON stdout, nonzero
fail-closed errors, and no exploratory switch.

**Step 2: Run RED**

Run the new CLI tests. Expected: unknown command/parser failure.

**Step 3: Implement the command**

Register `formal-smoke-replay`, validate exact evidence and clean commit, call the new executor, and
print one JSON object suitable for deployment evidence capture. Validate the real Git HEAD and dirty
state; an injected deployment commit may confirm HEAD but cannot replace it.

Before hashing or saving formal results, canonicalize every result table by normalized full-row key.
The same rows in a different SQL/strategy return order must preserve the result hash.

**Step 4: Run GREEN**

```bash
PYTHONPATH=src /Users/roxor/brain/30-projects/rQuant/.venv/bin/pytest \
  tests/unit/test_cli.py tests/unit/test_formal_smoke_replay.py -q
```

**Step 5: Commit**

```bash
git add src/rquant/cli.py tests/unit/test_cli.py
git commit -m "feat(cli): add formal Stage 1 smoke replay"
```

### Task 7: Document moving boundaries and release

**Files:**
- Modify: `README.md`
- Modify: `CHANGELOG.md`
- Modify: `docs/plans/2026-07-17-stage1-execution-snapshot-implementation.md`
- Modify: `pyproject.toml`
- Modify: `src/rquant/__init__.py`
- Modify: `uv.lock`

**Step 1: Add documentation assertions if an existing docs test applies**

Ensure examples say omitted `backfill-plan --end-date` selects the latest fully observable eligibility
date, while every persisted manifest remains immutable.

**Step 2: Update documentation and version**

Document the bounded-memory two-pass repair, formal-only smoke command, fixed v1 specs, evidence
fields and rollback behavior. Bump `0.24.0` to `0.25.0`; update lock metadata mechanically.

**Step 3: Run full verification**

```bash
bash scripts/check-core-quality.sh
PYTHONPATH=src /Users/roxor/brain/30-projects/rQuant/.venv/bin/pytest -q
```

Also run Ruff, lock check and changed-files diff check used by CI.

**Step 4: Independent review and commit**

Review for P0/P1/P2 findings, fix with TDD if needed, then commit:

```bash
git add README.md CHANGELOG.md docs pyproject.toml src/rquant/__init__.py uv.lock
git commit -m "docs(research): explain reproducible Stage 1 acceptance"
```

### Task 8: Merge, tag, deploy and execute production acceptance

**Files:**
- Modify after successful deployment: `DEPLOY.md`

**Step 1: PR and exact release**

Push the feature branch, open one PR, wait for Python 3.11/3.12 checks, squash merge only when green,
and create annotated `v0.25.0` at the exact merged `origin/main` commit.

**Step 2: Deploy outside the protected window**

Run the deployment dry-run, then:

```bash
bash scripts/deploy-production.sh --target v0.25.0
```

Verify exact tag/SHA, preflight, services, timers, authority and replica hashes.

**Step 3: Build final moving manifests**

For each strategy, omit `--end-date` so the planner selects the latest fully observable eligibility
date under the final code commit. Record the selected date, manifest ID and eligibility resolution
hash. Complete production source backfill as needed.

**Step 4: Repair, snapshot and replay**

For `n_shape`, `growth_board_surge`, then `auction_gap`: preview/apply bounded minute repair; verify
zero remaining missing sessions; preview/apply `dataset-snapshot`; run `formal-smoke-replay` with the
exact audit/snapshot/binding evidence.

**Step 5: Final evidence and deployment log**

Verify coverage thresholds, comparable status, result hashes, backups, catalog/readonly equality,
authority, replica, services and timers. Add exact production evidence and rollback command to
`DEPLOY.md`, merge the documentation PR, and remove temporary automation/worktrees only after all
checks pass.
