# Combined Pool Replay Cache And Risk Search Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Deep-dive the `Pool2 + vp_risk_only` candidate direction by replaying Pool1+Pool2 as one deduplicated universe, comparing hold days 1-10, and searching stop/take-profit/trailing parameters plus feature-score weights without repeating expensive minute replay work.

**Architecture:** Keep minute signal generation deterministic and unchanged. Add a combined-pool candidate mode, a replay-cache layer that stores completed trade samples keyed by replay parameters, then run topN/profile/weight/risk searches from cached trades. Risk search changes `PaperTradeConfig` before replay; feature search changes only the post-trigger ranking layer.

**Tech Stack:** Python 3.11+, pandas, Pydantic, DuckDB readonly store, Streamlit, pytest, ruff.

---

### Task 1: Combined Pool Candidate Query

**Files:**
- Modify: `src/rquant/minute_replay.py`
- Test: `tests/unit/test_minute_replay.py`

**Steps:**
1. Write a failing test that seeds the same code in Pool1 and Pool2 and asks `preset_name="n-shape-combined"`.
2. Verify it fails because combined preset is not supported.
3. Implement `_query_candidates()` support for `n-shape-combined`, selecting Pool2 when duplicate `(trade_date, ts_code)` exists.
4. Verify the combined replay test passes.

### Task 2: Replay Trade Cache

**Files:**
- Create: `src/rquant/replay_cache.py`
- Test: `tests/unit/test_replay_cache.py`

**Steps:**
1. Write failing tests for deterministic cache keys and avoiding repeated replay calls.
2. Verify tests fail because module does not exist.
3. Implement in-memory `ReplayTradeCache` with Pydantic `ReplayCacheKey`.
4. Verify cache tests pass.

### Task 3: Risk Parameter Search

**Files:**
- Create: `src/rquant/risk_search.py`
- Modify: `src/rquant/strategy_compare.py`
- Test: `tests/unit/test_risk_search.py`

**Steps:**
1. Write failing tests that generate a compact risk grid over stop loss, take profit, and trailing stop.
2. Verify tests fail because search module does not exist.
3. Allow `run_entry_mode_comparison()` to accept `PaperTradeConfig`.
4. Implement risk-profile grid search using replay cache.

### Task 4: Feature Weight Search

**Files:**
- Modify: `src/rquant/topn_selection.py`
- Create/Modify: `src/rquant/feature_weight_search.py`
- Test: `tests/unit/test_feature_weight_search.py`

**Steps:**
1. Write failing tests for custom group multipliers producing distinct score profiles.
2. Implement compact multiplier grid search over intraday / accumulation / position / market groups.
3. Rank results by mean return, win rate, and sample-size-aware robust score.

### Task 5: Dashboard And Real Sample Verification

**Files:**
- Modify: `src/rquant/dashboard/strategy_lab.py`
- Modify: `CHANGELOG.md`

**Steps:**
1. Add combined-pool option.
2. Add hold-day default 1-10 for optimizer.
3. Add risk-grid controls only as explicit opt-in.
4. Run unit tests, lint, syntax checks, real sample search, and Streamlit health check.
