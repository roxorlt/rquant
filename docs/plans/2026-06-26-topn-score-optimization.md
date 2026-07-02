# TopN Score Optimization Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Turn the current fixed `feature_score_v1` topN filter into configurable score profiles, ablation comparisons, and optional walk-forward validation in Strategy Lab.

**Architecture:** Keep replay and trade triggering unchanged. Add a scoring-profile layer on top of completed trade samples, then compare all triggered trades with per-day topN selections across score profiles. Walk-forward validation evaluates those same topN/profile combinations on chronological folds without using later folds for earlier fold scoring.

**Tech Stack:** Python 3.11+, pandas, Pydantic, Streamlit, pytest, ruff.

---

### Task 1: Configurable TopN Score Profiles

**Files:**
- Modify: `src/rquant/topn_selection.py`
- Test: `tests/unit/test_topn_selection.py`

**Steps:**
1. Write failing tests for named score profiles and group ablation.
2. Verify tests fail because profile APIs do not exist.
3. Add Pydantic score-term/profile models, default profiles, and compatibility wrappers.
4. Verify topN tests pass.

### Task 2: Optimizer Profile Search

**Files:**
- Modify: `src/rquant/strategy_optimizer.py`
- Test: `tests/unit/test_strategy_optimizer.py`

**Steps:**
1. Write failing tests that optimizer rankings include `score_profile`.
2. Verify tests fail on missing parameter / columns.
3. Thread score-profile names through train/test topN comparison.
4. Verify optimizer tests pass.

### Task 3: Walk-Forward TopN Validation

**Files:**
- Create: `src/rquant/topn_walk_forward.py`
- Test: `tests/unit/test_topn_walk_forward.py`
- Modify: `src/rquant/strategy_optimizer.py`

**Steps:**
1. Write failing tests for expanding chronological folds and out-of-sample aggregate metrics.
2. Verify tests fail because module does not exist.
3. Implement fold generation and fold-level topN aggregation from already replayed trade samples.
4. Connect optional `walk_forward_folds` into the optimizer result.

### Task 4: Strategy Lab UI

**Files:**
- Modify: `src/rquant/dashboard/strategy_lab.py`

**Steps:**
1. Add score-profile multi-select and walk-forward fold control.
2. Display score profile in topN rankings and selected samples.
3. Display walk-forward rankings when requested.
4. Run syntax, lint, tests, a real sample optimization, and Streamlit health check.
