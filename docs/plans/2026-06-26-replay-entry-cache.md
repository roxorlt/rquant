# Replay Entry Cache Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Cache minute replay at the entry-event layer so risk grids can replay exits without recomputing signal detection and feature snapshots.

**Architecture:** Keep the existing `run_minute_strong_carry_replay` behavior unchanged. Add a small cache module that stores open `PaperPosition` snapshots plus their already-fetched exit minute window, then replays exits by cloning the position with a new `PaperTradeConfig`.

**Tech Stack:** Python 3.12, Pydantic models, pandas, DuckDB, pytest.

---

### Task 1: Entry Snapshot Cache

**Files:**
- Create: `src/rquant/replay_entry_cache.py`
- Test: `tests/unit/test_replay_entry_cache.py`

**Steps:**
1. Write failing tests for replaying one cached entry with two different risk configs.
2. Verify tests fail because the module does not exist.
3. Implement `ReplayEntrySnapshot`, `RiskReplayQuote`, `EntryReplayCache`, and `replay_snapshot_exit`.
4. Verify the new tests pass.

### Task 2: Risk Search Adapter

**Files:**
- Modify: `src/rquant/risk_search.py`
- Test: `tests/unit/test_risk_search.py`

**Steps:**
1. Write a failing test showing risk search can call a single cached snapshot loader once and evaluate multiple configs.
2. Add `run_risk_grid_search_from_entry_cache`.
3. Verify targeted tests pass.

### Task 3: Integration Safety

**Files:**
- Modify only if needed: `src/rquant/minute_replay.py`

**Steps:**
1. Keep existing minute replay tests green.
2. Run full unit suite and lint on touched files.
3. Document the behavior in `CHANGELOG.md`.
