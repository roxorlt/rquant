# Surge Event Cloud Ingest Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Ensure every stock recorded by the intraday surge watcher is included in that day's cloud research minute ingestion without retaining full-market minute history.

**Architecture:** Add a strict reader for `data/surge_live/events-YYYY-MM-DD.jsonl` and union confirmed event codes with the immutable pre-market research watchlist. The existing `research-ingest` fetch, exact 241-minute audit, lake publication, authority observation, and fail-closed status remain the only publication path.

**Tech Stack:** Python 3.11+, Pydantic, pandas, DuckDB, pytest.

---

### Task 1: Bind confirmed surge events into the minute universe

**Files:**
- Modify: `src/rquant/research_ingest.py`
- Test: `tests/unit/test_research_ingest.py`

**Step 1: Write the failing tests**

Add tests proving that both `confirmed` and `unbuyable` surge codes outside Pool 1/2 are included in `rt_min_daily`, the minute audit expects the union, duplicate events are deduplicated, and malformed event JSON fails before any network call or publication.

**Step 2: Run the focused tests to verify they fail**

Run: `python -m pytest tests/unit/test_research_ingest.py -k surge -q`

Expected: FAIL because the current ingest only uses the pre-market watchlist.

**Step 3: Implement the minimal reader and union**

Read only `paths.state_dir / "surge_live" / f"events-{trade_date}.jsonl"`; accept valid six-digit exchange-qualified codes whose status is `confirmed` or `unbuyable`; return a sorted unique tuple. A missing file means no surge candidates. Invalid JSON, an invalid code/status, or a non-object line raises `ValueError`. Build `expected_minute_codes` as watchlist/observed fallback union surge candidates before the existing fetch and audit.

**Step 4: Run focused and full research-ingest tests**

Run: `python -m pytest tests/unit/test_research_ingest.py -q`

Expected: 44+ tests pass with no failures.

### Task 2: Document and release the behavior

**Files:**
- Modify: `CHANGELOG.md`
- Modify: `pyproject.toml`

**Step 1: Document the cloud-only data ownership change**

Add an `[Unreleased]` entry explaining that daily minute research ingestion now includes confirmed surge events and that malformed event evidence fails closed.

**Step 2: Bump the patch version**

Advance the project from `0.27.0` to `0.27.1`.

**Step 3: Verify the relevant suite and clean diff**

Run: `python -m pytest tests/unit/test_research_ingest.py tests/unit/test_surge_watch.py -q`

Expected: all tests pass. Inspect `git diff --check` and `git status --short`.
