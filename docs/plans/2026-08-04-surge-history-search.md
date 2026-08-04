# Surge History Search Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add cross-day symbol search to the surge ledger and show the selected event day's minute trend with every trigger marked.

**Architecture:** Keep JSONL as the event authority and `minute_bar` as the historical chart authority. Add pure read-only loaders in `panorama_data.py`, then wire them into the existing Streamlit master/detail UI without changing surge-watch writes or DuckDB schema.

**Tech Stack:** Python 3.11+, pandas, DuckDB read-only replica helpers, Streamlit 1.57, Altair, pytest, Playwright.

---

### Task 1: Cross-day surge history loader

**Files:**
- Modify: `src/rquant/panorama_data.py`
- Modify: `tests/unit/test_panorama_pulse_loaders.py`

**Step 1: Write failing tests**

Add tests that create several `events-YYYY-MM-DD.jsonl` files and assert:

```python
history = search_surge_history("688255", live_dir=tmp_path)
assert list(history["trade_date"]) == [date(2026, 7, 29), date(2026, 7, 27)]
assert set(history["ts_code"]) == {"688255.SH"}

by_name = search_surge_history("芯片", live_dir=tmp_path)
assert list(by_name["name"]) == ["芯片先锋"]
```

Also cover trimmed/case-insensitive queries, no match, malformed filenames, malformed JSON lines, and the empty result column contract.

**Step 2: Verify RED**

Run:

```bash
TUSHARE_TOKEN_MAIN=00000000000000000000000000000000 \
DATA_DIR=/private/tmp/rquant-surge-history-test/data \
DUCKDB_PATH=/private/tmp/rquant-surge-history-test/data/rquant.duckdb \
PARQUET_DIR=/private/tmp/rquant-surge-history-test/parquet \
LOG_DIR=/private/tmp/rquant-surge-history-test/log \
.venv/bin/pytest tests/unit/test_panorama_pulse_loaders.py -q
```

Expected: FAIL because `search_surge_history` does not exist.

**Step 3: Implement the minimal loader**

Add a stable history contract including `trade_date`, scan only `events-*.jsonl`, parse dates strictly from the filename, call `load_surge_log` for per-day ledger semantics, filter `ts_code` and `name` using a trimmed casefolded substring, and sort by `trade_date DESC, confirmed_at DESC`.

Keep this pure file I/O. Do not cache here and do not add a database table.

**Step 4: Verify GREEN**

Run the Task 1 test command. Expected: all tests pass.

**Step 5: Commit**

```bash
git add src/rquant/panorama_data.py tests/unit/test_panorama_pulse_loaders.py
git commit -m "feat(panorama): add cross-day surge history search"
```

### Task 2: Historical minute trend loader and all-event marks

**Files:**
- Modify: `src/rquant/panorama_data.py`
- Modify: `tests/unit/test_panorama_pulse_loaders.py`

**Step 1: Write failing tests**

Create a temporary `DuckDBStore`, insert overlapping `tushare`/`tushare_rt` 1-minute rows, and assert:

```python
trend = load_historical_intraday_trend("688255.SH", date(2026, 7, 29), store=store)
assert list(trend.columns) == ["dt", "price", "avg_price", "volume"]
assert len(trend) == 2
assert trend.iloc[-1]["avg_price"] == pytest.approx(total_amount / total_vol)
```

Add tests for an empty day, unavailable read-only store returning an empty stable contract, and a new `load_surge_event_marks` loader that preserves every valid same-day event for one code in chronological order rather than reducing to the first event.

**Step 2: Verify RED**

Run the Task 1 command. Expected: FAIL for missing historical loaders.

**Step 3: Implement minimal read-only loaders**

`load_historical_intraday_trend` must:

- accept an injectable `DuckDBStore` for tests;
- otherwise open `open_readonly_store(required_tables=("minute_bar",))`;
- query only `[day 00:00:00, day 23:59:59.999999]`, `freq="1min"`;
- normalize `trade_time/close/vol/amount` to the existing trend chart contract;
- compute cumulative VWAP with numeric coercion and `NaN` when cumulative volume is non-positive;
- close only stores it owns and fail soft with a warning plus empty contract.

`load_surge_event_marks` must read the raw daily JSONL, keep all valid rows matching the code, and return `date/confirmed_at/rel_cum` in chronological order. This function is for chart marks; do not change `load_surge_log`'s one-row-per-symbol ledger contract.

**Step 4: Verify GREEN**

Run the focused tests. Expected: all pass.

**Step 5: Commit**

```bash
git add src/rquant/panorama_data.py tests/unit/test_panorama_pulse_loaders.py
git commit -m "feat(panorama): load historical surge minute trends"
```

### Task 3: Streamlit search and selected-day detail

**Files:**
- Modify: `src/rquant/dashboard/market_panorama.py`
- Create or Modify: `tests/unit/test_market_panorama_helpers.py`

**Step 1: Write failing helper tests**

Extract/test pure presentation helpers so tests do not import and execute the whole Streamlit page. Assert the history display includes `日期` as the first column, preserves row order, and maps status/numerics exactly like the existing daily display. Test the selected-row data passed to the historical detail helper.

**Step 2: Verify RED**

Run:

```bash
TUSHARE_TOKEN_MAIN=00000000000000000000000000000000 \
DATA_DIR=/private/tmp/rquant-surge-history-test/data \
DUCKDB_PATH=/private/tmp/rquant-surge-history-test/data/rquant.duckdb \
PARQUET_DIR=/private/tmp/rquant-surge-history-test/parquet \
LOG_DIR=/private/tmp/rquant-surge-history-test/log \
.venv/bin/pytest tests/unit/test_market_panorama_helpers.py -q
```

Expected: FAIL because the history UI helper is missing.

**Step 3: Implement the UI**

- Add a `st.text_input` search field accepting code/name fragments.
- Empty query preserves the existing date picker and daily ledger.
- Non-empty query calls a cached wrapper around `search_surge_history`, shows the cross-day table with a `日期` column, reports the match count, and uses a distinct table key.
- Selecting a daily or search row renders a dedicated historical detail: header with code/name/day, `cached_historical_trend`, and `_trend_chart` with `load_surge_event_marks` for the selected day.
- Historical minute empty state says the selected date has no local minute data; do not call `fetch_intraday_trend` as fallback.
- Keep the existing overview stock chart behavior untouched.
- Use an industrial/utilitarian visual direction consistent with the current panorama: compact controls, dense sortable ledger, blue price line, orange trigger rules/points, restrained status color. Do not introduce new fonts, gradients, or decorative motion.

**Step 4: Verify GREEN**

Run Task 3 tests plus:

```bash
.venv/bin/ruff check src/rquant/panorama_data.py src/rquant/dashboard/market_panorama.py \
  tests/unit/test_panorama_pulse_loaders.py tests/unit/test_market_panorama_helpers.py
```

Expected: all tests pass and Ruff exits 0.

**Step 5: Commit**

```bash
git add src/rquant/dashboard/market_panorama.py tests/unit/test_market_panorama_helpers.py
git commit -m "feat(panorama): search surge history and inspect trigger day"
```

### Task 4: Documentation, browser smoke, and regression verification

**Files:**
- Modify: `CHANGELOG.md`
- Modify: `docs/plans/2026-08-04-surge-history-search-design.md` only if implementation decisions changed

**Step 1: Update changelog**

Add an `[Unreleased] / Added` entry describing code/name cross-day search, exact selected-day local minute charts, all trigger marks, and the read-only replica/empty-state behavior. Do not bump the version for an unmerged feature branch.

**Step 2: Run focused and full tests**

Run focused panorama tests, Ruff for changed files, then the full non-network suite with the test environment variables used for the baseline. Any sandbox-only HTTP/`ps` failures must be rerun outside the sandbox and reported separately.

**Step 3: Run Streamlit fake-mode smoke**

Follow `webapp-testing`: first run the bundled `with_server.py --help`, then launch the panorama page in fake mode on an unused local port. Use headless Chromium to verify the surge tab, search input, result selection, historical detail chart, orange trigger mark layers, and zero browser console errors. Save a screenshot under `/private/tmp/rquant-surge-history-search/`.

If fake mode lacks multi-day history/minute fixtures needed for this path, extend deterministic fake loaders under TDD rather than reading production data.

**Step 4: Review the diff and requirements**

Check every user requirement against the implementation, inspect `git diff origin/main...HEAD`, and confirm no write path, schema, systemd, nginx, secrets, or unrelated files changed.

**Step 5: Commit**

```bash
git add CHANGELOG.md docs/plans/2026-08-04-surge-history-search-design.md
git commit -m "docs: record surge history search"
```
