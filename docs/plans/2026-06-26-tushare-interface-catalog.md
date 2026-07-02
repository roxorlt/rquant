# Tushare Interface Catalog Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Convert all audited A-share-related Tushare interfaces into a structured rQuant integration catalog that can drive dashboard planning and later data ingestion work.

**Architecture:** Keep raw official-document audit rows as source-of-truth, then derive a second catalog layer with project-specific integration stage, update cadence, target storage hint, and strategy value. Persist both tables in the standalone audit DuckDB, and expose the catalog in Strategy Lab without touching the production DuckDB.

**Tech Stack:** Python, Pydantic, DuckDB, Streamlit, pytest, official Tushare docs cache.

---

### Task 1: Catalog Derivation

**Files:**
- Modify: `src/rquant/tushare_docs.py`
- Modify: `tests/unit/test_tushare_docs.py`

**Step 1:** Write failing tests for `derive_interface_catalog_rows`.

**Step 2:** Implement a small Pydantic catalog model with:
- `doc_id`
- `api_name`
- `integration_stage`
- `update_cadence`
- `target_table_hint`
- `strategy_value`
- `permission_level`

**Step 3:** Run `uv run pytest tests/unit/test_tushare_docs.py -q`.

### Task 2: Audit Script Persistence

**Files:**
- Modify: `scripts/audit_tushare_docs.py`

**Step 1:** Persist derived catalog rows into `tushare_interface_catalog`.

**Step 2:** Rebuild `data/tushare_interface_audit.duckdb` from cached docs.

### Task 3: Strategy Lab Page

**Files:**
- Modify: `src/rquant/dashboard/strategy_lab.py`
- Modify: `src/rquant/dashboard/strategy_lab_data.py`
- Create or modify tests if logic is extracted.

**Step 1:** Add a cached reader for the standalone audit DB.

**Step 2:** Add a `数据接口` tab with filters for stage, status, capability tag, and permission.

**Step 3:** Display summary metrics and a compact table of all catalog rows.

### Task 4: Verification

**Step 1:** Run focused tests and ruff.

**Step 2:** Run all unit tests.

**Step 3:** Optionally run Streamlit locally if needed for UI validation.
