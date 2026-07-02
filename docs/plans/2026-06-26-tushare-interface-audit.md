# Tushare Interface Audit Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a reusable Tushare interface audit that logs in with `TUSHARE_COOKIE`, crawls official document pages, merges MCP tool metadata, and identifies A-share-relevant interfaces for rQuant strategy upgrades.

**Architecture:** Add a small parser/classifier module with Pydantic models, plus a one-off script that writes a separate DuckDB audit database under `data/` and exports a Markdown report. Keep it separate from the production rQuant DuckDB to avoid monitor/dashboard lock conflicts.

**Tech Stack:** Python standard library HTML parsing, Pydantic, DuckDB, python-dotenv, Tushare official HTML docs, Tushare MCP metadata.

---

### Task 1: Parser And Classifier Tests

**Files:**
- Create: `tests/unit/test_tushare_docs.py`
- Create: `src/rquant/tushare_docs.py`

**Steps:**
1. Write failing tests for menu path extraction, document body extraction, and strategy classification.
2. Run `uv run pytest tests/unit/test_tushare_docs.py -q` and verify the module import fails.
3. Implement the minimum parser/classifier code.
4. Re-run the focused test.

### Task 2: Crawl And Persist Script

**Files:**
- Create: `scripts/audit_tushare_docs.py`
- Modify: `src/rquant/tushare_docs.py`

**Steps:**
1. Add script-level fetch functions using `TUSHARE_COOKIE` and optional `TUSHARE_TOKEN_MAIN`.
2. Write audit rows to `data/tushare_interface_audit.duckdb`.
3. Export `docs/analysis/2026-06-26-tushare-interface-audit.md`.
4. Run the script against official Tushare docs.

### Task 3: Verification

**Steps:**
1. Run focused unit tests.
2. Run the audit script and inspect row counts.
3. Query the audit DB for high-priority A-share interfaces.
4. Summarize which interfaces are already integrated, can be added now, and require paid or real-time permissions.
