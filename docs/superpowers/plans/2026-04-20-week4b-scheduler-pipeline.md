# Week 4b: Scheduler + Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add CLI entry points (`rquant serve` / `rquant run-daily`), screen_result persistence, preset registry with N-shape Pool 1 + Pool 2, and daily pipeline orchestration.

**Architecture:** `presets.py` defines strategy presets as Python data. `pipeline.py` orchestrates: check data → run presets in dependency order → persist to `screen_result`. `cli.py` provides `serve` (APScheduler cron) and `run-daily` (one-shot) subcommands.

**Tech Stack:** APScheduler 3.x (BlockingScheduler), argparse, DuckDB JSON column, existing screen() engine.

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| Modify | `src/rquant/storage/schema.py` | Add SCREEN_RESULT_DDL |
| Modify | `src/rquant/storage/duckdb.py` | Add upsert/query screen_result |
| Modify | `src/rquant/screen/core.py` | Add ts_code_whitelist param |
| Create | `src/rquant/presets.py` | ScreenPreset dataclass + PRESET_SCREENS registry |
| Create | `src/rquant/pipeline.py` | run_daily_pipeline() orchestration |
| Create | `src/rquant/cli.py` | argparse serve/run-daily subcommands |
| Modify | `pyproject.toml` | Add apscheduler dep + project.scripts entry |
| Modify | `tests/unit/test_storage_duckdb.py` | screen_result CRUD tests |
| Modify | `tests/unit/test_screen_core.py` | whitelist tests |
| Create | `tests/unit/test_presets.py` | Preset registry tests |
| Create | `tests/unit/test_pipeline.py` | Pipeline mock tests |
| Create | `tests/unit/test_cli.py` | CLI argparse tests |
| Modify | `CHANGELOG.md` | v0.4.0 entry |

---

### Task 1: screen_result DDL + CRUD

**Files:**
- Modify: `src/rquant/storage/schema.py`
- Modify: `src/rquant/storage/duckdb.py`
- Modify: `tests/unit/test_storage_duckdb.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/unit/test_storage_duckdb.py`:

```python
class TestScreenResult:
    def test_upsert_and_query(self, store: DuckDBStore) -> None:
        df = pd.DataFrame({
            "trade_date": ["2026-04-18", "2026-04-18"],
            "preset_name": ["n-shape-pool1", "n-shape-pool1"],
            "ts_code": ["000001.SZ", "300001.SZ"],
            "name": ["平安银行", "某科技"],
            "close": [10.5, 25.0],
            "pct_chg": [5.0, 3.2],
            "extra": ['{"CIRC_MV[0]": 50000}', None],
        })
        n = store.upsert_screen_result(df)
        assert n == 2

        result = store.query_screen_result("2026-04-18", "n-shape-pool1")
        assert len(result) == 2
        assert list(result["ts_code"]) == ["000001.SZ", "300001.SZ"]

    def test_upsert_idempotent(self, store: DuckDBStore) -> None:
        df = pd.DataFrame({
            "trade_date": ["2026-04-18"],
            "preset_name": ["pool1"],
            "ts_code": ["000001.SZ"],
            "name": ["平安银行"],
            "close": [10.0],
            "pct_chg": [5.0],
            "extra": [None],
        })
        store.upsert_screen_result(df)
        df2 = df.copy()
        df2["close"] = [11.0]
        store.upsert_screen_result(df2)

        result = store.query_screen_result("2026-04-18", "pool1")
        assert len(result) == 1
        assert result.iloc[0]["close"] == 11.0

    def test_upsert_empty(self, store: DuckDBStore) -> None:
        n = store.upsert_screen_result(pd.DataFrame())
        assert n == 0

    def test_query_not_found(self, store: DuckDBStore) -> None:
        result = store.query_screen_result("2099-01-01", "nonexistent")
        assert result.empty

    def test_table_exists(self, store: DuckDBStore) -> None:
        tables = store.query("SHOW TABLES")
        assert "screen_result" in tables["name"].values
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week4 && uv run pytest tests/unit/test_storage_duckdb.py::TestScreenResult -v`
Expected: FAIL — `TestScreenResult` class not found or `upsert_screen_result` not defined.

- [ ] **Step 3: Add SCREEN_RESULT_DDL to schema.py**

Add after DAILY_BASIC_DDL in `src/rquant/storage/schema.py`:

```python
SCREEN_RESULT_DDL = """
CREATE TABLE IF NOT EXISTS screen_result (
    trade_date    DATE    NOT NULL,
    preset_name   VARCHAR NOT NULL,
    ts_code       VARCHAR NOT NULL,
    name          VARCHAR,
    close         DOUBLE,
    pct_chg       DOUBLE,
    extra         JSON,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, preset_name, ts_code)
);
"""
```

Update `ALL_DDL` to include `SCREEN_RESULT_DDL`:

```python
ALL_DDL = [
    DAILY_BAR_DDL,
    STOCK_BASIC_DDL,
    ADJ_FACTOR_DDL,
    DAILY_INDICATOR_DDL,
    DAILY_STATE_DDL,
    DAILY_BASIC_DDL,
    SCREEN_RESULT_DDL,
]
```

- [ ] **Step 4: Add CRUD methods to duckdb.py**

Add to `src/rquant/storage/duckdb.py` (after `count_daily_basic`):

```python
def upsert_screen_result(self, df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    self._conn.register("screen_result_tmp", df)
    self._conn.execute(
        """
        INSERT OR REPLACE INTO screen_result
        (trade_date, preset_name, ts_code, name, close, pct_chg, extra)
        SELECT trade_date, preset_name, ts_code, name, close, pct_chg, extra
        FROM screen_result_tmp
        """
    )
    self._conn.unregister("screen_result_tmp")

    count = len(df)
    logger.info(f"DuckDB upsert screen_result: {count} 行")
    return count

def query_screen_result(
    self, trade_date: str, preset_name: str
) -> pd.DataFrame:
    return self._conn.execute(
        """
        SELECT ts_code, name, close, pct_chg, extra
        FROM screen_result
        WHERE strftime(trade_date, '%Y-%m-%d') = ?
          AND preset_name = ?
        ORDER BY ts_code
        """,
        [trade_date, preset_name],
    ).fetchdf()
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week4 && uv run pytest tests/unit/test_storage_duckdb.py::TestScreenResult -v`
Expected: 5 PASSED

- [ ] **Step 6: Commit**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week4
git add src/rquant/storage/schema.py src/rquant/storage/duckdb.py tests/unit/test_storage_duckdb.py
git commit -m "feat(storage): add screen_result table with upsert/query"
```

---

### Task 2: screen() ts_code_whitelist support

**Files:**
- Modify: `src/rquant/screen/core.py`
- Modify: `tests/unit/test_screen_core.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/unit/test_screen_core.py`:

```python
class TestWhitelist:
    def test_whitelist_filters_to_subset(self) -> None:
        df = make_wide_frame()
        with patch("rquant.screen.core.load_universe", return_value=df):
            result = screen(
                trade_date="2026-04-15",
                rules=[not_st()],
                ts_code_whitelist=["300001.SZ"],
            )
        assert list(result["ts_code"]) == ["300001.SZ"]

    def test_whitelist_none_returns_all(self) -> None:
        df = make_wide_frame()
        with patch("rquant.screen.core.load_universe", return_value=df):
            result = screen(
                trade_date="2026-04-15",
                rules=[not_st()],
                ts_code_whitelist=None,
            )
        # Default fixture: 000001.SZ is_st=False, 300001.SZ ok, 688001.SH ok
        assert len(result) >= 2

    def test_whitelist_empty_returns_empty(self) -> None:
        df = make_wide_frame()
        with patch("rquant.screen.core.load_universe", return_value=df):
            result = screen(
                trade_date="2026-04-15",
                rules=[not_st()],
                ts_code_whitelist=[],
            )
        assert len(result) == 0
        assert "ts_code" in result.columns

    def test_whitelist_with_nonexistent_code(self) -> None:
        df = make_wide_frame()
        with patch("rquant.screen.core.load_universe", return_value=df):
            result = screen(
                trade_date="2026-04-15",
                rules=[not_st()],
                ts_code_whitelist=["999999.SZ"],
            )
        assert len(result) == 0
```

Add the necessary imports at the top of the test file if not already present:

```python
from tests.fixtures.wide_frames import make_wide_frame
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week4 && uv run pytest tests/unit/test_screen_core.py::TestWhitelist -v`
Expected: FAIL — `screen()` does not accept `ts_code_whitelist`.

- [ ] **Step 3: Add ts_code_whitelist to screen()**

Modify `src/rquant/screen/core.py`. Change the `screen()` signature and add filtering after `load_universe`:

```python
def screen(
    trade_date: str,
    rules: list[Rule],
    lookback: int | None = None,
    include_columns: list[str] | None = None,
    store: DuckDBStore | None = None,
    ts_code_whitelist: list[str] | None = None,
) -> pd.DataFrame:
```

After the `df = load_universe(...)` call and before the `if df.empty:` check, add:

```python
    if ts_code_whitelist is not None:
        df = df[df["ts_code"].isin(ts_code_whitelist)]
```

The existing `if df.empty:` block handles the empty result case.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week4 && uv run pytest tests/unit/test_screen_core.py -v`
Expected: ALL PASSED (old tests + 4 new whitelist tests)

- [ ] **Step 5: Commit**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week4
git add src/rquant/screen/core.py tests/unit/test_screen_core.py
git commit -m "feat(screen): add ts_code_whitelist param for subset filtering"
```

---

### Task 3: ScreenPreset registry + N-shape presets

**Files:**
- Create: `src/rquant/presets.py`
- Create: `tests/unit/test_presets.py`

- [ ] **Step 1: Write failing tests**

Create `tests/unit/test_presets.py`:

```python
"""ScreenPreset 注册表单测。"""

from rquant.presets import PRESET_SCREENS, ScreenPreset


class TestScreenPreset:
    def test_pool1_registered(self) -> None:
        assert "n-shape-pool1" in PRESET_SCREENS

    def test_pool2_registered(self) -> None:
        assert "n-shape-pool2" in PRESET_SCREENS

    def test_pool1_has_11_rules(self) -> None:
        p = PRESET_SCREENS["n-shape-pool1"]
        assert len(p.rules) == 11

    def test_pool1_no_dependency(self) -> None:
        p = PRESET_SCREENS["n-shape-pool1"]
        assert p.depends_on is None

    def test_pool2_depends_on_pool1(self) -> None:
        p = PRESET_SCREENS["n-shape-pool2"]
        assert p.depends_on == "n-shape-pool1"
        assert p.offset_days == 1

    def test_pool2_has_3_rules(self) -> None:
        p = PRESET_SCREENS["n-shape-pool2"]
        assert len(p.rules) == 3

    def test_all_rules_callable(self) -> None:
        for name, preset in PRESET_SCREENS.items():
            for i, rule in enumerate(preset.rules):
                assert callable(rule), f"{name} rule[{i}] not callable"

    def test_pool1_include_columns(self) -> None:
        p = PRESET_SCREENS["n-shape-pool1"]
        assert "CIRC_MV[0]" in p.include_columns
        assert "BODY_UPPER[0]" in p.include_columns

    def test_preset_is_dataclass(self) -> None:
        p = PRESET_SCREENS["n-shape-pool1"]
        assert isinstance(p, ScreenPreset)

    def test_depends_on_target_exists(self) -> None:
        """所有 depends_on 指向的预设必须存在。"""
        for name, preset in PRESET_SCREENS.items():
            if preset.depends_on is not None:
                assert preset.depends_on in PRESET_SCREENS, (
                    f"{name}.depends_on='{preset.depends_on}' not in registry"
                )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week4 && uv run pytest tests/unit/test_presets.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'rquant.presets'`

- [ ] **Step 3: Create presets.py**

Create `src/rquant/presets.py`:

```python
"""筛选预设注册表：每个 ScreenPreset 是一套命名的规则组合。"""

from __future__ import annotations

from dataclasses import dataclass, field

from rquant.screen.rules import (
    Rule,
    circ_mv_lt,
    first_limit_up,
    gt,
    has_lower_shadow,
    has_prior_limit_up,
    lt,
    no_consec_ups_in_window,
    no_limit_down_in_window,
    not_bj,
    not_limit_up,
    not_st,
    not_yiziban,
)


@dataclass
class ScreenPreset:
    """一套命名的筛选策略。"""

    name: str
    description: str
    rules: list[Rule]
    include_columns: list[str] = field(default_factory=list)
    depends_on: str | None = None
    offset_days: int = 0


PRESET_SCREENS: dict[str, ScreenPreset] = {
    "n-shape-pool1": ScreenPreset(
        name="n-shape-pool1",
        description="N形态-Pool1：昨首板+安全过滤+下影线",
        rules=[
            not_st(),
            not_bj(),
            first_limit_up(offset=1),
            not_limit_up(offset=0),
            not_yiziban(offset=1),
            gt("HIGH[0]", "CLOSE[1]"),
            circ_mv_lt(150),
            has_lower_shadow(1.5, 0.02, 0),
            no_consec_ups_in_window(3, 8),
            no_limit_down_in_window(30),
            has_prior_limit_up(90, 1),
        ],
        include_columns=[
            "CIRC_MV[0]",
            "BODY_UPPER[0]",
            "BODY_LOWER[0]",
            "CONSECUTIVE_LIMIT_UPS[1]",
        ],
    ),
    "n-shape-pool2": ScreenPreset(
        name="n-shape-pool2",
        description="N形态-Pool2：Pool1子集T+1实体收缩+下影线",
        depends_on="n-shape-pool1",
        offset_days=1,
        rules=[
            lt("BODY_UPPER[0]", "BODY_UPPER[1]"),
            lt("BODY_LOWER[0]", "BODY_LOWER[1]"),
            has_lower_shadow(1.5, 0.02, 0),
        ],
        include_columns=[
            "BODY_UPPER[0]",
            "BODY_LOWER[0]",
            "BODY_UPPER[1]",
            "BODY_LOWER[1]",
        ],
    ),
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week4 && uv run pytest tests/unit/test_presets.py -v`
Expected: 10 PASSED

- [ ] **Step 5: Commit**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week4
git add src/rquant/presets.py tests/unit/test_presets.py
git commit -m "feat(presets): add ScreenPreset registry with N-shape Pool1/Pool2"
```

---

### Task 4: Pipeline orchestration

**Files:**
- Create: `src/rquant/pipeline.py`
- Create: `tests/unit/test_pipeline.py`

**Context:** The pipeline checks daily_bar data exists for the date, then runs presets in dependency order (no depends_on first, then dependents). For presets with `depends_on`, it queries the parent preset's results from `offset_days` trading days prior, extracts ts_codes as whitelist. Results are converted to screen_result format and upserted.

- [ ] **Step 1: Write failing tests**

Create `tests/unit/test_pipeline.py`:

```python
"""pipeline 流水线单测 —— mock screen() 和 PRESET_SCREENS。"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest

from rquant.pipeline import (
    _get_prev_trading_date,
    _resolve_execution_order,
    _to_screen_result_df,
    run_daily_pipeline,
)
from rquant.presets import ScreenPreset
from rquant.screen.rules import not_st
from rquant.storage.duckdb import DuckDBStore


@pytest.fixture()
def store(tmp_path):
    s = DuckDBStore(tmp_path / "test.duckdb")
    yield s
    s.close()


class TestToScreenResultDf:
    def test_converts_screen_output(self) -> None:
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "name": ["平安银行"],
            "CLOSE[0]": [10.5],
            "PCT_CHG[0]": [5.0],
            "CIRC_MV[0]": [80000.0],
        })
        result = _to_screen_result_df(df, "2026-04-18", "pool1")
        assert len(result) == 1
        assert result.iloc[0]["trade_date"] == "2026-04-18"
        assert result.iloc[0]["preset_name"] == "pool1"
        assert result.iloc[0]["ts_code"] == "000001.SZ"
        assert result.iloc[0]["close"] == 10.5
        assert result.iloc[0]["pct_chg"] == 5.0
        assert "CIRC_MV" in result.iloc[0]["extra"]

    def test_empty_input(self) -> None:
        df = pd.DataFrame(columns=["ts_code", "name", "CLOSE[0]", "PCT_CHG[0]"])
        result = _to_screen_result_df(df, "2026-04-18", "pool1")
        assert result.empty

    def test_no_extra_columns(self) -> None:
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "name": ["平安银行"],
            "CLOSE[0]": [10.5],
            "PCT_CHG[0]": [5.0],
        })
        result = _to_screen_result_df(df, "2026-04-18", "pool1")
        assert result.iloc[0]["extra"] is None


class TestResolveExecutionOrder:
    def test_no_dep_first(self) -> None:
        presets = {
            "child": ScreenPreset(
                name="child", description="", rules=[],
                depends_on="parent", offset_days=1,
            ),
            "parent": ScreenPreset(
                name="parent", description="", rules=[],
            ),
        }
        order = _resolve_execution_order(presets)
        assert order.index("parent") < order.index("child")

    def test_filter_by_names(self) -> None:
        presets = {
            "a": ScreenPreset(name="a", description="", rules=[]),
            "b": ScreenPreset(name="b", description="", rules=[]),
        }
        order = _resolve_execution_order(presets, names=["a"])
        assert order == ["a"]


class TestGetPrevTradingDate:
    def test_returns_previous_date(self, store: DuckDBStore) -> None:
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('X', '2026-04-17', 1,1,1,1,1,0,0,0,0),"
            "('X', '2026-04-18', 1,1,1,1,1,0,0,0,0)"
        )
        assert _get_prev_trading_date(store, "2026-04-18", 1) == "2026-04-17"

    def test_returns_none_when_no_data(self, store: DuckDBStore) -> None:
        assert _get_prev_trading_date(store, "2026-04-18", 1) is None

    def test_offset_2(self, store: DuckDBStore) -> None:
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('X', '2026-04-16', 1,1,1,1,1,0,0,0,0),"
            "('X', '2026-04-17', 1,1,1,1,1,0,0,0,0),"
            "('X', '2026-04-18', 1,1,1,1,1,0,0,0,0)"
        )
        assert _get_prev_trading_date(store, "2026-04-18", 2) == "2026-04-16"


class TestRunDailyPipeline:
    def test_skips_non_trading_day(self, store: DuckDBStore) -> None:
        result = run_daily_pipeline("2026-04-20", store=store)
        assert result == {}

    def test_runs_preset_and_persists(self, store: DuckDBStore) -> None:
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('000001.SZ', '2026-04-18', 10,11,9,10.5,10,0.5,5,1000,10000)"
        )
        mock_df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "name": ["平安银行"],
            "CLOSE[0]": [10.5],
            "PCT_CHG[0]": [5.0],
        })
        test_presets = {
            "test-pool": ScreenPreset(
                name="test-pool", description="test", rules=[not_st()],
            ),
        }
        with (
            patch("rquant.pipeline.PRESET_SCREENS", test_presets),
            patch("rquant.pipeline.screen", return_value=mock_df),
        ):
            result = run_daily_pipeline("2026-04-18", store=store)
        assert result == {"test-pool": 1}
        sr = store.query_screen_result("2026-04-18", "test-pool")
        assert len(sr) == 1
        assert sr.iloc[0]["ts_code"] == "000001.SZ"

    def test_specific_preset_only(self, store: DuckDBStore) -> None:
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('000001.SZ', '2026-04-18', 10,11,9,10.5,10,0.5,5,1000,10000)"
        )
        mock_df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "name": ["平安银行"],
            "CLOSE[0]": [10.5],
            "PCT_CHG[0]": [5.0],
        })
        test_presets = {
            "a": ScreenPreset(name="a", description="", rules=[not_st()]),
            "b": ScreenPreset(name="b", description="", rules=[not_st()]),
        }
        with (
            patch("rquant.pipeline.PRESET_SCREENS", test_presets),
            patch("rquant.pipeline.screen", return_value=mock_df),
        ):
            result = run_daily_pipeline(
                "2026-04-18", preset_names=["a"], store=store
            )
        assert "a" in result
        assert "b" not in result

    def test_child_uses_parent_whitelist(self, store: DuckDBStore) -> None:
        # T-1 and T data
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('000001.SZ', '2026-04-17', 1,1,1,1,1,0,0,0,0),"
            "('000001.SZ', '2026-04-18', 1,1,1,1,1,0,0,0,0),"
            "('300001.SZ', '2026-04-18', 1,1,1,1,1,0,0,0,0)"
        )
        # Parent results on T-1
        parent_sr = pd.DataFrame({
            "trade_date": ["2026-04-17"],
            "preset_name": ["parent"],
            "ts_code": ["000001.SZ"],
            "name": ["平安银行"],
            "close": [10.0],
            "pct_chg": [5.0],
            "extra": [None],
        })
        store.upsert_screen_result(parent_sr)

        child_preset = ScreenPreset(
            name="child", description="", rules=[not_st()],
            depends_on="parent", offset_days=1,
        )
        empty_df = pd.DataFrame(
            columns=["ts_code", "name", "CLOSE[0]", "PCT_CHG[0]"]
        )
        with (
            patch("rquant.pipeline.PRESET_SCREENS", {"child": child_preset}),
            patch("rquant.pipeline.screen", return_value=empty_df) as mock_scr,
        ):
            run_daily_pipeline("2026-04-18", store=store)
            kw = mock_scr.call_args.kwargs
            assert kw["ts_code_whitelist"] == ["000001.SZ"]

    def test_child_skips_when_parent_empty(self, store: DuckDBStore) -> None:
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('000001.SZ', '2026-04-17', 1,1,1,1,1,0,0,0,0),"
            "('000001.SZ', '2026-04-18', 1,1,1,1,1,0,0,0,0)"
        )
        child_preset = ScreenPreset(
            name="child", description="", rules=[not_st()],
            depends_on="parent", offset_days=1,
        )
        with (
            patch("rquant.pipeline.PRESET_SCREENS", {"child": child_preset}),
            patch("rquant.pipeline.screen") as mock_scr,
        ):
            result = run_daily_pipeline("2026-04-18", store=store)
            mock_scr.assert_not_called()
            assert result == {"child": 0}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week4 && uv run pytest tests/unit/test_pipeline.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'rquant.pipeline'`

- [ ] **Step 3: Create pipeline.py**

Create `src/rquant/pipeline.py`:

```python
"""每日全流水线：检查数据 → 遍历预设 → 落库结果。"""

from __future__ import annotations

import json

import pandas as pd
from loguru import logger

from rquant.presets import PRESET_SCREENS, ScreenPreset
from rquant.screen.core import screen
from rquant.storage.duckdb import DuckDBStore


def _get_prev_trading_date(
    store: DuckDBStore, trade_date: str, n: int = 1
) -> str | None:
    """trade_date 前第 n 个交易日（n=1 = 前一天）。"""
    row = store._conn.execute(
        """
        SELECT strftime(trade_date, '%Y-%m-%d') AS d
        FROM (
            SELECT DISTINCT trade_date FROM daily_bar
            WHERE trade_date < ?
            ORDER BY trade_date DESC
            LIMIT 1 OFFSET ?
        )
        """,
        [trade_date, n - 1],
    ).fetchone()
    return row[0] if row else None


def _to_screen_result_df(
    screen_df: pd.DataFrame,
    trade_date: str,
    preset_name: str,
) -> pd.DataFrame:
    """将 screen() 返回的 DataFrame 转为 screen_result 表格式。"""
    if screen_df.empty:
        return pd.DataFrame(
            columns=[
                "trade_date", "preset_name", "ts_code",
                "name", "close", "pct_chg", "extra",
            ]
        )

    base = {"ts_code", "name", "CLOSE[0]", "PCT_CHG[0]"}
    extra_cols = [c for c in screen_df.columns if c not in base]

    result = pd.DataFrame({
        "trade_date": trade_date,
        "preset_name": preset_name,
        "ts_code": screen_df["ts_code"].values,
        "name": screen_df.get("name"),
        "close": screen_df.get("CLOSE[0]"),
        "pct_chg": screen_df.get("PCT_CHG[0]"),
    })

    if extra_cols:
        result["extra"] = screen_df[extra_cols].apply(
            lambda row: json.dumps(
                {k: v for k, v in row.items() if pd.notna(v)},
                ensure_ascii=False,
            ),
            axis=1,
        )
    else:
        result["extra"] = None

    return result


def _resolve_execution_order(
    presets: dict[str, ScreenPreset],
    names: list[str] | None = None,
) -> list[str]:
    """按依赖拓扑排序：无 depends_on 的先跑。"""
    if names:
        selected = {n: presets[n] for n in names if n in presets}
    else:
        selected = presets

    no_dep = [n for n, p in selected.items() if p.depends_on is None]
    has_dep = [n for n, p in selected.items() if p.depends_on is not None]
    return no_dep + has_dep


def run_daily_pipeline(
    trade_date: str,
    preset_names: list[str] | None = None,
    store: DuckDBStore | None = None,
) -> dict[str, int]:
    """遍历预设筛选并落库，返回 {preset_name: 命中数}。

    前置条件：trade_date 的 daily_bar / daily_indicator / daily_state
    数据已通过 ingest_daily.py 入库。
    """
    owns_store = store is None
    store = store or DuckDBStore()

    try:
        # 检查是否有数据
        count = store._conn.execute(
            "SELECT COUNT(*) FROM daily_bar WHERE trade_date = ?",
            [trade_date],
        ).fetchone()[0]
        if count == 0:
            logger.warning(f"{trade_date} 无 daily_bar 数据，跳过")
            return {}

        order = _resolve_execution_order(PRESET_SCREENS, preset_names)
        summary: dict[str, int] = {}

        for name in order:
            preset = PRESET_SCREENS[name]
            ts_whitelist: list[str] | None = None

            if preset.depends_on:
                parent_date = _get_prev_trading_date(
                    store, trade_date, preset.offset_days
                )
                if parent_date is None:
                    logger.warning(
                        f"{name}: 找不到 {preset.offset_days} "
                        f"个交易日前的日期，跳过"
                    )
                    summary[name] = 0
                    continue

                parent_df = store.query_screen_result(
                    parent_date, preset.depends_on
                )
                ts_whitelist = (
                    parent_df["ts_code"].tolist()
                    if not parent_df.empty
                    else []
                )
                if not ts_whitelist:
                    logger.info(
                        f"{name}: 父预设 {preset.depends_on} "
                        f"在 {parent_date} 无命中，跳过"
                    )
                    summary[name] = 0
                    continue

            result_df = screen(
                trade_date=trade_date,
                rules=preset.rules,
                include_columns=preset.include_columns or None,
                store=store,
                ts_code_whitelist=ts_whitelist,
            )

            sr_df = _to_screen_result_df(result_df, trade_date, name)
            store.upsert_screen_result(sr_df)

            hit_count = len(result_df)
            summary[name] = hit_count
            logger.info(f"  {name}: {hit_count} 命中")

        logger.info(f"流水线完成 {trade_date}: {summary}")
        return summary
    finally:
        if owns_store:
            store.close()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week4 && uv run pytest tests/unit/test_pipeline.py -v`
Expected: ALL PASSED

- [ ] **Step 5: Commit**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week4
git add src/rquant/pipeline.py tests/unit/test_pipeline.py
git commit -m "feat(pipeline): add run_daily_pipeline with preset dependency chain"
```

---

### Task 5: CLI + APScheduler

**Files:**
- Create: `src/rquant/cli.py`
- Create: `tests/unit/test_cli.py`
- Modify: `pyproject.toml`

- [ ] **Step 1: Add apscheduler dependency**

In `pyproject.toml`, add `"apscheduler>=3.10,<4"` to the `dependencies` list.

Also add the `[project.scripts]` section after `[build-system]` (before `[tool.ruff]`):

```toml
[project.scripts]
rquant = "rquant.cli:main"
```

Then install: `cd /Users/roxor/brain/30-projects/rQuant-week4 && uv sync`

- [ ] **Step 2: Write failing tests**

Create `tests/unit/test_cli.py`:

```python
"""CLI 入口单测 —— 仅验证 argparse 解析，不启动调度器。"""

from __future__ import annotations

import subprocess
import sys

from rquant.cli import build_parser


class TestBuildParser:
    def test_serve_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["serve"])
        assert args.command == "serve"
        assert args.hour == 17

    def test_serve_custom_hour(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["serve", "--hour", "16"])
        assert args.hour == 16

    def test_run_daily_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["run-daily"])
        assert args.command == "run-daily"
        assert args.date is None
        assert args.preset is None

    def test_run_daily_with_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "run-daily", "--date", "2026-04-18", "--preset", "n-shape-pool1"
        ])
        assert args.date == "2026-04-18"
        assert args.preset == "n-shape-pool1"

    def test_no_command_returns_none(self) -> None:
        parser = build_parser()
        args = parser.parse_args([])
        assert args.command is None


class TestCLISmoke:
    def test_help_exits_0(self) -> None:
        result = subprocess.run(
            [sys.executable, "-m", "rquant.cli", "--help"],
            capture_output=True, text=True,
        )
        assert result.returncode == 0
        assert "rquant" in result.stdout

    def test_run_daily_help(self) -> None:
        result = subprocess.run(
            [sys.executable, "-m", "rquant.cli", "run-daily", "--help"],
            capture_output=True, text=True,
        )
        assert result.returncode == 0
        assert "--date" in result.stdout
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week4 && uv run pytest tests/unit/test_cli.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'rquant.cli'`

- [ ] **Step 4: Create cli.py**

Create `src/rquant/cli.py`:

```python
"""CLI 入口：rquant serve / rquant run-daily。"""

from __future__ import annotations

import argparse
import signal
import sys
from datetime import date

from loguru import logger

from rquant.logging import setup_logging


def cmd_serve(args: argparse.Namespace) -> int:
    """启动 APScheduler 常驻进程。"""
    from apscheduler.schedulers.blocking import BlockingScheduler

    from rquant.pipeline import run_daily_pipeline

    setup_logging()
    scheduler = BlockingScheduler()

    @scheduler.scheduled_job(
        "cron", hour=args.hour, minute=0, day_of_week="mon-fri"
    )
    def daily_job() -> None:
        run_daily_pipeline(date.today().isoformat())

    def handle_signal(signum: int, frame: object) -> None:
        logger.info("收到退出信号，正在关闭调度器...")
        scheduler.shutdown(wait=False)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    logger.info(f"调度器启动，每日 {args.hour}:00 (Mon-Fri) 执行")
    scheduler.start()
    return 0


def cmd_run_daily(args: argparse.Namespace) -> int:
    """一次性执行全流水线。"""
    from rquant.pipeline import run_daily_pipeline

    setup_logging()
    trade_date = args.date or date.today().isoformat()
    preset_names = [args.preset] if args.preset else None

    logger.info(f"手动执行流水线: {trade_date}")
    summary = run_daily_pipeline(trade_date, preset_names=preset_names)

    if not summary:
        logger.warning("无结果（非交易日或无数据）")
        return 1

    for name, count in summary.items():
        logger.info(f"  {name}: {count} 命中")
    return 0


def build_parser() -> argparse.ArgumentParser:
    """构建 CLI 参数解析器。"""
    parser = argparse.ArgumentParser(
        prog="rquant", description="rQuant 量化选股平台"
    )
    sub = parser.add_subparsers(dest="command")

    serve_p = sub.add_parser("serve", help="启动 APScheduler 常驻进程")
    serve_p.add_argument(
        "--hour", type=int, default=17, help="每日触发小时 (默认 17)"
    )

    run_p = sub.add_parser("run-daily", help="一次性执行全流水线")
    run_p.add_argument(
        "--date", type=str, default=None,
        help="交易日期 YYYY-MM-DD (默认今天)",
    )
    run_p.add_argument(
        "--preset", type=str, default=None,
        help="只跑指定预设 (默认全部)",
    )

    return parser


def main() -> int:
    """CLI 入口函数。"""
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "serve":
        return cmd_serve(args)
    elif args.command == "run-daily":
        return cmd_run_daily(args)
    else:
        parser.print_help()
        return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week4 && uv run pytest tests/unit/test_cli.py -v`
Expected: ALL PASSED

- [ ] **Step 6: Commit**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week4
git add src/rquant/cli.py tests/unit/test_cli.py pyproject.toml
git commit -m "feat(cli): add rquant serve + run-daily subcommands with APScheduler"
```

---

### Task 6: Lint + CHANGELOG + full test suite

**Files:**
- Modify: `CHANGELOG.md`
- Possibly fix lint in new files

- [ ] **Step 1: Run full test suite**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week4 && uv run pytest -v`
Expected: ALL PASSED (162 existing + ~30 new ≈ 190+)

- [ ] **Step 2: Run lint and fix issues**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week4 && uv run ruff check src/ tests/`

Fix any errors in files we created/modified. Do NOT fix pre-existing errors in untouched files (status.py, logging.py have known E501/SIM).

- [ ] **Step 3: Update CHANGELOG.md**

Add `[v0.4.0]` section at the top of the changelog (before `[v0.3.1]`):

```markdown
## [v0.4.0] - 2026-04-20

### Added
- **CLI**: `rquant serve` (APScheduler cron, Mon-Fri 17:00) and `rquant run-daily --date --preset` subcommands
- **screen_result**: DuckDB table for persisting screen hits (trade_date + preset_name + ts_code, with JSON extra column)
- **ScreenPreset**: Python dataclass registry for named screening strategies with dependency chains
- **N-shape presets**: Pool 1 (11 rules, full market) and Pool 2 (3 rules, depends on Pool 1 T-1 results)
- **Pipeline**: `run_daily_pipeline()` orchestrates presets in dependency order, handles parent→child whitelist filtering
- **screen() whitelist**: New `ts_code_whitelist` parameter for subset screening

### Changed
- `pyproject.toml`: Added `apscheduler>=3.10` dependency and `[project.scripts]` entry point
```

- [ ] **Step 4: Run full tests one more time**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week4 && uv run pytest -v`
Expected: ALL PASSED

- [ ] **Step 5: Commit**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week4
git add CHANGELOG.md
git add -u  # any lint fixes
git commit -m "chore: CHANGELOG v0.4.0 + lint fixes"
```
