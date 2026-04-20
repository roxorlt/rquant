# Week 5a: 盘中监控 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Real-time intraday price monitoring for Pool 1/Pool 2 stocks with macOS alert notifications and persistent Pool 2 lifecycle management.

**Architecture:** Standalone `rquant monitor` process polls akshare every 5 seconds during trading hours (09:30-15:00). Checks prices against 5 levels derived from limit-up day's candle body. Events stored in DuckDB + macOS modal alerts via osascript. Pool 2 upgraded from daily snapshot to persistent watchlist with entry/exit lifecycle managed across pipeline (entry) and monitor (exit).

**Tech Stack:** akshare (real-time quotes + trading calendar), DuckDB (storage), osascript (macOS alerts), loguru (logging)

**Spec:** `docs/superpowers/specs/2026-04-21-week5a-monitor-design.md`

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `src/rquant/storage/schema.py` | Modify | Add `POOL2_WATCH_DDL` + `MONITOR_EVENT_DDL` |
| `src/rquant/storage/duckdb.py` | Modify | Add CRUD methods for both new tables |
| `src/rquant/pipeline.py` | Modify | Add pool2_watch sync after Pool 2 screening |
| `src/rquant/monitor.py` | Create | All monitoring logic: watchlist, polling, alerts, exit checks |
| `src/rquant/cli.py` | Modify | Add `monitor` and `pool2` subcommands |
| `pyproject.toml` | Modify | Add `akshare` dependency |
| `deploy/com.roxor.rquant-monitor.plist` | Create | launchd auto-start config |
| `tests/unit/test_storage_pool2.py` | Create | Storage CRUD tests for new tables |
| `tests/unit/test_pipeline_sync.py` | Create | Pipeline pool2_watch sync tests |
| `tests/unit/test_monitor.py` | Create | Monitor logic tests |

---

### Task 1: Schema + Storage — pool2_watch and monitor_event

**Files:**
- Modify: `src/rquant/storage/schema.py:109-113`
- Modify: `src/rquant/storage/duckdb.py:229-243`
- Create: `tests/unit/test_storage_pool2.py`

- [ ] **Step 1: Write failing tests for pool2_watch and monitor_event storage**

```python
# tests/unit/test_storage_pool2.py
"""pool2_watch + monitor_event 存储层测试。"""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd
import pytest

from rquant.storage.duckdb import DuckDBStore


@pytest.fixture()
def store(tmp_path):
    s = DuckDBStore(tmp_path / "test.duckdb")
    yield s
    s.close()


class TestPool2Watch:
    def test_upsert_and_query_active(self, store: DuckDBStore) -> None:
        df = pd.DataFrame([{
            "ts_code": "002415.SZ",
            "entry_date": date(2026, 4, 18),
            "limit_up_date": date(2026, 4, 17),
            "body_upper": 13.20,
            "body_lower": 11.80,
            "level_40": 12.36,
            "level_30": 12.22,
            "level_20": 12.08,
            "stop_strong": 11.80,
            "stop_weak": 11.52,
            "status": "active",
        }])
        count = store.upsert_pool2_watch(df)
        assert count == 1

        active = store.query_pool2_active()
        assert len(active) == 1
        assert active.iloc[0]["ts_code"] == "002415.SZ"
        assert active.iloc[0]["level_40"] == 12.36

    def test_exited_not_in_active(self, store: DuckDBStore) -> None:
        df = pd.DataFrame([{
            "ts_code": "002415.SZ",
            "entry_date": date(2026, 4, 18),
            "limit_up_date": date(2026, 4, 17),
            "body_upper": 13.20, "body_lower": 11.80,
            "level_40": 12.36, "level_30": 12.22, "level_20": 12.08,
            "stop_strong": 11.80, "stop_weak": 11.52,
            "status": "active",
        }])
        store.upsert_pool2_watch(df)
        store.update_pool2_exit("002415.SZ", date(2026, 4, 21), "expired")

        active = store.query_pool2_active()
        assert len(active) == 0

    def test_update_exit(self, store: DuckDBStore) -> None:
        df = pd.DataFrame([{
            "ts_code": "002415.SZ",
            "entry_date": date(2026, 4, 18),
            "limit_up_date": date(2026, 4, 17),
            "body_upper": 13.20, "body_lower": 11.80,
            "level_40": 12.36, "level_30": 12.22, "level_20": 12.08,
            "stop_strong": 11.80, "stop_weak": 11.52,
            "status": "active",
        }])
        store.upsert_pool2_watch(df)
        store.update_pool2_exit("002415.SZ", date(2026, 4, 21), "breakdown")

        all_rows = store.query("SELECT * FROM pool2_watch")
        assert all_rows.iloc[0]["status"] == "exited"
        assert all_rows.iloc[0]["exit_reason"] == "breakdown"

    def test_remove(self, store: DuckDBStore) -> None:
        df = pd.DataFrame([{
            "ts_code": "002415.SZ",
            "entry_date": date(2026, 4, 18),
            "limit_up_date": date(2026, 4, 17),
            "body_upper": 13.20, "body_lower": 11.80,
            "level_40": 12.36, "level_30": 12.22, "level_20": 12.08,
            "stop_strong": 11.80, "stop_weak": 11.52,
            "status": "active",
        }])
        store.upsert_pool2_watch(df)
        store.remove_pool2("002415.SZ")
        assert len(store.query("SELECT * FROM pool2_watch")) == 0

    def test_reentry_after_exit(self, store: DuckDBStore) -> None:
        df = pd.DataFrame([{
            "ts_code": "002415.SZ",
            "entry_date": date(2026, 4, 18),
            "limit_up_date": date(2026, 4, 17),
            "body_upper": 13.20, "body_lower": 11.80,
            "level_40": 12.36, "level_30": 12.22, "level_20": 12.08,
            "stop_strong": 11.80, "stop_weak": 11.52,
            "status": "active",
        }])
        store.upsert_pool2_watch(df)
        store.update_pool2_exit("002415.SZ", date(2026, 4, 21), "expired")

        # Re-enter with new data
        df2 = df.copy()
        df2["entry_date"] = date(2026, 4, 22)
        df2["status"] = "active"
        store.upsert_pool2_watch(df2)

        active = store.query_pool2_active()
        assert len(active) == 1
        assert active.iloc[0]["status"] == "active"


class TestMonitorEvent:
    def test_upsert_and_query(self, store: DuckDBStore) -> None:
        df = pd.DataFrame([{
            "trade_date": date(2026, 4, 21),
            "ts_code": "002415.SZ",
            "level": "30",
            "trigger_price": 12.35,
            "level_price": 12.22,
            "trigger_time": datetime(2026, 4, 21, 10, 23, 15),
            "trigger_type": "realtime",
            "pool": "pool2",
            "body_upper": 13.20,
            "body_lower": 11.80,
        }])
        count = store.upsert_monitor_event(df)
        assert count == 1

        events = store.query_monitor_events("2026-04-21")
        assert len(events) == 1
        assert events.iloc[0]["level"] == "30"

    def test_dedup_same_level(self, store: DuckDBStore) -> None:
        row = {
            "trade_date": date(2026, 4, 21),
            "ts_code": "002415.SZ",
            "level": "30",
            "trigger_price": 12.35,
            "level_price": 12.22,
            "trigger_time": datetime(2026, 4, 21, 10, 23, 15),
            "trigger_type": "realtime",
            "pool": "pool2",
            "body_upper": 13.20,
            "body_lower": 11.80,
        }
        store.upsert_monitor_event(pd.DataFrame([row]))
        row["trigger_price"] = 12.30
        store.upsert_monitor_event(pd.DataFrame([row]))

        events = store.query_monitor_events("2026-04-21")
        assert len(events) == 1

    def test_query_events_for_stock(self, store: DuckDBStore) -> None:
        rows = [
            {
                "trade_date": date(2026, 4, 21), "ts_code": "002415.SZ",
                "level": "40", "trigger_price": 12.50, "level_price": 12.36,
                "trigger_time": datetime(2026, 4, 21, 10, 0),
                "trigger_type": "realtime", "pool": "pool2",
                "body_upper": 13.20, "body_lower": 11.80,
            },
            {
                "trade_date": date(2026, 4, 21), "ts_code": "002415.SZ",
                "level": "30", "trigger_price": 12.20, "level_price": 12.22,
                "trigger_time": datetime(2026, 4, 21, 10, 30),
                "trigger_type": "realtime", "pool": "pool2",
                "body_upper": 13.20, "body_lower": 11.80,
            },
        ]
        store.upsert_monitor_event(pd.DataFrame(rows))
        events = store.query_monitor_events("2026-04-21", ts_code="002415.SZ")
        assert len(events) == 2
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week5a && uv run pytest tests/unit/test_storage_pool2.py -v`
Expected: FAIL — `AttributeError: 'DuckDBStore' object has no attribute 'upsert_pool2_watch'`

- [ ] **Step 3: Add DDL definitions to schema.py**

Add after `SCREEN_RESULT_DDL` (line 107) in `src/rquant/storage/schema.py`:

```python
POOL2_WATCH_DDL = """
CREATE TABLE IF NOT EXISTS pool2_watch (
    ts_code       VARCHAR   PRIMARY KEY,
    entry_date    DATE      NOT NULL,
    limit_up_date DATE      NOT NULL,
    body_upper    DOUBLE    NOT NULL,
    body_lower    DOUBLE    NOT NULL,
    level_40      DOUBLE    NOT NULL,
    level_30      DOUBLE    NOT NULL,
    level_20      DOUBLE    NOT NULL,
    stop_strong   DOUBLE    NOT NULL,
    stop_weak     DOUBLE    NOT NULL,
    status        VARCHAR   DEFAULT 'active',
    exit_date     DATE,
    exit_reason   VARCHAR,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

MONITOR_EVENT_DDL = """
CREATE TABLE IF NOT EXISTS monitor_event (
    trade_date    DATE      NOT NULL,
    ts_code       VARCHAR   NOT NULL,
    level         VARCHAR   NOT NULL,
    trigger_price DOUBLE,
    level_price   DOUBLE,
    trigger_time  TIMESTAMP NOT NULL,
    trigger_type  VARCHAR,
    pool          VARCHAR,
    body_upper    DOUBLE,
    body_lower    DOUBLE,
    PRIMARY KEY (trade_date, ts_code, level)
);
"""
```

Update `ALL_DDL` (line 109-113) to include both new DDLs:

```python
ALL_DDL = [
    DAILY_BAR_DDL, STOCK_BASIC_DDL, ADJ_FACTOR_DDL,
    DAILY_INDICATOR_DDL, DAILY_STATE_DDL, DAILY_BASIC_DDL,
    SCREEN_RESULT_DDL, POOL2_WATCH_DDL, MONITOR_EVENT_DDL,
]
```

- [ ] **Step 4: Add storage methods to duckdb.py**

Add after `query_screen_result` method (line 243) in `src/rquant/storage/duckdb.py`:

```python
    # ── pool2_watch ──

    def upsert_pool2_watch(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        self._conn.register("p2w_tmp", df)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO pool2_watch
            (ts_code, entry_date, limit_up_date,
             body_upper, body_lower,
             level_40, level_30, level_20,
             stop_strong, stop_weak, status)
            SELECT ts_code, entry_date, limit_up_date,
                   body_upper, body_lower,
                   level_40, level_30, level_20,
                   stop_strong, stop_weak, status
            FROM p2w_tmp
            """
        )
        self._conn.unregister("p2w_tmp")
        count = len(df)
        logger.info(f"DuckDB upsert pool2_watch: {count} 行")
        return count

    def query_pool2_active(self) -> pd.DataFrame:
        return self._conn.execute(
            """
            SELECT ts_code, entry_date, limit_up_date,
                   body_upper, body_lower,
                   level_40, level_30, level_20,
                   stop_strong, stop_weak
            FROM pool2_watch
            WHERE status = 'active'
            ORDER BY entry_date DESC
            """
        ).fetchdf()

    def update_pool2_exit(
        self, ts_code: str, exit_date: date, exit_reason: str
    ) -> None:
        self._conn.execute(
            """
            UPDATE pool2_watch
            SET status = 'exited', exit_date = ?, exit_reason = ?
            WHERE ts_code = ?
            """,
            [exit_date, exit_reason, ts_code],
        )

    def remove_pool2(self, ts_code: str) -> None:
        self._conn.execute(
            "DELETE FROM pool2_watch WHERE ts_code = ?", [ts_code]
        )

    def query_pool2_all(self) -> pd.DataFrame:
        return self._conn.execute(
            """
            SELECT ts_code, entry_date, limit_up_date,
                   body_upper, body_lower,
                   level_40, level_30, level_20,
                   stop_strong, stop_weak,
                   status, exit_date, exit_reason
            FROM pool2_watch
            ORDER BY status, entry_date DESC
            """
        ).fetchdf()

    # ── monitor_event ──

    def upsert_monitor_event(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        self._conn.register("mev_tmp", df)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO monitor_event
            (trade_date, ts_code, level, trigger_price, level_price,
             trigger_time, trigger_type, pool, body_upper, body_lower)
            SELECT trade_date, ts_code, level, trigger_price, level_price,
                   trigger_time, trigger_type, pool, body_upper, body_lower
            FROM mev_tmp
            """
        )
        self._conn.unregister("mev_tmp")
        count = len(df)
        logger.info(f"DuckDB upsert monitor_event: {count} 行")
        return count

    def query_monitor_events(
        self, trade_date: str, ts_code: str | None = None
    ) -> pd.DataFrame:
        if ts_code:
            return self._conn.execute(
                """
                SELECT * FROM monitor_event
                WHERE strftime(trade_date, '%Y-%m-%d') = ?
                  AND ts_code = ?
                ORDER BY trigger_time
                """,
                [trade_date, ts_code],
            ).fetchdf()
        return self._conn.execute(
            """
            SELECT * FROM monitor_event
            WHERE strftime(trade_date, '%Y-%m-%d') = ?
            ORDER BY trigger_time
            """,
            [trade_date],
        ).fetchdf()
```

Note: import `date` from `datetime` at the top of `duckdb.py` (line 1-3 area):

```python
from datetime import date
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week5a && uv run pytest tests/unit/test_storage_pool2.py -v`
Expected: all 8 tests PASS

- [ ] **Step 6: Run full test suite to check for regressions**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week5a && uv run pytest tests/unit/ -v`
Expected: all existing tests PASS (201+)

- [ ] **Step 7: Commit**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week5a
git add src/rquant/storage/schema.py src/rquant/storage/duckdb.py tests/unit/test_storage_pool2.py
git commit -m "feat(storage): add pool2_watch and monitor_event tables with CRUD methods"
```

---

### Task 2: Pipeline pool2_watch sync

**Files:**
- Modify: `src/rquant/pipeline.py:88-169`
- Create: `tests/unit/test_pipeline_sync.py`

**Context:** After the daily pipeline finishes Pool 2 screening, new Pool 2 results need to be synced to the persistent `pool2_watch` table. For each new stock, we find the limit-up day from `daily_state` and compute 5 price levels.

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/test_pipeline_sync.py
"""pipeline pool2_watch 同步测试。"""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

import pandas as pd
import pytest

from rquant.pipeline import (
    _compute_levels,
    _sync_pool2_watch,
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


class TestComputeLevels:
    def test_basic_computation(self) -> None:
        levels = _compute_levels(body_upper=13.20, body_lower=11.80)
        body = 13.20 - 11.80  # 1.40
        assert levels["level_40"] == pytest.approx(11.80 + body * 0.4)
        assert levels["level_30"] == pytest.approx(11.80 + body * 0.3)
        assert levels["level_20"] == pytest.approx(11.80 + body * 0.2)
        assert levels["stop_strong"] == pytest.approx(11.80)
        assert levels["stop_weak"] == pytest.approx(11.80 - body * 0.2)

    def test_yiziban_zero_body(self) -> None:
        """一字板 body=0 时所有档位 = body_lower。"""
        levels = _compute_levels(body_upper=10.0, body_lower=10.0)
        assert levels["level_40"] == 10.0
        assert levels["stop_weak"] == 10.0


class TestSyncPool2Watch:
    def test_new_stock_added(self, store: DuckDBStore) -> None:
        # Pool 2 screen_result
        sr = pd.DataFrame([{
            "trade_date": "2026-04-21",
            "preset_name": "n-shape-pool2",
            "ts_code": "002415.SZ",
            "name": "海康威视",
            "close": 12.50,
            "pct_chg": -2.0,
            "extra": None,
        }])
        store.upsert_screen_result(sr)

        # daily_state: limit-up day's body
        store._conn.execute(
            """
            INSERT INTO daily_state VALUES
            ('002415.SZ', '2026-04-18', false, false, 'main', 0.10,
             13.20, 10.80, true, false, true, false, 1, 13.20, 11.80),
            ('002415.SZ', '2026-04-19', false, false, 'main', 0.10,
             13.20, 10.80, false, false, false, false, 0, 12.50, 12.00)
            """
        )

        # daily_bar for prev trading date lookup
        store._conn.execute(
            """
            INSERT INTO daily_bar VALUES
            ('002415.SZ', '2026-04-18', 12,13,11,12,11,1,5,1000,10000),
            ('002415.SZ', '2026-04-19', 12,13,11,12,11,1,5,1000,10000),
            ('002415.SZ', '2026-04-20', 12,13,11,12,11,1,5,1000,10000),
            ('002415.SZ', '2026-04-21', 12,13,11,12.5,12,0.5,5,1000,10000)
            """
        )

        _sync_pool2_watch(store, "2026-04-21")

        active = store.query_pool2_active()
        assert len(active) == 1
        assert active.iloc[0]["ts_code"] == "002415.SZ"
        assert active.iloc[0]["body_upper"] == 13.20
        assert active.iloc[0]["body_lower"] == 11.80

    def test_existing_active_not_duplicated(self, store: DuckDBStore) -> None:
        # Already in pool2_watch
        existing = pd.DataFrame([{
            "ts_code": "002415.SZ",
            "entry_date": date(2026, 4, 20),
            "limit_up_date": date(2026, 4, 18),
            "body_upper": 13.20, "body_lower": 11.80,
            "level_40": 12.36, "level_30": 12.22, "level_20": 12.08,
            "stop_strong": 11.80, "stop_weak": 11.52,
            "status": "active",
        }])
        store.upsert_pool2_watch(existing)

        # Same stock in today's Pool 2
        sr = pd.DataFrame([{
            "trade_date": "2026-04-21",
            "preset_name": "n-shape-pool2",
            "ts_code": "002415.SZ",
            "name": "海康威视",
            "close": 12.50, "pct_chg": -2.0, "extra": None,
        }])
        store.upsert_screen_result(sr)

        _sync_pool2_watch(store, "2026-04-21")

        active = store.query_pool2_active()
        assert len(active) == 1
        # entry_date stays original
        assert str(active.iloc[0]["entry_date"]) == "2026-04-20"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week5a && uv run pytest tests/unit/test_pipeline_sync.py -v`
Expected: FAIL — `ImportError: cannot import name '_compute_levels'`

- [ ] **Step 3: Implement _compute_levels and _sync_pool2_watch in pipeline.py**

Add after `_resolve_execution_order` (line 85) in `src/rquant/pipeline.py`:

```python
def _compute_levels(body_upper: float, body_lower: float) -> dict[str, float]:
    """根据涨停日实体算 5 个档位价。"""
    body = body_upper - body_lower
    return {
        "level_40": body_lower + body * 0.4,
        "level_30": body_lower + body * 0.3,
        "level_20": body_lower + body * 0.2,
        "stop_strong": body_lower,
        "stop_weak": body_lower - body * 0.2,
    }


def _sync_pool2_watch(store: DuckDBStore, trade_date: str) -> None:
    """将今日 Pool 2 screen_result 同步到 pool2_watch 持久池。

    只添加新票（pool2_watch 中不存在或已 exited 的重新激活）。
    """
    pool2_sr = store.query_screen_result(trade_date, "n-shape-pool2")
    if pool2_sr.empty:
        return

    existing = store.query_pool2_active()
    existing_codes = set(existing["ts_code"].tolist()) if not existing.empty else set()

    new_rows = []
    for _, row in pool2_sr.iterrows():
        code = row["ts_code"]
        if code in existing_codes:
            continue

        # 找涨停日：最近 5 个交易日内 is_first_limit_up=True
        state_df = store._conn.execute(
            """
            SELECT trade_date, body_upper, body_lower
            FROM daily_state
            WHERE ts_code = ? AND is_first_limit_up = true
            ORDER BY trade_date DESC
            LIMIT 1
            """,
            [code],
        ).fetchdf()

        if state_df.empty:
            logger.warning(f"pool2_watch 同步跳过 {code}：找不到涨停日")
            continue

        limit_up_date = state_df.iloc[0]["trade_date"]
        bu = float(state_df.iloc[0]["body_upper"])
        bl = float(state_df.iloc[0]["body_lower"])
        levels = _compute_levels(bu, bl)

        new_rows.append({
            "ts_code": code,
            "entry_date": date.fromisoformat(trade_date),
            "limit_up_date": limit_up_date,
            "body_upper": bu,
            "body_lower": bl,
            **levels,
            "status": "active",
        })

    if new_rows:
        store.upsert_pool2_watch(pd.DataFrame(new_rows))
        logger.info(f"pool2_watch 新增 {len(new_rows)} 只")
```

Add `from datetime import date` to imports at top of `pipeline.py`.

- [ ] **Step 4: Call _sync_pool2_watch at end of run_daily_pipeline**

In `run_daily_pipeline`, add after `logger.info(f"  {name}: {hit_count} 命中")` (line 163), before the final `logger.info(f"流水线完成")`:

```python
        # 同步 Pool 2 到持久池
        _sync_pool2_watch(store, trade_date)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week5a && uv run pytest tests/unit/test_pipeline_sync.py -v`
Expected: all 3 tests PASS

- [ ] **Step 6: Run full test suite**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week5a && uv run pytest tests/unit/ -v`
Expected: all tests PASS

- [ ] **Step 7: Commit**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week5a
git add src/rquant/pipeline.py tests/unit/test_pipeline_sync.py
git commit -m "feat(pipeline): sync Pool 2 screening results to persistent pool2_watch"
```

---

### Task 3: Monitor — WatchItem + watchlist builder

**Files:**
- Create: `src/rquant/monitor.py`
- Create: `tests/unit/test_monitor.py`

**Context:** The monitor needs a `WatchItem` dataclass and a function to build the watchlist at startup: load active pool2_watch + yesterday's Pool 1 from screen_result, compute levels for Pool 1 stocks, de-duplicate.

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/test_monitor.py
"""monitor 模块单测。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from rquant.storage.duckdb import DuckDBStore


@pytest.fixture()
def store(tmp_path):
    s = DuckDBStore(tmp_path / "test.duckdb")
    yield s
    s.close()


class TestBuildWatchlist:
    def test_loads_pool2_active(self, store: DuckDBStore) -> None:
        from rquant.monitor import build_watchlist

        p2 = pd.DataFrame([{
            "ts_code": "002415.SZ",
            "entry_date": date(2026, 4, 18),
            "limit_up_date": date(2026, 4, 17),
            "body_upper": 13.20, "body_lower": 11.80,
            "level_40": 12.36, "level_30": 12.22, "level_20": 12.08,
            "stop_strong": 11.80, "stop_weak": 11.52,
            "status": "active",
        }])
        store.upsert_pool2_watch(p2)

        items = build_watchlist(store)
        assert len(items) == 1
        assert items[0].ts_code == "002415.SZ"
        assert items[0].pool == "pool2"
        assert items[0].level_40 == 12.36

    def test_loads_pool1_with_level_calc(self, store: DuckDBStore) -> None:
        from rquant.monitor import build_watchlist

        # Pool 1 screen_result from yesterday
        sr = pd.DataFrame([{
            "trade_date": "2026-04-21",
            "preset_name": "n-shape-pool1",
            "ts_code": "300001.SZ",
            "name": "特锐德", "close": 15.0, "pct_chg": 3.0, "extra": None,
        }])
        store.upsert_screen_result(sr)

        # daily_bar (so _get_latest_screen_date can find it)
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('300001.SZ', '2026-04-21', 14,16,13,15,14,1,3,500,5000)"
        )

        # daily_state: limit-up was 04-20
        store._conn.execute(
            """
            INSERT INTO daily_state VALUES
            ('300001.SZ', '2026-04-20', false, false, 'gem', 0.20,
             18.0, 12.0, true, false, true, false, 1, 16.50, 14.80)
            """
        )

        items = build_watchlist(store, screen_date="2026-04-21")
        assert len(items) == 1
        assert items[0].pool == "pool1"
        body = 16.50 - 14.80
        assert items[0].level_40 == pytest.approx(14.80 + body * 0.4)

    def test_dedup_pool2_wins(self, store: DuckDBStore) -> None:
        from rquant.monitor import build_watchlist

        # Same stock in both Pool 1 and Pool 2
        p2 = pd.DataFrame([{
            "ts_code": "002415.SZ",
            "entry_date": date(2026, 4, 18),
            "limit_up_date": date(2026, 4, 17),
            "body_upper": 13.20, "body_lower": 11.80,
            "level_40": 12.36, "level_30": 12.22, "level_20": 12.08,
            "stop_strong": 11.80, "stop_weak": 11.52,
            "status": "active",
        }])
        store.upsert_pool2_watch(p2)

        sr = pd.DataFrame([{
            "trade_date": "2026-04-21",
            "preset_name": "n-shape-pool1",
            "ts_code": "002415.SZ",
            "name": "海康威视", "close": 12.50, "pct_chg": -2.0, "extra": None,
        }])
        store.upsert_screen_result(sr)

        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('002415.SZ', '2026-04-21', 12,13,11,12.5,12,0.5,5,1000,10000)"
        )

        items = build_watchlist(store, screen_date="2026-04-21")
        assert len(items) == 1
        assert items[0].pool == "pool2"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week5a && uv run pytest tests/unit/test_monitor.py::TestBuildWatchlist -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'rquant.monitor'`

- [ ] **Step 3: Create monitor.py with WatchItem and build_watchlist**

```python
# src/rquant/monitor.py
"""盘中实时监控：加载 watchlist → 轮询 akshare → 档位检测 → 弹窗 + 存库。"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

import pandas as pd
from loguru import logger

from rquant.pipeline import _compute_levels
from rquant.storage.duckdb import DuckDBStore


@dataclass
class WatchItem:
    """单只监控标的。"""
    ts_code: str
    pool: str  # 'pool1' or 'pool2'
    limit_up_date: date
    body_upper: float
    body_lower: float
    body: float
    level_40: float
    level_30: float
    level_20: float
    stop_strong: float
    stop_weak: float
    triggered: dict[str, bool] = field(default_factory=lambda: {
        "40": False, "30": False, "20": False,
        "strong": False, "weak": False,
    })


def _get_latest_screen_date(store: DuckDBStore) -> str | None:
    """screen_result 中最新的 Pool 1 筛选日期。"""
    row = store._conn.execute(
        """
        SELECT strftime(MAX(trade_date), '%Y-%m-%d')
        FROM screen_result
        WHERE preset_name = 'n-shape-pool1'
        """
    ).fetchone()
    return row[0] if row and row[0] else None


def build_watchlist(
    store: DuckDBStore,
    screen_date: str | None = None,
) -> list[WatchItem]:
    """加载 Pool 2 active + 指定日期 Pool 1，去重后返回 watchlist。"""
    items: dict[str, WatchItem] = {}

    # 1. Pool 2 active（优先级高）
    p2_df = store.query_pool2_active()
    for _, row in p2_df.iterrows():
        code = row["ts_code"]
        bu, bl = float(row["body_upper"]), float(row["body_lower"])
        items[code] = WatchItem(
            ts_code=code,
            pool="pool2",
            limit_up_date=row["limit_up_date"],
            body_upper=bu,
            body_lower=bl,
            body=bu - bl,
            level_40=float(row["level_40"]),
            level_30=float(row["level_30"]),
            level_20=float(row["level_20"]),
            stop_strong=float(row["stop_strong"]),
            stop_weak=float(row["stop_weak"]),
        )

    # 2. Pool 1（screen_date 当天的，补充不在 Pool 2 中的）
    sd = screen_date or _get_latest_screen_date(store)
    if sd is None:
        logger.warning("无 Pool 1 数据")
        return list(items.values())

    p1_df = store.query_screen_result(sd, "n-shape-pool1")
    for _, row in p1_df.iterrows():
        code = row["ts_code"]
        if code in items:
            continue  # Pool 2 优先

        # 查涨停日 body
        state_df = store._conn.execute(
            """
            SELECT trade_date, body_upper, body_lower
            FROM daily_state
            WHERE ts_code = ? AND is_first_limit_up = true
            ORDER BY trade_date DESC
            LIMIT 1
            """,
            [code],
        ).fetchdf()

        if state_df.empty:
            logger.warning(f"跳过 Pool 1 {code}：找不到涨停日")
            continue

        bu = float(state_df.iloc[0]["body_upper"])
        bl = float(state_df.iloc[0]["body_lower"])
        levels = _compute_levels(bu, bl)

        items[code] = WatchItem(
            ts_code=code,
            pool="pool1",
            limit_up_date=state_df.iloc[0]["trade_date"],
            body_upper=bu,
            body_lower=bl,
            body=bu - bl,
            level_40=levels["level_40"],
            level_30=levels["level_30"],
            level_20=levels["level_20"],
            stop_strong=levels["stop_strong"],
            stop_weak=levels["stop_weak"],
        )

    logger.info(
        f"Watchlist: {len(items)} 只 "
        f"(pool2={sum(1 for i in items.values() if i.pool == 'pool2')}, "
        f"pool1={sum(1 for i in items.values() if i.pool == 'pool1')})"
    )
    return list(items.values())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week5a && uv run pytest tests/unit/test_monitor.py::TestBuildWatchlist -v`
Expected: all 3 tests PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week5a
git add src/rquant/monitor.py tests/unit/test_monitor.py
git commit -m "feat(monitor): add WatchItem dataclass and watchlist builder"
```

---

### Task 4: Monitor — trading calendar + akshare price fetching

**Files:**
- Modify: `src/rquant/monitor.py`
- Modify: `tests/unit/test_monitor.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/unit/test_monitor.py`:

```python
class TestIsTradingDay:
    @patch("rquant.monitor.ak")
    def test_trading_day_returns_true(self, mock_ak) -> None:
        from rquant.monitor import is_trading_day

        mock_ak.tool_trade_date_hist_sina.return_value = pd.DataFrame(
            {"trade_date": ["2026-04-21", "2026-04-22"]}
        )
        assert is_trading_day(date(2026, 4, 21)) is True

    @patch("rquant.monitor.ak")
    def test_non_trading_day_returns_false(self, mock_ak) -> None:
        from rquant.monitor import is_trading_day

        mock_ak.tool_trade_date_hist_sina.return_value = pd.DataFrame(
            {"trade_date": ["2026-04-21", "2026-04-22"]}
        )
        assert is_trading_day(date(2026, 4, 19)) is False


class TestFetchRealtimePrices:
    @patch("rquant.monitor.ak")
    def test_returns_price_and_low(self, mock_ak) -> None:
        from rquant.monitor import fetch_realtime_prices

        mock_ak.stock_zh_a_spot_em.return_value = pd.DataFrame({
            "代码": ["002415", "300001", "600000"],
            "最新价": [12.35, 15.00, 8.50],
            "最低": [12.10, 14.80, 8.30],
        })
        result = fetch_realtime_prices(["002415.SZ", "300001.SZ"])
        assert "002415.SZ" in result
        assert result["002415.SZ"]["price"] == 12.35
        assert result["002415.SZ"]["low"] == 12.10
        assert "300001.SZ" in result
        assert "600000.SH" not in result

    @patch("rquant.monitor.ak")
    def test_missing_stock_skipped(self, mock_ak) -> None:
        from rquant.monitor import fetch_realtime_prices

        mock_ak.stock_zh_a_spot_em.return_value = pd.DataFrame({
            "代码": ["600000"],
            "最新价": [8.50],
            "最低": [8.30],
        })
        result = fetch_realtime_prices(["002415.SZ"])
        assert result == {}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week5a && uv run pytest tests/unit/test_monitor.py::TestIsTradingDay tests/unit/test_monitor.py::TestFetchRealtimePrices -v`
Expected: FAIL — `AttributeError: module 'rquant.monitor' has no attribute 'is_trading_day'`

- [ ] **Step 3: Implement is_trading_day and fetch_realtime_prices**

Add to `src/rquant/monitor.py`, after the imports section:

```python
import akshare as ak
```

Add after `build_watchlist`:

```python
def is_trading_day(check_date: date) -> bool:
    """通过 akshare 交易日历检查是否为 A 股交易日。"""
    try:
        df = ak.tool_trade_date_hist_sina()
        trade_dates = set(
            pd.to_datetime(df["trade_date"]).dt.date
        )
        return check_date in trade_dates
    except Exception:
        logger.error("获取交易日历失败，默认当作交易日")
        return True


def fetch_realtime_prices(
    ts_codes: list[str],
) -> dict[str, dict[str, float]]:
    """批量获取实时行情，返回 {ts_code: {price, low}}。

    akshare 代码格式 "002415"，rQuant 用 "002415.SZ"。
    """
    try:
        df = ak.stock_zh_a_spot_em()
    except Exception:
        logger.error("akshare 实时行情获取失败")
        return {}

    # ts_code -> akshare 代码映射
    code_map = {c.split(".")[0]: c for c in ts_codes}
    wanted = set(code_map.keys())

    result = {}
    for _, row in df.iterrows():
        ak_code = str(row["代码"])
        if ak_code in wanted:
            ts_code = code_map[ak_code]
            price = row["最新价"]
            low = row["最低"]
            if pd.notna(price) and pd.notna(low):
                result[ts_code] = {
                    "price": float(price),
                    "low": float(low),
                }
    return result
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week5a && uv run pytest tests/unit/test_monitor.py::TestIsTradingDay tests/unit/test_monitor.py::TestFetchRealtimePrices -v`
Expected: all 4 tests PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week5a
git add src/rquant/monitor.py tests/unit/test_monitor.py
git commit -m "feat(monitor): add trading calendar check and akshare price fetching"
```

---

### Task 5: Monitor — level detection + event recording

**Files:**
- Modify: `src/rquant/monitor.py`
- Modify: `tests/unit/test_monitor.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/unit/test_monitor.py`:

```python
class TestCheckLevels:
    def _make_item(self) -> "WatchItem":
        from rquant.monitor import WatchItem
        return WatchItem(
            ts_code="002415.SZ", pool="pool2",
            limit_up_date=date(2026, 4, 17),
            body_upper=13.20, body_lower=11.80, body=1.40,
            level_40=12.36, level_30=12.22, level_20=12.08,
            stop_strong=11.80, stop_weak=11.52,
        )

    def test_no_trigger_above_all_levels(self) -> None:
        from rquant.monitor import check_levels
        item = self._make_item()
        events = check_levels(item, current_price=12.50, daily_low=12.50)
        assert events == []

    def test_triggers_40_level(self) -> None:
        from rquant.monitor import check_levels
        item = self._make_item()
        events = check_levels(item, current_price=12.30, daily_low=12.30)
        assert len(events) == 1
        assert events[0]["level"] == "40"
        assert events[0]["trigger_type"] == "realtime"
        assert item.triggered["40"] is True

    def test_triggers_multiple_levels(self) -> None:
        from rquant.monitor import check_levels
        item = self._make_item()
        events = check_levels(item, current_price=12.00, daily_low=12.00)
        triggered_levels = {e["level"] for e in events}
        assert "40" in triggered_levels
        assert "30" in triggered_levels
        assert "20" in triggered_levels

    def test_daily_low_backup_trigger(self) -> None:
        from rquant.monitor import check_levels
        item = self._make_item()
        # Price bounced back above 40, but daily low touched it
        events = check_levels(item, current_price=12.50, daily_low=12.30)
        assert len(events) == 1
        assert events[0]["trigger_type"] == "daily_low"

    def test_no_retrigger(self) -> None:
        from rquant.monitor import check_levels
        item = self._make_item()
        check_levels(item, current_price=12.30, daily_low=12.30)
        assert item.triggered["40"] is True

        events2 = check_levels(item, current_price=12.30, daily_low=12.30)
        assert events2 == []

    def test_strong_stop_trigger(self) -> None:
        from rquant.monitor import check_levels
        item = self._make_item()
        events = check_levels(item, current_price=11.75, daily_low=11.75)
        levels = {e["level"] for e in events}
        assert "strong" in levels
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week5a && uv run pytest tests/unit/test_monitor.py::TestCheckLevels -v`
Expected: FAIL — `ImportError: cannot import name 'check_levels'`

- [ ] **Step 3: Implement check_levels**

Add to `src/rquant/monitor.py`:

```python
def check_levels(
    item: WatchItem,
    current_price: float,
    daily_low: float,
) -> list[dict]:
    """检查实时价/当日最低是否触达各档位，返回新触发的事件列表。"""
    levels = [
        ("40", item.level_40),
        ("30", item.level_30),
        ("20", item.level_20),
        ("strong", item.stop_strong),
        ("weak", item.stop_weak),
    ]

    events = []
    for level_name, level_price in levels:
        if item.triggered[level_name]:
            continue

        if current_price <= level_price:
            item.triggered[level_name] = True
            events.append({
                "level": level_name,
                "trigger_price": current_price,
                "level_price": level_price,
                "trigger_type": "realtime",
            })
        elif daily_low <= level_price:
            item.triggered[level_name] = True
            events.append({
                "level": level_name,
                "trigger_price": daily_low,
                "level_price": level_price,
                "trigger_type": "daily_low",
            })

    return events
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week5a && uv run pytest tests/unit/test_monitor.py::TestCheckLevels -v`
Expected: all 6 tests PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week5a
git add src/rquant/monitor.py tests/unit/test_monitor.py
git commit -m "feat(monitor): add level detection with daily-low backup"
```

---

### Task 6: Monitor — macOS alerts

**Files:**
- Modify: `src/rquant/monitor.py`
- Modify: `tests/unit/test_monitor.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/unit/test_monitor.py`:

```python
class TestAlertPriceLevel:
    @patch("rquant.monitor.subprocess")
    def test_formats_alert_correctly(self, mock_sub) -> None:
        from rquant.monitor import WatchItem, alert_price_level

        item = WatchItem(
            ts_code="002415.SZ", pool="pool2",
            limit_up_date=date(2026, 4, 17),
            body_upper=13.20, body_lower=11.80, body=1.40,
            level_40=12.36, level_30=12.22, level_20=12.08,
            stop_strong=11.80, stop_weak=11.52,
        )
        alert_price_level(item, "30", 12.18)

        mock_sub.Popen.assert_called_once()
        cmd = mock_sub.Popen.call_args[0][0]
        script = cmd[2]  # osascript -e "..."
        assert "002415.SZ | 30%" in script
        assert "current" in script
        assert "12.18" in script
        assert "强止" in script

    @patch("rquant.monitor.subprocess")
    def test_strong_stop_label(self, mock_sub) -> None:
        from rquant.monitor import WatchItem, alert_price_level

        item = WatchItem(
            ts_code="002415.SZ", pool="pool2",
            limit_up_date=date(2026, 4, 17),
            body_upper=13.20, body_lower=11.80, body=1.40,
            level_40=12.36, level_30=12.22, level_20=12.08,
            stop_strong=11.80, stop_weak=11.52,
        )
        alert_price_level(item, "strong", 11.75)

        cmd = mock_sub.Popen.call_args[0][0]
        script = cmd[2]
        assert "002415.SZ | 强止" in script


class TestAlertExitConfirm:
    @patch("rquant.monitor.subprocess")
    def test_returns_true_on_kick(self, mock_sub) -> None:
        from rquant.monitor import alert_exit_confirm

        mock_sub.run.return_value = MagicMock(
            stdout="button returned:踢出\n"
        )
        result = alert_exit_confirm(
            ts_code="002415.SZ",
            reason="跌破强止 ¥11.80",
            entry_date="04-18",
            days_in_pool=2,
            close_price=11.65,
            levels={"40": 12.36, "30": 12.22, "20": 12.08},
            stop_strong=11.80,
            stop_weak=11.52,
            triggered_levels=["40"],
        )
        assert result is True

    @patch("rquant.monitor.subprocess")
    def test_returns_false_on_keep(self, mock_sub) -> None:
        from rquant.monitor import alert_exit_confirm

        mock_sub.run.return_value = MagicMock(
            stdout="button returned:保留\n"
        )
        result = alert_exit_confirm(
            ts_code="002415.SZ",
            reason="观察期满",
            entry_date="04-18",
            days_in_pool=3,
            close_price=12.50,
            levels={"40": 12.36, "30": 12.22, "20": 12.08},
            stop_strong=11.80,
            stop_weak=11.52,
            triggered_levels=["40"],
        )
        assert result is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week5a && uv run pytest tests/unit/test_monitor.py::TestAlertPriceLevel tests/unit/test_monitor.py::TestAlertExitConfirm -v`
Expected: FAIL — `ImportError: cannot import name 'alert_price_level'`

- [ ] **Step 3: Implement alert functions**

Add `import subprocess` to imports in `src/rquant/monitor.py`.

Add after `check_levels`:

```python
_LEVEL_LABELS = {
    "40": "40%", "30": "30%", "20": "20%",
    "strong": "强止", "weak": "弱止",
}


def alert_price_level(item: WatchItem, level: str, price: float) -> None:
    """Popen osascript 弹出档位提醒（非阻塞）。"""
    label = _LEVEL_LABELS.get(level, level)
    title = f"{item.ts_code} | {label}"
    body = (
        f"current：¥{price:.2f}\\n"
        f"40：¥{item.level_40:.2f} | 30：¥{item.level_30:.2f} | "
        f"20：¥{item.level_20:.2f}\\n"
        f"body：¥{item.body_lower:.2f} — ¥{item.body_upper:.2f}\\n"
        f"强止：¥{item.stop_strong:.2f} | 弱止：¥{item.stop_weak:.2f}"
    )
    subprocess.Popen([
        "osascript", "-e",
        f'display alert "{title}" message "{body}"',
    ])
    logger.info(f"弹窗: {title} ¥{price:.2f}")


def alert_exit_confirm(
    ts_code: str,
    reason: str,
    entry_date: str,
    days_in_pool: int,
    close_price: float,
    levels: dict[str, float],
    stop_strong: float,
    stop_weak: float,
    triggered_levels: list[str],
) -> bool:
    """弹出退出确认弹窗，返回 True=踢出, False=保留。"""
    # 已触达的档位标 ✓
    l40 = f"¥{levels['40']:.2f}" + (" ✓" if "40" in triggered_levels else "")
    l30 = f"¥{levels['30']:.2f}" + (" ✓" if "30" in triggered_levels else "")
    l20 = f"¥{levels['20']:.2f}" + (" ✓" if "20" in triggered_levels else "")

    title = f"{ts_code} | 退出确认"
    body = (
        f"{reason}\\n"
        f"入池：{entry_date}（第{days_in_pool}天）\\n"
        f"昨收：¥{close_price:.2f}\\n"
        f"40：{l40} | 30：{l30} | 20：{l20}\\n"
        f"强止：¥{stop_strong:.2f} | 弱止：¥{stop_weak:.2f}"
    )

    result = subprocess.run(
        [
            "osascript", "-e",
            f'display alert "{title}" message "{body}" '
            f'buttons {{"保留", "踢出"}} default button "保留"',
        ],
        capture_output=True, text=True,
    )
    return "踢出" in result.stdout
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week5a && uv run pytest tests/unit/test_monitor.py::TestAlertPriceLevel tests/unit/test_monitor.py::TestAlertExitConfirm -v`
Expected: all 4 tests PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week5a
git add src/rquant/monitor.py tests/unit/test_monitor.py
git commit -m "feat(monitor): add macOS alert for price levels and exit confirmation"
```

---

### Task 7: Monitor — exit checks

**Files:**
- Modify: `src/rquant/monitor.py`
- Modify: `tests/unit/test_monitor.py`

**Context:** At 15:05, after market close, the monitor checks each active Pool 2 stock for exit conditions (breakdown: close < stop level; expiry: 3+ trading days in pool). All exits prompt user with `alert_exit_confirm`.

- [ ] **Step 1: Write failing tests**

Append to `tests/unit/test_monitor.py`:

```python
class TestCheckExits:
    def test_breakdown_detected(self, store: DuckDBStore) -> None:
        from rquant.monitor import check_exits

        p2 = pd.DataFrame([{
            "ts_code": "002415.SZ",
            "entry_date": date(2026, 4, 18),
            "limit_up_date": date(2026, 4, 17),
            "body_upper": 13.20, "body_lower": 11.80,
            "level_40": 12.36, "level_30": 12.22, "level_20": 12.08,
            "stop_strong": 11.80, "stop_weak": 11.52,
            "status": "active",
        }])
        store.upsert_pool2_watch(p2)

        # Close price below stop_strong
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('002415.SZ', '2026-04-21', 12,12,11.5,11.65,12,0,0,1000,10000)"
        )

        with patch("rquant.monitor.alert_exit_confirm", return_value=True):
            check_exits(store, date(2026, 4, 21))

        active = store.query_pool2_active()
        assert len(active) == 0

    def test_expiry_detected(self, store: DuckDBStore) -> None:
        from rquant.monitor import check_exits

        p2 = pd.DataFrame([{
            "ts_code": "002415.SZ",
            "entry_date": date(2026, 4, 16),  # 3+ trading days ago
            "limit_up_date": date(2026, 4, 15),
            "body_upper": 13.20, "body_lower": 11.80,
            "level_40": 12.36, "level_30": 12.22, "level_20": 12.08,
            "stop_strong": 11.80, "stop_weak": 11.52,
            "status": "active",
        }])
        store.upsert_pool2_watch(p2)

        # Close above stop levels but 3+ days old
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('002415.SZ', '2026-04-16', 12,13,11,12,11,1,5,1000,10000),"
            "('002415.SZ', '2026-04-17', 12,13,11,12,11,1,5,1000,10000),"
            "('002415.SZ', '2026-04-18', 12,13,11,12,11,1,5,1000,10000),"
            "('002415.SZ', '2026-04-21', 12,13,11,12.5,12,0.5,5,1000,10000)"
        )

        with patch("rquant.monitor.alert_exit_confirm", return_value=True):
            check_exits(store, date(2026, 4, 21))

        active = store.query_pool2_active()
        assert len(active) == 0

    def test_user_keeps_stock(self, store: DuckDBStore) -> None:
        from rquant.monitor import check_exits

        p2 = pd.DataFrame([{
            "ts_code": "002415.SZ",
            "entry_date": date(2026, 4, 16),
            "limit_up_date": date(2026, 4, 15),
            "body_upper": 13.20, "body_lower": 11.80,
            "level_40": 12.36, "level_30": 12.22, "level_20": 12.08,
            "stop_strong": 11.80, "stop_weak": 11.52,
            "status": "active",
        }])
        store.upsert_pool2_watch(p2)

        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('002415.SZ', '2026-04-16', 12,13,11,12,11,1,5,1000,10000),"
            "('002415.SZ', '2026-04-17', 12,13,11,12,11,1,5,1000,10000),"
            "('002415.SZ', '2026-04-18', 12,13,11,12,11,1,5,1000,10000),"
            "('002415.SZ', '2026-04-21', 12,13,11,12.5,12,0.5,5,1000,10000)"
        )

        with patch("rquant.monitor.alert_exit_confirm", return_value=False):
            check_exits(store, date(2026, 4, 21))

        active = store.query_pool2_active()
        assert len(active) == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week5a && uv run pytest tests/unit/test_monitor.py::TestCheckExits -v`
Expected: FAIL — `ImportError: cannot import name 'check_exits'`

- [ ] **Step 3: Implement check_exits**

Add to `src/rquant/monitor.py`:

```python
def _count_trading_days_since(
    store: DuckDBStore, entry_date: date, today: date
) -> int:
    """entry_date 到 today 之间有多少个交易日（含两端）。"""
    row = store._conn.execute(
        """
        SELECT COUNT(DISTINCT trade_date) FROM daily_bar
        WHERE trade_date >= ? AND trade_date <= ?
        """,
        [entry_date, today],
    ).fetchone()
    return row[0] if row else 0


def check_exits(store: DuckDBStore, today: date) -> None:
    """收盘后检查 Pool 2 退出条件：跌破位 / 超期。"""
    active = store.query_pool2_active()
    if active.empty:
        return

    today_str = today.isoformat()

    # 获取今日事件记录（用于标记已触达档位）
    events_df = store.query_monitor_events(today_str)

    for _, row in active.iterrows():
        code = row["ts_code"]
        entry_date = row["entry_date"]
        bu = float(row["body_upper"])
        bl = float(row["body_lower"])
        stop_s = float(row["stop_strong"])
        stop_w = float(row["stop_weak"])

        # 取今日收盘价
        close_row = store._conn.execute(
            "SELECT close FROM daily_bar WHERE ts_code = ? AND trade_date = ?",
            [code, today],
        ).fetchone()

        if close_row is None:
            continue

        close_price = float(close_row[0])
        days = _count_trading_days_since(store, entry_date, today)

        # 已触达的档位
        triggered = []
        if not events_df.empty:
            stock_events = events_df[events_df["ts_code"] == code]
            triggered = stock_events["level"].tolist()

        levels = {
            "40": float(row["level_40"]),
            "30": float(row["level_30"]),
            "20": float(row["level_20"]),
        }
        entry_str = str(entry_date)[5:]  # "04-18"

        reason = None
        exit_reason = None

        # 条件 1：跌破止损
        if close_price < stop_w:
            reason = f"跌破弱止 ¥{stop_w:.2f}"
            exit_reason = "breakdown"
        elif close_price < stop_s:
            reason = f"跌破强止 ¥{stop_s:.2f}"
            exit_reason = "breakdown"
        # 条件 2：超期
        elif days >= 3:
            reason = "观察期满"
            exit_reason = "expired"

        if reason is None:
            continue

        should_kick = alert_exit_confirm(
            ts_code=code,
            reason=reason,
            entry_date=entry_str,
            days_in_pool=days,
            close_price=close_price,
            levels=levels,
            stop_strong=stop_s,
            stop_weak=stop_w,
            triggered_levels=triggered,
        )

        if should_kick:
            store.update_pool2_exit(code, today, exit_reason)
            logger.info(f"Pool 2 退出: {code} ({exit_reason})")
        else:
            logger.info(f"Pool 2 保留: {code}")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week5a && uv run pytest tests/unit/test_monitor.py::TestCheckExits -v`
Expected: all 3 tests PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week5a
git add src/rquant/monitor.py tests/unit/test_monitor.py
git commit -m "feat(monitor): add exit checks with interactive confirmation dialogs"
```

---

### Task 8: Monitor — main loop

**Files:**
- Modify: `src/rquant/monitor.py`
- Modify: `tests/unit/test_monitor.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/unit/test_monitor.py`:

```python
class TestRunMonitor:
    @patch("rquant.monitor.is_trading_day", return_value=False)
    def test_exits_on_non_trading_day(self, _mock) -> None:
        from rquant.monitor import run_monitor
        result = run_monitor(interval=5)
        assert result == 0

    @patch("rquant.monitor.check_exits")
    @patch("rquant.monitor.fetch_realtime_prices")
    @patch("rquant.monitor.build_watchlist")
    @patch("rquant.monitor.is_trading_day", return_value=True)
    @patch("rquant.monitor._is_trading_hours")
    @patch("rquant.monitor._now")
    def test_polls_and_detects(
        self, mock_now, mock_hours, _td, mock_build, mock_fetch, mock_exits
    ) -> None:
        from rquant.monitor import WatchItem, run_monitor

        item = WatchItem(
            ts_code="002415.SZ", pool="pool2",
            limit_up_date=date(2026, 4, 17),
            body_upper=13.20, body_lower=11.80, body=1.40,
            level_40=12.36, level_30=12.22, level_20=12.08,
            stop_strong=11.80, stop_weak=11.52,
        )
        mock_build.return_value = [item]
        mock_fetch.return_value = {
            "002415.SZ": {"price": 12.30, "low": 12.30}
        }

        # First call: trading hours. Second call: after close.
        mock_hours.side_effect = [True, False]
        mock_now.return_value = datetime(2026, 4, 21, 10, 0, 0)

        with patch("rquant.monitor.alert_price_level"):
            with patch("rquant.monitor.DuckDBStore") as MockStore:
                mock_store = MockStore.return_value.__enter__.return_value
                mock_store.upsert_monitor_event.return_value = 1
                mock_store.query_monitor_events.return_value = pd.DataFrame()
                run_monitor(interval=5)

        assert item.triggered["40"] is True
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week5a && uv run pytest tests/unit/test_monitor.py::TestRunMonitor -v`
Expected: FAIL — `ImportError: cannot import name 'run_monitor'`

- [ ] **Step 3: Implement run_monitor**

Add to imports in `src/rquant/monitor.py`:

```python
import time
from datetime import datetime
```

Add at end of `src/rquant/monitor.py`:

```python
def _now() -> datetime:
    """当前时间（方便测试 mock）。"""
    return datetime.now()


def _is_trading_hours() -> bool:
    """当前是否在交易时段（09:30-11:30 或 13:00-15:00）。"""
    now = _now()
    t = now.hour * 100 + now.minute
    return (930 <= t <= 1130) or (1300 <= t <= 1500)


def run_monitor(interval: int = 5) -> int:
    """盘中监控主循环。"""
    today = date.today()

    if not is_trading_day(today):
        logger.info(f"{today} 非交易日，退出")
        return 0

    with DuckDBStore() as store:
        watchlist = build_watchlist(store)

        if not watchlist:
            logger.warning("Watchlist 为空，退出")
            return 0

        logger.info(f"开始监控 {len(watchlist)} 只，间隔 {interval} 秒")

        ts_codes = [item.ts_code for item in watchlist]
        item_map = {item.ts_code: item for item in watchlist}
        today_str = today.isoformat()

        while _is_trading_hours():
            prices = fetch_realtime_prices(ts_codes)

            for code, pdata in prices.items():
                item = item_map.get(code)
                if item is None:
                    continue

                events = check_levels(
                    item, pdata["price"], pdata["low"]
                )

                for evt in events:
                    # 存库
                    evt_df = pd.DataFrame([{
                        "trade_date": today,
                        "ts_code": code,
                        "level": evt["level"],
                        "trigger_price": evt["trigger_price"],
                        "level_price": evt["level_price"],
                        "trigger_time": _now(),
                        "trigger_type": evt["trigger_type"],
                        "pool": item.pool,
                        "body_upper": item.body_upper,
                        "body_lower": item.body_lower,
                    }])
                    store.upsert_monitor_event(evt_df)

                    # 弹窗
                    alert_price_level(
                        item, evt["level"], evt["trigger_price"]
                    )

            time.sleep(interval)

        # 收盘后：事件汇总
        all_events = store.query_monitor_events(today_str)
        if not all_events.empty:
            logger.info(f"当日事件汇总: {len(all_events)} 条")
            for _, e in all_events.iterrows():
                logger.info(
                    f"  {e['ts_code']} {e['level']} "
                    f"¥{e['trigger_price']:.2f} @ {e['trigger_time']}"
                )
        else:
            logger.info("当日无事件触发")

        # 收盘后：退出检查
        check_exits(store, today)

    logger.info("监控结束")
    return 0
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week5a && uv run pytest tests/unit/test_monitor.py::TestRunMonitor -v`
Expected: all 2 tests PASS

- [ ] **Step 5: Run full test suite**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week5a && uv run pytest tests/unit/ -v`
Expected: all tests PASS

- [ ] **Step 6: Commit**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week5a
git add src/rquant/monitor.py tests/unit/test_monitor.py
git commit -m "feat(monitor): add main polling loop with trading hours awareness"
```

---

### Task 9: CLI — monitor + pool2 commands

**Files:**
- Modify: `src/rquant/cli.py:119-171`
- Modify: `tests/unit/test_cli.py`

- [ ] **Step 1: Read existing CLI tests to understand patterns**

Read: `tests/unit/test_cli.py`

- [ ] **Step 2: Add new CLI functions and parser entries**

Add after `cmd_ingest` (line 116) in `src/rquant/cli.py`:

```python
def cmd_monitor(args: argparse.Namespace) -> int:
    """启动盘中实时监控。"""
    from rquant.monitor import run_monitor

    setup_logging()
    return run_monitor(interval=args.interval)


def cmd_pool2(args: argparse.Namespace) -> int:
    """管理 Pool 2 持久池。"""
    from rquant.storage.duckdb import DuckDBStore

    setup_logging()
    with DuckDBStore() as store:
        if args.pool2_action == "list":
            df = store.query_pool2_all()
            if df.empty:
                logger.info("Pool 2 持久池为空")
                return 0
            for _, row in df.iterrows():
                status_mark = "🟢" if row["status"] == "active" else "⬜"
                logger.info(
                    f"  {status_mark} {row['ts_code']} "
                    f"入池 {row['entry_date']} "
                    f"涨停 {row['limit_up_date']} "
                    f"body ¥{row['body_lower']:.2f}-¥{row['body_upper']:.2f} "
                    f"[{row['status']}]"
                )
            return 0

        elif args.pool2_action == "remove":
            store.remove_pool2(args.ts_code)
            logger.info(f"已从 Pool 2 移除: {args.ts_code}")
            return 0

    return 0
```

In `build_parser()`, add after the `ingest` subparser (before `return parser`):

```python
    monitor_p = sub.add_parser("monitor", help="启动盘中实时监控")
    monitor_p.add_argument(
        "--interval", type=int, default=5,
        help="轮询间隔秒数 (默认 5)",
    )

    pool2_p = sub.add_parser("pool2", help="管理 Pool 2 持久池")
    pool2_sub = pool2_p.add_subparsers(dest="pool2_action")
    pool2_sub.add_parser("list", help="列出 Pool 2 标的")
    pool2_rm = pool2_sub.add_parser("remove", help="移除标的")
    pool2_rm.add_argument("ts_code", type=str, help="股票代码 (如 002415.SZ)")
```

In `main()`, add to the dispatch block:

```python
    elif args.command == "monitor":
        return cmd_monitor(args)
    elif args.command == "pool2":
        return cmd_pool2(args)
```

- [ ] **Step 3: Write tests for new CLI commands**

Add to `tests/unit/test_cli.py`:

```python
class TestMonitorParser:
    def test_default_interval(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["monitor"])
        assert args.command == "monitor"
        assert args.interval == 5

    def test_custom_interval(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["monitor", "--interval", "10"])
        assert args.interval == 10


class TestPool2Parser:
    def test_list(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["pool2", "list"])
        assert args.command == "pool2"
        assert args.pool2_action == "list"

    def test_remove(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["pool2", "remove", "002415.SZ"])
        assert args.pool2_action == "remove"
        assert args.ts_code == "002415.SZ"
```

- [ ] **Step 4: Run tests**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week5a && uv run pytest tests/unit/test_cli.py -v`
Expected: all tests PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week5a
git add src/rquant/cli.py tests/unit/test_cli.py
git commit -m "feat(cli): add monitor and pool2 subcommands"
```

---

### Task 10: launchd + dependency + CHANGELOG

**Files:**
- Modify: `pyproject.toml:10-20`
- Create: `deploy/com.roxor.rquant-monitor.plist`
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Add akshare dependency to pyproject.toml**

Add `akshare>=1.14` to the `dependencies` list in `pyproject.toml` (after the `apscheduler` line):

```toml
dependencies = [
    "akshare>=1.14",
    "apscheduler>=3.10,<4",
    "duckdb>=1.5.2",
    ...
]
```

- [ ] **Step 2: Install new dependency**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week5a && uv sync`
Expected: akshare installed successfully

- [ ] **Step 3: Create launchd plist**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.roxor.rquant-monitor</string>

    <key>ProgramArguments</key>
    <array>
        <string>/Users/roxor/brain/30-projects/rQuant/.venv/bin/rquant</string>
        <string>monitor</string>
    </array>

    <key>WorkingDirectory</key>
    <string>/Users/roxor/brain/30-projects/rQuant</string>

    <key>RunAtLoad</key>
    <true/>

    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
    </dict>

    <key>StandardOutPath</key>
    <string>/Users/roxor/brain/30-projects/rQuant/logs/launchd-monitor-stdout.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/roxor/brain/30-projects/rQuant/logs/launchd-monitor-stderr.log</string>

    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/Users/roxor/brain/30-projects/rQuant/.venv/bin:/usr/local/bin:/usr/bin:/bin</string>
    </dict>
</dict>
</plist>
```

- [ ] **Step 4: Update CHANGELOG.md Unreleased section**

Add under `[Unreleased]` in `CHANGELOG.md`:

```markdown
### Added
- `rquant monitor` 命令：盘中实时监控 Pool 1 + Pool 2 标的价格
  - akshare 实时行情轮询（5 秒间隔），检测 5 个档位（40%/30%/20%/强止/弱止）
  - macOS 原生弹窗提醒（osascript display alert），非阻塞
  - 当日最低价补漏机制，防止闪跌遗漏
  - 交易日历检查，非交易日自动跳过
- `pool2_watch` 表：Pool 2 持久池，从每日快照升级为有进出机制的持久池子
  - 入池：pipeline 跑完 Pool 2 筛选后自动同步
  - 退出：收盘后检查跌破止损/超期（3 天），所有退出弹窗确认
- `monitor_event` 表：盘中事件日志，为 Week 6 推送做数据准备
- `rquant pool2 list / remove` 命令：查看和管理持久池
- `deploy/com.roxor.rquant-monitor.plist`：盘中监控 launchd 自启配置

### Changed
- `pipeline.py`：run_daily_pipeline() 尾部新增 pool2_watch 同步逻辑
```

- [ ] **Step 5: Run full test suite one final time**

Run: `cd /Users/roxor/brain/30-projects/rQuant-week5a && uv run pytest tests/unit/ -v`
Expected: all tests PASS

- [ ] **Step 6: Commit**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week5a
git add pyproject.toml deploy/com.roxor.rquant-monitor.plist CHANGELOG.md
git commit -m "chore: add akshare dependency, monitor launchd plist, and CHANGELOG"
```
