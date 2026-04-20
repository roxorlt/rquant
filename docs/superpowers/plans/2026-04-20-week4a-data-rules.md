# Week 4a Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add daily_basic data, body/shadow columns, and 6 new screening rules to support the N-shape strategy.

**Architecture:** Extend the existing storage → adapter → loader → rules pipeline. New daily_basic table for market cap data. Loader gets body_upper/body_lower columns from daily_state + circ_mv from daily_basic. AggregateRequest mechanism lets rules declare long-window needs (8/30/90 day) that the loader satisfies via DuckDB SQL. Six new rule building blocks complete the N-shape Pool 1 ruleset.

**Tech Stack:** Python 3.12, DuckDB, pandas, Tushare Pro, pytest

---

## File Structure

```
src/rquant/
├── storage/
│   ├── schema.py        # Modify: add DAILY_BASIC_DDL, add to ALL_DDL
│   └── duckdb.py        # Modify: add upsert_daily_basic() + count_daily_basic()
├── adapter/
│   └── tushare.py       # Modify: add daily_basic() method
├── screen/
│   ├── __init__.py      # Modify: export 6 new rules + AggregateRequest
│   ├── loader.py        # Modify: STATE_COLS_MAP + BASIC_COLS_MAP + aggregate columns
│   ├── rules.py         # Modify: 6 new rules + AggregateRequest dataclass
│   └── core.py          # Modify: collect aggregate_requests, pass to load_universe
└── ...

tests/
├── fixtures/
│   └── wide_frames.py   # Modify: add BODY_UPPER/BODY_LOWER/CIRC_MV/TOTAL_MV/TURNOVER_RATE
├── unit/
│   ├── test_storage_duckdb.py   # Modify: add TestDailyBasic class
│   ├── test_screen_loader.py    # Modify: add body/basic/aggregate column tests
│   ├── test_screen_rules.py     # Modify: add 6 new rule test classes
│   └── test_screen_core.py      # Modify: add aggregate collection tests
└── ...

scripts/
└── ingest_daily.py      # Modify: add daily_basic fetch loop
```

Wide table column additions:

| Source | Wide column | Origin |
|---|---|---|
| daily_state.body_upper | `BODY_UPPER[n]` | STATE_COLS_MAP expansion |
| daily_state.body_lower | `BODY_LOWER[n]` | STATE_COLS_MAP expansion |
| daily_basic.circ_mv | `CIRC_MV[n]` | New BASIC_COLS_MAP |
| daily_basic.total_mv | `TOTAL_MV[n]` | New BASIC_COLS_MAP |
| daily_basic.turnover_rate | `TURNOVER_RATE[n]` | New BASIC_COLS_MAP |
| DuckDB aggregate SQL | `max_consec_ups_Nd` / `has_limit_down_Nd` / `count_limit_up_Nd_exM` | AggregateRequest mechanism |

---

### Task 1: daily_basic DDL + upsert + adapter

**Files:**
- Modify: `src/rquant/storage/schema.py`
- Modify: `src/rquant/storage/duckdb.py`
- Modify: `src/rquant/adapter/tushare.py`
- Modify: `tests/unit/test_storage_duckdb.py`

- [ ] **Step 1: Add DAILY_BASIC_DDL to schema.py**

In `src/rquant/storage/schema.py`, after the `DAILY_STATE_DDL` block (line 81) and before the `ALL_DDL` line (line 83), add the new DDL and update ALL_DDL:

```python
DAILY_BASIC_DDL = """
CREATE TABLE IF NOT EXISTS daily_basic (
    ts_code        VARCHAR NOT NULL,
    trade_date     DATE    NOT NULL,
    turnover_rate  DOUBLE,
    volume_ratio   DOUBLE,
    total_mv       DOUBLE,
    circ_mv        DOUBLE,
    PRIMARY KEY (ts_code, trade_date)
);
"""

ALL_DDL = [DAILY_BAR_DDL, STOCK_BASIC_DDL, ADJ_FACTOR_DDL, DAILY_INDICATOR_DDL, DAILY_STATE_DDL, DAILY_BASIC_DDL]
```

Replace the existing `ALL_DDL` line (line 83) so it includes `DAILY_BASIC_DDL`.

- [ ] **Step 2: Add upsert_daily_basic() and count_daily_basic() to duckdb.py**

In `src/rquant/storage/duckdb.py`, after the `count_state()` method (ends at line 190), add:

```python
    def upsert_daily_basic(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0

        self._conn.register("basic_mkt_tmp", df)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO daily_basic
            SELECT ts_code, trade_date,
                   turnover_rate, volume_ratio,
                   total_mv, circ_mv
            FROM basic_mkt_tmp
            """
        )
        self._conn.unregister("basic_mkt_tmp")

        count = len(df)
        logger.info(f"DuckDB upsert daily_basic: {count} 行")
        return count

    def count_daily_basic(self, ts_code: str | None = None) -> int:
        if ts_code:
            result = self._conn.execute(
                "SELECT COUNT(*) FROM daily_basic WHERE ts_code = ?", [ts_code]
            ).fetchone()
        else:
            result = self._conn.execute("SELECT COUNT(*) FROM daily_basic").fetchone()
        return result[0] if result else 0
```

- [ ] **Step 3: Add daily_basic() method to tushare.py**

In `src/rquant/adapter/tushare.py`, after the `stock_basic()` method (ends at line 142), add:

```python
    def daily_basic(
        self,
        ts_codes: list[str],
        trade_date: date,
    ) -> pd.DataFrame:
        """拉取每日基本面指标（换手率、量比、市值等）。

        注意：Tushare daily_basic 接口只支持按单日查询（trade_date），
        不支持 start_date/end_date 范围。
        """
        codes_str = ",".join(ts_codes)
        trade_date_str = trade_date.strftime("%Y%m%d")

        logger.info(
            f"Tushare daily_basic 请求：codes={codes_str} trade_date={trade_date_str}"
        )

        try:
            df = self._pro.daily_basic(
                ts_code=codes_str,
                trade_date=trade_date_str,
                fields="ts_code,trade_date,turnover_rate,volume_ratio,total_mv,circ_mv",
            )
        except Exception as e:
            if self._switch_to_backup():
                df = self._pro.daily_basic(
                    ts_code=codes_str,
                    trade_date=trade_date_str,
                    fields="ts_code,trade_date,turnover_rate,volume_ratio,total_mv,circ_mv",
                )
            else:
                raise RuntimeError(f"Tushare daily_basic 调用失败：{e}") from e

        if df is None or df.empty:
            logger.warning(
                f"Tushare daily_basic 返回空：codes={codes_str} date={trade_date_str}"
            )
            return pd.DataFrame()

        df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d").dt.date
        df = df.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)

        logger.info(f"Tushare daily_basic 返回 {len(df)} 行")
        return df
```

- [ ] **Step 4: Write tests — TestDailyBasic class**

In `tests/unit/test_storage_duckdb.py`, after the `TestDailyState` class (ends at line 225), add:

```python
class TestDailyBasic:
    def _basic_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "ts_code": "600519.SH",
                    "trade_date": date(2026, 4, 15),
                    "turnover_rate": 0.35,
                    "volume_ratio": 1.2,
                    "total_mv": 20000000.0,
                    "circ_mv": 18000000.0,
                },
                {
                    "ts_code": "000001.SZ",
                    "trade_date": date(2026, 4, 15),
                    "turnover_rate": 0.80,
                    "volume_ratio": 0.9,
                    "total_mv": 3000000.0,
                    "circ_mv": 2800000.0,
                },
            ]
        )

    def test_upsert_daily_basic_inserts_rows(self, tmp_store: DuckDBStore) -> None:
        count = tmp_store.upsert_daily_basic(self._basic_df())
        assert count == 2
        assert tmp_store.count_daily_basic("600519.SH") == 1

    def test_upsert_daily_basic_idempotent(self, tmp_store: DuckDBStore) -> None:
        tmp_store.upsert_daily_basic(self._basic_df())
        tmp_store.upsert_daily_basic(self._basic_df())
        assert tmp_store.count_daily_basic() == 2

    def test_empty_daily_basic_returns_zero(self, tmp_store: DuckDBStore) -> None:
        assert tmp_store.upsert_daily_basic(pd.DataFrame()) == 0

    def test_daily_basic_table_exists_on_init(self, tmp_store: DuckDBStore) -> None:
        tables = tmp_store.query("SHOW TABLES")
        names = set(tables["name"].tolist())
        assert "daily_basic" in names
```

- [ ] **Step 5: Run tests and verify**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week4 && uv run pytest tests/unit/test_storage_duckdb.py -v
```

Expected: All existing tests pass + 4 new TestDailyBasic tests pass. `daily_basic` table is created on init.

- [ ] **Step 6: Commit**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week4 && git add src/rquant/storage/schema.py src/rquant/storage/duckdb.py src/rquant/adapter/tushare.py tests/unit/test_storage_duckdb.py && git commit -m "feat(storage): add daily_basic table, upsert, and Tushare adapter method

DAILY_BASIC_DDL for turnover_rate/volume_ratio/total_mv/circ_mv.
DuckDBStore.upsert_daily_basic() follows existing upsert_daily() pattern.
TushareAdapter.daily_basic() uses single trade_date (API constraint)."
```

---

### Task 2: Loader expansion — BODY_UPPER/BODY_LOWER + BASIC_COLS_MAP

**Files:**
- Modify: `src/rquant/screen/loader.py`
- Modify: `tests/unit/test_screen_loader.py`

- [ ] **Step 1: Add body_upper/body_lower to STATE_COLS_MAP**

In `src/rquant/screen/loader.py`, replace the existing `STATE_COLS_MAP` (lines 28-34) with:

```python
STATE_COLS_MAP = {
    "is_limit_up": "IS_LIMIT_UP",
    "is_limit_down": "IS_LIMIT_DOWN",
    "is_first_limit_up": "IS_FIRST_LIMIT_UP",
    "is_yiziban": "IS_YIZIBAN",
    "consecutive_limit_ups": "CONSECUTIVE_LIMIT_UPS",
    "body_upper": "BODY_UPPER",
    "body_lower": "BODY_LOWER",
}
```

- [ ] **Step 2: Add BASIC_COLS_MAP after STATE_COLS_MAP**

In `src/rquant/screen/loader.py`, right after the updated `STATE_COLS_MAP` block, add:

```python
BASIC_COLS_MAP = {
    "circ_mv": "CIRC_MV",
    "total_mv": "TOTAL_MV",
    "turnover_rate": "TURNOVER_RATE",
}
```

- [ ] **Step 3: Expand state_sql to include body_upper/body_lower**

The existing `state_sql` (lines 134-142) already uses `", ".join(STATE_COLS_MAP.keys())` to generate the SELECT list. Since we added `body_upper` and `body_lower` to `STATE_COLS_MAP`, they will be automatically included in the state query and pivoted into `BODY_UPPER[n]` / `BODY_LOWER[n]` columns. No SQL change needed here because the join uses `STATE_COLS_MAP.keys()`.

However, we must also remove `body_upper` and `body_lower` from the explicit extra columns in `state_sql` if they were listed there. Looking at the current code (line 137-138), the state SQL selects `{", ".join(STATE_COLS_MAP.keys())}, is_st, is_bj, board_type`. Since `body_upper` and `body_lower` are now in `STATE_COLS_MAP`, they'll be included via the join. This is correct — no change needed to the SQL template itself.

- [ ] **Step 4: Add daily_basic query + wide conversion in load_universe()**

In `src/rquant/screen/loader.py`, inside `load_universe()`, after the state_wide block (after line 144 `state_wide = _wide_from_long(...)`) and before the `state_t0` block (line 147), add the daily_basic query:

```python
        # daily_basic: circ_mv / total_mv / turnover_rate
        basic_mkt_sql = f"""
        SELECT ts_code,
               strftime(trade_date, '%Y-%m-%d') AS trade_date_str,
               {", ".join(BASIC_COLS_MAP.keys())}
        FROM daily_basic
        WHERE ts_code IN ({placeholders})
          AND trade_date IN ({",".join(["?"] * len(dates))})
        """
        basic_mkt_long = store._conn.execute(basic_mkt_sql, in_universe + dates).fetchdf()
        basic_mkt_wide = _wide_from_long(basic_mkt_long, BASIC_COLS_MAP, date_to_offset)
```

Then update the merge loop (around line 160). Replace:

```python
        for wide in (bar_wide, ind_wide, state_wide):
```

with:

```python
        for wide in (bar_wide, ind_wide, state_wide, basic_mkt_wide):
```

- [ ] **Step 5: Write loader tests for new columns**

In `tests/unit/test_screen_loader.py`, add daily_basic fixture data inside the existing `store` fixture, right after `s.upsert_state(state)` (line 82) and before `yield s` (line 83):

```python
    daily_basic_data = pd.DataFrame([
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 14),
         "turnover_rate": 1.5, "volume_ratio": 1.1,
         "total_mv": 5000000.0, "circ_mv": 4000000.0},
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 15),
         "turnover_rate": 2.0, "volume_ratio": 1.3,
         "total_mv": 6000000.0, "circ_mv": 5000000.0},
        {"ts_code": "000001.SZ", "trade_date": date(2026, 4, 15),
         "turnover_rate": 0.8, "volume_ratio": 0.9,
         "total_mv": 30000000.0, "circ_mv": 28000000.0},
    ])
    s.upsert_daily_basic(daily_basic_data)
```

Then add a new test class after `TestLoadUniverse` (after line 133):

```python
class TestLoadUniverseBodyAndBasic:
    def test_body_upper_lower_in_wide_table(self, store: DuckDBStore) -> None:
        df = load_universe("2026-04-15", lookback=1, store=store)
        assert "BODY_UPPER[0]" in df.columns
        assert "BODY_LOWER[0]" in df.columns
        row = df.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert row["BODY_UPPER[0]"] == pytest.approx(12.5)
        assert row["BODY_LOWER[0]"] == pytest.approx(11.0)

    def test_body_upper_lower_at_offset_1(self, store: DuckDBStore) -> None:
        df = load_universe("2026-04-15", lookback=1, store=store)
        row = df.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert row["BODY_UPPER[1]"] == pytest.approx(11.0)
        assert row["BODY_LOWER[1]"] == pytest.approx(10.5)

    def test_circ_mv_in_wide_table(self, store: DuckDBStore) -> None:
        df = load_universe("2026-04-15", lookback=1, store=store)
        assert "CIRC_MV[0]" in df.columns
        assert "TOTAL_MV[0]" in df.columns
        assert "TURNOVER_RATE[0]" in df.columns
        row = df.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert row["CIRC_MV[0]"] == pytest.approx(5000000.0)
        assert row["TOTAL_MV[0]"] == pytest.approx(6000000.0)
        assert row["TURNOVER_RATE[0]"] == pytest.approx(2.0)

    def test_circ_mv_at_offset_1(self, store: DuckDBStore) -> None:
        df = load_universe("2026-04-15", lookback=1, store=store)
        row = df.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert row["CIRC_MV[1]"] == pytest.approx(4000000.0)

    def test_missing_daily_basic_gives_nan(self, store: DuckDBStore) -> None:
        """stock 000001.SZ has daily_basic only for 4/15, not 4/14 — offset 1 should be NaN."""
        df = load_universe("2026-04-15", lookback=1, store=store)
        row = df.loc[df["ts_code"] == "000001.SZ"].iloc[0]
        assert row["CIRC_MV[0]"] == pytest.approx(28000000.0)
        assert pd.isna(row["CIRC_MV[1]"])
```

- [ ] **Step 6: Run tests and verify**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week4 && uv run pytest tests/unit/test_screen_loader.py -v
```

Expected: All existing `TestLoadUniverse` tests pass + 5 new `TestLoadUniverseBodyAndBasic` tests pass. `BODY_UPPER[0]`, `BODY_LOWER[0]`, `CIRC_MV[0]` appear with correct values.

- [ ] **Step 7: Commit**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week4 && git add src/rquant/screen/loader.py tests/unit/test_screen_loader.py && git commit -m "feat(loader): expose BODY_UPPER/BODY_LOWER from daily_state + CIRC_MV/TOTAL_MV/TURNOVER_RATE from daily_basic

STATE_COLS_MAP gains body_upper/body_lower.
New BASIC_COLS_MAP for daily_basic fields.
load_universe() queries daily_basic and merges into wide table."
```

---

### Task 3: Fixture update + not_yiziban rule

**Files:**
- Modify: `tests/fixtures/wide_frames.py`
- Modify: `src/rquant/screen/rules.py`
- Modify: `src/rquant/screen/__init__.py`
- Modify: `tests/unit/test_screen_rules.py`

- [ ] **Step 1: Update make_wide_frame() to include new columns**

In `tests/fixtures/wide_frames.py`, add new column groups. Replace the `bool_state_cols` and `int_state_cols` lists (lines 35-38) with:

```python
    bool_state_cols = [
        "IS_LIMIT_UP", "IS_LIMIT_DOWN", "IS_FIRST_LIMIT_UP", "IS_YIZIBAN",
    ]
    int_state_cols = ["CONSECUTIVE_LIMIT_UPS"]
    float_state_cols = ["BODY_UPPER", "BODY_LOWER"]
    basic_mkt_cols = ["CIRC_MV", "TOTAL_MV", "TURNOVER_RATE"]
```

Then in the loop body (inside `for n in range(lookback + 1):`, after line 62), add:

```python
            for c in float_state_cols:
                row[f"{c}[{n}]"] = 0.0
            for c in basic_mkt_cols:
                row[f"{c}[{n}]"] = 0.0
```

Also update the docstring (lines 22-25) to include the new columns. Replace it with:

```python
    """构造一个宽表 DataFrame。

    默认每只股票每字段全填 0.0（或 False），lookback+1 天。
    overrides 按 {(ts_code, 列名): value} 局部覆盖。

    - `CLOSE[n]`, `OPEN[n]`, `HIGH[n]`, `LOW[n]`, `VOL[n]`, `PCT_CHG[n]`, `PRE_CLOSE[n]`
    - `MA5[n]`, `MA20[n]`, `MA60[n]`, `RSI14[n]`, `MACD[n]`
    - `IS_LIMIT_UP[n]`, `IS_LIMIT_DOWN[n]`, `IS_FIRST_LIMIT_UP[n]`, `IS_YIZIBAN[n]`,
      `CONSECUTIVE_LIMIT_UPS[n]`
    - `BODY_UPPER[n]`, `BODY_LOWER[n]`
    - `CIRC_MV[n]`, `TOTAL_MV[n]`, `TURNOVER_RATE[n]`
    - `is_st`, `is_bj`, `board_type`, `ts_code`, `name`
    """
```

- [ ] **Step 2: Add not_yiziban rule to rules.py**

In `src/rquant/screen/rules.py`, after the `yiziban()` function (line 67), add:

```python
def not_yiziban(offset: int = 0) -> Rule:
    """某日非一字板。"""
    return _bool_state_rule("IS_YIZIBAN", offset, negate=True)
```

- [ ] **Step 3: Export not_yiziban from __init__.py**

In `src/rquant/screen/__init__.py`, add `not_yiziban` to the imports (after line 30 `yiziban,`) and to `__all__` (after `"yiziban"` in line 37).

Replace the imports block (lines 5-31) with:

```python
from rquant.screen.rules import (
    above_ma,
    between,
    board_in,
    consecutive_ups_gte,
    # 指标
    cross_above,
    cross_below,
    first_limit_up,
    # 比较
    gt,
    gte,
    limit_down,
    # 涨跌停 / 连板
    limit_up,
    lt,
    lte,
    not_bj,
    not_limit_up,
    # 属性类
    not_st,
    not_yiziban,
    rsi_overbought,
    rsi_oversold,
    # 成交量
    volume_ratio_gte,
    yiziban,
)
```

Replace `__all__` (lines 33-41) with:

```python
__all__ = [
    "screen", "load_universe",
    "not_st", "not_bj", "board_in",
    "limit_up", "not_limit_up", "first_limit_up", "yiziban", "not_yiziban", "limit_down",
    "consecutive_ups_gte",
    "gt", "lt", "gte", "lte", "between",
    "cross_above", "cross_below", "above_ma", "rsi_oversold", "rsi_overbought",
    "volume_ratio_gte",
]
```

- [ ] **Step 4: Write not_yiziban tests**

In `tests/unit/test_screen_rules.py`, add `not_yiziban` to the imports (after `yiziban,` on line 26):

```python
    not_yiziban,
```

Then after the `TestLimitRules` class (after line 142), add a new test method inside `TestLimitRules`. Or add to the bottom of `TestLimitRules` before the class ends. Insert before `class TestCompareRules:` (line 144):

```python
    def test_not_yiziban_passes_non_yiziban(self) -> None:
        df = make_wide_frame(
            overrides={
                ("300001.SZ", "IS_YIZIBAN[0]"): False,
                ("000001.SZ", "IS_YIZIBAN[0]"): True,
            },
        )
        mask = not_yiziban(offset=0)(df)
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert not mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]

    def test_not_yiziban_at_offset_1(self) -> None:
        df = make_wide_frame(
            lookback=2,
            overrides={("300001.SZ", "IS_YIZIBAN[1]"): True},
        )
        mask = not_yiziban(offset=1)(df)
        assert not mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]
        assert not_yiziban(offset=1).min_lookback == 1

    def test_not_yiziban_nan_treated_as_false(self) -> None:
        """NaN in IS_YIZIBAN should be treated as False (not yiziban), so not_yiziban passes."""
        df = make_wide_frame(lookback=1)
        # Default IS_YIZIBAN[0] is False, so not_yiziban should pass
        mask = not_yiziban(offset=0)(df)
        assert mask.all()
```

Also add the fixture test for new columns. Inside `TestFixture` class (after line 38), add:

```python
    def test_new_columns_in_fixture(self) -> None:
        df = make_wide_frame(lookback=2)
        assert "BODY_UPPER[0]" in df.columns
        assert "BODY_LOWER[1]" in df.columns
        assert "CIRC_MV[0]" in df.columns
        assert "TOTAL_MV[2]" in df.columns
        assert "TURNOVER_RATE[0]" in df.columns
```

- [ ] **Step 5: Run tests and verify**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week4 && uv run pytest tests/unit/test_screen_rules.py -v -k "yiziban or new_columns"
```

Expected: `test_not_yiziban_passes_non_yiziban`, `test_not_yiziban_at_offset_1`, `test_not_yiziban_nan_treated_as_false`, and `test_new_columns_in_fixture` all pass.

Then run the full rules test suite:

```bash
cd /Users/roxor/brain/30-projects/rQuant-week4 && uv run pytest tests/unit/test_screen_rules.py -v
```

Expected: All existing tests still pass (fixture change is backward compatible since new cols default to 0.0).

- [ ] **Step 6: Commit**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week4 && git add tests/fixtures/wide_frames.py src/rquant/screen/rules.py src/rquant/screen/__init__.py tests/unit/test_screen_rules.py && git commit -m "feat(rules): add not_yiziban rule + update fixture with BODY/CIRC_MV columns

not_yiziban(offset) reuses _bool_state_rule with negate=True.
make_wide_frame() now generates BODY_UPPER/BODY_LOWER/CIRC_MV/TOTAL_MV/TURNOVER_RATE columns."
```

---

### Task 4: circ_mv_lt rule

**Files:**
- Modify: `src/rquant/screen/rules.py`
- Modify: `src/rquant/screen/__init__.py`
- Modify: `tests/unit/test_screen_rules.py`

- [ ] **Step 1: Add circ_mv_lt() to rules.py**

In `src/rquant/screen/rules.py`, after the `not_yiziban()` function (added in Task 3), add:

```python
def circ_mv_lt(threshold_yi: float, offset: int = 0) -> Rule:
    """流通市值 < threshold_yi 亿元。

    Tushare circ_mv 单位是万元，1 亿 = 10000 万，
    所以 threshold_yi * 10000 与 CIRC_MV[offset] 比较。
    """
    threshold_wan = threshold_yi * 10000
    col = f"CIRC_MV[{offset}]"

    def _rule(df: pd.DataFrame) -> pd.Series:
        return df[col].fillna(float("inf")) < threshold_wan

    return _tag_lookback(_rule, offset)
```

- [ ] **Step 2: Export circ_mv_lt from __init__.py**

In `src/rquant/screen/__init__.py`, add `circ_mv_lt` to imports and `__all__`.

Add to imports (after `consecutive_ups_gte,`):

```python
    circ_mv_lt,
```

Add to `__all__` (after `"consecutive_ups_gte",`):

```python
    "circ_mv_lt",
```

- [ ] **Step 3: Write circ_mv_lt tests**

In `tests/unit/test_screen_rules.py`, add `circ_mv_lt` to imports:

```python
    circ_mv_lt,
```

Then add a new test class (after `TestLimitRules`, alongside the new not_yiziban tests, or as its own class). Place before `TestCompareRules`:

```python
class TestCircMvRule:
    def test_circ_mv_lt_passes_small_cap(self) -> None:
        """100亿 = 1000000万 < 150亿 = 1500000万 → passes."""
        df = make_wide_frame(
            overrides={
                ("300001.SZ", "CIRC_MV[0]"): 1000000.0,  # 100亿万元
                ("000001.SZ", "CIRC_MV[0]"): 2000000.0,  # 200亿万元
            },
        )
        mask = circ_mv_lt(150)(df)
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert not mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]

    def test_circ_mv_lt_boundary(self) -> None:
        """Exactly 150亿 = 1500000万 should NOT pass (strict <)."""
        df = make_wide_frame(
            overrides={("300001.SZ", "CIRC_MV[0]"): 1500000.0},
        )
        mask = circ_mv_lt(150)(df)
        assert not mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]

    def test_circ_mv_lt_nan_fails(self) -> None:
        """NaN circ_mv should fail (fillna(inf) makes it exceed any threshold)."""
        df = make_wide_frame()  # CIRC_MV[0] defaults to 0.0
        # Explicitly set NaN
        df.loc[df["ts_code"] == "300001.SZ", "CIRC_MV[0]"] = float("nan")
        mask = circ_mv_lt(150)(df)
        assert not mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]

    def test_circ_mv_lt_at_offset(self) -> None:
        df = make_wide_frame(
            lookback=2,
            overrides={("300001.SZ", "CIRC_MV[1]"): 500000.0},  # 50亿
        )
        rule = circ_mv_lt(100, offset=1)
        mask = rule(df)
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert rule.min_lookback == 1

    def test_circ_mv_lt_unit_conversion(self) -> None:
        """Verify: 1亿 = 10000万元 conversion."""
        # Stock with circ_mv = 9999万 (just under 1亿) should pass circ_mv_lt(1)
        df = make_wide_frame(overrides={("300001.SZ", "CIRC_MV[0]"): 9999.0})
        assert circ_mv_lt(1)(df).loc[df["ts_code"] == "300001.SZ"].iloc[0]
        # Stock with circ_mv = 10001万 (just over 1亿) should fail circ_mv_lt(1)
        df = make_wide_frame(overrides={("300001.SZ", "CIRC_MV[0]"): 10001.0})
        assert not circ_mv_lt(1)(df).loc[df["ts_code"] == "300001.SZ"].iloc[0]
```

- [ ] **Step 4: Run tests and verify**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week4 && uv run pytest tests/unit/test_screen_rules.py::TestCircMvRule -v
```

Expected: All 5 circ_mv_lt tests pass.

- [ ] **Step 5: Commit**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week4 && git add src/rquant/screen/rules.py src/rquant/screen/__init__.py tests/unit/test_screen_rules.py && git commit -m "feat(rules): add circ_mv_lt rule for market cap filtering

circ_mv_lt(threshold_yi) filters by circulating market value.
User-facing unit: 亿元; internal conversion: ×10000 to match Tushare 万元.
NaN circ_mv is treated as infinity (fails the filter)."
```

---

### Task 5: has_lower_shadow rule

**Files:**
- Modify: `src/rquant/screen/rules.py`
- Modify: `src/rquant/screen/__init__.py`
- Modify: `tests/unit/test_screen_rules.py`

- [ ] **Step 1: Add has_lower_shadow() to rules.py**

In `src/rquant/screen/rules.py`, after `circ_mv_lt()` (added in Task 4), add:

```python
def has_lower_shadow(
    min_ratio: float = 1.5,
    min_amplitude: float = 0.02,
    offset: int = 0,
) -> Rule:
    """下影线达标：下影 / 实体 ≥ min_ratio 且振幅 ≥ min_amplitude。

    - 下影线 = BODY_LOWER[offset] - LOW[offset]
    - 实体 = BODY_UPPER[offset] - BODY_LOWER[offset]
    - 振幅 = (HIGH[offset] - LOW[offset]) / LOW[offset]
    - 实体为 0（一字线/十字星）直接返回 False
    """
    def _rule(df: pd.DataFrame) -> pd.Series:
        body_lower = df[f"BODY_LOWER[{offset}]"]
        body_upper = df[f"BODY_UPPER[{offset}]"]
        low = df[f"LOW[{offset}]"]
        high = df[f"HIGH[{offset}]"]

        lower_shadow = body_lower - low
        body = body_upper - body_lower
        amplitude = (high - low) / low.replace(0, float("nan"))

        has_body = body > 0
        ratio_ok = lower_shadow / body.replace(0, float("nan")) >= min_ratio
        amp_ok = amplitude >= min_amplitude

        return has_body & ratio_ok & amp_ok

    return _tag_lookback(_rule, offset)
```

- [ ] **Step 2: Export has_lower_shadow from __init__.py**

Add `has_lower_shadow` to imports and `__all__` in `src/rquant/screen/__init__.py`.

Add to imports (alphabetical, after `gt,` or after `gte,`):

```python
    has_lower_shadow,
```

Add to `__all__` (after `"consecutive_ups_gte",` or similar logical grouping):

```python
    "has_lower_shadow",
```

- [ ] **Step 3: Write has_lower_shadow tests**

In `tests/unit/test_screen_rules.py`, add `has_lower_shadow` to imports:

```python
    has_lower_shadow,
```

Then add a new test class:

```python
class TestHasLowerShadow:
    def test_clear_lower_shadow_passes(self) -> None:
        """O=10, H=11, L=8, C=10.5 → body_upper=10.5, body_lower=10,
        lower_shadow=10-8=2, body=10.5-10=0.5, ratio=4.0 ≥ 1.5,
        amplitude=(11-8)/8=0.375 ≥ 0.02 → passes."""
        df = make_wide_frame(
            overrides={
                ("300001.SZ", "OPEN[0]"): 10.0,
                ("300001.SZ", "HIGH[0]"): 11.0,
                ("300001.SZ", "LOW[0]"): 8.0,
                ("300001.SZ", "CLOSE[0]"): 10.5,
                ("300001.SZ", "BODY_UPPER[0]"): 10.5,
                ("300001.SZ", "BODY_LOWER[0]"): 10.0,
            },
        )
        mask = has_lower_shadow(1.5, 0.02, 0)(df)
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]

    def test_no_lower_shadow_fails(self) -> None:
        """O=10, H=12, L=10, C=11 → body_upper=11, body_lower=10,
        lower_shadow=10-10=0, ratio=0 < 1.5 → fails."""
        df = make_wide_frame(
            overrides={
                ("300001.SZ", "OPEN[0]"): 10.0,
                ("300001.SZ", "HIGH[0]"): 12.0,
                ("300001.SZ", "LOW[0]"): 10.0,
                ("300001.SZ", "CLOSE[0]"): 11.0,
                ("300001.SZ", "BODY_UPPER[0]"): 11.0,
                ("300001.SZ", "BODY_LOWER[0]"): 10.0,
            },
        )
        mask = has_lower_shadow(1.5, 0.02, 0)(df)
        assert not mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]

    def test_doji_zero_body_fails(self) -> None:
        """一字线/十字星: body=0 → has_body=False → fails regardless of shadow."""
        df = make_wide_frame(
            overrides={
                ("300001.SZ", "HIGH[0]"): 11.0,
                ("300001.SZ", "LOW[0]"): 9.0,
                ("300001.SZ", "BODY_UPPER[0]"): 10.0,
                ("300001.SZ", "BODY_LOWER[0]"): 10.0,
            },
        )
        mask = has_lower_shadow(1.5, 0.02, 0)(df)
        assert not mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]

    def test_small_amplitude_fails(self) -> None:
        """Shadow ratio OK but amplitude < 0.02 → fails.
        O=10, H=10.1, L=9.95, C=10.05 → body_upper=10.05, body_lower=10,
        lower_shadow=10-9.95=0.05, body=0.05, ratio=1.0 (< 1.5 anyway, but let's
        test with min_ratio=0.5).
        amplitude=(10.1-9.95)/9.95=0.015 < 0.02 → fails."""
        df = make_wide_frame(
            overrides={
                ("300001.SZ", "HIGH[0]"): 10.1,
                ("300001.SZ", "LOW[0]"): 9.95,
                ("300001.SZ", "BODY_UPPER[0]"): 10.05,
                ("300001.SZ", "BODY_LOWER[0]"): 10.0,
            },
        )
        mask = has_lower_shadow(0.5, 0.02, 0)(df)
        assert not mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]

    def test_ratio_below_threshold_fails(self) -> None:
        """lower_shadow / body < min_ratio → fails.
        body_upper=11, body_lower=10, low=9.8 → shadow=0.2, body=1.0, ratio=0.2 < 1.5."""
        df = make_wide_frame(
            overrides={
                ("300001.SZ", "HIGH[0]"): 12.0,
                ("300001.SZ", "LOW[0]"): 9.8,
                ("300001.SZ", "BODY_UPPER[0]"): 11.0,
                ("300001.SZ", "BODY_LOWER[0]"): 10.0,
            },
        )
        mask = has_lower_shadow(1.5, 0.02, 0)(df)
        assert not mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]

    def test_at_offset_1(self) -> None:
        df = make_wide_frame(
            lookback=2,
            overrides={
                ("300001.SZ", "HIGH[1]"): 11.0,
                ("300001.SZ", "LOW[1]"): 8.0,
                ("300001.SZ", "BODY_UPPER[1]"): 10.5,
                ("300001.SZ", "BODY_LOWER[1]"): 10.0,
            },
        )
        rule = has_lower_shadow(1.5, 0.02, offset=1)
        mask = rule(df)
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert rule.min_lookback == 1

    def test_custom_thresholds(self) -> None:
        """With min_ratio=2.0 (TA-Lib hammer standard):
        shadow=2, body=0.5, ratio=4.0 ≥ 2.0 → passes."""
        df = make_wide_frame(
            overrides={
                ("300001.SZ", "HIGH[0]"): 11.0,
                ("300001.SZ", "LOW[0]"): 8.0,
                ("300001.SZ", "BODY_UPPER[0]"): 10.5,
                ("300001.SZ", "BODY_LOWER[0]"): 10.0,
            },
        )
        mask = has_lower_shadow(min_ratio=2.0, min_amplitude=0.01)(df)
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]
```

- [ ] **Step 4: Run tests and verify**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week4 && uv run pytest tests/unit/test_screen_rules.py::TestHasLowerShadow -v
```

Expected: All 7 has_lower_shadow tests pass.

- [ ] **Step 5: Commit**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week4 && git add src/rquant/screen/rules.py src/rquant/screen/__init__.py tests/unit/test_screen_rules.py && git commit -m "feat(rules): add has_lower_shadow rule for candlestick pattern detection

Computes lower_shadow/body ratio and amplitude from BODY_UPPER/BODY_LOWER/HIGH/LOW.
Zero-body candles (doji/yiziban) always fail. NaN-safe division."
```

---

### Task 6: AggregateRequest infrastructure

**Files:**
- Modify: `src/rquant/screen/rules.py`
- Modify: `src/rquant/screen/core.py`
- Modify: `src/rquant/screen/loader.py`
- Modify: `src/rquant/screen/__init__.py`
- Modify: `tests/unit/test_screen_core.py`
- Modify: `tests/unit/test_screen_loader.py`

- [ ] **Step 1: Add AggregateRequest dataclass and _tag_aggregates helper to rules.py**

In `src/rquant/screen/rules.py`, add `dataclass` import at the top. After the existing imports (line 1-8), update:

```python
from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field

import pandas as pd
```

Then after the `Rule` type alias (line 10) and `_tag_lookback` function (line 13-16), add:

```python
@dataclass(frozen=True)
class AggregateRequest:
    """规则声明的长窗口聚合需求，由 load_universe() 执行 SQL 实现。"""

    name: str               # 结果列名，如 "max_consec_ups_8d"
    source_table: str       # "daily_state" | "daily_bar" | "daily_basic"
    source_col: str         # "consecutive_limit_ups" | "is_limit_down" 等
    agg_func: str           # "max" | "sum" | "any" | "count_nonzero"
    window: int             # 交易日窗口大小
    exclude_offset: int | None = None  # 排除某个 offset 的日期（从 T 日起算）


def _tag_aggregates(fn: Rule, requests: list[AggregateRequest]) -> Rule:
    """给规则函数挂上 aggregate_requests 属性。"""
    fn.aggregate_requests = requests  # type: ignore[attr-defined]
    return fn
```

- [ ] **Step 2: Modify core.py to collect aggregate_requests and pass to load_universe**

Replace the entire `src/rquant/screen/core.py` with:

```python
"""screen() 主流程：加载宽表 → 应用规则 → 返回结果。"""

from __future__ import annotations

import pandas as pd

from rquant.screen.loader import load_universe
from rquant.screen.rules import AggregateRequest, Rule
from rquant.storage.duckdb import DuckDBStore

BASE_COLUMNS = ["ts_code", "name", "CLOSE[0]", "PCT_CHG[0]"]


def _infer_lookback(rules: list[Rule]) -> int:
    return max((getattr(r, "min_lookback", 0) for r in rules), default=0)


def _collect_aggregates(rules: list[Rule]) -> list[AggregateRequest]:
    """从所有规则中收集去重后的 AggregateRequest 列表。"""
    seen: set[str] = set()
    result: list[AggregateRequest] = []
    for rule in rules:
        for req in getattr(rule, "aggregate_requests", []):
            if req.name not in seen:
                seen.add(req.name)
                result.append(req)
    return result


def screen(
    trade_date: str,
    rules: list[Rule],
    lookback: int | None = None,
    include_columns: list[str] | None = None,
    store: DuckDBStore | None = None,
) -> pd.DataFrame:
    """筛选：给定 trade_date 和 rules，返回命中股票。

    - rules 列表内部按 AND 合并
    - lookback 默认按 rules 的 min_lookback 推断，最小 0
    - include_columns 控制结果附加列（base 列 ts_code/name/CLOSE[0]/PCT_CHG[0] 必出）
    """
    if lookback is None:
        lookback = _infer_lookback(rules)

    aggregates = _collect_aggregates(rules)

    df = load_universe(
        trade_date, lookback=lookback, store=store, aggregate_requests=aggregates
    )

    if df.empty:
        cols = list(BASE_COLUMNS)
        if include_columns:
            cols += [c for c in include_columns if c not in cols]
        return pd.DataFrame(columns=cols)

    mask = pd.Series(True, index=df.index)
    for rule in rules:
        mask &= rule(df)

    result = df.loc[mask].copy()

    cols = list(BASE_COLUMNS)
    if include_columns:
        cols += [c for c in include_columns if c not in cols]
    cols = [c for c in cols if c in result.columns]

    return result[cols].sort_values("ts_code").reset_index(drop=True)
```

- [ ] **Step 3: Modify load_universe() signature and add aggregate SQL generation**

In `src/rquant/screen/loader.py`, add the `AggregateRequest` import at the top. After line 8 (`from rquant.storage.duckdb import DuckDBStore`), add:

```python
from rquant.screen.rules import AggregateRequest
```

Then change the `load_universe()` signature (line 83-87) to accept aggregate_requests:

```python
def load_universe(
    trade_date: str,
    lookback: int = 5,
    store: DuckDBStore | None = None,
    aggregate_requests: list[AggregateRequest] | None = None,
) -> pd.DataFrame:
```

At the end of `load_universe()`, after all the existing merges but before the default value fills (before line 165 `if "is_st" in out.columns:`), add the aggregate column computation:

```python
        # 聚合列：根据 AggregateRequest 动态生成 SQL
        if aggregate_requests:
            t0_date_val = dates[0]  # T 日
            for req in aggregate_requests:
                agg_col = _compute_aggregate(store, req, t0_date_val, in_universe)
                if not agg_col.empty:
                    out = out.merge(agg_col, on="ts_code", how="left")
```

Then add the `_compute_aggregate()` helper function before `load_universe()` (after `_wide_from_long`, around line 81):

```python
def _compute_aggregate(
    store: DuckDBStore,
    req: AggregateRequest,
    t0_date: str,
    ts_codes: list[str],
) -> pd.DataFrame:
    """根据 AggregateRequest 生成 DuckDB SQL，返回 (ts_code, <agg_col>) DataFrame。"""
    placeholders = ",".join(["?"] * len(ts_codes))

    # 找到 T 日前 window 个交易日的日期范围
    date_sql = """
    SELECT strftime(trade_date, '%Y-%m-%d') AS d
    FROM (
        SELECT DISTINCT trade_date FROM daily_bar
        WHERE trade_date <= ?
        ORDER BY trade_date DESC
        LIMIT ?
    )
    ORDER BY d
    """
    date_rows = store._conn.execute(date_sql, [t0_date, req.window]).fetchall()
    window_dates = [r[0] for r in date_rows]

    if not window_dates:
        return pd.DataFrame(columns=["ts_code", req.name])

    # 排除 exclude_offset 对应的日期
    if req.exclude_offset is not None:
        # 获取完整日期列表（倒序），找到 offset 对应的日期
        all_dates_sql = """
        SELECT strftime(trade_date, '%Y-%m-%d') AS d
        FROM (
            SELECT DISTINCT trade_date FROM daily_bar
            WHERE trade_date <= ?
            ORDER BY trade_date DESC
            LIMIT ?
        )
        ORDER BY d DESC
        """
        all_date_rows = store._conn.execute(
            all_dates_sql, [t0_date, req.window]
        ).fetchall()
        all_dates_desc = [r[0] for r in all_date_rows]
        if req.exclude_offset < len(all_dates_desc):
            exclude_date = all_dates_desc[req.exclude_offset]
            window_dates = [d for d in window_dates if d != exclude_date]

    if not window_dates:
        return pd.DataFrame(columns=["ts_code", req.name])

    date_placeholders = ",".join(["?"] * len(window_dates))

    # 根据 agg_func 生成 SQL
    if req.agg_func == "max":
        agg_expr = f"MAX({req.source_col})"
    elif req.agg_func == "sum":
        agg_expr = f"SUM({req.source_col})"
    elif req.agg_func == "any":
        agg_expr = f"BOOL_OR(CAST({req.source_col} AS BOOLEAN))"
    elif req.agg_func == "count_nonzero":
        agg_expr = f"SUM(CASE WHEN CAST({req.source_col} AS BOOLEAN) THEN 1 ELSE 0 END)"
    else:
        raise ValueError(f"Unsupported agg_func: {req.agg_func}")

    sql = f"""
    SELECT ts_code, {agg_expr} AS {req.name}
    FROM {req.source_table}
    WHERE ts_code IN ({placeholders})
      AND strftime(trade_date, '%Y-%m-%d') IN ({date_placeholders})
    GROUP BY ts_code
    """
    params = ts_codes + window_dates
    result = store._conn.execute(sql, params).fetchdf()
    return result
```

- [ ] **Step 4: Export AggregateRequest from __init__.py**

In `src/rquant/screen/__init__.py`, add:

To imports:
```python
from rquant.screen.rules import AggregateRequest
```

To `__all__`:
```python
    "AggregateRequest",
```

- [ ] **Step 5: Write tests for aggregate infrastructure in test_screen_core.py**

In `tests/unit/test_screen_core.py`, update imports:

```python
from unittest.mock import patch

from rquant.screen import screen
from rquant.screen.rules import AggregateRequest, Rule, gt, not_bj, not_st, _tag_lookback, _tag_aggregates
from tests.fixtures.wide_frames import make_wide_frame
```

Then add a new test class:

```python
class TestAggregateCollection:
    def test_collect_aggregates_from_rules(self) -> None:
        """Rules with aggregate_requests should have them collected by _collect_aggregates."""
        from rquant.screen.core import _collect_aggregates

        def dummy_rule(df):
            return df["ts_code"].notna()

        dummy_rule = _tag_lookback(dummy_rule, 0)
        req = AggregateRequest(
            name="max_consec_ups_8d",
            source_table="daily_state",
            source_col="consecutive_limit_ups",
            agg_func="max",
            window=8,
        )
        dummy_rule = _tag_aggregates(dummy_rule, [req])

        aggregates = _collect_aggregates([not_st(), dummy_rule])
        assert len(aggregates) == 1
        assert aggregates[0].name == "max_consec_ups_8d"

    def test_collect_aggregates_deduplicates(self) -> None:
        from rquant.screen.core import _collect_aggregates

        req = AggregateRequest(
            name="same_name", source_table="daily_state",
            source_col="x", agg_func="max", window=5,
        )

        def r1(df):
            return df["ts_code"].notna()
        r1 = _tag_lookback(r1, 0)
        r1 = _tag_aggregates(r1, [req])

        def r2(df):
            return df["ts_code"].notna()
        r2 = _tag_lookback(r2, 0)
        r2 = _tag_aggregates(r2, [req])

        aggregates = _collect_aggregates([r1, r2])
        assert len(aggregates) == 1

    def test_collect_aggregates_empty_when_no_requests(self) -> None:
        from rquant.screen.core import _collect_aggregates

        aggregates = _collect_aggregates([not_st(), not_bj()])
        assert aggregates == []

    def test_screen_passes_aggregates_to_loader(self) -> None:
        """Verify screen() extracts aggregate_requests and passes them to load_universe."""
        df = make_wide_frame()
        # Add a dummy aggregate column to the frame
        df["max_consec_ups_8d"] = 0

        req = AggregateRequest(
            name="max_consec_ups_8d",
            source_table="daily_state",
            source_col="consecutive_limit_ups",
            agg_func="max",
            window=8,
        )

        def rule_with_agg(df):
            return df["max_consec_ups_8d"] < 3
        rule_with_agg = _tag_lookback(rule_with_agg, 0)
        rule_with_agg = _tag_aggregates(rule_with_agg, [req])

        with patch("rquant.screen.core.load_universe") as mock_loader:
            mock_loader.return_value = df
            screen(trade_date="2026-04-15", rules=[rule_with_agg])
            call_kwargs = mock_loader.call_args.kwargs
            assert "aggregate_requests" in call_kwargs
            assert len(call_kwargs["aggregate_requests"]) == 1
            assert call_kwargs["aggregate_requests"][0].name == "max_consec_ups_8d"
```

- [ ] **Step 6: Write aggregate SQL test in test_screen_loader.py**

In `tests/unit/test_screen_loader.py`, add `AggregateRequest` to imports:

```python
from rquant.screen.rules import AggregateRequest
```

Then extend the existing `store` fixture to include more state data for aggregate testing. Add after the existing `s.upsert_daily_basic(daily_basic_data)` block (added in Task 2), before `yield s`:

```python
    # Extra state data for aggregate testing (older dates need daily_bar too)
    extra_daily = pd.DataFrame([
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 7), "open": 8.0,
         "high": 9.0, "low": 7.5, "close": 8.5, "pre_close": 8.0,
         "change": 0.5, "pct_chg": 6.25, "vol": 800.0, "amount": 6800.0},
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 8), "open": 8.5,
         "high": 9.5, "low": 8.0, "close": 9.0, "pre_close": 8.5,
         "change": 0.5, "pct_chg": 5.88, "vol": 900.0, "amount": 8100.0},
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 9), "open": 9.0,
         "high": 10.0, "low": 9.0, "close": 9.5, "pre_close": 9.0,
         "change": 0.5, "pct_chg": 5.56, "vol": 950.0, "amount": 9025.0},
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 10), "open": 9.5,
         "high": 10.5, "low": 9.0, "close": 10.0, "pre_close": 9.5,
         "change": 0.5, "pct_chg": 5.26, "vol": 1100.0, "amount": 11000.0},
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 11), "open": 10.0,
         "high": 11.0, "low": 9.5, "close": 10.5, "pre_close": 10.0,
         "change": 0.5, "pct_chg": 5.0, "vol": 1050.0, "amount": 11025.0},
    ])
    s.upsert_daily(extra_daily)

    extra_state = pd.DataFrame([
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 7),
         "is_st": False, "is_bj": False, "board_type": "gem",
         "limit_pct": 0.20, "limit_up_price": 9.60, "limit_down_price": 6.40,
         "is_limit_up": True, "is_limit_down": False, "is_first_limit_up": True,
         "is_yiziban": False, "consecutive_limit_ups": 1,
         "body_upper": 8.5, "body_lower": 8.0},
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 8),
         "is_st": False, "is_bj": False, "board_type": "gem",
         "limit_pct": 0.20, "limit_up_price": 10.20, "limit_down_price": 6.80,
         "is_limit_up": True, "is_limit_down": False, "is_first_limit_up": False,
         "is_yiziban": False, "consecutive_limit_ups": 2,
         "body_upper": 9.0, "body_lower": 8.5},
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 9),
         "is_st": False, "is_bj": False, "board_type": "gem",
         "limit_pct": 0.20, "limit_up_price": 10.80, "limit_down_price": 7.20,
         "is_limit_up": False, "is_limit_down": False, "is_first_limit_up": False,
         "is_yiziban": False, "consecutive_limit_ups": 0,
         "body_upper": 9.5, "body_lower": 9.0},
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 10),
         "is_st": False, "is_bj": False, "board_type": "gem",
         "limit_pct": 0.20, "limit_up_price": 11.40, "limit_down_price": 7.60,
         "is_limit_up": False, "is_limit_down": True, "is_first_limit_up": False,
         "is_yiziban": False, "consecutive_limit_ups": 0,
         "body_upper": 10.0, "body_lower": 9.5},
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 11),
         "is_st": False, "is_bj": False, "board_type": "gem",
         "limit_pct": 0.20, "limit_up_price": 12.00, "limit_down_price": 8.00,
         "is_limit_up": False, "is_limit_down": False, "is_first_limit_up": False,
         "is_yiziban": False, "consecutive_limit_ups": 0,
         "body_upper": 10.5, "body_lower": 10.0},
    ])
    s.upsert_state(extra_state)
```

Then add aggregate tests:

```python
class TestLoadUniverseAggregates:
    def test_max_aggregate(self, store: DuckDBStore) -> None:
        """300001.SZ has consecutive_limit_ups: [1,2,0,0,0,...,0,1] over window.
        Max in 8-day window ending 4/15 should be 2."""
        req = AggregateRequest(
            name="max_consec_ups_8d",
            source_table="daily_state",
            source_col="consecutive_limit_ups",
            agg_func="max",
            window=8,
        )
        df = load_universe(
            "2026-04-15", lookback=1, store=store, aggregate_requests=[req]
        )
        assert "max_consec_ups_8d" in df.columns
        row = df.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert row["max_consec_ups_8d"] == 2

    def test_any_aggregate(self, store: DuckDBStore) -> None:
        """300001.SZ has is_limit_down=True on 4/10. any in 8-day window should be True."""
        req = AggregateRequest(
            name="has_limit_down_8d",
            source_table="daily_state",
            source_col="is_limit_down",
            agg_func="any",
            window=8,
        )
        df = load_universe(
            "2026-04-15", lookback=1, store=store, aggregate_requests=[req]
        )
        row = df.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert row["has_limit_down_8d"] is True or row["has_limit_down_8d"] == True

    def test_count_nonzero_aggregate(self, store: DuckDBStore) -> None:
        """300001.SZ has is_limit_up=True on 4/7, 4/8, 4/15. Count in 8-day window should be >=2."""
        req = AggregateRequest(
            name="count_limit_up_8d",
            source_table="daily_state",
            source_col="is_limit_up",
            agg_func="count_nonzero",
            window=8,
        )
        df = load_universe(
            "2026-04-15", lookback=1, store=store, aggregate_requests=[req]
        )
        row = df.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert row["count_limit_up_8d"] >= 2

    def test_count_nonzero_with_exclude_offset(self, store: DuckDBStore) -> None:
        """Exclude offset=0 (4/15, which has is_limit_up=True for 300001.SZ).
        Count should decrease by 1."""
        req_with = AggregateRequest(
            name="count_limit_up_8d",
            source_table="daily_state",
            source_col="is_limit_up",
            agg_func="count_nonzero",
            window=8,
        )
        req_without = AggregateRequest(
            name="count_limit_up_8d_ex0",
            source_table="daily_state",
            source_col="is_limit_up",
            agg_func="count_nonzero",
            window=8,
            exclude_offset=0,
        )
        df = load_universe(
            "2026-04-15", lookback=1, store=store,
            aggregate_requests=[req_with, req_without],
        )
        row = df.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert row["count_limit_up_8d_ex0"] == row["count_limit_up_8d"] - 1

    def test_empty_aggregates_no_extra_columns(self, store: DuckDBStore) -> None:
        df = load_universe("2026-04-15", lookback=1, store=store, aggregate_requests=[])
        assert "max_consec_ups_8d" not in df.columns

    def test_no_aggregates_param_backward_compatible(self, store: DuckDBStore) -> None:
        """Calling without aggregate_requests should work as before."""
        df = load_universe("2026-04-15", lookback=1, store=store)
        assert "CLOSE[0]" in df.columns
```

- [ ] **Step 7: Run tests and verify**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week4 && uv run pytest tests/unit/test_screen_core.py tests/unit/test_screen_loader.py -v
```

Expected: All existing tests pass + new aggregate tests pass. `_collect_aggregates` correctly collects and deduplicates. `load_universe` generates correct aggregate SQL.

- [ ] **Step 8: Commit**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week4 && git add src/rquant/screen/rules.py src/rquant/screen/core.py src/rquant/screen/loader.py src/rquant/screen/__init__.py tests/unit/test_screen_core.py tests/unit/test_screen_loader.py && git commit -m "feat(screen): add AggregateRequest infrastructure for long-window rule needs

AggregateRequest dataclass lets rules declare SQL aggregate needs.
screen() collects requests from rules, passes to load_universe().
load_universe() generates DuckDB SQL per request (max/any/count_nonzero/sum).
Supports exclude_offset for skipping specific days."
```

---

### Task 7: Window-scan rules

**Files:**
- Modify: `src/rquant/screen/rules.py`
- Modify: `src/rquant/screen/__init__.py`
- Modify: `tests/unit/test_screen_rules.py`
- Modify: `tests/unit/test_screen_loader.py`

- [ ] **Step 1: Add no_consec_ups_in_window() to rules.py**

In `src/rquant/screen/rules.py`, after the `has_lower_shadow()` function (added in Task 5), add:

```python
def no_consec_ups_in_window(threshold: int = 3, window: int = 8) -> Rule:
    """近 window 日内无 threshold 连板（含）以上。

    声明 AggregateRequest：近 window 日 consecutive_limit_ups 的 max。
    规则：max_value < threshold。
    """
    agg_name = f"max_consec_ups_{window}d"
    req = AggregateRequest(
        name=agg_name,
        source_table="daily_state",
        source_col="consecutive_limit_ups",
        agg_func="max",
        window=window,
    )

    def _rule(df: pd.DataFrame) -> pd.Series:
        return df[agg_name].fillna(0) < threshold

    fn = _tag_lookback(_rule, 0)
    fn = _tag_aggregates(fn, [req])
    return fn
```

- [ ] **Step 2: Add no_limit_down_in_window() to rules.py**

After `no_consec_ups_in_window()`, add:

```python
def no_limit_down_in_window(window: int = 30) -> Rule:
    """近 window 日无跌停。

    声明 AggregateRequest：近 window 日 is_limit_down 的 any（BOOL_OR）。
    规则：has_limit_down == False。
    """
    agg_name = f"has_limit_down_{window}d"
    req = AggregateRequest(
        name=agg_name,
        source_table="daily_state",
        source_col="is_limit_down",
        agg_func="any",
        window=window,
    )

    def _rule(df: pd.DataFrame) -> pd.Series:
        return ~df[agg_name].fillna(False).astype(bool)

    fn = _tag_lookback(_rule, 0)
    fn = _tag_aggregates(fn, [req])
    return fn
```

- [ ] **Step 3: Add has_prior_limit_up() to rules.py**

After `no_limit_down_in_window()`, add:

```python
def has_prior_limit_up(window: int = 90, exclude_offset: int = 1) -> Rule:
    """近 window 日内（排除 T-exclude_offset 日）至少有 1 次涨停。

    声明 AggregateRequest：近 window 日 is_limit_up 的 count_nonzero，排除 exclude_offset。
    规则：count >= 1。
    """
    agg_name = f"count_limit_up_{window}d_ex{exclude_offset}"
    req = AggregateRequest(
        name=agg_name,
        source_table="daily_state",
        source_col="is_limit_up",
        agg_func="count_nonzero",
        window=window,
        exclude_offset=exclude_offset,
    )

    def _rule(df: pd.DataFrame) -> pd.Series:
        return df[agg_name].fillna(0) >= 1

    fn = _tag_lookback(_rule, 0)
    fn = _tag_aggregates(fn, [req])
    return fn
```

- [ ] **Step 4: Export 3 new rules from __init__.py**

In `src/rquant/screen/__init__.py`, add to imports:

```python
    has_prior_limit_up,
    no_consec_ups_in_window,
    no_limit_down_in_window,
```

Add to `__all__`:

```python
    "no_consec_ups_in_window", "no_limit_down_in_window", "has_prior_limit_up",
```

- [ ] **Step 5: Write unit tests for window rules (mock-based, using make_wide_frame)**

In `tests/unit/test_screen_rules.py`, add to imports:

```python
    has_prior_limit_up,
    no_consec_ups_in_window,
    no_limit_down_in_window,
```

Then add test classes:

```python
class TestNoConsecUpsInWindow:
    def test_passes_when_max_below_threshold(self) -> None:
        """max_consec_ups_8d=2 < threshold=3 → passes."""
        df = make_wide_frame()
        df["max_consec_ups_8d"] = 2
        rule = no_consec_ups_in_window(threshold=3, window=8)
        mask = rule(df)
        assert mask.all()

    def test_fails_when_max_equals_threshold(self) -> None:
        """max_consec_ups_8d=3 NOT < threshold=3 → fails."""
        df = make_wide_frame()
        df["max_consec_ups_8d"] = 3
        rule = no_consec_ups_in_window(threshold=3, window=8)
        mask = rule(df)
        assert not mask.any()

    def test_fails_when_max_exceeds_threshold(self) -> None:
        df = make_wide_frame()
        df["max_consec_ups_8d"] = 5
        rule = no_consec_ups_in_window(threshold=3, window=8)
        mask = rule(df)
        assert not mask.any()

    def test_nan_treated_as_zero(self) -> None:
        df = make_wide_frame()
        df["max_consec_ups_8d"] = float("nan")
        rule = no_consec_ups_in_window(threshold=3, window=8)
        mask = rule(df)
        assert mask.all()

    def test_has_aggregate_request(self) -> None:
        rule = no_consec_ups_in_window(threshold=3, window=8)
        assert hasattr(rule, "aggregate_requests")
        assert len(rule.aggregate_requests) == 1
        req = rule.aggregate_requests[0]
        assert req.name == "max_consec_ups_8d"
        assert req.agg_func == "max"
        assert req.window == 8
        assert req.source_col == "consecutive_limit_ups"

    def test_custom_window(self) -> None:
        rule = no_consec_ups_in_window(threshold=2, window=5)
        assert rule.aggregate_requests[0].name == "max_consec_ups_5d"
        assert rule.aggregate_requests[0].window == 5


class TestNoLimitDownInWindow:
    def test_passes_when_no_limit_down(self) -> None:
        df = make_wide_frame()
        df["has_limit_down_30d"] = False
        rule = no_limit_down_in_window(window=30)
        mask = rule(df)
        assert mask.all()

    def test_fails_when_has_limit_down(self) -> None:
        df = make_wide_frame()
        df["has_limit_down_30d"] = True
        rule = no_limit_down_in_window(window=30)
        mask = rule(df)
        assert not mask.any()

    def test_nan_treated_as_no_limit_down(self) -> None:
        df = make_wide_frame()
        df["has_limit_down_30d"] = float("nan")
        rule = no_limit_down_in_window(window=30)
        mask = rule(df)
        assert mask.all()

    def test_has_aggregate_request(self) -> None:
        rule = no_limit_down_in_window(window=30)
        assert len(rule.aggregate_requests) == 1
        req = rule.aggregate_requests[0]
        assert req.name == "has_limit_down_30d"
        assert req.agg_func == "any"
        assert req.window == 30

    def test_custom_window(self) -> None:
        rule = no_limit_down_in_window(window=10)
        assert rule.aggregate_requests[0].name == "has_limit_down_10d"


class TestHasPriorLimitUp:
    def test_passes_when_has_prior_limit_up(self) -> None:
        df = make_wide_frame()
        df["count_limit_up_90d_ex1"] = 2
        rule = has_prior_limit_up(window=90, exclude_offset=1)
        mask = rule(df)
        assert mask.all()

    def test_fails_when_no_prior_limit_up(self) -> None:
        df = make_wide_frame()
        df["count_limit_up_90d_ex1"] = 0
        rule = has_prior_limit_up(window=90, exclude_offset=1)
        mask = rule(df)
        assert not mask.any()

    def test_boundary_exactly_one(self) -> None:
        df = make_wide_frame()
        df["count_limit_up_90d_ex1"] = 1
        rule = has_prior_limit_up(window=90, exclude_offset=1)
        mask = rule(df)
        assert mask.all()

    def test_nan_treated_as_zero(self) -> None:
        df = make_wide_frame()
        df["count_limit_up_90d_ex1"] = float("nan")
        rule = has_prior_limit_up(window=90, exclude_offset=1)
        mask = rule(df)
        assert not mask.any()

    def test_has_aggregate_request_with_exclude(self) -> None:
        rule = has_prior_limit_up(window=90, exclude_offset=1)
        assert len(rule.aggregate_requests) == 1
        req = rule.aggregate_requests[0]
        assert req.name == "count_limit_up_90d_ex1"
        assert req.agg_func == "count_nonzero"
        assert req.window == 90
        assert req.exclude_offset == 1

    def test_custom_params(self) -> None:
        rule = has_prior_limit_up(window=30, exclude_offset=2)
        req = rule.aggregate_requests[0]
        assert req.name == "count_limit_up_30d_ex2"
        assert req.window == 30
        assert req.exclude_offset == 2
```

- [ ] **Step 6: Write integration test using DuckDB fixture**

In `tests/unit/test_screen_loader.py`, add imports:

```python
from rquant.screen.rules import no_consec_ups_in_window, no_limit_down_in_window, has_prior_limit_up
```

Then add:

```python
class TestWindowRulesIntegration:
    """End-to-end test: rule declares aggregate → loader generates SQL → rule evaluates."""

    def test_no_consec_ups_in_window_integration(self, store: DuckDBStore) -> None:
        """300001.SZ has max consecutive_limit_ups=2 in 8d window. threshold=3 → passes."""
        rule = no_consec_ups_in_window(threshold=3, window=8)
        reqs = rule.aggregate_requests
        df = load_universe("2026-04-15", lookback=0, store=store, aggregate_requests=reqs)
        mask = rule(df)
        row_mask = mask.loc[df["ts_code"] == "300001.SZ"]
        assert row_mask.iloc[0]

    def test_no_consec_ups_in_window_fails(self, store: DuckDBStore) -> None:
        """threshold=2: max_consec=2 NOT < 2 → fails."""
        rule = no_consec_ups_in_window(threshold=2, window=8)
        reqs = rule.aggregate_requests
        df = load_universe("2026-04-15", lookback=0, store=store, aggregate_requests=reqs)
        mask = rule(df)
        row_mask = mask.loc[df["ts_code"] == "300001.SZ"]
        assert not row_mask.iloc[0]

    def test_no_limit_down_in_window_integration(self, store: DuckDBStore) -> None:
        """300001.SZ has is_limit_down=True on 4/10. Window=8 covers 4/10 → fails."""
        rule = no_limit_down_in_window(window=8)
        reqs = rule.aggregate_requests
        df = load_universe("2026-04-15", lookback=0, store=store, aggregate_requests=reqs)
        mask = rule(df)
        row_mask = mask.loc[df["ts_code"] == "300001.SZ"]
        assert not row_mask.iloc[0]

    def test_no_limit_down_passes_for_clean_stock(self, store: DuckDBStore) -> None:
        """000001.SZ has no limit_down in any date → passes."""
        rule = no_limit_down_in_window(window=8)
        reqs = rule.aggregate_requests
        df = load_universe("2026-04-15", lookback=0, store=store, aggregate_requests=reqs)
        mask = rule(df)
        row_mask = mask.loc[df["ts_code"] == "000001.SZ"]
        assert row_mask.iloc[0]

    def test_has_prior_limit_up_integration(self, store: DuckDBStore) -> None:
        """300001.SZ has limit_up on 4/7 and 4/8 (excluding 4/15 at offset=0).
        With window=8, exclude_offset=0 → count >= 1 → passes."""
        rule = has_prior_limit_up(window=8, exclude_offset=0)
        reqs = rule.aggregate_requests
        df = load_universe("2026-04-15", lookback=0, store=store, aggregate_requests=reqs)
        mask = rule(df)
        row_mask = mask.loc[df["ts_code"] == "300001.SZ"]
        assert row_mask.iloc[0]
```

- [ ] **Step 7: Run tests and verify**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week4 && uv run pytest tests/unit/test_screen_rules.py::TestNoConsecUpsInWindow tests/unit/test_screen_rules.py::TestNoLimitDownInWindow tests/unit/test_screen_rules.py::TestHasPriorLimitUp tests/unit/test_screen_loader.py::TestWindowRulesIntegration -v
```

Expected: All 18 unit tests + 5 integration tests pass.

Then run the full test suite:

```bash
cd /Users/roxor/brain/30-projects/rQuant-week4 && uv run pytest tests/ -v
```

Expected: All tests pass (existing + new).

- [ ] **Step 8: Commit**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week4 && git add src/rquant/screen/rules.py src/rquant/screen/__init__.py tests/unit/test_screen_rules.py tests/unit/test_screen_loader.py && git commit -m "feat(rules): add 3 window-scan rules using AggregateRequest

no_consec_ups_in_window(threshold, window) — max consecutive_limit_ups < threshold.
no_limit_down_in_window(window) — no is_limit_down=True in window.
has_prior_limit_up(window, exclude_offset) — at least 1 limit_up excluding offset day.
Each rule declares AggregateRequest; loader generates SQL; rule evaluates result."
```

---

### Task 8: ingest update + exports + CHANGELOG + smoke + tag

**Files:**
- Modify: `scripts/ingest_daily.py`
- Modify: `src/rquant/screen/__init__.py` (verify final exports)
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Update ingest_daily.py to fetch daily_basic**

In `scripts/ingest_daily.py`, after the adj_factor fetch (line 49, `df_factor = adapter.adj_factor(...)`) and before `df_basic = adapter.stock_basic()` (line 52), insert a daily_basic fetch loop. But note: Tushare daily_basic takes a single trade_date, not a date range. We need to iterate over all trading dates in the range.

Replace the main() function body (lines 26-96) with updated version. After the `df_factor` line, before `df_basic`, add:

```python
    # daily_basic: 按日逐天拉取（API 只支持单日查询）
    logger.info("开始拉取 daily_basic（按日查询）")
    from datetime import timedelta

    all_basic_dfs: list[pd.DataFrame] = []
    current = args.start
    while current <= args.end:
        try:
            df_day_basic = adapter.daily_basic(ts_codes=ts_codes, trade_date=current)
            if not df_day_basic.empty:
                all_basic_dfs.append(df_day_basic)
        except Exception as e:
            logger.warning(f"daily_basic {current} 拉取失败，跳过：{e}")
        current += timedelta(days=1)

    df_all_basic = pd.concat(all_basic_dfs, ignore_index=True) if all_basic_dfs else pd.DataFrame()
```

Then in the `with DuckDBStore() as store:` block (line 55), after `n_factor = store.upsert_adj_factor(df_factor)`, add:

```python
        n_daily_basic = store.upsert_daily_basic(df_all_basic) if not df_all_basic.empty else 0
```

Update the logger.info line (around line 59-61) to include daily_basic:

```python
        logger.info(
            f"入库完成：daily {n_daily} / adj_factor {n_factor} / daily_basic {n_daily_basic} / stock_basic {n_basic}"
        )
```

- [ ] **Step 2: Verify __init__.py has all 26+ exports**

Verify the final `src/rquant/screen/__init__.py` exports all new rules. The complete `__all__` should be:

```python
__all__ = [
    "screen", "load_universe",
    "AggregateRequest",
    "not_st", "not_bj", "board_in",
    "limit_up", "not_limit_up", "first_limit_up", "yiziban", "not_yiziban", "limit_down",
    "consecutive_ups_gte", "circ_mv_lt",
    "has_lower_shadow",
    "gt", "lt", "gte", "lte", "between",
    "cross_above", "cross_below", "above_ma", "rsi_oversold", "rsi_overbought",
    "volume_ratio_gte",
    "no_consec_ups_in_window", "no_limit_down_in_window", "has_prior_limit_up",
]
```

That's 28 names total (was 20, added: `AggregateRequest`, `not_yiziban`, `circ_mv_lt`, `has_lower_shadow`, `no_consec_ups_in_window`, `no_limit_down_in_window`, `has_prior_limit_up` = +8).

- [ ] **Step 3: Update CHANGELOG.md**

Replace the `[Unreleased]` section in `CHANGELOG.md` with:

```markdown
## [Unreleased]

### Added
-

### Changed
-

### Deprecated
-

### Removed
-

### Fixed
-

### Security
-

---

## [v0.3.1] — 2026-04-XX — Week 4a: daily_basic + N 形态积木

为 N 形态策略补全数据层和规则积木。新增 `daily_basic` 表接入流通市值/换手率/量比，宽表暴露 `BODY_UPPER[n]`/`BODY_LOWER[n]`/`CIRC_MV[n]`，6 个新积木 + AggregateRequest 长窗口聚合机制。

### Added
- `daily_basic` 表（turnover_rate / volume_ratio / total_mv / circ_mv）
  - `DuckDBStore.upsert_daily_basic()` / `count_daily_basic()`
  - `TushareAdapter.daily_basic(ts_codes, trade_date)` — 单日查询
  - `ingest_daily.py` 追加按日逐天拉取 daily_basic
- 宽表扩展：
  - `STATE_COLS_MAP` 新增 body_upper / body_lower → `BODY_UPPER[n]` / `BODY_LOWER[n]`
  - 新增 `BASIC_COLS_MAP`（circ_mv / total_mv / turnover_rate）→ `CIRC_MV[n]` / `TOTAL_MV[n]` / `TURNOVER_RATE[n]`
- AggregateRequest 机制：规则声明长窗口聚合需求（max / any / sum / count_nonzero），load_universe 动态生成 DuckDB SQL，支持 exclude_offset
- 6 个新积木：
  - `not_yiziban(offset)` — 某日非一字板
  - `circ_mv_lt(threshold_yi, offset)` — 流通市值 < N 亿
  - `has_lower_shadow(min_ratio, min_amplitude, offset)` — 下影线达标
  - `no_consec_ups_in_window(threshold, window)` — 近 N 日无 M 连板
  - `no_limit_down_in_window(window)` — 近 N 日无跌停
  - `has_prior_limit_up(window, exclude_offset)` — 近 N 日（排除某日）有涨停
- 测试：新增 ~50 个单测（storage 4 + loader 11 + rules 30+ + core 4），累计 ~215 个
```

(Replace `04-XX` with the actual date when committing.)

- [ ] **Step 4: Run full test suite**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week4 && uv run pytest tests/ -v --tb=short
```

Expected: All tests pass (~215 total: 165 existing + ~50 new).

- [ ] **Step 5: Lint check**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week4 && uv run ruff check src/ tests/ scripts/
```

Expected: Clean, no errors.

- [ ] **Step 6: Smoke test with recent data (optional, if DB has data)**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week4 && uv run python -c "
from rquant.screen import (
    screen, not_st, not_bj, first_limit_up, not_limit_up, not_yiziban,
    gt, circ_mv_lt, has_lower_shadow,
    no_consec_ups_in_window, no_limit_down_in_window, has_prior_limit_up,
)
result = screen(
    trade_date='2026-04-15',
    rules=[
        not_st(),
        not_bj(),
        first_limit_up(offset=1),
        not_limit_up(offset=0),
        not_yiziban(offset=1),
        gt('HIGH[0]', 'CLOSE[1]'),
        circ_mv_lt(150),
        has_lower_shadow(1.5, 0.02, 0),
        no_consec_ups_in_window(3, 8),
        no_limit_down_in_window(30),
        has_prior_limit_up(90, 1),
    ],
    include_columns=['CIRC_MV[0]', 'BODY_UPPER[0]', 'BODY_LOWER[0]'],
)
print(f'N-shape Pool 1 hits: {len(result)}')
print(result.to_string())
"
```

- [ ] **Step 7: Commit + tag**

```bash
cd /Users/roxor/brain/30-projects/rQuant-week4 && git add scripts/ingest_daily.py src/rquant/screen/__init__.py CHANGELOG.md && git commit -m "chore: update ingest_daily.py + CHANGELOG for v0.3.1

ingest_daily.py iterates dates to fetch daily_basic (single-day API).
__init__.py exports 28 names (was 20). CHANGELOG documents Week 4a additions."
```

```bash
cd /Users/roxor/brain/30-projects/rQuant-week4 && git tag -a v0.3.1 -m "Week 4a: daily_basic + N-shape rule building blocks"
```
