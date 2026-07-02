# Intraday Attack Monitor + Paper Trading Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Turn the post-market attribution result into a no-lookahead intraday monitor, alert stream, and paper-trading tracker for Pool 1 / Pool 2 candidates.

**Architecture:** Keep `rquant-monitor` as the only intraday writer to DuckDB. It polls quote sources, records attack signals in `monitor_event`, opens/updates paper positions, and sends alerts; dashboard reads only through the readonly replica. Historical minute data is used for replay/backtest, not for live decisioning.

**Tech Stack:** Python 3.11+ / Pydantic / AKShare / optional mootdx / DuckDB / Streamlit / PushDeer + PushPlus.

---

## Source Decision

Use a staged source strategy instead of buying data first.

1. **Live quote, immediate:** keep the existing Sina quote path via AKShare `stock_zh_a_spot`. It already works with the current monitor shape and avoids the Eastmoney realtime block seen on Tencent Cloud.
2. **Historical minute replay:** add AKShare minute adapters for:
   - Eastmoney historical minute: `stock_zh_a_hist_min_em`, useful for recent 1-minute replay and signal-time reconstruction.
   - Sina minute: `stock_zh_a_minute`, useful as a second free source.
3. **Optional free fallback:** add `mootdx` only if cloud probes show it is stable from `82.156.0.68`. It supports Mac/Linux and TDX online quote/minute access, but needs dependency and server-connectivity validation.
4. **Paid fallback:** do not buy yet. Consider paid minute data only if free sources fail one of these checks:
   - cloud cannot reliably access free minute endpoints during trading hours;
   - historical 1-minute coverage is too short for replay;
   - source drift makes signal timestamps untrustworthy.

Paid candidates, in order:

1. **Tushare Pro minute permissions** if the current token/account can enable it. Lowest integration cost because rQuant already has Tushare config.
2. **JoinQuant / JQData or RiceQuant data service** if longer stable historical minute data is needed for research.
3. Avoid Wind/iFinD/Choice at this stage unless the project scope and budget explicitly expand.

## Signal Semantics

Pool membership is only a candidate state. A paper trade opens only when a live intraday `attack_*` signal fires.

Attack signals:

- `attack_open_strength`: open is 2%-4% above T close.
- `attack_strong_carry`: intraday low so far stays above T close, and current price is above T close.
- `attack_break_high`: intraday high breaks T-day high.
- `attack_near_limit`: price is within 1.5% of next-day limit-up price, while also breaking T high and holding above T close.

Historical replay must compute these only from minute bars available up to each timestamp. Daily full-session low/high/close can only be labels, never live decision inputs.

## Research Rule: No Hand-Written Strategy Conclusions

All N-pattern strategy details are hypotheses until they survive replay and paper-trading validation.

Do not treat user suggestions or Codex suggestions as final rules. This includes:

- stop-loss percentage;
- structural stop candidates;
- profit-taking threshold;
- trailing drawdown percentage;
- scale-out ratios;
- holding-day exits;
- entry-signal priority.

The system must separate **mechanism** from **parameters**:

- Mechanism: the code can express a stop, a profit-protection state, a scale-out action, or a time exit.
- Parameters: values such as 3%, 5%, 2.5%, 1/3 position, or 5 trading days are experiment candidates.

Every replay / paper-trading run must store a `candidate_id` and parameter payload, so dashboard performance can compare candidates instead of mixing rules together.

## B Entry, T+1, Stop Loss

Define **B entry** as the first eligible attack signal that opens a paper position for a `ts_code`.

At B entry, freeze these fields:

- `entry_time`
- `entry_price`
- `entry_signal`
- `entry_pool`
- `entry_t_date`
- `t_close`
- `t_high`
- `limit_up_price_next`
- `stop_loss_price`
- `stop_loss_basis`
- `earliest_exit_date`
- `candidate_id`

Initial stop loss formula candidate:

```python
structural_floor = max(
    value for value in [
        item.t_close,
        quote.low,
        item.stop_weak if item.pool == "pool2" else None,
    ]
    if value is not None and value < entry_price
)
percent_floor = entry_price * (1 - stop_loss_pct)
candidate = max(structural_floor, percent_floor)
stop_loss_price = min(candidate, entry_price * entry_buffer)
```

Baseline candidate values, not final strategy:

- `stop_loss_pct = 0.03`
- `entry_buffer = 0.995`
- no re-entry for the same stock on the same trading day after a stop-out;
- A-share T+1: B entry day cannot exit even if stop loss or profit protection is triggered;
- `earliest_exit_date` must be the next trading day, not simply calendar `T+1` in production;
- after the earliest exit date, close at stop price when minute low crosses the stop;
- after the earliest exit date, if a gap or live quote is already below stop, close at current quote price and mark `exit_reason = "gap_stop"`.

The stop line is a paper-trade field, not a Pool 2 lifecycle field. The old `40 / 30 / 20 / 强止 / 弱止` intraday pullback alerts stay removed; `stop_weak` can still be reused as one structural stop candidate for Pool 2 paper trades.

## Profit Protection Candidate Set

Profit-taking should be evaluated as a state machine, not a single fixed sell price.

Candidate mechanisms:

1. **No take-profit, time exit only**: hold until `N` trading days or stop loss.
2. **High-water trailing protection**: after MFE reaches `activation_pct`, keep tracking `max_price_seen`; exit only after price pulls back by `trailing_stop_pct`.
3. **Scale-out + trailing remainder**: after MFE reaches tier 1, mark partial exit for 1/3 or 1/2 size; recompute protection on remaining size.
4. **R-multiple protection**: activation and trailing use risk unit `R = entry_price - stop_loss_price`, not fixed percentage.
5. **Limit-up-aware protection**: if the stock touches/near limit-up, use stricter next-day protection because liquidity and T+1 constraints dominate.

Baseline candidate for first replay only:

- `activation_pct = 0.05`
- `trailing_stop_pct = 0.025`
- position size remains 100%, no scale-out

This baseline exists to verify the simulator, not to choose a strategy.

## Parameter Calibration

Historical replay should produce a candidate leaderboard before any rule is promoted to live monitoring.

Required replay constraints:

- only use minute bars available up to the decision timestamp;
- entry execution uses the next available quote/minute after signal confirmation, with configurable slippage;
- A-share T+1 blocks same-day sell;
- failed exits due to limit-down / no-liquidity must be represented as separate exit reasons if the data source can support it;
- include fees and slippage in pnl.

Candidate grid:

- entry signal priority: first signal / break-high only / strong-carry then break-high / near-limit only;
- stop-loss structure: T close / signal-day low-so-far / Pool2 weak stop / percent floor / combinations;
- stop-loss percent: e.g. 2%, 3%, 4%, 5%;
- profit protection activation: no activation / 3% / 5% / 8% / 1R / 1.5R / 2R;
- trailing drawdown: 1.5%, 2.5%, 3.5%, ATR-like band if minute volatility is available;
- time exit: 1 / 3 / 5 / 10 trading days;
- scale-out: none / 1/3 at tier 1 / 1/2 at tier 1.

Selection method:

- split by time, not random rows;
- use walk-forward validation by month or quarter;
- rank by sample-out expectancy, max drawdown, profit factor, tail loss, and trade count;
- prefer a broad stable plateau over the single highest backtest point;
- compare against simple baselines: buy at next open and hold 1/3/5 days, and no-profit-taking time exit.

## Task 1: Intraday Source Abstraction

**Files:**

- Create: `src/rquant/adapter/intraday.py`
- Test: `tests/unit/test_intraday_adapter.py`

**Step 1: Write failing tests**

Cover:

- ts code conversion:
  - `600519.SH -> sh600519`
  - `000001.SZ -> sz000001`
  - `920001.BJ -> bj920001`
- Eastmoney historical minute rows normalize to Pydantic `IntradayBar`.
- Sina minute rows normalize to the same model.
- source failure returns an empty list and logs an error.

**Step 2: Implement minimal adapter**

Add:

- `IntradayBar(BaseModel)`
- `to_sina_symbol(ts_code: str) -> str`
- `fetch_hist_minute_bars_em(ts_code, start_time, end_time, period="1")`
- `fetch_hist_minute_bars_sina(ts_code, period="1")`

**Step 3: Verify**

Run:

```bash
.venv/bin/python -m pytest tests/unit/test_intraday_adapter.py -q
.venv/bin/python -m ruff check src/rquant/adapter/intraday.py tests/unit/test_intraday_adapter.py
```

## Task 2: Paper Trade Domain Model

**Files:**

- Create: `src/rquant/paper.py`
- Test: `tests/unit/test_paper.py`

**Step 1: Write failing tests**

Cover:

- opening a position from `attack_break_high` freezes entry fields;
- stop loss uses structural floor when it is tighter than the configured percent floor;
- stop loss falls back to configured percent floor when structure is too far away;
- stop loss is capped below entry price;
- A-share T+1 blocks same-day exits;
- after earliest exit date, stop crosses close the position with `exit_reason = "stop_loss"`;
- after earliest exit date, quote already below stop closes with `exit_reason = "gap_stop"`;
- candidate id is frozen on the position;
- same stock cannot open twice on the same trading day.

**Step 2: Implement minimal code**

Add Pydantic models:

- `PaperPosition`
- `PaperTradeConfig`
- `PaperExit`

Add functions:

- `make_position_id(trade_date, ts_code, entry_signal) -> str`
- `calculate_initial_stop_loss(item, quote, entry_price, config) -> tuple[float, str]`
- `open_position_from_signal(item, quote, signal, now, config) -> PaperPosition`
- `check_position_exit(position, quote, now, config) -> PaperExit | None`

**Step 3: Verify**

Run:

```bash
.venv/bin/python -m pytest tests/unit/test_paper.py -q
.venv/bin/python -m ruff check src/rquant/paper.py tests/unit/test_paper.py
```

## Task 3: DuckDB Tables And Store Methods

**Files:**

- Modify: `src/rquant/storage/schema.py`
- Modify: `src/rquant/storage/duckdb.py`
- Test: `tests/unit/test_storage_paper.py`

**Step 1: Write failing tests**

Cover:

- `paper_position` table is created by schema init;
- `upsert_paper_position` inserts and updates one position;
- `query_active_paper_positions` returns only `status = 'open'`;
- `close_paper_position` writes exit fields and realized pnl;
- readonly query methods work through existing readonly helpers.

**Step 2: Add schema**

Add:

```sql
CREATE TABLE IF NOT EXISTS paper_position (
    position_id          VARCHAR PRIMARY KEY,
    trade_date           DATE NOT NULL,
    ts_code              VARCHAR NOT NULL,
    name                 VARCHAR,
    pool                 VARCHAR,
    entry_time           TIMESTAMP NOT NULL,
    entry_price          DOUBLE NOT NULL,
    entry_signal         VARCHAR NOT NULL,
    candidate_id         VARCHAR NOT NULL,
    entry_level_price    DOUBLE,
    entry_t_date         DATE,
    earliest_exit_date   DATE NOT NULL,
    t_close              DOUBLE,
    t_high               DOUBLE,
    limit_up_price_next  DOUBLE,
    stop_loss_price      DOUBLE NOT NULL,
    stop_loss_basis      VARCHAR NOT NULL,
    stop_loss_pct        DOUBLE NOT NULL,
    take_profit_price    DOUBLE,
    take_profit_pct      DOUBLE,
    trailing_stop_pct    DOUBLE,
    trailing_stop_price  DOUBLE,
    status               VARCHAR NOT NULL,
    exit_time            TIMESTAMP,
    exit_price           DOUBLE,
    exit_reason          VARCHAR,
    holding_trading_days INTEGER,
    pnl_pct              DOUBLE,
    max_price_seen       DOUBLE,
    max_drawdown_pct     DOUBLE,
    param_payload        JSON,
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Step 3: Add store methods**

Add:

- `upsert_paper_position(df: pd.DataFrame) -> int`
- `query_active_paper_positions(trade_date: str | None = None) -> pd.DataFrame`
- `query_paper_positions(start: str | None = None, end: str | None = None) -> pd.DataFrame`
- `close_paper_position(position_id, exit_time, exit_price, exit_reason, holding_days) -> None`

**Step 4: Verify**

Run:

```bash
.venv/bin/python -m pytest tests/unit/test_storage_paper.py -q
.venv/bin/python -m ruff check src/rquant/storage/schema.py src/rquant/storage/duckdb.py tests/unit/test_storage_paper.py
```

## Task 4: Parameter Replay And Candidate Leaderboard

**Files:**

- Create: `src/rquant/research/attack_replay.py`
- Create: `src/rquant/research/candidate_grid.py`
- Test: `tests/unit/test_attack_replay.py`
- Test: `tests/unit/test_candidate_grid.py`

**Step 1: Write failing tests**

Cover:

- candidate grid expands into distinct `candidate_id` values;
- replay stores one row per candidate per trade opportunity;
- same minute bars produce different exits for different candidate configs;
- output includes trade count, expectancy, win rate, profit factor, max drawdown proxy, tail loss, and average holding days;
- date split prevents training rows from leaking into validation rows.

**Step 2: Implement**

Add:

- `PaperCandidateConfig`
- `iter_candidate_grid()`
- `run_attack_replay(start, end, candidates, source, dry_run=True)`
- `summarize_candidate_results(results_df)`

**Step 3: Verify**

Run:

```bash
.venv/bin/python -m pytest tests/unit/test_attack_replay.py tests/unit/test_candidate_grid.py -q
.venv/bin/python -m ruff check src/rquant/research tests/unit/test_attack_replay.py tests/unit/test_candidate_grid.py
```

## Task 5: Monitor Integration

**Files:**

- Modify: `src/rquant/monitor.py`
- Modify: `src/rquant/notify/messages.py`
- Test: `tests/unit/test_monitor.py`
- Test: `tests/unit/test_notify_messages.py`

**Step 1: Write failing tests**

Cover:

- attack signal creates `monitor_event` and opens one paper position;
- opened paper position stores `candidate_id` and parameter payload;
- duplicate attack signals do not open duplicate positions;
- active positions are marked to quote on every quote cycle;
- A-share T+1 blocks same-day exits;
- a stop-loss or profit-protection exit sends a paper-trade notification after earliest exit date;
- monitor continues when paper-trade persistence fails for one stock.

**Step 2: Implement**

In the monitor loop:

1. fetch quotes;
2. update exits for existing open positions;
3. evaluate new attack signals;
4. write `monitor_event`;
5. open paper position on first eligible B signal;
6. send alert with entry price and stop loss line.

**Step 3: Verify**

Run:

```bash
.venv/bin/python -m pytest tests/unit/test_monitor.py tests/unit/test_notify_messages.py -q
.venv/bin/python -m ruff check src/rquant/monitor.py src/rquant/notify/messages.py tests/unit/test_monitor.py tests/unit/test_notify_messages.py
```

## Task 6: Historical Replay CLI

**Files:**

- Modify: `src/rquant/cli.py`
- Create: `src/rquant/replay.py`
- Test: `tests/unit/test_replay.py`

**Step 1: Write failing tests**

Cover:

- replay consumes minute bars in timestamp order;
- signal decisions use only bars up to current timestamp;
- paper position opens at first signal timestamp;
- stop loss can close later in the same day;
- replay output matches expected event and trade counts on a small fixture.

**Step 2: Implement CLI**

Add:

```bash
rquant replay-attack --start 2026-05-01 --end 2026-06-24 --source akshare-em --dry-run
```

Dry-run prints summary without writing. Non-dry-run writes `monitor_event` and `paper_position` to DuckDB.

**Step 3: Verify**

Run:

```bash
.venv/bin/python -m pytest tests/unit/test_replay.py -q
.venv/bin/python -m rquant replay-attack --start 2026-06-01 --end 2026-06-05 --dry-run
```

## Task 7: Dashboard Upgrade

**Files:**

- Modify: `src/rquant/dashboard/app.py`

**Step 1: Replace old Section 8**

Rename `Pool 2 实时价位 vs 档位` to `攻击雷达`.

Show:

- code / name / pool / blacklist;
- current price / pct change / intraday low / intraday high;
- T close / T high / next limit-up price;
- distance to T high;
- distance to limit-up price;
- attack signal status;
- suggested B entry price if triggered;
- frozen stop loss if paper position is open.

**Step 2: Add paper-trading section**

Show:

- open positions;
- candidate id and parameter payload;
- stop loss line;
- profit-protection activation and trailing stop line;
- current pnl;
- max price seen;
- max drawdown;
- closed trades table;
- candidate leaderboard: trade count / win rate / average pnl / expectancy / profit factor / stop-loss count / profit-protection count / average holding trading days.

**Step 3: Verify**

Run locally:

```bash
streamlit run src/rquant/dashboard/app.py --server.port 8501 --server.address 0.0.0.0 --server.headless true
```

Open:

```text
http://127.0.0.1:8501/
```

Dashboard must use `open_readonly_connection()` only.

## Task 8: Cloud Probe Before Deployment

Because production is on `82.156.0.68` and Codex does not SSH directly to production, use pair mode.

Ask the user to run these on the server:

```bash
cd /home/lighthouse/rquant
.venv/bin/python - <<'PY'
import akshare as ak
print("akshare", ak.__version__)
print(ak.stock_zh_a_spot().head(2))
print(ak.stock_zh_a_hist_min_em(symbol="000001", period="1").tail(2))
PY
```

If Eastmoney minute fails, test Sina minute:

```bash
cd /home/lighthouse/rquant
.venv/bin/python - <<'PY'
import akshare as ak
print(ak.stock_zh_a_minute(symbol="sz000001", period="1").tail(2))
PY
```

Only add `mootdx` after AKShare probes fail or are too unstable.

## Completion Criteria

- Historical replay can reproduce attack signals without daily-lookahead fields.
- Live monitor sends attack alerts with suggested B entry and stop-loss line.
- Paper positions record entry, stop, exit, pnl, and holding trading days.
- Dashboard shows candidate attack radar and paper-trade performance.
- Full test suite passes.
- `CHANGELOG.md` is updated under `[Unreleased]`.
