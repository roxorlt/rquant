"""市场全景 readers over one borrowed generation cursor.

The queries read the projections the Streamlit panorama reads (market_snapshot,
market_overview, dc_board(_member), kpl_concept_member, market_liquidity, screen_result,
pool2_watch, intraday_kline, daily_bar, surge_event, pulse_history, pulse_alert,
surge_runtime_config) with the same bounds; the calculations are the panorama's own pure
functions (limit prices, the pulse, board members' strength score). Two things differ on
purpose:

* limit prices are derived from the snapshot (``add_limit_prices``) before the pulse and
  the members' 涨停 flags are computed — the projection carries no limit prices, and
  without them every count is zero;
* the average-price line is estimated from minute closes (Σ close×volume / Σ volume),
  because ``intraday_kline`` publishes no turnover column.

An unpublished projection reads as empty; nothing here raises for missing data.
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime
from typing import Any

import pandas as pd

from rquant.panorama_data import (
    BOARD_SYSTEMS,
    SurgeRuntimeConfig,
    add_limit_prices,
    board_constituents,
    compute_market_pulse,
    volume_directions,
)
from rquant.web.labels import PRESET_LABELS
from rquant.web.market import MARKET_TIMEZONE
from rquant.web.models.panorama import (
    BoardRow,
    DailyBar,
    MemberRow,
    MinuteBar,
    PulseAlert,
    PulseCounts,
    PulsePoint,
    SurgeConfig,
    SurgeMark,
    SurgeRow,
)
from rquant.web.readers import TableState

KPL_SYSTEM = "开盘啦题材"
SLOTS_PER_DAY = 241
_MORNING_OPEN = 9 * 60 + 30
_MORNING_CLOSE = 11 * 60 + 30
_AFTERNOON_OPEN = 13 * 60
_AFTERNOON_CLOSE = 15 * 60
_SURGE_STATUS = {"confirmed": "可买", "unbuyable": "已涨停"}
_BOARD_LABELS = {"main": "主板", "gem": "创业板", "star": "科创板", "bj": "北交所"}
MAX_SEARCH_ROWS = 500


def _readable(tables: Mapping[str, TableState], *names: str) -> bool:
    return all(name in tables and tables[name].available for name in names)


def _frame(cursor: Any, sql: str, parameters: Sequence[object] = ()) -> pd.DataFrame:
    return cursor.execute(sql, tuple(parameters)).fetchdf()


def _num(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return None if math.isnan(number) or math.isinf(number) else number


def _int(value: object) -> int | None:
    number = _num(value)
    return None if number is None else int(round(number))


def _text(value: object) -> str | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    return text or None


def _utc(value: object) -> datetime | None:
    if value is None:
        return None
    timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        return None
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC").to_pydatetime()


# ------------------------------------------------------------------ session axis


def session_slot(local: datetime) -> int | None:
    """Position of a minute on the 241-point session axis (None outside the session).

    09:30 → 0 … 11:30 → 120, 13:00 → 120 (the noon break shares one point), 13:01 → 121 …
    15:00 → 240. Minute bars labelled by their start (09:30–14:59) and by their end
    (09:31–15:00) both land on the axis, so a partial day stops where the data stops.
    """

    minute = local.hour * 60 + local.minute
    if _MORNING_OPEN <= minute <= _MORNING_CLOSE:
        return minute - _MORNING_OPEN
    if _AFTERNOON_OPEN <= minute <= _AFTERNOON_CLOSE:
        return 120 + minute - _AFTERNOON_OPEN
    return None


# ------------------------------------------------------------------ snapshot & pulse


def snapshot(cursor: Any, tables: Mapping[str, TableState]) -> pd.DataFrame:
    """The latest market snapshot with derived limit prices (empty when unpublished)."""

    if not _readable(tables, "market_snapshot"):
        return pd.DataFrame()
    frame = _frame(
        cursor,
        """
        SELECT as_of, ts_code, name, price, open, high, low, pre_close,
               pct_chg, volume, amount
        FROM market_snapshot
        WHERE as_of = (SELECT MAX(as_of) FROM market_snapshot)
        ORDER BY ts_code
        LIMIT 8000
        """,
    )
    if frame.empty:
        return frame
    as_of = frame["as_of"].iloc[0]
    frame = add_limit_prices(frame.drop(columns=["as_of"]))
    frame.attrs["as_of"] = as_of
    return frame


def snapshot_as_of(frame: pd.DataFrame) -> datetime | None:
    return _utc(frame.attrs.get("as_of")) if not frame.empty else None


def pulse_counts(frame: pd.DataFrame) -> PulseCounts | None:
    pulse = compute_market_pulse(frame)
    if pulse.total_count == 0:
        return None
    return PulseCounts(
        limit_up=pulse.limit_up_count,
        limit_down=pulse.limit_down_count,
        broken=pulse.broken_count,
        up=pulse.up_count,
        down=pulse.down_count,
        flat=pulse.flat_count,
        total=pulse.total_count,
        up_ratio_pct=pulse.up_ratio_pct,
    )


def pulse_history(
    cursor: Any, tables: Mapping[str, TableState], day: date
) -> tuple[list[PulsePoint], PulseCounts | None]:
    """The day's minute pulse, and its last minute as counts (a snapshot fallback)."""

    if not _readable(tables, "pulse_history"):
        return [], None
    frame = _frame(
        cursor,
        """
        SELECT t, limit_up, limit_down, broken, up, down, up_ratio_pct, total
        FROM pulse_history
        WHERE trade_date = ?
        ORDER BY as_of
        LIMIT 512
        """,
        (day,),
    )
    points = [
        PulsePoint(
            t=str(row.t),
            limit_up=_int(row.limit_up),
            broken=_int(row.broken),
            limit_down=_int(row.limit_down),
            up_ratio_pct=_num(row.up_ratio_pct),
        )
        for row in frame.itertuples(index=False)
    ]
    last = None
    if not frame.empty:
        row = frame.iloc[-1]
        up, down, total = _int(row["up"]) or 0, _int(row["down"]) or 0, _int(row["total"]) or 0
        if total > 0:
            last = PulseCounts(
                limit_up=_int(row["limit_up"]) or 0,
                limit_down=_int(row["limit_down"]) or 0,
                broken=_int(row["broken"]) or 0,
                up=up,
                down=down,
                flat=max(total - up - down, 0),
                total=total,
                up_ratio_pct=_num(row["up_ratio_pct"]),
            )
    return points, last


def pulse_alerts(cursor: Any, tables: Mapping[str, TableState], day: date) -> list[PulseAlert]:
    if not _readable(tables, "pulse_alert"):
        return []
    frame = _frame(
        cursor,
        """
        SELECT t, kind, kind_label, message
        FROM pulse_alert
        WHERE trade_date = ?
        ORDER BY as_of, kind
        LIMIT 512
        """,
        (day,),
    )
    return [
        PulseAlert(
            t=str(row.t),
            kind=str(row.kind),
            kind_label=_text(row.kind_label) or "异动",
            message=_text(row.message) or "",
        )
        for row in frame.itertuples(index=False)
    ]


# ------------------------------------------------------------------ boards


def boards(
    cursor: Any, tables: Mapping[str, TableState], system: str
) -> tuple[list[BoardRow], datetime | None]:
    """One system's board overview, most limit-ups first, then turnover."""

    if system not in BOARD_SYSTEMS or not _readable(tables, "market_overview"):
        return [], None
    frame = _frame(
        cursor,
        """
        SELECT as_of, board_code, board_name, amount, main_net_amount, main_net_rate,
               pct_chg_median, limit_up_count, broken_count, stock_count,
               limit_up_ratio_pct, leading_stock
        FROM market_overview
        WHERE as_of = (SELECT MAX(as_of) FROM market_overview) AND system = ?
        ORDER BY limit_up_count DESC NULLS LAST, amount DESC NULLS LAST, board_code
        LIMIT 3000
        """,
        (system,),
    )
    if frame.empty:
        return [], None
    flow = system != KPL_SYSTEM
    rows = [
        BoardRow(
            board_code=str(row.board_code),
            board_name=_text(row.board_name) or str(row.board_code),
            amount=_num(row.amount),
            main_net_amount=_num(row.main_net_amount) if flow else None,
            main_net_rate=_num(row.main_net_rate) if flow else None,
            pct_chg_median=_num(row.pct_chg_median),
            limit_up_count=_int(row.limit_up_count),
            broken_count=_int(row.broken_count),
            limit_up_ratio_pct=_num(row.limit_up_ratio_pct),
            stock_count=_int(row.stock_count),
            leading_stock=_text(row.leading_stock) if flow else None,
        )
        for row in frame.itertuples(index=False)
    ]
    return rows, _utc(frame["as_of"].iloc[0])


def _members(cursor: Any, tables: Mapping[str, TableState]) -> pd.DataFrame:
    """东财 members (else the stock_basic industry fallback) plus 开盘啦 members."""

    columns = ["board_code", "board_name", "con_code"]
    frames: list[pd.DataFrame] = []
    dc = pd.DataFrame(columns=columns)
    if _readable(tables, "dc_board", "dc_board_member"):
        dc = _frame(
            cursor,
            """
            SELECT m.board_code, b.name AS board_name, m.con_code
            FROM dc_board_member m
            JOIN dc_board b ON m.board_code = b.ts_code
            WHERE b.idx_type IN ('行业板块', '概念板块')
            LIMIT 50000
            """,
        )
    if dc.empty and _readable(tables, "stock_basic"):
        dc = _frame(
            cursor,
            """
            SELECT industry AS board_code, industry AS board_name, ts_code AS con_code
            FROM stock_basic
            WHERE industry IS NOT NULL AND industry != ''
            LIMIT 8000
            """,
        )
    frames.append(dc)
    if _readable(tables, "kpl_concept_member"):
        frames.append(
            _frame(
                cursor,
                "SELECT board_code, board_name, con_code FROM kpl_concept_member LIMIT 50000",
            )
        )
    frames = [frame[columns] for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame(columns=columns)
    return pd.concat(frames, ignore_index=True)


def _pool_labels(cursor: Any, tables: Mapping[str, TableState]) -> dict[str, list[str]]:
    labels: dict[str, list[str]] = {}
    if _readable(tables, "screen_result"):
        rows = cursor.execute(
            "SELECT ts_code, preset_name FROM screen_result "
            "WHERE trade_date = (SELECT MAX(trade_date) FROM screen_result) LIMIT 20000"
        ).fetchall()
        for code, preset in rows:
            labels.setdefault(str(code), []).append(PRESET_LABELS.get(str(preset), "选股结果"))
    if _readable(tables, "pool2_watch"):
        rows = cursor.execute(
            "SELECT ts_code FROM pool2_watch WHERE status = 'active' LIMIT 2000"
        ).fetchall()
        for (code,) in rows:
            labels.setdefault(str(code), []).append("二池盯盘")
    return labels


def _liquidity(cursor: Any, tables: Mapping[str, TableState]) -> pd.DataFrame:
    if not _readable(tables, "market_liquidity"):
        return pd.DataFrame(columns=["ts_code", "circ_mv", "avg_amount_5d"])
    return _frame(
        cursor,
        "SELECT ts_code, circ_mv, avg_amount_5d FROM market_liquidity ORDER BY ts_code LIMIT 8000",
    )


def members(
    cursor: Any,
    tables: Mapping[str, TableState],
    board_code: str,
    snap: pd.DataFrame,
) -> tuple[str | None, list[MemberRow]]:
    """One board's members joined with the snapshot, strongest first."""

    combined = _members(cursor, tables)
    names = combined.loc[combined["board_code"] == board_code, "board_name"]
    board_name = _text(names.iloc[0]) if not names.empty else None
    labels = _pool_labels(cursor, tables)
    table = board_constituents(
        board_code,
        combined,
        snap,
        pool_flags={code: " + ".join(value) for code, value in labels.items()},
        liquidity=_liquidity(cursor, tables),
    )
    rows = [
        MemberRow(
            ts_code=str(row["ts_code"]),
            name=_text(row.get("name")),
            price=_num(row.get("price")),
            pct_chg=_num(row.get("pct_chg")),
            amount=_num(row.get("amount")),
            strength=_num(row.get("strength")),
            turnover_pct=_num(row.get("turnover_pct")),
            rel_volume_5d=_num(row.get("rel_volume_5d")),
            is_limit_up=bool(row.get("is_limit_up", False)),
            pools=labels.get(str(row["ts_code"]), []),
        )
        for _, row in table.iterrows()
    ]
    return board_name, rows


# ------------------------------------------------------------------ stock charts


def stock_name(
    cursor: Any, tables: Mapping[str, TableState], ts_code: str, snap: pd.DataFrame
) -> str | None:
    if not snap.empty:
        hit = snap.loc[snap["ts_code"] == ts_code, "name"]
        if not hit.empty and _text(hit.iloc[0]):
            return _text(hit.iloc[0])
    if _readable(tables, "stock_basic"):
        row = cursor.execute(
            "SELECT name FROM stock_basic WHERE ts_code = ? LIMIT 1", (ts_code,)
        ).fetchone()
        if row is not None:
            return _text(row[0])
    return None


def _local(value: object) -> datetime:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    return timestamp.tz_convert(MARKET_TIMEZONE).to_pydatetime()


def minute_bars(
    cursor: Any,
    tables: Mapping[str, TableState],
    ts_code: str,
    *,
    days: int = 1,
    day: date | None = None,
) -> tuple[list[date], list[MinuteBar]]:
    """The last ``days`` trading days of minute bars (or exactly ``day``)."""

    if not _readable(tables, "intraday_kline"):
        return [], []
    # At most five days of one stock (≤ 1,210 bars); the day is taken in Shanghai time
    # here rather than in SQL, so no time-zone extension is needed.
    frame = _frame(
        cursor,
        """
        SELECT trade_time, close, vol
        FROM intraday_kline
        WHERE ts_code = ?
        ORDER BY trade_time
        LIMIT 3000
        """,
        (ts_code,),
    )
    if frame.empty:
        return [], []
    frame["local"] = [_local(value) for value in frame["trade_time"]]
    frame["day"] = [value.date() for value in frame["local"]]
    if day is not None:
        frame = frame[frame["day"] == day]
    else:
        keep = sorted(set(frame["day"]))[-days:]
        frame = frame[frame["day"].isin(keep)]
    if frame.empty:
        return [], []
    frame["price"] = pd.to_numeric(frame["close"], errors="coerce")
    frame["volume"] = pd.to_numeric(frame["vol"], errors="coerce")
    frame = frame.dropna(subset=["price"]).reset_index(drop=True)
    bars: list[MinuteBar] = []
    for trading_day, group in frame.groupby("day", sort=True):
        group = group.reset_index(drop=True)
        weight = group["volume"].fillna(0.0)
        cumulative_volume = weight.cumsum()
        average = (group["price"] * weight).cumsum() / cumulative_volume.where(
            cumulative_volume > 0
        )
        directions = volume_directions(group["price"])
        for index, row in group.iterrows():
            slot = session_slot(row["local"])
            if slot is None:
                continue
            bars.append(
                MinuteBar(
                    day=trading_day,
                    t=row["local"].strftime("%H:%M"),
                    slot=slot,
                    price=float(row["price"]),
                    avg_price=_num(average.iloc[index]),
                    volume=_num(row["volume"]),
                    direction=str(directions.iloc[index]),  # type: ignore[arg-type]
                )
            )
    return sorted({bar.day for bar in bars}), bars


def surge_marks(
    cursor: Any,
    tables: Mapping[str, TableState],
    ts_code: str,
    days: Iterable[date],
    bars: Sequence[MinuteBar],
    *,
    every_event: bool,
) -> list[SurgeMark]:
    """爆量确认 points on the chart: the first per day, or every one of one day."""

    wanted = sorted(set(days))
    if not wanted or not _readable(tables, "surge_event"):
        return []
    marks = ",".join("?" for _ in wanted)
    rows = cursor.execute(
        f"""
        SELECT trade_date, confirmed_at, rel_cum
        FROM surge_event
        WHERE ts_code = ? AND trade_date IN ({marks})
        ORDER BY trade_date, confirmed_at
        LIMIT 1000
        """,
        (ts_code, *wanted),
    ).fetchall()
    grouped: dict[tuple[date, str], list[float | None]] = {}
    seen_days: set[date] = set()
    for trade_date, confirmed_at, rel_cum in rows:
        if not every_event and trade_date in seen_days:
            continue
        seen_days.add(trade_date)
        grouped.setdefault((trade_date, str(confirmed_at)), []).append(_num(rel_cum))
    by_day: dict[date, list[MinuteBar]] = {}
    for bar in bars:
        by_day.setdefault(bar.day, []).append(bar)
    out: list[SurgeMark] = []
    for (trade_date, confirmed_at), multiples in grouped.items():
        try:
            hour, minute = (int(part) for part in confirmed_at.split(":"))
        except ValueError:
            continue
        target = session_slot(datetime(2000, 1, 1, hour, minute))
        day_bars = [
            bar for bar in by_day.get(trade_date, []) if target is not None and bar.slot <= target
        ]
        if target is None or not day_bars:
            continue
        anchor = day_bars[-1]
        values = " / ".join("—" if value is None else f"{value:.1f}×" for value in multiples)
        label = f"{confirmed_at} 爆量确认"
        if len(multiples) > 1:
            label += f" {len(multiples)} 次 · {values}"
        elif values != "—":
            label += f" · {values}"
        out.append(
            SurgeMark(
                day=trade_date,
                t=confirmed_at,
                slot=anchor.slot,
                price=anchor.price,
                label=label,
                count=len(multiples),
            )
        )
    return out


def daily_bars(
    cursor: Any,
    tables: Mapping[str, TableState],
    ts_code: str,
    snap: pd.DataFrame,
    *,
    count: int = 120,
) -> list[DailyBar]:
    """Recent daily bars with MA5/10/20, plus today's bar from the snapshot if missing."""

    if not _readable(tables, "daily_bar"):
        return []
    frame = _frame(
        cursor,
        """
        SELECT trade_date, open, high, low, close, vol AS volume
        FROM daily_bar
        WHERE ts_code = ?
        ORDER BY trade_date DESC
        LIMIT ?
        """,
        (ts_code, count),
    )
    if frame.empty:
        return []
    frame = frame.sort_values("trade_date").reset_index(drop=True)
    frame["provisional"] = False
    as_of = snapshot_as_of(snap)
    if as_of is not None:
        today = as_of.astimezone(MARKET_TIMEZONE).date()
        last = pd.Timestamp(frame["trade_date"].iloc[-1]).date()
        hit = snap.loc[snap["ts_code"] == ts_code]
        if last < today and not hit.empty:
            quote = hit.iloc[0]
            values = [_num(quote.get(key)) for key in ("open", "high", "low", "price")]
            if all(value is not None and value > 0 for value in values):
                volume = _num(quote.get("volume"))
                frame.loc[len(frame)] = {
                    "trade_date": today,
                    "open": values[0],
                    "high": values[1],
                    "low": values[2],
                    "close": values[3],
                    # The snapshot counts shares, daily bars count lots of 100.
                    "volume": None if volume is None else volume / 100.0,
                    "provisional": True,
                }
    close = pd.to_numeric(frame["close"], errors="coerce")
    for window in (5, 10, 20):
        frame[f"ma{window}"] = close.rolling(window=window, min_periods=window).mean()
    return [
        DailyBar(
            date=pd.Timestamp(row.trade_date).date(),
            open=float(row.open),
            high=float(row.high),
            low=float(row.low),
            close=float(row.close),
            volume=_num(row.volume),
            ma5=_num(row.ma5),
            ma10=_num(row.ma10),
            ma20=_num(row.ma20),
            provisional=bool(row.provisional),
        )
        for row in frame.itertuples(index=False)
    ]


# ------------------------------------------------------------------ surge ledger


def _surge_row(row: Any) -> SurgeRow:
    status = str(row.status or "confirmed")
    return SurgeRow(
        trade_date=pd.Timestamp(row.trade_date).date(),
        confirmed_at=str(row.confirmed_at),
        ts_code=str(row.ts_code),
        name=_text(row.name),
        theme=_text(row.theme),
        price=_num(row.price),
        pct_chg=_num(row.pct_chg),
        rel_cum=_num(row.rel_cum),
        cum_amount=_num(row.cum_amount),
        room_to_limit_pct=_num(row.room_to_limit_pct),
        status=status,
        status_label=_SURGE_STATUS.get(status, "其他"),
    )


def surge_dates(cursor: Any, tables: Mapping[str, TableState]) -> list[date]:
    if not _readable(tables, "surge_event"):
        return []
    rows = cursor.execute(
        "SELECT DISTINCT trade_date FROM surge_event ORDER BY trade_date DESC LIMIT 120"
    ).fetchall()
    return [pd.Timestamp(row[0]).date() for row in rows]


def surge_day(cursor: Any, tables: Mapping[str, TableState], day: date) -> list[SurgeRow]:
    """The day's ledger: each stock's first confirmation, in time order."""

    if not _readable(tables, "surge_event"):
        return []
    frame = _frame(
        cursor,
        """
        SELECT trade_date, confirmed_at, ts_code, name, theme, price, pct_chg,
               cum_amount, rel_cum, room_to_limit_pct, status
        FROM surge_event
        WHERE trade_date = ?
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY confirmed_at) = 1
        ORDER BY confirmed_at, ts_code
        LIMIT 10000
        """,
        (day,),
    )
    return [_surge_row(row) for row in frame.itertuples(index=False)]


def surge_search(
    cursor: Any, tables: Mapping[str, TableState], query: str
) -> tuple[list[SurgeRow], bool]:
    """Every day's first confirmation of stocks whose code or name contains ``query``."""

    needle = query.strip().casefold()
    if not needle or not _readable(tables, "surge_event"):
        return [], False
    pattern = f"%{needle}%"
    frame = _frame(
        cursor,
        """
        SELECT trade_date, confirmed_at, ts_code, name, theme, price, pct_chg,
               cum_amount, rel_cum, room_to_limit_pct, status
        FROM surge_event
        WHERE LOWER(CAST(ts_code AS VARCHAR)) LIKE ? OR LOWER(COALESCE(name, '')) LIKE ?
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY trade_date, ts_code ORDER BY confirmed_at
        ) = 1
        ORDER BY trade_date DESC, confirmed_at DESC, ts_code
        LIMIT ?
        """,
        (pattern, pattern, MAX_SEARCH_ROWS + 1),
    )
    rows = [_surge_row(row) for row in frame.itertuples(index=False)]
    return rows[:MAX_SEARCH_ROWS], len(rows) > MAX_SEARCH_ROWS


def surge_config(cursor: Any, tables: Mapping[str, TableState]) -> SurgeConfig | None:
    if not _readable(tables, "surge_runtime_config"):
        return None
    frame = _frame(
        cursor,
        """
        SELECT trade_date, as_of, boards_json, k_rough, k_cum, ratio_cap,
               skip_first_minutes, tushare_rate_per_min, require_price_strength,
               max_room_to_limit_pct
        FROM surge_runtime_config
        WHERE snapshot_key = 'current'
        LIMIT 1
        """,
    )
    if frame.empty:
        return None
    row = frame.iloc[0]
    try:
        boards_list = json.loads(str(row["boards_json"]))
        config = SurgeRuntimeConfig(
            trade_date=row["trade_date"],
            as_of=row["as_of"],
            boards=tuple(boards_list),
            k_rough=row["k_rough"],
            k_cum=row["k_cum"],
            ratio_cap=row["ratio_cap"],
            skip_first_minutes=row["skip_first_minutes"],
            tushare_rate_per_min=row["tushare_rate_per_min"],
            require_price_strength=row["require_price_strength"],
            max_room_to_limit_pct=row["max_room_to_limit_pct"],
        )
    except (TypeError, ValueError):
        return None
    boards_named = [_BOARD_LABELS[board] for board in config.boards]
    scope = "、".join(boards_named) or "全部板块"
    summary = (
        f"检测{scope}：累计放量 {config.k_cum:g}–{config.ratio_cap:g} 倍、当前分钟上涨且外盘占优；"
        "每只股票只记当天第一次确认。观察提示，不是买入信号。"
    )
    return SurgeConfig(
        boards=boards_named, k_cum=config.k_cum, ratio_cap=config.ratio_cap, summary=summary
    )


__all__ = [
    "KPL_SYSTEM",
    "MAX_SEARCH_ROWS",
    "SLOTS_PER_DAY",
    "boards",
    "daily_bars",
    "members",
    "minute_bars",
    "pulse_alerts",
    "pulse_counts",
    "pulse_history",
    "session_slot",
    "snapshot",
    "snapshot_as_of",
    "stock_name",
    "surge_config",
    "surge_dates",
    "surge_day",
    "surge_marks",
    "surge_search",
]
