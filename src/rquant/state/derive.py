"""Point-in-time daily state derivation from prices, status, and trade calendar.

IPO and relisting no-price-limit windows are not inferred from the first stored
bar. Until an authoritative eligibility fact is available, callers must treat
those windows as unsupported rather than assuming an exchange price cap.
"""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import ROUND_HALF_UP, Decimal
from zoneinfo import ZoneInfo

import pandas as pd
from loguru import logger
from pandas.api.types import is_bool
from pydantic import BaseModel, ConfigDict, Field, model_validator

SHANGHAI = ZoneInfo("Asia/Shanghai")
DAILY_STATE_DECISION_TIME = time(15, 0)
CHINEXT_20_PERCENT_START = date(2020, 8, 24)
MAIN_BOARD_FIVE_DAY_NO_LIMIT_START = date(2023, 4, 10)
_STATUS_REQUIRED_COLUMNS = {
    "ts_code",
    "trade_date",
    "name",
    "is_st",
    "available_at",
}
_LISTING_ELIGIBILITY_COLUMNS = {
    "list_date",
    "fifth_listing_trade_date",
}


class DailyStateSeed(BaseModel):
    """Immediate authoritative predecessor state for incremental derivation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    trade_date: date
    is_limit_up: bool | None
    consecutive_limit_ups: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def validate_chain(self) -> DailyStateSeed:
        if self.is_limit_up is None and self.consecutive_limit_ups is not None:
            raise ValueError("unknown predecessor cannot have a consecutive count")
        if self.is_limit_up is False and self.consecutive_limit_ups != 0:
            raise ValueError("non-limit predecessor must have consecutive count zero")
        if self.is_limit_up is True and self.consecutive_limit_ups == 0:
            raise ValueError("limit-up predecessor count must be positive or unknown")
        return self


class ListingPriceLimitFact(BaseModel):
    """Authoritative listing window needed before applying exchange limits."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    list_date: date
    fifth_listing_trade_date: date | None = None

    @model_validator(mode="after")
    def validate_window(self) -> ListingPriceLimitFact:
        if (
            self.fifth_listing_trade_date is not None
            and self.fifth_listing_trade_date < self.list_date
        ):
            raise ValueError("fifth listing trade date cannot precede list date")
        return self


def _round_half_up(series: pd.Series) -> pd.Series:
    """Round exchange limit prices to cents using ROUND_HALF_UP."""
    return series.apply(
        lambda value: float(
            Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        )
        if pd.notna(value)
        else pd.NA
    ).astype("Float64")


def _classify_board(ts_code: str) -> str:
    """Classify a Tushare code as main/gem/star/bj."""
    if ts_code.endswith(".BJ"):
        return "bj"
    if ts_code.startswith(("688", "689")):
        return "star"
    if ts_code.startswith(("300", "301")):
        return "gem"
    return "main"


def _detect_st(name: str | None) -> bool:
    """Detect ST prefixes for live snapshot consumers, never historical derivation."""
    if not isinstance(name, str) or not name:
        return False
    normalized = name.upper().replace(" ", "").replace("\u3000", "")
    return normalized.startswith(("ST", "*ST", "SST"))


def _limit_pct(is_st: bool, board_type: str) -> float:
    """Return the current simplified exchange limit percentage."""
    if board_type == "bj":
        return 0.30
    if board_type in ("gem", "star"):
        return 0.20
    return 0.05 if is_st else 0.10


def _historical_limit_pct(
    is_st: bool,
    board_type: str,
    trade_date: date,
) -> float:
    if board_type == "gem" and trade_date < CHINEXT_20_PERCENT_START:
        return 0.05 if is_st else 0.10
    return _limit_pct(is_st, board_type)


def _requires_five_day_listing_window(
    board_type: str,
    list_date: date,
) -> bool:
    if board_type == "star":
        return True
    if board_type == "gem":
        return list_date >= CHINEXT_20_PERCENT_START
    if board_type == "main":
        return list_date >= MAIN_BOARD_FIVE_DAY_NO_LIMIT_START
    return False


def _civil_date(value: object) -> date | None:
    if value is None:
        return None
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        return None
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return pd.Timestamp(value).date()
    except (TypeError, ValueError):
        return None


def _single_fact_date(values: pd.Series) -> date | None:
    resolved = {value for raw in values.tolist() if (value := _civil_date(raw)) is not None}
    return next(iter(resolved)) if len(resolved) == 1 else None


def _listing_price_limit_fact(df: pd.DataFrame) -> ListingPriceLimitFact | None:
    list_date = _single_fact_date(df["list_date"])
    if list_date is None:
        return None
    fifth_date = _single_fact_date(df["fifth_listing_trade_date"])
    try:
        return ListingPriceLimitFact(
            list_date=list_date,
            fifth_listing_trade_date=fifth_date,
        )
    except ValueError:
        return None


def _price_limit_eligibility(
    trade_dates: list[date | None],
    *,
    board_type: str,
    fact: ListingPriceLimitFact | None,
) -> pd.Series:
    values: list[object] = []
    for trade_date in trade_dates:
        if trade_date is None or fact is None or trade_date < fact.list_date:
            values.append(pd.NA)
            continue
        if trade_date == fact.list_date:
            values.append(False)
            continue
        if _requires_five_day_listing_window(board_type, fact.list_date):
            if fact.fifth_listing_trade_date is None:
                values.append(pd.NA)
            else:
                values.append(trade_date > fact.fifth_listing_trade_date)
            continue
        values.append(True)
    return pd.Series(values, dtype="boolean")


def _aware_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        return None
    if isinstance(value, pd.Timestamp):
        resolved = value.to_pydatetime()
    elif isinstance(value, datetime):
        resolved = value
    else:
        return None
    if resolved.tzinfo is None or resolved.utcoffset() is None:
        return None
    return resolved


def _has_conflict(value: object) -> bool:
    if value is None:
        return False
    try:
        if bool(pd.isna(value)):
            return False
    except (TypeError, ValueError):
        return True
    if isinstance(value, bool):
        return value
    return bool(str(value).strip())


def _status_by_trade_date(
    status_daily: pd.DataFrame,
    *,
    ts_code: str,
) -> dict[date, bool]:
    if status_daily.empty:
        return {}
    missing = _STATUS_REQUIRED_COLUMNS - set(status_daily.columns)
    conflict_column = (
        "conflict_reason"
        if "conflict_reason" in status_daily.columns
        else "conflict"
        if "conflict" in status_daily.columns
        else None
    )
    if missing or conflict_column is None:
        details = sorted(missing | ({"conflict_reason"} if conflict_column is None else set()))
        raise ValueError(f"status_daily missing required columns: {details}")

    candidates: dict[date, list[bool]] = {}
    for row in status_daily.to_dict(orient="records"):
        if row["ts_code"] != ts_code:
            continue
        trade_date = _civil_date(row["trade_date"])
        if trade_date is None or _has_conflict(row[conflict_column]):
            continue
        is_st = row["is_st"]
        if not is_bool(is_st):
            continue
        name = row["name"]
        if not isinstance(name, str) or not name.strip():
            continue
        available_at = _aware_datetime(row["available_at"])
        decision_at = datetime.combine(
            trade_date,
            DAILY_STATE_DECISION_TIME,
            tzinfo=SHANGHAI,
        )
        if available_at is None or available_at > decision_at:
            continue
        candidates.setdefault(trade_date, []).append(bool(is_st))

    return {
        trade_date: values[0]
        for trade_date, values in candidates.items()
        if len(values) == 1
    }


def _nullable_limit_chain(
    is_limit_up: pd.Series,
    trade_dates: list[date | None],
    expected_pretrade_dates: list[date | None],
    seed: DailyStateSeed | None,
) -> tuple[pd.Series, pd.Series]:
    first_values: list[object] = []
    consecutive_values: list[object] = []
    previous_count = seed.consecutive_limit_ups if seed is not None else None
    previous_observed_date = seed.trade_date if seed is not None else None
    for value, trade_date, expected_pretrade_date in zip(
        is_limit_up,
        trade_dates,
        expected_pretrade_dates,
        strict=True,
    ):
        has_authoritative_predecessor = (
            previous_observed_date is not None
            and expected_pretrade_date is not None
            and expected_pretrade_date == previous_observed_date
        )
        if pd.isna(value):
            first_values.append(pd.NA)
            consecutive_values.append(pd.NA)
            previous_count = None
        elif not bool(value):
            first_values.append(False)
            consecutive_values.append(0)
            previous_count = 0
        elif previous_count is None or not has_authoritative_predecessor:
            first_values.append(pd.NA)
            consecutive_values.append(pd.NA)
            previous_count = None
        else:
            first_values.append(previous_count == 0)
            previous_count += 1
            consecutive_values.append(previous_count)
        previous_observed_date = trade_date
    return (
        pd.Series(first_values, index=is_limit_up.index, dtype="boolean"),
        pd.Series(consecutive_values, index=is_limit_up.index, dtype="Int64"),
    )


def derive_state(
    df_daily: pd.DataFrame,
    ts_code: str,
    status_daily: pd.DataFrame,
    price_tol: float = 0.01,
    *,
    seed: DailyStateSeed | None = None,
) -> pd.DataFrame:
    """Derive one security's daily state using exact-date point-in-time status.

    A status fact is usable only when it belongs to ``ts_code`` and the exact
    trade date, is non-conflicting, has an explicit boolean ``is_st``, and was
    visible by that date's 15:00 Asia/Shanghai decision timestamp.
    """
    if df_daily.empty:
        return pd.DataFrame()

    missing_listing_columns = _LISTING_ELIGIBILITY_COLUMNS - set(df_daily.columns)
    if missing_listing_columns:
        raise ValueError(f"listing eligibility requires columns: {sorted(missing_listing_columns)}")

    df = df_daily.sort_values("trade_date").reset_index(drop=True)
    status_by_date = _status_by_trade_date(status_daily, ts_code=ts_code)
    trade_dates = [_civil_date(value) for value in df["trade_date"]]
    if "expected_pretrade_date" in df.columns:
        expected_pretrade_dates = [
            _civil_date(value) for value in df["expected_pretrade_date"]
        ]
    else:
        expected_pretrade_dates = [None] * len(df)
    is_st = pd.Series(
        [status_by_date.get(trade_date, pd.NA) for trade_date in trade_dates],
        dtype="boolean",
    )

    is_bj = ts_code.endswith(".BJ")
    board_type = _classify_board(ts_code)
    listing_fact = _listing_price_limit_fact(df)
    price_limit_eligible = _price_limit_eligibility(
        trade_dates,
        board_type=board_type,
        fact=listing_fact,
    )
    limit_pct = pd.Series(
        [
            pd.NA
            if (pd.isna(value) or trade_date is None or pd.isna(eligible) or not bool(eligible))
            else _historical_limit_pct(bool(value), board_type, trade_date)
            for value, trade_date, eligible in zip(
                is_st,
                trade_dates,
                price_limit_eligible,
                strict=True,
            )
        ],
        dtype="Float64",
    )

    open_ = pd.to_numeric(df["open"], errors="coerce").astype("Float64")
    close = pd.to_numeric(df["close"], errors="coerce").astype("Float64")
    high = pd.to_numeric(df["high"], errors="coerce").astype("Float64")
    low = pd.to_numeric(df["low"], errors="coerce").astype("Float64")
    pre_close = pd.to_numeric(df["pre_close"], errors="coerce").astype("Float64")

    limit_up_price = _round_half_up(pre_close * (1 + limit_pct))
    limit_down_price = _round_half_up(pre_close * (1 - limit_pct))

    known_status = is_st.notna()
    valid_price = close.notna() & pre_close.notna() & (pre_close > 0)
    is_limit_up = pd.Series(pd.NA, index=df.index, dtype="boolean")
    is_limit_down = pd.Series(pd.NA, index=df.index, dtype="boolean")
    comparable = known_status & valid_price & price_limit_eligible.fillna(False)
    is_limit_up.loc[comparable] = (
        close.loc[comparable] >= limit_up_price.loc[comparable] - price_tol
    )
    is_limit_down.loc[comparable] = (
        close.loc[comparable] <= limit_down_price.loc[comparable] + price_tol
    )

    is_first_limit_up, consecutive = _nullable_limit_chain(
        is_limit_up,
        trade_dates,
        expected_pretrade_dates,
        seed,
    )

    ohlc = pd.concat([high, low, open_, close], axis=1)
    cent_ohlc = pd.concat(
        [_round_half_up(series) for _, series in ohlc.items()],
        axis=1,
    )
    equal_at_cent = cent_ohlc.eq(cent_ohlc.iloc[:, 0], axis=0).all(axis=1)
    is_yiziban = pd.Series(pd.NA, index=df.index, dtype="boolean")
    known_limit_up = is_limit_up.notna()
    complete_ohlc = ohlc.notna().all(axis=1)
    observable_yiziban = known_limit_up & complete_ohlc
    is_yiziban.loc[observable_yiziban] = False
    flat_limit_up = (
        observable_yiziban & is_limit_up.fillna(False) & equal_at_cent
    )
    is_yiziban.loc[flat_limit_up] = True

    body_upper = pd.concat([open_, close], axis=1).max(axis=1)
    body_lower = pd.concat([open_, close], axis=1).min(axis=1)

    result = pd.DataFrame(
        {
            "ts_code": ts_code,
            "trade_date": df["trade_date"],
            "is_st": is_st,
            "is_bj": is_bj,
            "board_type": board_type,
            "limit_pct": limit_pct,
            "limit_up_price": limit_up_price,
            "limit_down_price": limit_down_price,
            "is_limit_up": is_limit_up,
            "is_limit_down": is_limit_down,
            "is_first_limit_up": is_first_limit_up,
            "is_yiziban": is_yiziban,
            "consecutive_limit_ups": consecutive,
            "body_upper": body_upper,
            "body_lower": body_lower,
        }
    )

    logger.debug(f"{ts_code} 派生状态 {len(result)} 行")
    return result
