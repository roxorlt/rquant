"""Pure point-in-time intraday features shared by live and replay runners."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Annotated, Literal
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from pydantic import Field, StringConstraints, model_validator

from rquant.feature_contracts import (
    FeatureAvailability,
    FeatureBatchEnvelope,
    FeatureFieldStatus,
)
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256

CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]

SHANGHAI = ZoneInfo("Asia/Shanghai")
OPENING_START_MINUTE = 9 * 60 + 30
INPUT_COLUMNS = (
    "ts_code",
    "trade_time",
    "available_at",
    "open",
    "high",
    "low",
    "close",
    "vol",
    "amount",
)
FEATURE_COLUMNS = (
    "ts_code",
    "feature_time",
    "latest_open",
    "latest_high",
    "latest_low",
    "latest_close",
    "minute_volume",
    "cumulative_volume",
    "session_open",
    "session_high",
    "session_low",
    "opening_bar_open",
    "opening_bar_high",
    "opening_bar_low",
    "opening_bar_close",
    "minute_amount",
    "cumulative_amount",
    "hist_same_minute_amount_median",
    "hist_cumulative_amount_median",
    "rel_same_minute",
    "rel_cumulative",
    "amount_accel_5m",
    "amount_accel_10m",
    "cumulative_vwap",
    "price_over_vwap",
    "tick_rule_buy_volume_proxy",
    "tick_rule_sell_volume_proxy",
    "tick_rule_buy_sell_ratio_proxy",
    "tick_rule_proxy_method",
    "tick_rule_proxy_quality",
    "historical_sessions",
    "same_clock_sessions",
)
STATUS_COLUMNS = FEATURE_COLUMNS[2:]


class IntradayFeatureValidationError(ValueError):
    pass


class FeatureComputationMode(StrEnum):
    LIVE = "live"
    REPLAY = "replay"


class IntradayFeatureConfig(RuntimeContractModel):
    lookback_sessions: int = Field(default=20, ge=1)
    opening_acceleration_block_minutes: int = Field(default=3, ge=0, le=30)
    bar_timestamp_semantics: Literal["bar_end"] = "bar_end"
    contract_id: str = Field(default="intraday-pit", min_length=1)
    contract_version: Literal[3] = 3
    schema_version: int = Field(default=2, ge=2)
    producer_commit: CommitSha


class FeatureComputationResult(RuntimeContractModel):
    mode: FeatureComputationMode
    payload_json: str = Field(min_length=1)
    envelope: FeatureBatchEnvelope

    @model_validator(mode="after")
    def validate_payload(self) -> FeatureComputationResult:
        if hashlib.sha256(self.payload_bytes).hexdigest() != self.envelope.content_hash:
            raise ValueError("payload hash does not match feature envelope")
        payload = json.loads(self.payload_json)
        if len(payload["rows"]) != self.envelope.row_count:
            raise ValueError("payload row count does not match feature envelope")
        return self

    @property
    def payload_bytes(self) -> bytes:
        return self.payload_json.encode("utf-8")

    @property
    def frame(self) -> pd.DataFrame:
        payload = json.loads(self.payload_json)
        frame = pd.DataFrame(payload["rows"], columns=FEATURE_COLUMNS)
        if not frame.empty:
            frame["feature_time"] = pd.to_datetime(frame["feature_time"], utc=True)
        return frame


def _as_shanghai_timestamp(value: object, *, field_name: str) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(value)
        if pd.isna(timestamp):
            raise ValueError("timestamp cannot be NaT")
        if timestamp.tzinfo is None:
            return timestamp.tz_localize(
                SHANGHAI,
                ambiguous="raise",
                nonexistent="raise",
            )
        return timestamp.tz_convert(SHANGHAI)
    except (TypeError, ValueError) as exc:
        raise IntradayFeatureValidationError(f"invalid {field_name}") from exc


def _normalize_frame(
    frame: pd.DataFrame,
    *,
    label: str,
    visible_through: datetime | None = None,
) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame):
        raise IntradayFeatureValidationError(f"{label} must be a DataFrame")
    missing = sorted(set(INPUT_COLUMNS) - set(frame.columns))
    if missing:
        raise IntradayFeatureValidationError(f"{label} missing columns: {missing}")

    normalized = frame.loc[:, INPUT_COLUMNS].copy()
    if normalized.empty:
        normalized["_local_time"] = pd.Series(dtype="datetime64[ns, Asia/Shanghai]")
        normalized["_utc_time"] = pd.Series(dtype="datetime64[ns, UTC]")
        normalized["_available_utc"] = pd.Series(dtype="datetime64[ns, UTC]")
        normalized["_trade_date"] = pd.Series(dtype="object")
        normalized["_clock_minute"] = pd.Series(dtype="int64")
        return normalized

    try:
        local_times = [
            _as_shanghai_timestamp(value, field_name=f"{label}.trade_time")
            for value in normalized["trade_time"]
        ]
        normalized["_local_time"] = pd.DatetimeIndex(local_times)
        normalized["_utc_time"] = normalized["_local_time"].dt.tz_convert(UTC)
    except (TypeError, ValueError) as exc:
        raise IntradayFeatureValidationError(f"invalid {label} value") from exc

    if visible_through is not None:
        normalized = normalized[normalized["_utc_time"] <= pd.Timestamp(visible_through)].copy()
        if normalized.empty:
            normalized["_available_utc"] = pd.Series(dtype="datetime64[ns, UTC]")
            normalized["_trade_date"] = pd.Series(dtype="object")
            normalized["_clock_minute"] = pd.Series(dtype="int64")
            return normalized

    if (
        (normalized["_local_time"].dt.second != 0) | (normalized["_local_time"].dt.microsecond != 0)
    ).any():
        raise IntradayFeatureValidationError(f"{label} trade_time must be whole-minute bars")
    clock_minute = normalized["_local_time"].dt.hour * 60 + normalized["_local_time"].dt.minute
    in_continuous_session = clock_minute.between(
        9 * 60 + 30,
        11 * 60 + 30,
    ) | clock_minute.between(13 * 60, 15 * 60)
    if not in_continuous_session.all():
        raise IntradayFeatureValidationError(
            f"{label} trade_time must be within a continuous auction session"
        )

    normalized["ts_code"] = normalized["ts_code"].astype("string").str.strip()
    if normalized["ts_code"].isna().any() or (normalized["ts_code"] == "").any():
        raise IntradayFeatureValidationError(f"{label} ts_code cannot be empty")
    try:
        for column in INPUT_COLUMNS[3:]:
            normalized[column] = pd.to_numeric(
                normalized[column],
                errors="raise",
            ).astype("float64")
    except (TypeError, ValueError) as exc:
        raise IntradayFeatureValidationError(f"invalid {label} value") from exc

    numeric = normalized.loc[:, INPUT_COLUMNS[3:]].to_numpy(dtype="float64")
    if not np.isfinite(numeric).all():
        raise IntradayFeatureValidationError(f"{label} numeric values must be finite")
    if (normalized[["vol", "amount"]] < 0).any().any():
        raise IntradayFeatureValidationError(f"{label} vol and amount cannot be negative")
    if (normalized[["open", "high", "low", "close"]] <= 0).any().any():
        raise IntradayFeatureValidationError(f"{label} OHLC prices must be strictly positive")
    required_high = normalized[["open", "close", "low"]].max(axis=1)
    required_low = normalized[["open", "close", "high"]].min(axis=1)
    if ((normalized["high"] < required_high) | (normalized["low"] > required_low)).any():
        raise IntradayFeatureValidationError(f"{label} contains invalid OHLC geometry")
    if normalized.duplicated(subset=["ts_code", "_utc_time"]).any():
        raise IntradayFeatureValidationError(
            f"{label} contains duplicate ts_code and trade_time rows"
        )
    natural_minute = normalized["_local_time"].dt.floor("min")
    if (
        normalized.assign(_natural_minute=natural_minute)
        .duplicated(subset=["ts_code", "_natural_minute"])
        .any()
    ):
        raise IntradayFeatureValidationError(
            f"{label} contains multiple bars for one ts_code natural minute"
        )

    try:
        available_times = [
            _as_shanghai_timestamp(value, field_name=f"{label}.available_at")
            for value in normalized["available_at"]
        ]
        normalized["_available_utc"] = pd.DatetimeIndex(available_times).tz_convert(UTC)
    except (TypeError, ValueError) as exc:
        raise IntradayFeatureValidationError(f"invalid {label} value") from exc
    if (normalized["_available_utc"] < normalized["_utc_time"]).any():
        raise IntradayFeatureValidationError(
            f"{label} available_at cannot precede bar-end trade_time"
        )

    normalized["_trade_date"] = normalized["_local_time"].dt.date
    normalized["_clock_minute"] = clock_minute.loc[normalized.index]
    return normalized.sort_values(
        ["ts_code", "_utc_time"],
        kind="stable",
    ).reset_index(drop=True)


def _median(values: pd.Series) -> float | None:
    if values.empty:
        return None
    value = float(values.median())
    return value if np.isfinite(value) else None


def _divide(
    numerator: float,
    denominator: float | None,
    *,
    missing_reason: str,
    zero_reason: str,
) -> tuple[float | None, str | None]:
    if denominator is None:
        return None, missing_reason
    if denominator <= 0:
        return None, zero_reason
    return numerator / denominator, None


def _acceleration(
    rows: pd.DataFrame,
    *,
    current_amount: float,
    feature_minute: int,
    window: int,
    opening_acceleration_block_minutes: int,
) -> tuple[float | None, str | None]:
    opening_end = OPENING_START_MINUTE + opening_acceleration_block_minutes
    if OPENING_START_MINUTE <= feature_minute < opening_end:
        return None, "opening_segment"
    prior = rows.iloc[:-1]["amount"].tail(window)
    if len(prior) < window:
        return None, "insufficient_prior_minutes"
    window_rows = rows.iloc[-(window + 1) :]
    gaps = window_rows["_utc_time"].diff().dropna()
    if not (gaps == pd.Timedelta(minutes=1)).all():
        latest = window_rows.iloc[-1]
        previous = window_rows.iloc[-2]
        if (
            int(previous["_clock_minute"]) <= 11 * 60 + 30
            and int(latest["_clock_minute"]) >= 13 * 60
        ):
            return None, "session_break"
        return None, "non_contiguous_minutes"
    baseline = float(prior.median())
    if baseline <= 0:
        return None, "zero_prior_minute_baseline"
    return current_amount / baseline, None


def _tick_rule_volume_proxies(rows: pd.DataFrame) -> tuple[float, float]:
    buy_proxy = 0.0
    sell_proxy = 0.0
    previous_close: float | None = None
    for row in rows.itertuples(index=False):
        close = float(row.close)
        comparison = float(row.open) if previous_close is None else previous_close
        if close > comparison:
            buy_proxy += float(row.vol)
        elif close < comparison:
            sell_proxy += float(row.vol)
        else:
            buy_proxy += float(row.vol) / 2
            sell_proxy += float(row.vol) / 2
        previous_close = close
    return buy_proxy, sell_proxy


def _history_for_code(
    historical: pd.DataFrame,
    *,
    ts_code: str,
    decision_date: date,
    lookback_sessions: int,
) -> pd.DataFrame:
    eligible = historical[
        (historical["ts_code"] == ts_code) & (historical["_trade_date"] < decision_date)
    ]
    dates = sorted(eligible["_trade_date"].unique(), reverse=True)[:lookback_sessions]
    return eligible[eligible["_trade_date"].isin(dates)]


def _compute_code_row(
    current: pd.DataFrame,
    historical: pd.DataFrame,
    *,
    ts_code: str,
    decision_date: date,
    lookback_sessions: int,
    opening_acceleration_block_minutes: int,
) -> tuple[dict[str, object], dict[str, str | None]]:
    rows = current[current["ts_code"] == ts_code].sort_values("_utc_time", kind="stable")
    latest = rows.iloc[-1]
    opening_rows = rows[rows["_clock_minute"] == OPENING_START_MINUTE]
    opening = None if opening_rows.empty else opening_rows.iloc[0]
    feature_minute = int(latest["_clock_minute"])
    minute_amount = float(latest["amount"])
    cumulative_amount = float(rows["amount"].sum())
    cumulative_volume = float(rows["vol"].sum())

    history = _history_for_code(
        historical,
        ts_code=ts_code,
        decision_date=decision_date,
        lookback_sessions=lookback_sessions,
    )
    history_to_clock = history[history["_clock_minute"] <= feature_minute]
    same_clock = history[history["_clock_minute"] == feature_minute]
    cumulative_by_date = history_to_clock.groupby("_trade_date", sort=True)["amount"].sum()
    same_median = _median(same_clock["amount"])
    cumulative_median = _median(cumulative_by_date)

    rel_same, rel_same_reason = _divide(
        minute_amount,
        same_median,
        missing_reason="missing_same_clock_history",
        zero_reason="zero_same_clock_baseline",
    )
    rel_cumulative, rel_cumulative_reason = _divide(
        cumulative_amount,
        cumulative_median,
        missing_reason="missing_cumulative_history",
        zero_reason="zero_cumulative_baseline",
    )
    accel_5m, accel_5m_reason = _acceleration(
        rows,
        current_amount=minute_amount,
        feature_minute=feature_minute,
        window=5,
        opening_acceleration_block_minutes=opening_acceleration_block_minutes,
    )
    accel_10m, accel_10m_reason = _acceleration(
        rows,
        current_amount=minute_amount,
        feature_minute=feature_minute,
        window=10,
        opening_acceleration_block_minutes=opening_acceleration_block_minutes,
    )
    cumulative_vwap, vwap_reason = _divide(
        cumulative_amount,
        cumulative_volume,
        missing_reason="missing_cumulative_volume",
        zero_reason="zero_cumulative_volume",
    )
    price_over_vwap, price_vwap_reason = _divide(
        float(latest["close"]),
        cumulative_vwap,
        missing_reason=vwap_reason or "missing_cumulative_vwap",
        zero_reason="zero_cumulative_vwap",
    )
    buy_proxy, sell_proxy = _tick_rule_volume_proxies(rows)
    buy_sell_proxy, buy_sell_reason = _divide(
        buy_proxy,
        sell_proxy,
        missing_reason="missing_tick_rule_sell_volume_proxy",
        zero_reason="zero_tick_rule_sell_volume_proxy",
    )

    row: dict[str, object] = {
        "ts_code": ts_code,
        "feature_time": latest["_utc_time"].isoformat(),
        "latest_open": float(latest["open"]),
        "latest_high": float(latest["high"]),
        "latest_low": float(latest["low"]),
        "latest_close": float(latest["close"]),
        "minute_volume": float(latest["vol"]),
        "cumulative_volume": cumulative_volume,
        "session_open": None if opening is None else float(opening["open"]),
        "session_high": float(rows["high"].max()),
        "session_low": float(rows["low"].min()),
        "opening_bar_open": None if opening is None else float(opening["open"]),
        "opening_bar_high": None if opening is None else float(opening["high"]),
        "opening_bar_low": None if opening is None else float(opening["low"]),
        "opening_bar_close": None if opening is None else float(opening["close"]),
        "minute_amount": minute_amount,
        "cumulative_amount": cumulative_amount,
        "hist_same_minute_amount_median": same_median,
        "hist_cumulative_amount_median": cumulative_median,
        "rel_same_minute": rel_same,
        "rel_cumulative": rel_cumulative,
        "amount_accel_5m": accel_5m,
        "amount_accel_10m": accel_10m,
        "cumulative_vwap": cumulative_vwap,
        "price_over_vwap": price_over_vwap,
        "tick_rule_buy_volume_proxy": buy_proxy,
        "tick_rule_sell_volume_proxy": sell_proxy,
        "tick_rule_buy_sell_ratio_proxy": buy_sell_proxy,
        "tick_rule_proxy_method": "minute_close_vs_previous_close",
        "tick_rule_proxy_quality": "proxy_not_order_flow",
        "historical_sessions": int(len(cumulative_by_date)),
        "same_clock_sessions": int(same_clock["_trade_date"].nunique()),
    }
    reasons: dict[str, str | None] = {
        "latest_open": None,
        "latest_high": None,
        "latest_low": None,
        "latest_close": None,
        "minute_volume": None,
        "cumulative_volume": None,
        "session_open": None if opening is not None else "missing_opening_bar",
        "session_high": None,
        "session_low": None,
        "opening_bar_open": None if opening is not None else "missing_opening_bar",
        "opening_bar_high": None if opening is not None else "missing_opening_bar",
        "opening_bar_low": None if opening is not None else "missing_opening_bar",
        "opening_bar_close": None if opening is not None else "missing_opening_bar",
        "minute_amount": None,
        "cumulative_amount": None,
        "hist_same_minute_amount_median": (
            None if same_median is not None else "missing_same_clock_history"
        ),
        "hist_cumulative_amount_median": (
            None if cumulative_median is not None else "missing_cumulative_history"
        ),
        "rel_same_minute": rel_same_reason,
        "rel_cumulative": rel_cumulative_reason,
        "amount_accel_5m": accel_5m_reason,
        "amount_accel_10m": accel_10m_reason,
        "cumulative_vwap": vwap_reason,
        "price_over_vwap": price_vwap_reason,
        "tick_rule_buy_volume_proxy": None,
        "tick_rule_sell_volume_proxy": None,
        "tick_rule_buy_sell_ratio_proxy": buy_sell_reason,
        "tick_rule_proxy_method": None,
        "tick_rule_proxy_quality": None,
        "historical_sessions": None,
        "same_clock_sessions": None,
    }
    return row, reasons


def _field_statuses(
    rows: list[dict[str, object]],
    row_reasons: list[dict[str, str | None]],
    *,
    source_event_times: dict[str, datetime],
    available_at: datetime,
    decision_cutoff: datetime,
) -> tuple[FeatureFieldStatus, ...]:
    statuses: list[FeatureFieldStatus] = []
    for row, reason_map in zip(rows, row_reasons, strict=True):
        candidate_id = str(row["ts_code"])
        source_event_time = source_event_times[candidate_id]
        for name in STATUS_COLUMNS:
            present = row[name] is not None
            statuses.append(
                FeatureFieldStatus(
                    candidate_id=candidate_id,
                    name=name,
                    status=(
                        FeatureAvailability.AVAILABLE
                        if present
                        else FeatureAvailability.UNAVAILABLE
                    ),
                    source_event_time=source_event_time,
                    available_at=available_at,
                    decision_cutoff=decision_cutoff,
                    actual_delay_seconds=(available_at - source_event_time).total_seconds(),
                    reason=None if present else reason_map[name] or "missing_value",
                )
            )
    return tuple(statuses)


def _semantic_compute(
    current_minutes: pd.DataFrame,
    historical_minutes: pd.DataFrame,
    *,
    mode: FeatureComputationMode,
    decision_time: datetime,
    input_available_at: datetime,
    input_batch_ids: tuple[str, ...],
    sequence: int,
    config: IntradayFeatureConfig,
) -> FeatureComputationResult:
    decision_local = _as_shanghai_timestamp(decision_time, field_name="decision_time")
    input_local = _as_shanghai_timestamp(
        input_available_at,
        field_name="input_available_at",
    )
    decision_utc = decision_local.tz_convert(UTC).to_pydatetime()
    input_available_utc = input_local.tz_convert(UTC).to_pydatetime()
    if input_available_utc > decision_utc:
        raise IntradayFeatureValidationError("input_available_at cannot be after decision_time")
    available_at = decision_utc

    current = _normalize_frame(
        current_minutes,
        label="current_minutes",
        visible_through=decision_utc,
    )
    historical = _normalize_frame(historical_minutes, label="historical_minutes")
    decision_date = decision_local.date()
    if (current["_trade_date"] < decision_date).any():
        raise IntradayFeatureValidationError(
            "current_minutes contains rows before decision trade date"
        )
    closed_current_day = current[current["_trade_date"] == decision_date]
    if (closed_current_day["_available_utc"] > decision_utc).any():
        raise IntradayFeatureValidationError(
            "current_minutes contains a bar not available at decision_time"
        )
    if (historical["_trade_date"] >= decision_date).any():
        raise IntradayFeatureValidationError(
            "historical_minutes must contain only prior trading sessions"
        )
    if (historical["_available_utc"] > decision_utc).any():
        raise IntradayFeatureValidationError(
            "historical_minutes contains data not available at decision_time"
        )
    if (closed_current_day["_available_utc"] > input_available_utc).any() or (
        historical["_available_utc"] > input_available_utc
    ).any():
        raise IntradayFeatureValidationError(
            "input_available_at precedes a constituent row availability"
        )
    visible = closed_current_day
    if visible.empty:
        raise IntradayFeatureValidationError("current_minutes has no rows visible at decision_time")
    source_event_time = visible["_utc_time"].max().to_pydatetime()
    source_event_times = {
        str(ts_code): group["_utc_time"].max().to_pydatetime()
        for ts_code, group in visible.groupby("ts_code", sort=True)
    }

    rows: list[dict[str, object]] = []
    reasons: list[dict[str, str | None]] = []
    for ts_code in sorted(str(value) for value in visible["ts_code"].unique()):
        row, row_reasons = _compute_code_row(
            visible,
            historical,
            ts_code=ts_code,
            decision_date=decision_date,
            lookback_sessions=config.lookback_sessions,
            opening_acceleration_block_minutes=config.opening_acceleration_block_minutes,
        )
        rows.append(row)
        reasons.append(row_reasons)

    payload_json = json.dumps(
        {"schema_version": config.schema_version, "rows": rows},
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    content_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    sorted_input_ids = tuple(sorted(input_batch_ids))
    batch_id = canonical_sha256(
        {
            "contract_id": config.contract_id,
            "contract_version": config.contract_version,
            "config_fingerprint": canonical_sha256(config.model_dump(mode="python")),
            "input_batch_ids": sorted_input_ids,
            "sequence": sequence,
            "event_time": source_event_time,
            "content_hash": content_hash,
            "producer_commit": config.producer_commit,
        }
    )
    envelope = FeatureBatchEnvelope(
        schema_version=config.schema_version,
        batch_id=batch_id,
        contract_id=config.contract_id,
        contract_version=config.contract_version,
        input_batch_ids=sorted_input_ids,
        sequence=sequence,
        event_time=source_event_time,
        available_at=available_at,
        decision_cutoff=decision_utc,
        actual_delay_seconds=(available_at - source_event_time).total_seconds(),
        row_count=len(rows),
        content_hash=content_hash,
        field_statuses=_field_statuses(
            rows,
            reasons,
            source_event_times=source_event_times,
            available_at=available_at,
            decision_cutoff=decision_utc,
        ),
        producer_commit=config.producer_commit,
    )
    return FeatureComputationResult(
        mode=mode,
        payload_json=payload_json,
        envelope=envelope,
    )


def live_compute(
    current_minutes: pd.DataFrame,
    historical_minutes: pd.DataFrame,
    *,
    decision_time: datetime,
    input_available_at: datetime,
    input_batch_ids: tuple[str, ...],
    sequence: int,
    config: IntradayFeatureConfig,
) -> FeatureComputationResult:
    return _semantic_compute(
        current_minutes,
        historical_minutes,
        mode=FeatureComputationMode.LIVE,
        decision_time=decision_time,
        input_available_at=input_available_at,
        input_batch_ids=input_batch_ids,
        sequence=sequence,
        config=config,
    )


def replay_compute(
    current_minutes: pd.DataFrame,
    historical_minutes: pd.DataFrame,
    *,
    decision_time: datetime,
    input_available_at: datetime,
    input_batch_ids: tuple[str, ...],
    sequence: int,
    config: IntradayFeatureConfig,
) -> FeatureComputationResult:
    return _semantic_compute(
        current_minutes,
        historical_minutes,
        mode=FeatureComputationMode.REPLAY,
        decision_time=decision_time,
        input_available_at=input_available_at,
        input_batch_ids=input_batch_ids,
        sequence=sequence,
        config=config,
    )
