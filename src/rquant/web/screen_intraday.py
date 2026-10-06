"""Read one pinned intraday projection and retain the original daily rule inputs."""

from __future__ import annotations

from datetime import datetime

import pandas as pd
from pydantic import BaseModel, ConfigDict

from rquant.feature_contracts import FeatureAvailability
from rquant.intraday_feature_engine import MARKET_MINUTE_FEATURE_MAX_DELAY_SECONDS
from rquant.runtime_contracts import canonical_sha256
from rquant.screen.core import _collect_aggregates
from rquant.screen.dynamic_rsi import VerifiedDynamicRsiProjection
from rquant.screen.intraday_contracts import (
    INTRADAY_FIELD_LABELS,
    IntradayMarketRow,
    IntradayScreenSnapshot,
    IntradayStockProjectionRow,
    decode_intraday_source_wire,
    decode_intraday_stock_rows,
)
from rquant.screen.replica_source import (
    ScreenReplicaDataError,
    ScreenReplicaUnavailableError,
    VerifiedReplicaScreenSource,
)
from rquant.screen.rules import Rule, required_rule_columns
from rquant.web import readers
from rquant.web.models.screen import ScreenSourceInfo
from rquant.web.serving import BorrowedGeneration


class IntradayScreenUnavailableError(ValueError):
    pass


class IntradayScreenContext(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    snapshot: IntradayScreenSnapshot
    source: ScreenSourceInfo
    daily_source_identity: str | None
    fundamental_fields: frozenset[str]
    dynamic_ma: bool
    dynamic_rsi: bool
    extra_fields: tuple[tuple[str, str], ...]


class IntradayScreenInputs(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    source: ScreenSourceInfo
    frame: pd.DataFrame


def read_intraday_screen_snapshot(
    borrowed: BorrowedGeneration | None, *, now: datetime
) -> IntradayScreenSnapshot:
    if borrowed is None or borrowed.fallback_detail is not None:
        raise IntradayScreenUnavailableError("intraday Serving generation is unavailable")
    states = readers.table_states(borrowed.cursor)
    names = ("intraday_screen_source", "intraday_feature_snapshot", "market_snapshot")
    if any(name not in states or not states[name].available for name in names):
        raise IntradayScreenUnavailableError("intraday projection is unpublished")
    source_rows = borrowed.cursor.execute(
        "SELECT source_identity,trade_date,cutoff,payload_json FROM intraday_screen_source "
        "ORDER BY source_identity LIMIT 2"
    ).fetchall()
    if len(source_rows) != 1 or len(source_rows[0][3].encode("utf-8")) > 2 * 1024 * 1024:
        raise IntradayScreenUnavailableError("intraday source evidence is unavailable")
    source = decode_intraday_source_wire(source_rows[0][3])
    if source_rows[0][:3] != (source.source_identity, source.trade_date, source.cutoff):
        raise IntradayScreenUnavailableError("intraday source row differs from its evidence")
    if (
        source.cutoff > now
        or source.cutoff > borrowed.manifest.built_at
        or (now - source.cutoff).total_seconds() > MARKET_MINUTE_FEATURE_MAX_DELAY_SECONDS
        or any(states[name].available_at != source.cutoff for name in names)
    ):
        raise IntradayScreenUnavailableError("intraday source cutoff is unavailable or stale")
    stock_rows = borrowed.cursor.execute(
        "SELECT source_identity,ts_code,payload_json FROM intraday_feature_snapshot "
        "ORDER BY source_identity,ts_code LIMIT 8001"
    ).fetchall()
    if (
        len(stock_rows) > 8000
        or sum(len(row[2].encode("utf-8")) for row in stock_rows) > 32 * 1024 * 1024
    ):
        raise IntradayScreenUnavailableError("intraday feature evidence exceeds its bound")
    stocks = decode_intraday_stock_rows(tuple(IntradayStockProjectionRow(source_identity=row[0], ts_code=row[1], payload_json=row[2]) for row in stock_rows), source_identity=source.source_identity)
    if any(
        row[:2] != (source.source_identity, stock.ts_code)
        for row, stock in zip(stock_rows, stocks, strict=True)
    ):
        raise IntradayScreenUnavailableError("intraday stock row differs from its evidence")
    market_rows = borrowed.cursor.execute(
        "SELECT as_of,ts_code,name,price,open,high,low,pre_close,pct_chg,volume,amount "
        "FROM market_snapshot ORDER BY ts_code LIMIT 8001"
    ).fetchall()
    if len(market_rows) > 8000:
        raise IntradayScreenUnavailableError("intraday market rows exceed their bound")
    market = tuple(
        IntradayMarketRow.model_validate(
            dict(zip(IntradayMarketRow.model_fields, row, strict=True))
        )
        for row in market_rows
    )
    return IntradayScreenSnapshot(source=source, stocks=stocks, market_rows=market)


def intraday_screen_context(
    borrowed: BorrowedGeneration | None,
    *,
    now: datetime,
    replica: VerifiedReplicaScreenSource | None,
    rsi: VerifiedDynamicRsiProjection | None,
) -> IntradayScreenContext:
    snapshot = read_intraday_screen_snapshot(borrowed, now=now)
    source = snapshot.source
    daily_identity = None
    fundamental_fields: frozenset[str] = frozenset()
    rsi_ready = False
    if replica is not None:
        try:
            dates = replica.available_dates()
            if source.daily_anchor_date in dates.dates and dates.updated_at <= source.cutoff:
                daily_identity = dates.identity
                fundamental_fields = replica.available_fundamental_fields(
                    expected_identity=dates.identity, dates=[source.daily_anchor_date]
                )
                if rsi is not None:
                    rsi_ready = source.daily_anchor_date in rsi.catalog(dates.identity).dates
                if replica.generation_identity() != daily_identity:
                    raise ScreenReplicaUnavailableError("intraday daily generation changed")
        except (ScreenReplicaUnavailableError, ScreenReplicaDataError, RuntimeError, ValueError):
            daily_identity, fundamental_fields, rsi_ready = None, frozenset(), False
    fields = tuple(
        (name, label)
        for name, label in INTRADAY_FIELD_LABELS.items()
        if any(
            fact.name == name and fact.status is FeatureAvailability.AVAILABLE
            for stock in snapshot.stocks
            for fact in stock.fields
        )
    )
    identity = canonical_sha256(
        {
            "contract": "intraday-screen-request/v1",
            "serving_generation": borrowed.manifest.generation_id,
            "intraday_source": source.source_identity,
            "daily_source": daily_identity,
            "daily_anchor": source.daily_anchor_date,
        }
    )
    info = ScreenSourceInfo(
        identity=identity,
        updated_at=source.cutoff,
        mode="intraday",
        cutoff=source.cutoff,
        daily_anchor_date=source.daily_anchor_date,
        intraday_source_identity=source.source_identity,
        coverage_count=len(source.universe_codes) - len(source.missing_codes),
        missing_count=len(source.missing_codes),
    )
    return IntradayScreenContext(
        snapshot=snapshot,
        source=info,
        daily_source_identity=daily_identity,
        fundamental_fields=fundamental_fields,
        dynamic_ma=daily_identity is not None,
        dynamic_rsi=rsi_ready,
        extra_fields=fields,
    )


def prepare_intraday_screen_frame(
    context: IntradayScreenContext,
    *,
    borrowed: BorrowedGeneration,
    rules: list[Rule],
    rank_columns: list[str],
    replica: VerifiedReplicaScreenSource | None,
    rsi: VerifiedDynamicRsiProjection | None,
    closed_bar: bool = False,
) -> IntradayScreenInputs:
    source = context.snapshot.source
    frame = pd.DataFrame({"ts_code": source.universe_codes})
    required = (
        required_rule_columns(rules)
        | set(rank_columns)
        | {request.name for request in _collect_aggregates(rules)}
    )
    daily_columns = required - set(INTRADAY_FIELD_LABELS)
    daily: pd.DataFrame | None = None
    if replica is not None and context.daily_source_identity is not None:
        daily = replica.load(
            source.daily_anchor_date,
            rules,
            expected_identity=context.daily_source_identity,
            include_columns=rank_columns,
            rsi_projection=rsi if context.dynamic_rsi else None,
            intraday_columns=frozenset(INTRADAY_FIELD_LABELS),
        ).frame
    elif replica is None:
        state = readers.table_states(borrowed.cursor).get("nl_screen_universe")
        if (
            state is not None
            and state.available
            and state.available_at is not None
            and state.available_at <= source.cutoff
        ):
            daily = borrowed.cursor.execute(
                "SELECT * FROM nl_screen_universe WHERE trade_date=? ORDER BY ts_code LIMIT 8001",
                [source.daily_anchor_date],
            ).fetchdf()
            if len(daily) > 8000 or daily.ts_code.duplicated().any():
                raise IntradayScreenUnavailableError("intraday daily anchor exceeds its bound")
    if daily is not None:
        daily = daily.drop(columns=["trade_date"], errors="ignore")
        frame = frame.merge(daily, on="ts_code", how="left", validate="one_to_one")
    for column in daily_columns | {"name", "CLOSE[0]", "PCT_CHG[0]"}:
        if column not in frame:
            frame[column] = None
    for name in INTRADAY_FIELD_LABELS:
        values = {
            stock.ts_code: next(
                (fact.value if fact.status is FeatureAvailability.AVAILABLE else None
                for fact in (stock.closed_bar.fields if stock.closed_bar else ()) if fact.name == name),
                None,
            ) if closed_bar else next(
                fact.value if fact.status is FeatureAvailability.AVAILABLE else None
                for fact in stock.fields if fact.name == name
            )
            for stock in context.snapshot.stocks
        }
        frame[name] = frame.ts_code.map(values).astype("float64")
    frame["trade_date"] = source.trade_date
    return IntradayScreenInputs(source=context.source, frame=frame)
