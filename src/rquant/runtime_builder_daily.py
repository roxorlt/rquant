"""Least-privilege runtime builder for immutable daily-close source batches."""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Literal, Protocol
from zoneinfo import ZoneInfo

import pandas as pd
from pydantic import Field, field_validator, model_validator

from rquant.daily_close_gateway import (
    DailyCloseFetchResult,
    DailyCloseGateway,
    DailyCloseGatewayConfig,
    DailyCloseSourceRequest,
)
from rquant.daily_close_source_service import capture_daily_close_step
from rquant.live_spool import LiveBatchSpool
from rquant.runtime_contracts import RuntimeContractModel, normalize_aware_utc
from rquant.runtime_market_session import load_market_calendar_authority
from rquant.runtime_service_control import RuntimeServicePlane, RuntimeStepResult
from rquant.runtime_service_entrypoint import (
    RuntimeServiceBuilder,
    RuntimeServiceKind,
    RuntimeServiceManifest,
    RuntimeServiceStep,
)
from rquant.source_quota_store import SourceQuotaStore
from rquant.source_quota_transport import QuotaBoundTransportObserver

_SHANGHAI = ZoneInfo("Asia/Shanghai")

DailyCloseFetcher = Callable[[DailyCloseSourceRequest], object]


class DailyCloseRuntimeAdapter(Protocol):
    def daily_by_date(self, trade_date: date) -> pd.DataFrame: ...

    def daily_basic_by_date(self, trade_date: date) -> pd.DataFrame: ...

    def adj_factor_by_date(self, trade_date: date) -> pd.DataFrame: ...

    def index_daily_major_by_date(self, trade_date: date) -> pd.DataFrame: ...

    def stock_basic(self, list_status: str = "L") -> pd.DataFrame: ...

    def stock_st_raw(self, trade_date: date) -> pd.DataFrame: ...

    def suspend_d_raw(self, trade_date: date) -> pd.DataFrame: ...


class DailyCloseSourceSettings(RuntimeContractModel):
    spool_root: Path
    producer_version: str = Field(min_length=1)
    calendar_path: Path
    calendar_expected_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    calendar_content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    quota_path: Path | None = None
    quota_units_per_window: int | None = Field(default=None, gt=0)
    quota_accounting_mode: Literal["request", "transport"] = "request"
    quota_cost_per_request: int | None = Field(default=1, gt=0)
    pending_recovery_min_age_seconds: int = Field(default=300, strict=True, ge=30)

    @field_validator("spool_root", "calendar_path", "quota_path")
    @classmethod
    def require_normalized_absolute_path(cls, value: Path | None) -> Path | None:
        if value is None:
            return None
        if not value.is_absolute() or value != value.resolve(strict=False):
            raise ValueError("daily-close runtime paths must be absolute and normalized")
        return value

    @model_validator(mode="after")
    def validate_quota_settings(self) -> DailyCloseSourceSettings:
        configured = self.quota_path is not None or self.quota_units_per_window is not None
        if configured and (self.quota_path is None or self.quota_units_per_window is None):
            raise ValueError(
                "daily-close quota_path and quota_units_per_window must be configured together"
            )
        if self.quota_accounting_mode == "request" and self.quota_cost_per_request is None:
            raise ValueError("request quota accounting requires quota_cost_per_request")
        if self.quota_accounting_mode == "transport" and self.quota_cost_per_request is not None:
            raise ValueError("transport quota accounting cannot declare a fixed request cost")
        if self.quota_accounting_mode == "transport" and not configured:
            raise ValueError("transport quota accounting requires a quota ledger")
        return self


def _scalar(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, pd.Timestamp):
        return value.date()
    return value


def _rows(frame: pd.DataFrame, *, fields: tuple[str, ...], label: str) -> list[dict[str, object]]:
    if not isinstance(frame, pd.DataFrame):
        raise TypeError(f"daily-close {label} adapter response must be a DataFrame")
    missing = set(fields) - set(frame.columns)
    if missing:
        raise ValueError(f"daily-close {label} response lacks fields: {sorted(missing)}")
    return [
        {field: _scalar(record[field]) for field in fields}
        for record in frame.loc[:, list(fields)].to_dict(orient="records")
    ]


def _tushare_daily_close_fetcher(
    runtime_capabilities: Mapping[str, str],
    *,
    transport_observer: QuotaBoundTransportObserver | None = None,
) -> DailyCloseFetcher:
    token = runtime_capabilities.get("TUSHARE_TOKEN_MAIN", "").strip()
    if not token:
        raise RuntimeError("TUSHARE_TOKEN_MAIN capability is required")
    from rquant.adapter.tushare import TushareAdapter

    adapter: DailyCloseRuntimeAdapter = (
        TushareAdapter(token=token)
        if transport_observer is None
        else TushareAdapter(token=token, transport_observer=transport_observer)
    )

    def fetch(request: DailyCloseSourceRequest) -> object:
        trade_date = request.trade_date
        interface_calls: list[str] = []

        def call(label: str, operation: Callable[[], pd.DataFrame]) -> pd.DataFrame:
            interface_calls.append(label)
            return operation()

        daily = _rows(
            call("daily_by_date", lambda: adapter.daily_by_date(trade_date)),
            fields=(
                "ts_code",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "pre_close",
                "change",
                "pct_chg",
                "vol",
                "amount",
            ),
            label="daily_bar",
        )
        basic = _rows(
            call("daily_basic_by_date", lambda: adapter.daily_basic_by_date(trade_date)),
            fields=(
                "ts_code",
                "trade_date",
                "turnover_rate",
                "volume_ratio",
                "total_mv",
                "circ_mv",
            ),
            label="daily_basic",
        )
        factors = _rows(
            call("adj_factor_by_date", lambda: adapter.adj_factor_by_date(trade_date)),
            fields=("ts_code", "trade_date", "adj_factor"),
            label="adj_factor",
        )
        indexes = _rows(
            call(
                "index_daily_major_by_date",
                lambda: adapter.index_daily_major_by_date(trade_date),
            ),
            fields=(
                "ts_code",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "pre_close",
                "change",
                "pct_chg",
                "vol",
                "amount",
            ),
            label="index_daily",
        )
        basics = _rows(
            call("stock_basic", adapter.stock_basic),
            fields=("ts_code", "name", "list_status"),
            label="stock_basic",
        )
        status_by_code = {str(row["ts_code"]): row for row in basics}
        st_codes = {
            str(row["ts_code"])
            for row in _rows(
                call("stock_st_raw", lambda: adapter.stock_st_raw(trade_date)),
                fields=("ts_code",),
                label="stock_st",
            )
        }
        statuses: list[dict[str, object]] = []
        for row in daily:
            code = str(row["ts_code"])
            security = status_by_code.get(code)
            if security is None:
                raise ValueError("daily-close stock_basic is missing a traded security")
            statuses.append(
                {
                    "ts_code": code,
                    "trade_date": trade_date,
                    "name": security["name"],
                    "is_st": code in st_codes,
                    "listing_status": security["list_status"],
                }
            )
        suspensions = _rows(
            call("suspend_d_raw", lambda: adapter.suspend_d_raw(trade_date)),
            fields=("ts_code", "trade_date", "suspend_type", "suspend_timing"),
            label="suspension_status",
        )
        call_receipts = () if transport_observer is None else transport_observer.current_receipts()
        return DailyCloseFetchResult(
            source="tushare.daily_close",
            actual_call_count=(len(interface_calls) if not call_receipts else len(call_receipts)),
            interface_calls=(
                tuple(interface_calls)
                if not call_receipts
                else tuple(receipt.api_name for receipt in call_receipts)
            ),
            call_receipts=call_receipts,
            payload={
                "daily_bar": daily,
                "daily_basic": basic,
                "adj_factor": factors,
                "index_daily": indexes,
                "security_status": statuses,
                "suspension_status": suspensions,
            },
        )

    return fetch


def daily_close_source_builder(
    *,
    runtime_capabilities: Mapping[str, str],
    clock: Callable[[], datetime],
    fetcher: DailyCloseFetcher | None = None,
) -> RuntimeServiceBuilder:
    """Build a source that writes only immutable daily-close batches."""

    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        if manifest.service_kind is not RuntimeServiceKind.DAILY_CLOSE_SOURCE:
            raise ValueError("runtime service kind must be daily_close_source")
        if manifest.plane is not RuntimeServicePlane.LIVE:
            raise ValueError("daily-close source must run on the live plane")
        settings = DailyCloseSourceSettings.model_validate(dict(manifest.settings))
        calendar = load_market_calendar_authority(
            settings.calendar_path,
            expected_commit=settings.calendar_expected_commit,
        )
        if calendar.content_sha256 != settings.calendar_content_sha256:
            raise ValueError(
                "daily-close calendar content identity does not match runtime settings"
            )
        using_default_fetcher = fetcher is None
        quota_store = None if settings.quota_path is None else SourceQuotaStore(settings.quota_path)
        transport_observer = (
            None
            if settings.quota_accounting_mode != "transport"
            else QuotaBoundTransportObserver(
                store=quota_store,
                source="tushare.daily_close",
                quota_units_per_window=settings.quota_units_per_window,
                window_kind="day",
                clock=clock,
            )
        )
        resolved_fetcher = fetcher or _tushare_daily_close_fetcher(
            runtime_capabilities,
            transport_observer=transport_observer,
        )
        if not callable(resolved_fetcher):
            raise TypeError("daily-close source fetcher must be callable")
        gateway = DailyCloseGateway(
            spool=LiveBatchSpool(settings.spool_root),
            fetcher=resolved_fetcher,
            config=DailyCloseGatewayConfig(
                producer_version=settings.producer_version,
                producer_commit=manifest.producer_commit,
                quota_units_per_window=settings.quota_units_per_window,
                quota_accounting_mode=settings.quota_accounting_mode,
                quota_cost_per_request=settings.quota_cost_per_request,
                require_source_usage_receipt=using_default_fetcher,
                pending_recovery_min_age_seconds=(settings.pending_recovery_min_age_seconds),
            ),
            completion_clock=clock,
            quota_store=quota_store,
            transport_observer=transport_observer,
        )

        def step() -> RuntimeStepResult:
            observed = normalize_aware_utc(clock())
            gateway.recover_stale_source_attempts(observed_at=observed)
            trade_date = observed.astimezone(_SHANGHAI).date()
            market_close = datetime.combine(trade_date, time(15), tzinfo=_SHANGHAI).astimezone(UTC)
            if trade_date not in calendar.open_dates or observed < market_close:
                return RuntimeStepResult()
            return capture_daily_close_step(
                gateway,
                trade_date=trade_date,
                observed_at=observed,
            )

        return step

    return build


__all__ = [
    "DailyCloseFetcher",
    "DailyCloseRuntimeAdapter",
    "DailyCloseSourceSettings",
    "daily_close_source_builder",
]
