from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

from rquant import runtime_service_builtin as builtin_module
from rquant.live_contracts import LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.market_minute_gateway import MarketMinuteGateway
from rquant.runtime_service_builtin import (
    build_builtin_registry,
    market_minute_source_builder,
)
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest

NOW = datetime(2026, 7, 31, 1, 40, 2, tzinfo=UTC)


class _Adapter:
    def __init__(self) -> None:
        self.calls: list[tuple[tuple[str, ...], str]] = []

    def rt_min(self, codes: list[str], freq: str = "1min") -> pd.DataFrame:
        self.calls.append((tuple(codes), freq))
        return pd.DataFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "trade_time": "2026-07-31 09:40:00",
                    "open": 10.0,
                    "high": 10.2,
                    "low": 9.9,
                    "close": 10.1,
                    "vol": 1_000.0,
                    "amount": 10_100.0,
                }
            ]
        )


class _UniverseAdapter:
    def __init__(self) -> None:
        self.calls: list[tuple[tuple[str, ...], str]] = []

    def rt_min(self, codes: list[str], freq: str = "1min") -> pd.DataFrame:
        self.calls.append((tuple(codes), freq))
        return pd.DataFrame(
            [
                {
                    "ts_code": code,
                    "trade_time": "2026-07-31 09:40:00",
                    "open": 10.0,
                    "high": 10.2,
                    "low": 9.9,
                    "close": 10.1,
                    "vol": 1_000.0,
                    "amount": 10_100.0,
                }
                for code in codes
            ]
        )


def _manifest(tmp_path: Path) -> RuntimeServiceManifest:
    return RuntimeServiceManifest(
        service_id="source.market-minute",
        service_kind=RuntimeServiceKind.MARKET_MINUTE_SOURCE,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=15,
        stale_after_seconds=45,
        producer_commit="a" * 40,
        settings={
            "spool_root": str(tmp_path / "live"),
            "quota_path": str(tmp_path / "quota.sqlite3"),
            "quota_units_per_window": 500,
            "quota_cost_per_request": 1,
            "producer_version": "market-minute-v1",
        },
    )


def test_source_builder_uses_one_sorted_universe_and_shared_quota(tmp_path: Path) -> None:
    adapter = _Adapter()
    builder = market_minute_source_builder(
        adapter_factory=lambda: adapter,
        universe_loader=lambda: ["600001.SH", "600000.SH", "600001.SH"],
        clock=lambda: NOW,
    )

    result = builder(_manifest(tmp_path))()

    assert adapter.calls == [(("600000.SH", "600001.SH"), "1min")]
    assert result.processed_count == 1
    assert result.output_sequence == 0
    assert (tmp_path / "quota.sqlite3").is_file()


def test_source_builder_reloads_universe_between_steps(tmp_path: Path) -> None:
    adapter = _UniverseAdapter()
    universes = iter(
        [
            ["600000.SH"],
            ["600001.SH", "600000.SH"],
            ["600002.SH"],
        ]
    )
    builder = market_minute_source_builder(
        adapter_factory=lambda: adapter,
        universe_loader=lambda: next(universes),
        clock=lambda: NOW,
    )

    step = builder(_manifest(tmp_path))
    step()
    step()

    assert adapter.calls == [
        (("600000.SH",), "1min"),
        (("600000.SH", "600001.SH"), "1min"),
    ]


def test_source_builder_chunks_large_universe_with_one_atomic_output(
    tmp_path: Path,
) -> None:
    adapter = _UniverseAdapter()
    codes = [f"{index:06d}.SH" for index in range(305)]
    manifest = _manifest(tmp_path).model_copy(
        update={
            "settings": {
                **_manifest(tmp_path).model_dump(mode="json")["settings"],
                "quota_cost_per_request": 2,
                "max_codes_per_source_call": 300,
            }
        }
    )
    step = market_minute_source_builder(
        adapter_factory=lambda: adapter,
        universe_loader=lambda: codes,
        clock=lambda: NOW,
    )(manifest)

    result = step()

    assert [len(call[0]) for call in adapter.calls] == [300, 5]
    assert result.processed_count == 1
    assert result.output_sequence == 0
    spool = LiveBatchSpool(tmp_path / "live")
    record = spool.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)[0]
    assert len(MarketMinuteGateway.decode_payload(spool.read_payload(record))) == 305


def test_source_builder_fails_closed_before_partial_fetch_when_call_budget_is_too_low(
    tmp_path: Path,
) -> None:
    adapter = _UniverseAdapter()
    codes = [f"{index:06d}.SH" for index in range(301)]
    step = market_minute_source_builder(
        adapter_factory=lambda: adapter,
        universe_loader=lambda: codes,
        clock=lambda: NOW,
    )(_manifest(tmp_path))

    result = step()

    assert adapter.calls == []
    assert result.processed_count == 1
    assert result.degraded_reasons == (
        "market_minute:stale:source_error:RuntimeError",
    )


def test_source_builder_rejects_empty_universe_wrong_kind_or_relative_path(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="universe"):
        market_minute_source_builder(
            adapter_factory=_Adapter,
            universe_loader=lambda: [],
            clock=lambda: NOW,
        )(_manifest(tmp_path))

    wrong_kind = RuntimeServiceManifest.model_validate(
        {**_manifest(tmp_path).model_dump(mode="json"), "service_kind": "feature_live"}
    )
    with pytest.raises(ValueError, match="kind"):
        market_minute_source_builder(
            adapter_factory=_Adapter,
            universe_loader=lambda: ["600000.SH"],
            clock=lambda: NOW,
        )(wrong_kind)

    relative = RuntimeServiceManifest.model_validate(
        {
            **_manifest(tmp_path).model_dump(mode="json"),
            "settings": {
                **_manifest(tmp_path).model_dump(mode="json")["settings"],
                "spool_root": "relative/live",
            },
        }
    )
    with pytest.raises(ValidationError, match="absolute"):
        market_minute_source_builder(
            adapter_factory=_Adapter,
            universe_loader=lambda: ["600000.SH"],
            clock=lambda: NOW,
        )(relative)


def test_builtin_registry_registers_dependency_free_concrete_builders(tmp_path: Path) -> None:
    registry = build_builtin_registry(
        adapter_factory=_Adapter,
        universe_loader=lambda: ["600000.SH"],
        clock=lambda: NOW,
    )

    assert registry.registered_kinds == (
        RuntimeServiceKind.MARKET_MINUTE_SOURCE,
        RuntimeServiceKind.FEATURE_LIVE,
        RuntimeServiceKind.PAPER_CONSUMER,
    )
    assert callable(registry.build(_manifest(tmp_path)))
    unsupported = RuntimeServiceManifest.model_validate(
        {**_manifest(tmp_path).model_dump(mode="json"), "service_kind": "strategy_live"}
    )
    with pytest.raises(KeyError, match="not registered"):
        registry.build(unsupported)


def test_builtin_registry_registers_all_explicitly_bound_services() -> None:
    registry = build_builtin_registry(
        adapter_factory=_Adapter,
        universe_loader=lambda: ["600000.SH"],
        clock=lambda: NOW,
        evaluator_loader=lambda *_args: object(),  # type: ignore[arg-type]
        signal_source_loader=lambda _source_id: object(),  # type: ignore[arg-type]
        target_resolver=lambda _signal: object(),  # type: ignore[arg-type]
        provider_loader=lambda: {},
        paper_quote_resolver=lambda *_args: object(),  # type: ignore[arg-type]
        trade_date_resolver=lambda _now: NOW.date(),
        serving_snapshot_loader=lambda _now: object(),  # type: ignore[arg-type]
    )

    assert registry.registered_kinds == tuple(RuntimeServiceKind)


def test_builtin_registry_rejects_partial_router_dependencies() -> None:
    with pytest.raises(ValueError, match="router dependencies"):
        build_builtin_registry(
            adapter_factory=_Adapter,
            universe_loader=lambda: ["600000.SH"],
            clock=lambda: NOW,
            signal_source_loader=lambda _source_id: object(),  # type: ignore[arg-type]
        )


def test_default_adapter_factory_uses_only_scoped_token_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    class Adapter:
        def __init__(self, token: str, backup_token: str | None = None) -> None:
            observed.update(token=token, backup_token=backup_token)

    monkeypatch.setenv("TUSHARE_TOKEN_MAIN", "main-token")
    monkeypatch.setenv("TUSHARE_TOKEN_BACKUP", "backup-token")
    monkeypatch.setattr("rquant.adapter.tushare.TushareAdapter", Adapter)

    assert isinstance(builtin_module._default_adapter_factory(), Adapter)
    assert observed == {"token": "main-token", "backup_token": "backup-token"}


def test_default_adapter_factory_requires_source_capability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("TUSHARE_TOKEN_MAIN", raising=False)

    with pytest.raises(RuntimeError, match="TUSHARE_TOKEN_MAIN"):
        builtin_module._default_adapter_factory()
