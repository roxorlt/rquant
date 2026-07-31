from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

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


def test_builtin_registry_registers_only_concrete_source_builder(tmp_path: Path) -> None:
    registry = build_builtin_registry(
        adapter_factory=_Adapter,
        universe_loader=lambda: ["600000.SH"],
        clock=lambda: NOW,
    )

    assert callable(registry.build(_manifest(tmp_path)))
    unsupported = RuntimeServiceManifest.model_validate(
        {**_manifest(tmp_path).model_dump(mode="json"), "service_kind": "feature_live"}
    )
    with pytest.raises(KeyError, match="not registered"):
        registry.build(unsupported)
