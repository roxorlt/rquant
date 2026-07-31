from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

from rquant import runtime_service_builtin as builtin_module
from rquant.live_contracts import LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.market_minute_gateway import MarketMinuteGateway
from rquant.runtime_candidate_universe import CandidateUniverseAuthority
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.runtime_service_builtin import (
    build_builtin_registry,
    market_minute_source_builder,
)
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import (
    RuntimeServiceKind,
    RuntimeServiceManifest,
    load_runtime_service_manifest,
)
from rquant.source_quota_store import SourceQuotaStore
from rquant.strategy_candidate_snapshot import (
    StrategyCandidatePriceBasis,
    StrategyCandidateRecord,
    StrategyCandidateSnapshotSpool,
)

NOW = datetime(2026, 7, 31, 1, 40, 2, tzinfo=UTC)
COMMIT = "a" * 40


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


def _publish_candidate_authority(
    root: Path,
    *,
    strategy_id: str,
    strategy_version: str,
    codes: tuple[str, ...],
) -> CandidateUniverseAuthority:
    rows = tuple(
        StrategyCandidateRecord(
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            candidate_id=code,
            variant="shadow",
            decision_at=datetime(2026, 7, 31, 1, 30, tzinfo=UTC),
            available_at=datetime(2026, 7, 31, 1, 31, tzinfo=UTC),
            effective_trade_date=NOW.date(),
            reference_trade_date=NOW.date(),
            price_basis=StrategyCandidatePriceBasis.QFQ_PIT,
            static_features={"score": 0.8},
            reference_snapshot_ids={"daily": "1" * 64},
        )
        for code in codes
    )
    StrategyCandidateSnapshotSpool(root.resolve()).publish_strategy_records(
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        source_snapshot_ids={"candidate_input": hashlib.sha256(str(root).encode()).hexdigest()},
        trade_date=NOW.date(),
        captured_at=datetime(2026, 7, 31, 1, 32, tzinfo=UTC),
        producer_commit=COMMIT,
        rows=rows,
    )
    return CandidateUniverseAuthority(
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        snapshot_root=root.resolve(),
        required=True,
        max_age_seconds=3_600,
    )


def _write_calendar(path: Path) -> Path:
    authority = MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=COMMIT,
        coverage_start=NOW.date(),
        coverage_end=NOW.date(),
        open_dates=(NOW.date(),),
        generated_at=datetime(2026, 7, 30, 8, 0, tzinfo=UTC),
    )
    path.write_text(
        json.dumps(
            authority.model_dump(mode="json"),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
    )
    path.chmod(0o600)
    return path


def _authoritative_manifest(tmp_path: Path) -> RuntimeServiceManifest:
    authorities = (
        _publish_candidate_authority(
            tmp_path / "n-candidates",
            strategy_id="n_shape",
            strategy_version="v1",
            codes=("600000.SH", "000001.SZ"),
        ),
        _publish_candidate_authority(
            tmp_path / "growth-candidates",
            strategy_id="growth_board_surge",
            strategy_version="v1",
            codes=("300001.SZ", "000001.SZ"),
        ),
    )
    base = _manifest(tmp_path)
    manifest = base.model_copy(
        update={
            "settings": {
                **base.model_dump(mode="json")["settings"],
                "calendar_path": str(_write_calendar(tmp_path / "calendar.json")),
                "candidate_authorities": [item.model_dump(mode="json") for item in authorities],
            }
        }
    )
    manifest_path = tmp_path / "source-manifest.json"
    manifest_path.write_text(
        json.dumps(
            manifest.model_dump(mode="json"),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
    )
    manifest_path.chmod(0o600)
    return load_runtime_service_manifest(manifest_path, expected_commit=COMMIT)


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


def test_default_source_registry_uses_manifest_candidate_authorities(
    tmp_path: Path,
) -> None:
    adapter = _UniverseAdapter()
    registry = build_builtin_registry(
        adapter_factory=lambda: adapter,
        clock=lambda: NOW,
    )

    result = registry.build(_authoritative_manifest(tmp_path))()

    assert adapter.calls == [
        (("000001.SZ", "300001.SZ", "600000.SH"), "1min"),
    ]
    assert result.processed_count == 1
    assert set(result.source_generations) == {
        "candidate_universe",
        "market_calendar",
        "market_minute",
    }


def test_authoritative_source_does_not_fetch_during_lunch(tmp_path: Path) -> None:
    adapter = _UniverseAdapter()
    lunch = datetime(2026, 7, 31, 4, 0, tzinfo=UTC)
    registry = build_builtin_registry(
        adapter_factory=lambda: adapter,
        clock=lambda: lunch,
    )

    result = registry.build(_authoritative_manifest(tmp_path))()

    assert adapter.calls == []
    assert result.processed_count == 0
    assert result.output_sequence == -1
    assert set(result.source_generations) == {"market_calendar"}


def test_authoritative_source_preserves_last_output_and_evidence_during_lunch(
    tmp_path: Path,
) -> None:
    adapter = _UniverseAdapter()
    observed = iter((NOW, datetime(2026, 7, 31, 4, 0, tzinfo=UTC)))
    registry = build_builtin_registry(
        adapter_factory=lambda: adapter,
        clock=lambda: next(observed),
    )
    step = registry.build(_authoritative_manifest(tmp_path))

    morning = step()
    lunch = step()

    assert morning.output_sequence == 0
    assert lunch.output_sequence == morning.output_sequence
    assert lunch.processed_count == 0
    assert lunch.source_generations == morning.source_generations
    assert len(adapter.calls) == 1


def test_default_source_requires_frozen_authorities_and_calendar(tmp_path: Path) -> None:
    registry = build_builtin_registry(adapter_factory=_Adapter, clock=lambda: NOW)

    with pytest.raises(ValueError, match="calendar|authorit"):
        registry.build(_manifest(tmp_path))


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


def test_source_builder_rejects_call_budget_before_quota_or_partial_fetch(
    tmp_path: Path,
) -> None:
    adapter = _UniverseAdapter()
    codes = [f"{index:06d}.SH" for index in range(301)]
    step = market_minute_source_builder(
        adapter_factory=lambda: adapter,
        universe_loader=lambda: codes,
        clock=lambda: NOW,
    )(_manifest(tmp_path))

    with pytest.raises(ValueError, match="call budget"):
        step()

    assert adapter.calls == []
    assert LiveBatchSpool(tmp_path / "live").current(LiveChannel.MARKET_MINUTE) is None


def test_source_builder_charges_quota_for_actual_chunks_only(tmp_path: Path) -> None:
    adapter = _UniverseAdapter()
    manifest = _manifest(tmp_path).model_copy(
        update={
            "settings": {
                **_manifest(tmp_path).model_dump(mode="json")["settings"],
                "quota_units_per_window": 2,
                "quota_cost_per_request": 2,
            }
        }
    )
    step = market_minute_source_builder(
        adapter_factory=lambda: adapter,
        universe_loader=lambda: ["600000.SH"],
        clock=lambda: NOW,
    )(manifest)

    step()

    assert SourceQuotaStore(tmp_path / "quota.sqlite3").remaining("tushare.rt_min", now=NOW) == 1


def test_source_builder_rejects_empty_universe_wrong_kind_or_relative_path(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="universe"):
        market_minute_source_builder(
            adapter_factory=_Adapter,
            universe_loader=lambda: [],
            clock=lambda: NOW,
        )(_manifest(tmp_path))()

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
        RuntimeServiceKind.SIGNAL_ROUTER,
        RuntimeServiceKind.PAPER_CONSUMER,
    )
    assert callable(registry.build(_manifest(tmp_path)))
    unsupported = RuntimeServiceManifest.model_validate(
        {**_manifest(tmp_path).model_dump(mode="json"), "service_kind": "strategy_live"}
    )
    with pytest.raises(KeyError, match="not registered"):
        registry.build(unsupported)


def test_default_registry_does_not_import_serving_or_production_storage_modules() -> None:
    src_root = Path(__file__).resolve().parents[2] / "src"
    script = "\n".join(
        (
            "import sys",
            f"sys.path.insert(0, {str(src_root)!r})",
            "from rquant.runtime_service_builtin import build_builtin_registry",
            "build_builtin_registry()",
            "forbidden = ('duckdb', 'rquant.storage.duckdb', 'rquant.monitor', 'rquant.config')",
            "unexpected = tuple(name for name in forbidden if name in sys.modules)",
            "if unexpected: raise SystemExit(f'unexpected imports: {unexpected!r}')",
        )
    )

    completed = subprocess.run(
        [sys.executable, "-I", "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


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


@pytest.mark.parametrize(
    ("source_loader", "target_resolver"),
    [
        (lambda _source_id: object(), None),
        (None, lambda _signal: object()),
    ],
)
def test_builtin_registry_rejects_partial_router_dependencies(
    source_loader: object,
    target_resolver: object,
) -> None:
    with pytest.raises(ValueError, match="router dependencies"):
        build_builtin_registry(
            adapter_factory=_Adapter,
            universe_loader=lambda: ["600000.SH"],
            clock=lambda: NOW,
            signal_source_loader=source_loader,  # type: ignore[arg-type]
            target_resolver=target_resolver,  # type: ignore[arg-type]
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


def test_default_adapter_factory_does_not_fall_back_to_global_backup_setting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    class Adapter:
        def __init__(self, token: str, backup_token: str | None = None) -> None:
            observed.update(token=token, backup_token=backup_token)

    monkeypatch.setenv("TUSHARE_TOKEN_MAIN", "main-token")
    monkeypatch.delenv("TUSHARE_TOKEN_BACKUP", raising=False)
    monkeypatch.setattr("rquant.adapter.tushare.TushareAdapter", Adapter)

    assert isinstance(builtin_module._default_adapter_factory(), Adapter)
    assert observed == {"token": "main-token", "backup_token": ""}


def test_default_adapter_factory_requires_source_capability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("TUSHARE_TOKEN_MAIN", raising=False)

    with pytest.raises(RuntimeError, match="TUSHARE_TOKEN_MAIN"):
        builtin_module._default_adapter_factory()
