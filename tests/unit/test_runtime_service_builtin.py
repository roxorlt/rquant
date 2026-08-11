from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

from rquant import runtime_service_builtin as builtin_module
from rquant.live_contracts import BatchQualityStatus, LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.market_minute_gateway import MarketMinuteGateway
from rquant.runtime_candidate_universe import CandidateUniverseAuthority
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.runtime_service_builtin import (
    build_builtin_registry,
    market_minute_source_builder,
    watchlist_quote_source_builder,
)
from rquant.runtime_service_control import RuntimeServicePlane, RuntimeStepResult
from rquant.runtime_service_entrypoint import (
    RuntimeServiceKind,
    RuntimeServiceManifest,
    load_runtime_service_manifest,
)
from rquant.source_quota_store import SourceQuotaStore
from rquant.strategy_candidate_producers import PublishedCandidateInputAuthority
from rquant.strategy_candidate_publish_service import (
    AuctionGapCandidateBatch,
    NShapeCandidateBatch,
)
from rquant.strategy_candidate_snapshot import (
    StrategyCandidatePriceBasis,
    StrategyCandidateRecord,
    StrategyCandidateSnapshotSpool,
    strategy_candidate_schema_fingerprint,
)
from rquant.watchlist_quote_gateway import decode_watchlist_quote_payload

NOW = datetime(2026, 7, 31, 1, 40, 2, tzinfo=UTC)
COMMIT = "a" * 40


STATIC_FEATURE_SCHEMA = {"score": {"dtype": "number", "semantic": "candidate ranking score"}}


def _strategy_fingerprints(
    strategy_id: str,
    strategy_version: str = "1",
) -> tuple[str, str, str]:
    return (
        hashlib.sha256(f"{strategy_id}:definition:v1".encode()).hexdigest(),
        hashlib.sha256(f"{strategy_id}:executable:v1".encode()).hexdigest(),
        strategy_candidate_schema_fingerprint(
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            static_feature_schema=STATIC_FEATURE_SCHEMA,
        ),
    )


def _candidate_identity_settings(strategy_id: str) -> dict[str, object]:
    definition, executable, candidate_schema = _strategy_fingerprints(strategy_id)
    return {
        "definition_fingerprint": definition,
        "executable_fingerprint": executable,
        "candidate_schema_fingerprint": candidate_schema,
        "static_feature_schema": STATIC_FEATURE_SCHEMA,
    }


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
    definition, executable, candidate_schema = _strategy_fingerprints(
        strategy_id,
        strategy_version,
    )
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
        definition_fingerprint=definition,
        executable_fingerprint=executable,
        candidate_schema_fingerprint=candidate_schema,
        static_feature_schema=STATIC_FEATURE_SCHEMA,
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
        definition_fingerprint=definition,
        executable_fingerprint=executable,
        candidate_schema_fingerprint=candidate_schema,
        static_feature_names=("score",),
        static_feature_schema=STATIC_FEATURE_SCHEMA,
    )


def _write_calendar(path: Path) -> tuple[Path, MarketCalendarAuthority]:
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
    return path, authority


def _authoritative_manifest(tmp_path: Path) -> RuntimeServiceManifest:
    calendar_path, calendar = _write_calendar(tmp_path / "calendar.json")
    authorities = (
        _publish_candidate_authority(
            tmp_path / "n-candidates",
            strategy_id="n_shape",
            strategy_version="1",
            codes=("600000.SH", "000001.SZ"),
        ),
        _publish_candidate_authority(
            tmp_path / "growth-candidates",
            strategy_id="growth_board_surge",
            strategy_version="1",
            codes=("300001.SZ", "000001.SZ"),
        ),
    )
    base = _manifest(tmp_path)
    manifest = base.model_copy(
        update={
            "settings": {
                **base.model_dump(mode="json")["settings"],
                "calendar_path": str(calendar_path),
                "calendar_expected_commit": COMMIT,
                "calendar_content_sha256": calendar.content_sha256,
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


def test_authoritative_source_rejects_calendar_content_mismatch(tmp_path: Path) -> None:
    manifest = _authoritative_manifest(tmp_path)
    settings = dict(manifest.settings)
    settings["calendar_content_sha256"] = "f" * 64
    registry = build_builtin_registry(
        adapter_factory=_UniverseAdapter,
        clock=lambda: NOW,
    )

    with pytest.raises(ValueError, match="content identity"):
        registry.build(manifest.model_copy(update={"settings": settings}))


def test_authoritative_source_accepts_independently_versioned_calendar(
    tmp_path: Path,
) -> None:
    calendar_commit = "b" * 40
    calendar = MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=calendar_commit,
        coverage_start=NOW.date(),
        coverage_end=NOW.date(),
        open_dates=(NOW.date(),),
        generated_at=datetime(2026, 7, 30, 8, 0, tzinfo=UTC),
    )
    calendar_path = tmp_path / "independent-calendar.json"
    calendar_path.write_text(
        json.dumps(
            calendar.model_dump(mode="json"),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
    )
    calendar_path.chmod(0o600)
    manifest = _authoritative_manifest(tmp_path)
    settings = dict(manifest.settings)
    settings.update(
        calendar_path=str(calendar_path),
        calendar_expected_commit=calendar_commit,
        calendar_content_sha256=calendar.content_sha256,
    )
    adapter = _UniverseAdapter()
    registry = build_builtin_registry(
        adapter_factory=lambda: adapter,
        clock=lambda: NOW,
    )

    result = registry.build(manifest.model_copy(update={"settings": settings}))()

    assert result.processed_count == 1
    assert result.source_generations["market_calendar"] == calendar.content_sha256


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
    observed = iter(
        (
            NOW,
            NOW + timedelta(seconds=1),
            NOW + timedelta(seconds=2),
            NOW + timedelta(seconds=3),
            datetime(2026, 7, 31, 4, 0, tzinfo=UTC),
        )
    )
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


def test_source_builder_charges_each_chunk_at_dispatch_across_minute_boundary(
    tmp_path: Path,
) -> None:
    before_boundary = datetime(2026, 7, 31, 9, 0, 59, tzinfo=UTC)
    after_boundary = datetime(2026, 7, 31, 9, 1, tzinfo=UTC)

    class _Clock:
        now = before_boundary

        def __call__(self) -> datetime:
            return self.now

    clock = _Clock()

    class _CrossMinuteAdapter(_UniverseAdapter):
        def __init__(self) -> None:
            super().__init__()
            self.call_times: list[datetime] = []

        def rt_min(self, codes: list[str], freq: str = "1min") -> pd.DataFrame:
            self.call_times.append(clock.now)
            frame = super().rt_min(codes, freq)
            if len(self.call_times) == 1:
                clock.now = after_boundary
            return frame

    adapter = _CrossMinuteAdapter()
    codes = [f"{index:06d}.SH" for index in range(12)]
    base = _manifest(tmp_path)
    manifest = base.model_copy(
        update={
            "settings": {
                **base.model_dump(mode="json")["settings"],
                "quota_units_per_window": 12,
                "quota_cost_per_request": 12,
                "max_codes_per_source_call": 1,
            }
        }
    )
    step = market_minute_source_builder(
        adapter_factory=lambda: adapter,
        universe_loader=lambda: codes,
        clock=clock,
    )(manifest)

    step()
    step()

    assert adapter.call_times.count(before_boundary) == 1
    assert adapter.call_times.count(after_boundary) == 12
    assert len(adapter.calls) == 13
    quota = SourceQuotaStore(tmp_path / "quota.sqlite3")
    assert quota.remaining("tushare.rt_min", now=before_boundary) == 11
    assert quota.remaining("tushare.rt_min", now=after_boundary) == 0


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


def test_source_builder_reuses_one_gateway_revision_index_per_process(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = builtin_module.MarketMinuteGateway._revision_index
    initialized_indexes = 0

    def counted_revision_index(gateway: MarketMinuteGateway):
        nonlocal initialized_indexes
        if gateway._latest_by_event_window is None:
            initialized_indexes += 1
        return original(gateway)

    monkeypatch.setattr(
        builtin_module.MarketMinuteGateway,
        "_revision_index",
        counted_revision_index,
    )
    step = market_minute_source_builder(
        adapter_factory=_UniverseAdapter,
        universe_loader=lambda: ["600000.SH"],
        clock=lambda: NOW,
    )(_manifest(tmp_path))

    step()
    step()

    assert initialized_indexes == 1


def test_watchlist_quote_builder_uses_its_own_provider_quota_and_spool(
    tmp_path: Path,
) -> None:
    calls: list[tuple[tuple[str, ...], float]] = []

    def provider(
        codes: tuple[str, ...],
        *,
        timeout_seconds: float,
        on_started: Callable[[datetime], None],
    ) -> pd.DataFrame:
        calls.append((codes, timeout_seconds))
        on_started(NOW)
        return pd.DataFrame(
            [
                {
                    "ts_code": code,
                    "observed_at": NOW - timedelta(seconds=1),
                    "price": 10.1,
                    "open": 10.0,
                    "high": 10.2,
                    "low": 9.9,
                    "volume": 1_000.0,
                    "amount": 10_100.0,
                }
                for code in codes
            ]
        )

    manifest = RuntimeServiceManifest(
        service_id="watchlist-quote.source.v1",
        service_kind=RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=5,
        stale_after_seconds=30,
        producer_commit=COMMIT,
        settings={
            "spool_root": str(tmp_path / "watchlist-quote"),
            "quota_path": str(tmp_path / "watchlist-quote" / "quota.sqlite3"),
            "quota_units_per_window": 12,
            "producer_version": "watchlist-quote-source-v1",
        },
    )
    step = watchlist_quote_source_builder(
        provider_factory=lambda: provider,
        universe_loader=lambda: ["600000.SH", "000001.SZ"],
        clock=lambda: NOW,
    )(manifest)

    result = step()

    assert calls == [(("000001.SZ", "600000.SH"), 2.5)]
    assert result.processed_count == 1
    assert set(result.source_generations) == {"watchlist_quote"}
    assert (tmp_path / "watchlist-quote" / "quota.sqlite3").exists()
    quote_spool = LiveBatchSpool(tmp_path / "watchlist-quote")
    record = quote_spool.list_after(
        LiveChannel.WATCHLIST_QUOTE,
        sequence=-1,
    )[0]
    payload = decode_watchlist_quote_payload(quote_spool.read_payload(record))
    assert payload.loc[0, "scheduled_at"] == NOW
    assert payload.loc[0, "universe_as_of"] == NOW
    assert payload.loc[0, "requested_at"] == NOW


@pytest.mark.parametrize(
    ("observed_at", "active"),
    (
        (datetime(2026, 7, 31, 1, 24, 59, tzinfo=UTC), False),
        (datetime(2026, 7, 31, 1, 25, tzinfo=UTC), True),
        (datetime(2026, 7, 31, 3, 30, tzinfo=UTC), True),
        (datetime(2026, 7, 31, 3, 30, 1, tzinfo=UTC), False),
        (datetime(2026, 7, 31, 5, 0, tzinfo=UTC), True),
        (datetime(2026, 7, 31, 7, 0, tzinfo=UTC), True),
        (datetime(2026, 7, 31, 7, 0, 1, tzinfo=UTC), False),
    ),
)
def test_watchlist_quote_session_includes_call_auction_and_excludes_lunch(
    observed_at: datetime,
    active: bool,
) -> None:
    assert builtin_module._watchlist_quote_session_active(scheduled_at=observed_at) is active


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
        RuntimeServiceKind.REFERENCE_SLOW_SOURCE,
        RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER,
        RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER,
        RuntimeServiceKind.AUCTION_MATCH_SOURCE,
        RuntimeServiceKind.MARKET_MINUTE_SOURCE,
        RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE,
        RuntimeServiceKind.DAILY_CLOSE_SOURCE,
        RuntimeServiceKind.SHADOW_SESSION,
        RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR,
        RuntimeServiceKind.CANDIDATE_PUBLISHER,
        RuntimeServiceKind.FEATURE_LIVE,
        RuntimeServiceKind.STRATEGY_LIVE,
        RuntimeServiceKind.SIGNAL_ROUTER,
        RuntimeServiceKind.NOTIFIER,
        RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER,
        RuntimeServiceKind.PAPER_BROKER,
        RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER,
        RuntimeServiceKind.LAB_JOBS_PUBLISHER,
        RuntimeServiceKind.LAB_ARTIFACT_CATALOG,
        RuntimeServiceKind.ARTIFACT_RETENTION,
        RuntimeServiceKind.PROMOTIONS_PUBLISHER,
        RuntimeServiceKind.SERVING_PUBLISHER,
    )
    assert callable(registry.build(_manifest(tmp_path)))


def test_builtin_registry_opens_lifecycle_only_for_terminal_owner_builders(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    lifecycle = object()
    factory_calls = 0

    def owner_builder(*, clock: object, open_artifact_terminal_lifecycle: object):
        assert callable(clock)
        assert callable(open_artifact_terminal_lifecycle)

        def build(_manifest: object):
            assert open_artifact_terminal_lifecycle() is lifecycle  # type: ignore[operator]
            events.append("owner")
            return lambda: RuntimeStepResult()

        return build

    def open_lifecycle() -> object:
        nonlocal factory_calls
        factory_calls += 1
        return lifecycle

    monkeypatch.setattr(
        "rquant.runtime_builder_authority.lab_jobs_publisher_builder",
        owner_builder,
    )
    monkeypatch.setattr(
        "rquant.runtime_builder_authority.promotions_publisher_builder",
        owner_builder,
    )
    monkeypatch.setattr(
        "rquant.runtime_builder_artifact_catalog.artifact_catalog_builder",
        owner_builder,
    )
    registry = build_builtin_registry(
        adapter_factory=_Adapter,
        universe_loader=lambda: ["600000.SH"],
        clock=lambda: NOW,
        artifact_terminal_lifecycle_factory=open_lifecycle,  # type: ignore[arg-type]
    )
    manifests = (
        RuntimeServiceManifest(
            service_id="lab-jobs-owner",
            service_kind=RuntimeServiceKind.LAB_JOBS_PUBLISHER,
            plane=RuntimeServicePlane.RESEARCH,
            interval_seconds=1,
            stale_after_seconds=10,
            producer_commit=COMMIT,
            settings={},
        ),
        RuntimeServiceManifest(
            service_id="artifact-catalog-owner",
            service_kind=RuntimeServiceKind.LAB_ARTIFACT_CATALOG,
            plane=RuntimeServicePlane.RESEARCH,
            interval_seconds=1,
            stale_after_seconds=10,
            producer_commit=COMMIT,
            settings={},
        ),
        RuntimeServiceManifest(
            service_id="promotions-owner",
            service_kind=RuntimeServiceKind.PROMOTIONS_PUBLISHER,
            plane=RuntimeServicePlane.RESEARCH,
            interval_seconds=1,
            stale_after_seconds=10,
            producer_commit=COMMIT,
            settings={},
        ),
    )

    registry.build(_manifest(tmp_path))
    for manifest in manifests:
        registry.build(manifest)

    assert events == ["owner", "owner", "owner"]
    assert factory_calls == 3


def test_builtin_registry_passes_injected_candidate_loader_to_default_builder(
    tmp_path: Path,
) -> None:
    calls: list[tuple[Path, str, str]] = []
    batch = NShapeCandidateBatch(
        authority=PublishedCandidateInputAuthority(
            trade_date=NOW.date(),
            captured_at=NOW,
            quality_status=BatchQualityStatus.PUBLISHED,
            authority_snapshot_id="f" * 64,
            producer_commit=COMMIT,
        ),
        facts=(),
    )

    def loader(
        path: Path,
        *,
        strategy_id: str,
        expected_commit: str,
    ) -> NShapeCandidateBatch:
        calls.append((path, strategy_id, expected_commit))
        return batch

    manifest = RuntimeServiceManifest(
        service_id="candidate.n-shape.v1",
        service_kind=RuntimeServiceKind.CANDIDATE_PUBLISHER,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=30,
        stale_after_seconds=90,
        producer_commit=COMMIT,
        settings={
            "strategy_id": "n_shape",
            "strategy_version": 1,
            **_candidate_identity_settings("n_shape"),
            "candidate_input_path": str(tmp_path / "input.json"),
            "snapshot_root": str(tmp_path / "live" / "candidate"),
        },
    )
    registry = build_builtin_registry(
        adapter_factory=_Adapter,
        universe_loader=lambda: ["600000.SH"],
        clock=lambda: NOW,
        candidate_input_loader=loader,
    )

    result = registry.build(manifest)()

    assert result.source_generations["candidate_input"] == "f" * 64
    assert calls == [(tmp_path / "input.json", "n_shape", COMMIT)]


def test_builtin_registry_passes_clock_and_live_auction_loader(tmp_path: Path) -> None:
    observed_at = datetime(2026, 7, 31, 1, 27, tzinfo=UTC)
    calls: list[dict[str, object]] = []
    batch = AuctionGapCandidateBatch(
        authority=PublishedCandidateInputAuthority(
            trade_date=observed_at.date(),
            captured_at=observed_at,
            quality_status=BatchQualityStatus.PUBLISHED,
            authority_snapshot_id="e" * 64,
            producer_commit=COMMIT,
        ),
        facts=(),
    )

    def loader(**kwargs: object) -> AuctionGapCandidateBatch:
        calls.append(dict(kwargs))
        return batch

    manifest = RuntimeServiceManifest(
        service_id="candidate.auction-gap.v1",
        service_kind=RuntimeServiceKind.CANDIDATE_PUBLISHER,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=15,
        stale_after_seconds=60,
        producer_commit=COMMIT,
        settings={
            "strategy_id": "auction_gap",
            "strategy_version": 1,
            **_candidate_identity_settings("auction_gap"),
            "input_mode": "auction_live",
            "auction_spool_root": str(tmp_path / "auction-spool"),
            "daily_database_path": str(tmp_path / "operational-ro.duckdb"),
            "reference_registry_path": str(tmp_path / "reference.sqlite3"),
            "calendar_path": str(tmp_path / "calendar.json"),
            "calendar_expected_commit": COMMIT,
            "calendar_content_sha256": "c" * 64,
            "snapshot_root": str(tmp_path / "candidate"),
        },
    )
    registry = build_builtin_registry(
        adapter_factory=_Adapter,
        universe_loader=lambda: ["600000.SH"],
        clock=lambda: observed_at,
        auction_candidate_input_loader=loader,
    )

    result = registry.build(manifest)()

    assert result.source_generations["candidate_input"] == "e" * 64
    assert calls[0]["observed_at"] == observed_at


def test_default_registry_does_not_import_optional_evaluators_or_production_storage() -> None:
    src_root = Path(__file__).resolve().parents[2] / "src"
    script = "\n".join(
        (
            "import sys",
            f"sys.path.insert(0, {str(src_root)!r})",
            "from rquant.runtime_service_builtin import build_builtin_registry",
            "build_builtin_registry()",
            "forbidden = ('duckdb', 'rquant.storage.duckdb', 'rquant.monitor', "
            "'rquant.config', 'rquant.strategy_evaluators')",
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


def test_complete_builtin_registry_registers_watchlist_quote_source() -> None:
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

    assert registry.registered_kinds == tuple(
        kind for kind in RuntimeServiceKind if kind is not RuntimeServiceKind.PAPER_CONSUMER
    )
    assert RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE in registry.registered_kinds


def test_default_registry_registers_serving_publisher_without_test_injection() -> None:
    registry = build_builtin_registry(
        adapter_factory=_Adapter,
        universe_loader=lambda: ["600000.SH"],
        clock=lambda: NOW,
    )

    assert RuntimeServiceKind.SERVING_PUBLISHER in registry.registered_kinds


def test_daily_close_source_is_built_from_the_allowlisted_tushare_capability(
    tmp_path: Path,
) -> None:
    calendar_path, calendar = _write_calendar(tmp_path / "calendar.json")
    manifest = RuntimeServiceManifest(
        service_id="daily-close.source.v1",
        service_kind=RuntimeServiceKind.DAILY_CLOSE_SOURCE,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=60,
        stale_after_seconds=3_600,
        producer_commit=COMMIT,
        settings={
            "spool_root": str(tmp_path / "daily-close"),
            "producer_version": "daily-close-source-v1",
            "calendar_path": str(calendar_path),
            "calendar_expected_commit": COMMIT,
            "calendar_content_sha256": calendar.content_sha256,
        },
    )
    calls: list[date] = []

    def fetcher(request: object) -> object:
        from rquant.daily_close_gateway import DailyCloseSourceRequest

        assert isinstance(request, DailyCloseSourceRequest)
        calls.append(request.trade_date)
        return {
            "daily_bar": [
                {
                    "ts_code": "600000.SH",
                    "trade_date": request.trade_date,
                    "open": 10.0,
                    "high": 10.2,
                    "low": 9.9,
                    "close": 10.1,
                    "pre_close": 10.0,
                    "change": 0.1,
                    "pct_chg": 1.0,
                    "vol": 1_000.0,
                    "amount": 10_100.0,
                }
            ],
            "daily_basic": [
                {
                    "ts_code": "600000.SH",
                    "trade_date": request.trade_date,
                    "turnover_rate": 1.0,
                    "volume_ratio": 1.2,
                    "total_mv": 1_000_000.0,
                    "circ_mv": 800_000.0,
                }
            ],
            "adj_factor": [
                {"ts_code": "600000.SH", "trade_date": request.trade_date, "adj_factor": 1.0}
            ],
            "index_daily": [
                {
                    "ts_code": "000001.SH",
                    "trade_date": request.trade_date,
                    "open": 3_000.0,
                    "high": 3_010.0,
                    "low": 2_990.0,
                    "close": 3_005.0,
                    "pre_close": 3_000.0,
                    "change": 5.0,
                    "pct_chg": 0.17,
                    "vol": 100_000.0,
                    "amount": 300_000_000.0,
                }
            ],
            "security_status": [
                {
                    "ts_code": "600000.SH",
                    "trade_date": request.trade_date,
                    "name": "PF Bank",
                    "is_st": False,
                    "listing_status": "L",
                }
            ],
            "suspension_status": [],
        }

    registry = build_builtin_registry(
        runtime_capabilities={"TUSHARE_TOKEN_MAIN": "tushare-secret"},
        daily_close_fetcher=fetcher,
        clock=lambda: datetime(2026, 7, 31, 9, 10, tzinfo=UTC),
    )

    result = registry.build(manifest)()

    assert calls == [NOW.date()]
    assert result.processed_count == 1
    assert set(result.source_generations) == {"daily_close"}


def test_builtin_registry_wires_shadow_to_its_production_session_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}
    input_loader = object()

    def session_executor(**_kwargs: object) -> object:
        return object()

    def recording_builder(**kwargs: object) -> object:
        observed.update(kwargs)
        return lambda _manifest: lambda: RuntimeStepResult()

    monkeypatch.setattr(
        "rquant.runtime_builder_shadow.shadow_session_builder",
        recording_builder,
    )

    registry = build_builtin_registry(
        adapter_factory=_Adapter,
        universe_loader=lambda: ["600000.SH"],
        clock=lambda: NOW,
        shadow_input_loader=input_loader,
        shadow_session_executor=session_executor,
    )

    assert RuntimeServiceKind.SHADOW_SESSION in registry.registered_kinds
    assert observed["input_loader"] is input_loader
    assert observed["session_executor"] is session_executor


def test_builtin_registry_preserves_injected_strategy_evaluator_loader(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    def injected_loader(*_args: object) -> object:
        return object()

    def recording_builder(*, evaluator_loader: object, clock: object) -> object:
        observed.update(evaluator_loader=evaluator_loader, clock=clock)
        return lambda _manifest: lambda: None

    monkeypatch.setattr(
        "rquant.runtime_builder_strategy.strategy_live_builder",
        recording_builder,
    )

    registry = build_builtin_registry(
        adapter_factory=_Adapter,
        universe_loader=lambda: ["600000.SH"],
        clock=lambda: NOW,
        evaluator_loader=injected_loader,  # type: ignore[arg-type]
    )

    assert RuntimeServiceKind.STRATEGY_LIVE in registry.registered_kinds
    assert observed["evaluator_loader"] is injected_loader


def test_builtin_registry_fans_in_profile_completion_signer_to_strategy_builder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}
    signer = object()

    def recording_builder(**kwargs: object) -> object:
        observed.update(kwargs)
        return lambda _manifest: lambda: None

    monkeypatch.setattr(
        "rquant.runtime_builder_strategy.strategy_live_builder",
        recording_builder,
    )

    registry = build_builtin_registry(
        adapter_factory=_Adapter,
        universe_loader=lambda: ["600000.SH"],
        clock=lambda: NOW,
        completion_attestation_signer=signer,  # type: ignore[arg-type]
        completion_attestation_active_key_id="shadow-completion-v1",
    )

    assert RuntimeServiceKind.STRATEGY_LIVE in registry.registered_kinds
    assert observed["completion_attestation_signer"] is signer
    assert observed["completion_attestation_active_key_id"] == "shadow-completion-v1"


def test_builtin_registry_passes_scoped_capabilities_to_notifier_builder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}
    capabilities = {"PUSHDEER_KEYS": "secret"}

    def recording_builder(**kwargs: object) -> object:
        observed.update(kwargs)
        return lambda _manifest: lambda: None

    monkeypatch.setattr(
        "rquant.runtime_builder_signal.notifier_builder",
        recording_builder,
    )

    registry = build_builtin_registry(
        adapter_factory=_Adapter,
        universe_loader=lambda: ["600000.SH"],
        clock=lambda: NOW,
        runtime_capabilities=capabilities,
    )
    registry.build(
        RuntimeServiceManifest(
            service_id="notifier.admin",
            service_kind=RuntimeServiceKind.NOTIFIER,
            plane=RuntimeServicePlane.LIVE,
            interval_seconds=1,
            stale_after_seconds=10,
            producer_commit=COMMIT,
            settings={},
        )
    )

    assert observed["capability_environment"] == capabilities


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


def test_default_adapter_factory_uses_only_explicit_scoped_capabilities(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    class Adapter:
        def __init__(self, token: str, backup_token: str | None = None) -> None:
            observed.update(token=token, backup_token=backup_token)

    monkeypatch.setenv("TUSHARE_TOKEN_MAIN", "ambient-token-must-be-ignored")
    monkeypatch.setattr("rquant.adapter.tushare.TushareAdapter", Adapter)

    assert isinstance(
        builtin_module._default_adapter_factory(
            {
                "TUSHARE_TOKEN_MAIN": "main-token",
                "TUSHARE_TOKEN_BACKUP": "backup-token",
            }
        ),
        Adapter,
    )
    assert observed == {"token": "main-token", "backup_token": "backup-token"}


def test_default_adapter_factory_does_not_fall_back_to_global_backup_setting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    class Adapter:
        def __init__(self, token: str, backup_token: str | None = None) -> None:
            observed.update(token=token, backup_token=backup_token)

    monkeypatch.setattr("rquant.adapter.tushare.TushareAdapter", Adapter)

    assert isinstance(
        builtin_module._default_adapter_factory({"TUSHARE_TOKEN_MAIN": "main-token"}),
        Adapter,
    )
    assert observed == {"token": "main-token", "backup_token": ""}


def test_default_adapter_factory_requires_source_capability() -> None:
    with pytest.raises(RuntimeError, match="TUSHARE_TOKEN_MAIN"):
        builtin_module._default_adapter_factory({})
