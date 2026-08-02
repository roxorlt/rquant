from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from rquant.paper_broker import BrokerExecutionContext
from rquant.paper_signal_worker import PaperQuoteSnapshot
from rquant.runtime_builder_paper import paper_broker_builder, paper_consumer_builder
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.runtime_serving_authority import ServingSourceAuthorityReader
from rquant.runtime_serving_snapshot import PAPER_ACCOUNTS_DATASET_ID, PaperAccountsPayload
from rquant.serving_contracts import FreshnessStatus
from rquant.signal_bus import SignalBusStore
from rquant.signal_contracts import SignalAction, SignalEnvelope
from rquant.signal_route_spool import SignalRouteSpool, publish_signal_bus_prefix
from rquant.signal_router_runtime import (
    RouteSourceDescriptor,
    RoutingDecision,
    RunnerSignalBatch,
    SignalRouteCursorStore,
    SourceSnapshot,
    route_runner_signals,
)
from rquant.strategy_runner import RunnerSignalRecord

NOW = datetime(2026, 7, 31, 1, 30, tzinfo=UTC)
COMMIT = "a" * 40


def _policy_settings() -> dict[str, object]:
    return {
        "account_id": "paper-main",
        "execution_lag_seconds": 60,
        "buy_quantity": 1_000,
        "reduce_quantity": 500,
        "sell_quantity": 1_000,
    }


def _consumer_settings(tmp_path: Path) -> dict[str, object]:
    return {
        "signal_bus_path": str(tmp_path / "bus.sqlite3"),
        "queue_path": str(tmp_path / "queue.sqlite3"),
        "consumer_state_path": str(tmp_path / "consumer.sqlite3"),
        **_policy_settings(),
        "limit": 10,
    }


def _broker_settings(tmp_path: Path) -> dict[str, object]:
    return {
        "signal_spool_root": str(tmp_path / "signal-spool"),
        "queue_path": str(tmp_path / "queue.sqlite3"),
        "consumer_state_path": str(tmp_path / "consumer.sqlite3"),
        "broker_path": str(tmp_path / "broker.sqlite3"),
        **_policy_settings(),
        "initial_cash": "100000",
        "commission_rate": "0.0003",
        "minimum_commission": "5",
        "sell_stamp_tax_rate": "0.001",
        "limit": 10,
    }


def _manifest(tmp_path: Path, kind: RuntimeServiceKind) -> RuntimeServiceManifest:
    return RuntimeServiceManifest(
        service_id=f"paper.{kind.value}",
        service_kind=kind,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=1,
        stale_after_seconds=10,
        producer_commit=COMMIT,
        settings=(
            _consumer_settings(tmp_path)
            if kind is RuntimeServiceKind.PAPER_CONSUMER
            else _broker_settings(tmp_path)
        ),
    )


def _signal() -> SignalEnvelope:
    return SignalEnvelope(
        schema_version=1,
        strategy_id="n-shape",
        strategy_version="1",
        parameter_fingerprint="b" * 64,
        dataset_snapshot_id="c" * 64,
        feature_snapshot_id="d" * 64,
        event_time=NOW - timedelta(seconds=5),
        available_at=NOW,
        candidate_id="600000.SH",
        action=SignalAction.B_INTENT,
        reason_codes=("paper-runtime",),
        evidence={},
        expires_at=NOW + timedelta(minutes=5),
        producer_commit="e" * 40,
    )


class _Source:
    def __init__(self, signal: SignalEnvelope) -> None:
        self.signal = signal

    def read_batch(self, *, after_sequence: int, limit: int) -> RunnerSignalBatch:
        return RunnerSignalBatch(
            snapshot=SourceSnapshot(
                descriptor=RouteSourceDescriptor(
                    source_id="n-shape-v1",
                    generation_id="f" * 64,
                    strategy_spec_fingerprint="1" * 64,
                    first_sequence=1,
                    high_watermark=1,
                )
            ),
            after_sequence=after_sequence,
            limit=limit,
            records=(
                (RunnerSignalRecord(sequence=1, signal=self.signal),)
                if after_sequence == 0 and limit > 0
                else ()
            ),
        )


def _publish_signal(tmp_path: Path) -> None:
    bus = SignalBusStore(tmp_path / "bus.sqlite3")
    route_runner_signals(
        source_id="n-shape-v1",
        source=_Source(_signal()),
        bus=bus,
        cursors=SignalRouteCursorStore(
            tmp_path / "route-cursor.sqlite3",
            routing_policy_fingerprint="2" * 64,
        ),
        routed_at=NOW,
        target_resolver=lambda _signal: RoutingDecision.no_target(
            routing_policy_fingerprint="2" * 64,
            reason_code="paper_only",
        ),
        limit=10,
    )
    publish_signal_bus_prefix(
        bus=bus,
        spool=SignalRouteSpool(tmp_path / "signal-spool"),
        limit=10,
    )


def test_paper_consumer_delegates_signal_with_durable_cursor(tmp_path: Path) -> None:
    bus = SignalBusStore(tmp_path / "bus.sqlite3")
    bus.ingest(_signal(), received_at=NOW)
    step = paper_consumer_builder(clock=lambda: NOW)(
        _manifest(tmp_path, RuntimeServiceKind.PAPER_CONSUMER)
    )

    first = step()
    replay = step()

    assert first.input_sequence == 0
    assert first.output_sequence == 1
    assert first.processed_count == 1
    assert first.backlog_count == 0
    assert first.source_generations["signal_bus"] == bus.source_descriptor().generation_id
    assert replay.processed_count == 0
    assert replay.output_sequence == 1


def test_retired_paper_consumer_pause_never_advances_its_cursor(tmp_path: Path) -> None:
    bus = SignalBusStore(tmp_path / "bus.sqlite3")
    bus.ingest(_signal(), received_at=NOW)
    manifest = _manifest(tmp_path, RuntimeServiceKind.PAPER_CONSUMER)
    manifest = manifest.model_copy(
        update={
            "settings": {
                **manifest.model_dump(mode="json")["settings"],
                "paused": True,
            }
        }
    )

    result = paper_consumer_builder(clock=lambda: NOW)(manifest)()

    assert result.input_sequence == 1
    assert result.output_sequence == 0
    assert result.processed_count == 0
    assert result.backlog_count == 1
    assert result.degraded_reasons == ("paper_consumer:paused",)


def test_paper_broker_executes_due_signal_from_independent_queue(tmp_path: Path) -> None:
    _publish_signal(tmp_path)
    execution_time = NOW + timedelta(minutes=1)

    def quote_resolver(_signal: SignalEnvelope, _now: datetime) -> PaperQuoteSnapshot:
        return PaperQuoteSnapshot(
            ts_code="600000.SH",
            event_time=execution_time,
            available_at=execution_time,
            context=BrokerExecutionContext(
                executable_price=Decimal("10.00"),
                acquisition_available_date=date(2026, 8, 3),
            ),
            producer_commit=COMMIT,
        )

    step = paper_broker_builder(
        clock=lambda: execution_time,
        quote_resolver=quote_resolver,
        trade_date_resolver=lambda _now: date(2026, 7, 31),
    )(_manifest(tmp_path, RuntimeServiceKind.PAPER_BROKER))

    result = step()

    assert result.processed_count == 1
    assert result.backlog_count == 0
    assert result.degraded_reasons == ()
    assert len(result.source_generations) == 3


def test_paper_broker_publishes_account_authority_with_explicit_stale_marks(
    tmp_path: Path,
) -> None:
    _publish_signal(tmp_path)
    execution_time = NOW + timedelta(minutes=1)
    authority_root = tmp_path / "paper-authority"
    settings = {
        **_broker_settings(tmp_path),
        "serving_authority_root": str(authority_root),
    }
    manifest = RuntimeServiceManifest(
        **{
            **_manifest(tmp_path, RuntimeServiceKind.PAPER_BROKER).model_dump(mode="json"),
            "settings": settings,
        }
    )

    def quote_resolver(_signal: SignalEnvelope, _now: datetime) -> PaperQuoteSnapshot:
        return PaperQuoteSnapshot(
            ts_code="600000.SH",
            event_time=execution_time,
            available_at=execution_time,
            context=BrokerExecutionContext(
                executable_price=Decimal("10.00"),
                acquisition_available_date=date(2026, 8, 3),
            ),
            producer_commit=COMMIT,
        )

    later = execution_time + timedelta(seconds=5)
    observed_times = iter((execution_time, execution_time, later, later))
    step = paper_broker_builder(
        clock=lambda: next(observed_times),
        quote_resolver=quote_resolver,
        trade_date_resolver=lambda _now: date(2026, 7, 31),
    )(manifest)

    first = step()
    replay = step()
    authority = ServingSourceAuthorityReader(
        root=authority_root,
        expected_producer_commit=COMMIT,
        expected_dataset_id=PAPER_ACCOUNTS_DATASET_ID,
        expected_payload_kind="paper_accounts",
    )(execution_time)

    assert authority.status is FreshnessStatus.DEGRADED
    assert authority.reason == "paper account marks use last execution prices"
    assert isinstance(authority.payload, PaperAccountsPayload)
    account = authority.payload.paper_accounts[0]
    assert account.holdings[0].market_price == Decimal("10.00")
    assert authority.sequence == 1
    assert first.source_generations[PAPER_ACCOUNTS_DATASET_ID] == authority.generation_id
    assert replay.source_generations[PAPER_ACCOUNTS_DATASET_ID] == authority.generation_id


def test_paused_paper_broker_publishes_empty_fresh_account_without_advancing(
    tmp_path: Path,
) -> None:
    _publish_signal(tmp_path)
    authority_root = tmp_path / "paper-authority"
    settings = {
        **_broker_settings(tmp_path),
        "paused": True,
        "serving_authority_root": str(authority_root),
    }
    manifest = RuntimeServiceManifest(
        **{
            **_manifest(tmp_path, RuntimeServiceKind.PAPER_BROKER).model_dump(mode="json"),
            "settings": settings,
        }
    )
    step = paper_broker_builder(
        clock=lambda: NOW,
        quote_resolver=lambda *_args: object(),  # type: ignore[arg-type]
        trade_date_resolver=lambda _now: date(2026, 7, 31),
    )(manifest)

    result = step()
    authority = ServingSourceAuthorityReader(
        root=authority_root,
        expected_producer_commit=COMMIT,
        expected_dataset_id=PAPER_ACCOUNTS_DATASET_ID,
        expected_payload_kind="paper_accounts",
    )(NOW)

    assert result.output_sequence == 0
    assert result.degraded_reasons == ("paper_broker:paused",)
    assert authority.status is FreshnessStatus.FRESH
    assert isinstance(authority.payload, PaperAccountsPayload)
    assert authority.payload.paper_accounts[0].holdings == ()


def test_paper_broker_default_loader_binds_manifest_pit_authorities(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _publish_signal(tmp_path)
    execution_time = NOW + timedelta(minutes=1)
    observed: dict[str, object] = {}

    class Resolver:
        def __init__(self, config: object) -> None:
            observed["config"] = config

        def __call__(
            self,
            _signal: SignalEnvelope,
            _now: datetime,
        ) -> PaperQuoteSnapshot:
            return PaperQuoteSnapshot(
                ts_code="600000.SH",
                event_time=execution_time,
                available_at=execution_time,
                context=BrokerExecutionContext(
                    executable_price=Decimal("10.00"),
                    acquisition_available_date=date(2026, 8, 3),
                ),
                producer_commit=COMMIT,
            )

        def trade_date_at(self, _now: datetime) -> date:
            return date(2026, 7, 31)

        def constraint_generation_at(self, _now: datetime) -> str:
            return "9" * 64

    monkeypatch.setattr("rquant.runtime_paper_quote.PaperPitQuoteResolver", Resolver)
    settings = {
        **_broker_settings(tmp_path),
        "raw_spool_root": str((tmp_path / "raw").resolve()),
        "trade_calendar_path": str((tmp_path / "calendar.json").resolve()),
        "trade_calendar_sha256": "f" * 64,
        "execution_constraint_root": str((tmp_path / "execution-constraints").resolve()),
        "quote_max_age_seconds": 75,
        "max_finalize_scan_batches": 24,
        "max_visible_scan_batches": 96,
        "timestamp_semantics": "provider_snapshot",
    }
    manifest = RuntimeServiceManifest(
        **{
            **_manifest(tmp_path, RuntimeServiceKind.PAPER_BROKER).model_dump(mode="json"),
            "settings": settings,
        }
    )

    result = paper_broker_builder(clock=lambda: execution_time)(manifest)()

    assert result.processed_count == 1
    config = observed["config"]
    assert config.raw_spool_root == (tmp_path / "raw").resolve()  # type: ignore[attr-defined]
    assert config.trade_calendar_sha256 == "f" * 64  # type: ignore[attr-defined]
    assert (
        config.execution_constraint_root
        == (  # type: ignore[attr-defined]
            tmp_path / "execution-constraints"
        ).resolve()
    )
    assert config.expected_producer_commit == COMMIT  # type: ignore[attr-defined]
    assert config.quote_max_age_seconds == 75  # type: ignore[attr-defined]
    assert config.max_finalize_scan_batches == 24  # type: ignore[attr-defined]
    assert config.max_visible_scan_batches == 96  # type: ignore[attr-defined]
    assert config.timestamp_semantics == "provider_snapshot"  # type: ignore[attr-defined]
    assert result.source_generations["paper_execution_constraints"] == "9" * 64


def test_paper_broker_default_loader_requires_complete_authorities(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="PIT authorities"):
        paper_broker_builder(clock=lambda: NOW)(
            _manifest(tmp_path, RuntimeServiceKind.PAPER_BROKER)
        )


def test_paper_builders_reject_wrong_plane_and_relative_paths(tmp_path: Path) -> None:
    wrong_plane = RuntimeServiceManifest.model_validate(
        {
            **_manifest(tmp_path, RuntimeServiceKind.PAPER_CONSUMER).model_dump(mode="json"),
            "plane": "research",
        }
    )
    with pytest.raises(ValueError, match="live plane"):
        paper_consumer_builder(clock=lambda: NOW)(wrong_plane)

    relative = RuntimeServiceManifest.model_validate(
        {
            **_manifest(tmp_path, RuntimeServiceKind.PAPER_CONSUMER).model_dump(mode="json"),
            "settings": {**_consumer_settings(tmp_path), "queue_path": "queue.sqlite3"},
        }
    )
    with pytest.raises(ValueError, match="absolute"):
        paper_consumer_builder(clock=lambda: NOW)(relative)


def test_paper_role_settings_reject_cross_role_storage_and_cost_fields(
    tmp_path: Path,
) -> None:
    consumer = RuntimeServiceManifest.model_validate(
        {
            **_manifest(tmp_path, RuntimeServiceKind.PAPER_CONSUMER).model_dump(mode="json"),
            "settings": {
                **_consumer_settings(tmp_path),
                "broker_path": str(tmp_path / "broker.sqlite3"),
            },
        }
    )
    broker = RuntimeServiceManifest.model_validate(
        {
            **_manifest(tmp_path, RuntimeServiceKind.PAPER_BROKER).model_dump(mode="json"),
            "settings": {
                **_broker_settings(tmp_path),
                "signal_bus_path": str(tmp_path / "bus.sqlite3"),
            },
        }
    )

    with pytest.raises(ValueError, match="broker_path"):
        paper_consumer_builder(clock=lambda: NOW)(consumer)
    with pytest.raises(ValueError, match="signal_bus_path"):
        paper_broker_builder(
            clock=lambda: NOW,
            quote_resolver=lambda *_args: object(),  # type: ignore[arg-type]
            trade_date_resolver=lambda _now: NOW.date(),
        )(broker)
