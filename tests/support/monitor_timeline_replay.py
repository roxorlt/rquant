"""Build a local Serving replay from legacy-shaped, invented event sources."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import duckdb

from rquant import runtime_builder_signal
from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget
from rquant.notification_state import NotificationStateStore
from rquant.runtime_serving_authority import (
    ServingSourceAuthorityPublisher,
    ServingSourceAuthorityReader,
)
from rquant.runtime_serving_snapshot import (
    LabJobsPayload,
    PaperAccountsPayload,
    PromotionsPayload,
    ReferenceSlowPayload,
    RuntimeHealthPayload,
    ServingSnapshotAssembler,
    SourcePayload,
    SourceReadResult,
)
from rquant.serving_contracts import FreshnessStatus, ServingGenerationManifest
from rquant.serving_page_projection_source import (
    DuckDBSignalPageProjectionSource,
    SignalPageProjectionProducer,
)
from rquant.serving_publisher import ServingPublisher
from rquant.serving_read_models import (
    SERVING_TABLE_SPECS,
    ServingProjectionPayload,
    build_serving_read_models,
)
from rquant.signal_bus import SignalBusStore
from rquant.signal_contracts import SignalAction, SignalEnvelope
from rquant.signal_route_spool import (
    ReadonlySignalRouteSpool,
    SignalRouteSpool,
    publish_signal_bus_prefix,
)
from rquant.signal_router_runtime import (
    RouteSourceDescriptor,
    RoutingDecision,
    RunnerSignalBatch,
    RunnerSignalRecord,
    SignalRouteCursorStore,
    SourceSnapshot,
    route_runner_signals,
)
from rquant.surge_watch import SurgeConfirmed
from tests.support.web_serving_fixture import (
    FIXTURE_BUILT_AT,
    FIXTURE_PRODUCER_COMMIT,
    FIXTURE_SCHEMA_VERSION,
    _runtime_services,
    _stock_basic,
    _trade_calendar,
)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _publish_replayed_signal(source_root: Path, store: NotificationStateStore) -> None:
    event_time = datetime(2026, 9, 24, 1, 47, tzinfo=UTC)
    available_at = event_time + timedelta(minutes=23)
    signal = SignalEnvelope(
        schema_version=1,
        strategy_id="n_shape",
        strategy_version="1",
        parameter_fingerprint=_digest("monitor-replay-parameters"),
        dataset_snapshot_id=_digest("monitor-replay-dataset"),
        feature_snapshot_id=_digest("monitor-replay-features"),
        event_time=event_time,
        available_at=available_at,
        candidate_id="600001.SH",
        action=SignalAction.WATCH,
        reason_codes=("sample_replay",),
        evidence={},
        expires_at=FIXTURE_BUILT_AT + timedelta(hours=1),
        producer_commit=FIXTURE_PRODUCER_COMMIT,
    )
    descriptor = RouteSourceDescriptor(
        source_id="strategy.n_shape.v1",
        generation_id=_digest("monitor-replay-source"),
        strategy_spec_fingerprint=_digest("monitor-replay-spec"),
        first_sequence=1,
        high_watermark=1,
    )

    class ReplaySource:
        def read_batch(self, *, after_sequence: int, limit: int) -> RunnerSignalBatch:
            return RunnerSignalBatch(
                snapshot=SourceSnapshot(descriptor=descriptor),
                after_sequence=after_sequence,
                limit=limit,
                records=(RunnerSignalRecord(sequence=1, signal=signal),)
                if after_sequence == 0
                else (),
            )

    bus = SignalBusStore(source_root / "signal-bus.sqlite3")
    policy = _digest("monitor-replay-policy")
    route_runner_signals(
        source_id=descriptor.source_id,
        source=ReplaySource(),
        bus=bus,
        cursors=SignalRouteCursorStore(
            source_root / "route-cursor.sqlite3", routing_policy_fingerprint=policy
        ),
        routed_at=available_at + timedelta(seconds=1),
        target_resolver=lambda _signal: RoutingDecision.route(
            routing_policy_fingerprint=policy,
            targets=(DeliveryTarget(recipient_id="admin", channel=DeliveryChannel.PUSHDEER),),
        ),
        limit=1,
    )
    spool_root = source_root / "signal-spool"
    publish_signal_bus_prefix(bus=bus, spool=SignalRouteSpool(spool_root), limit=1)
    spool = ReadonlySignalRouteSpool(spool_root)
    routed = spool.routed_after_global_sequence(after_sequence=0, through_sequence=1, limit=1)
    store.replicate(
        spool.source_descriptor(),
        routed,
        observed_at=available_at + timedelta(seconds=2),
    )


def _auxiliary_read(dataset_id: str, payload: SourcePayload) -> SourceReadResult:
    observed = FIXTURE_BUILT_AT - timedelta(seconds=1)
    return SourceReadResult(
        dataset_id=dataset_id,
        generation_id=_digest("monitor-replay:" + dataset_id),
        sequence=1,
        event_time=observed,
        published_at=observed,
        status=FreshnessStatus.FRESH,
        payload=payload,
    )


def build_monitor_timeline_replay(
    root: Path,
    *,
    source_failure: str | None = None,
    with_notifications: bool = False,
    notification_failure: str | None = None,
) -> ServingGenerationManifest:
    """Exercise operational file readers and notification publication before Serving."""

    if source_failure not in (None, "missing", "read_error"):
        raise ValueError("unknown replay source failure")
    if notification_failure not in (None, "missing", "half_line"):
        raise ValueError("unknown replay notification failure")
    if notification_failure is not None and not with_notifications:
        raise ValueError("notification failure requires an injected log")
    if source_failure is not None and notification_failure is not None:
        raise ValueError("only one replay source failure may be selected")

    with TemporaryDirectory(prefix="rquant-monitor-replay-") as directory:
        source_root = Path(os.path.realpath(directory))
        database = source_root / "rquant_ro.duckdb"
        with duckdb.connect(str(database)) as connection:
            connection.execute(
                """
                CREATE TABLE screen_result (
                    trade_date DATE, preset_name VARCHAR, ts_code VARCHAR, name VARCHAR,
                    close DOUBLE, pct_chg DOUBLE, extra JSON, created_at TIMESTAMP
                )
                """
            )
            connection.execute(
                "INSERT INTO screen_result VALUES "
                "('2026-09-24', 'sample', '600001.SH', '样本01', 12.34, 2.5, '{}', "
                "'2026-09-24 09:35:00')"
            )
            connection.execute(
                """
                CREATE TABLE minute_bar (
                    ts_code VARCHAR, trade_time TIMESTAMP, freq VARCHAR, open DOUBLE,
                    high DOUBLE, low DOUBLE, close DOUBLE, vol DOUBLE, amount DOUBLE,
                    source VARCHAR, created_at TIMESTAMP
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE monitor_event (
                    trade_date DATE, ts_code VARCHAR, level VARCHAR,
                    trigger_price DOUBLE, level_price DOUBLE, trigger_time TIMESTAMP,
                    trigger_type VARCHAR, pool VARCHAR
                )
                """
            )
            connection.execute(
                "INSERT INTO monitor_event VALUES "
                "('2026-09-24', '600005.SH', 'attack_break_high', 12.34, 12.00, "
                "'2026-09-24 10:05:00', 'attack', 'pool2')"
            )
        live_root = source_root / "surge_live"
        live_root.mkdir()
        surge_path = live_root / "events-2026-09-24.jsonl"
        surge_path.write_text(
            json.dumps(
                SurgeConfirmed(
                    ts_code="600006.SH",
                    name="样本06",
                    confirmed_at="09:52",
                    price=11.25,
                    pct_chg=3.15,
                    status="confirmed",
                ).model_dump(),
                ensure_ascii=False,
            )
            + "\n",
            encoding="utf-8",
        )
        notification_path = source_root / "logs" / "notification_log.jsonl"
        if with_notifications:
            notification_path.parent.mkdir()
            notification_path.write_text(
                "".join(
                    json.dumps(
                        {
                            "sent_at": "2026-09-24T10:06:00.123456",
                            "scene": "price_level",
                            "channel": channel,
                            "target": "SECRET-CANARY-target",
                            "success": success,
                            "error_msg": "SECRET-CANARY-error",
                            "title": "SECRET-CANARY-title",
                        },
                        ensure_ascii=False,
                    ) + "\n"
                    for channel, success in (("pushdeer", True), ("pushplus", False))
                ),
                encoding="utf-8",
            )
        source_stamp = (FIXTURE_BUILT_AT - timedelta(seconds=10)).timestamp()
        os.utime(database, (source_stamp, source_stamp))
        os.utime(surge_path, (source_stamp, source_stamp))
        if with_notifications:
            os.utime(notification_path, (source_stamp, source_stamp))

        store = NotificationStateStore(source_root / "notification.sqlite3")
        _publish_replayed_signal(source_root, store)
        page_producer = SignalPageProjectionProducer(
            source=DuckDBSignalPageProjectionSource(
                database,
                surge_live_root=live_root,
                notification_log_path=notification_path if with_notifications else None,
            ),
            store=store,
        )
        page_producer.publish(FIXTURE_BUILT_AT)
        authority_root = source_root / "signals-authority"
        authority_clock = [FIXTURE_BUILT_AT]
        publisher = ServingSourceAuthorityPublisher(
            root=authority_root,
            producer_commit=FIXTURE_PRODUCER_COMMIT,
            dataset_id="signals",
            payload_kind="signal_delivery",
            clock=lambda: authority_clock[0],
        )
        reader = ServingSourceAuthorityReader(
            root=authority_root,
            expected_producer_commit=FIXTURE_PRODUCER_COMMIT,
            expected_dataset_id="signals",
            expected_payload_kind="signal_delivery",
        )
        reference_at = datetime(2026, 9, 24, 1, 25, tzinfo=UTC)
        reference = ReferenceSlowPayload(
            reference_generation_id=_digest("monitor-replay-reference"),
            revision=1,
            price_basis="raw_session",
            adjustment_basis="tushare_adj_factor",
            available_at=reference_at,
            projections=(
                ServingProjectionPayload(
                    table_name="trade_calendar",
                    available_at=reference_at,
                    rows=tuple(_trade_calendar()),
                ),
                ServingProjectionPayload(
                    table_name="stock_basic",
                    available_at=reference_at,
                    rows=tuple(_stock_basic()),
                ),
            ),
        )
        assembler = ServingSnapshotAssembler(
            signal_reader=reader,
            paper_accounts_reader=lambda _as_of: _auxiliary_read(
                "paper_accounts", PaperAccountsPayload()
            ),
            runtime_health_reader=lambda _as_of: _auxiliary_read(
                "runtime_health",
                RuntimeHealthPayload(
                    runtime_services=_runtime_services(FIXTURE_BUILT_AT - timedelta(seconds=5))
                ),
            ),
            lab_jobs_reader=lambda _as_of: _auxiliary_read("lab_jobs", LabJobsPayload()),
            promotions_reader=lambda _as_of: _auxiliary_read("promotions", PromotionsPayload()),
            reference_slow_reader=lambda _as_of: _auxiliary_read(
                "reference_slow_authority", reference
            ),
        )
        serving = ServingPublisher(
            root,
            producer_commit=FIXTURE_PRODUCER_COMMIT,
            schema_version=FIXTURE_SCHEMA_VERSION,
            table_specs=SERVING_TABLE_SPECS,
        )

        def publish_generation(observed: datetime) -> ServingGenerationManifest:
            runtime_builder_signal._publish_signal_authority(
                store=store,
                publisher=publisher,
                reader=reader,
                previous_reader=None,
                observed_at=observed,
                history_limit=100,
            )
            snapshot = assembler.assemble(observed)
            return serving.publish(
                build_serving_read_models(snapshot.read_model),
                watermarks=snapshot.watermarks,
                source_generations=snapshot.source_generations,
                built_at=observed,
            )

        first = publish_generation(FIXTURE_BUILT_AT)
        if source_failure is None and notification_failure is None:
            return first
        later = FIXTURE_BUILT_AT + timedelta(minutes=1)
        authority_clock[0] = later
        if notification_failure == "missing":
            notification_path.unlink()
            page_producer.publish(later)
        elif notification_failure == "half_line":
            notification_path.write_text('{"sent_at":', encoding="utf-8")
            page_producer.publish(later)
        elif source_failure == "missing":
            with duckdb.connect(str(database)) as connection:
                connection.execute("DROP TABLE monitor_event")
            page_producer.publish(later)
        else:
            with patch.object(
                DuckDBSignalPageProjectionSource,
                "__call__",
                side_effect=duckdb.IOException("synthetic replica read failure"),
            ):
                page_producer.publish(later)
        return publish_generation(later)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--with-notifications", action="store_true")
    args = parser.parse_args()
    manifest = build_monitor_timeline_replay(
        args.out, with_notifications=args.with_notifications
    )
    print(manifest.generation_id)


if __name__ == "__main__":
    main()
