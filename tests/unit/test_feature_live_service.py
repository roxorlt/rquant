from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from rquant.feature_contracts import FeatureAvailability, FeatureFieldStatus
from rquant.feature_live_service import _degrade_result, run_feature_live_batch
from rquant.feature_spool import FeatureBatchSpool
from rquant.intraday_feature_engine import (
    FeatureComputationMode,
    FeatureComputationResult,
    IntradayFeatureConfig,
)
from rquant.live_contracts import BatchEnvelope, BatchQualityStatus, LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.market_minute_gateway import MarketMinuteGateway, MarketMinuteGatewayConfig
from rquant.runtime_market_session import MarketCalendarAuthority

SHANGHAI = timezone(timedelta(hours=8))
RECEIVED = datetime(2026, 7, 31, 1, 40, 2, tzinfo=UTC)
GEOMETRY_FIELDS = {
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
}
SESSION_CLOSE = datetime(2026, 7, 31, 7, 0, tzinfo=UTC)


def _calendar(*, open_dates: tuple[date, ...] = (date(2026, 7, 31),)) -> MarketCalendarAuthority:
    return MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit="c" * 40,
        coverage_start=date(2026, 7, 1),
        coverage_end=date(2026, 8, 31),
        open_dates=open_dates,
        generated_at=datetime(2026, 7, 1, tzinfo=UTC),
    )


def _close_frame(*, minute: int = 0) -> pd.DataFrame:
    frame = _raw_frame()
    frame.loc[:, "trade_time"] = f"2026-07-31 14:{59 if minute == -1 else minute:02d}:00"
    if minute == 0:
        frame.loc[:, "trade_time"] = "2026-07-31 15:00:00"
    return frame


def _raw_frame(*, minute: int = 40, amount: float = 10_000.0) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ts_code": "600000.SH",
                "trade_time": f"2026-07-31 09:{minute:02d}:00",
                "open": 10.0,
                "high": 10.2,
                "low": 9.9,
                "close": 10.1,
                "vol": amount / 10.1,
                "amount": amount,
            }
        ]
    )


def _history() -> pd.DataFrame:
    rows = []
    for day, amount in ((29, 4_000.0), (30, 6_000.0)):
        rows.append(
            {
                "ts_code": "600000.SH",
                "trade_time": datetime(2026, 7, day, 9, 40, tzinfo=SHANGHAI),
                "available_at": datetime(2026, 7, day, 9, 40, 2, tzinfo=SHANGHAI),
                "open": 10.0,
                "high": 10.1,
                "low": 9.9,
                "close": 10.0,
                "vol": amount / 10.0,
                "amount": amount,
            }
        )
    return pd.DataFrame(rows)


def _gateway(root: Path, frames: list[pd.DataFrame]) -> MarketMinuteGateway:
    return MarketMinuteGateway(
        spool=LiveBatchSpool(root / "live"),
        fetcher=lambda: frames.pop(0),
        config=MarketMinuteGatewayConfig(
            producer_version="market-minute-v1",
            producer_commit="a" * 40,
        ),
    )


def _publish_degraded_raw_batch(spool: LiveBatchSpool) -> None:
    frame = pd.DataFrame(
        [
            {
                "ts_code": "600000.SH",
                "trade_time": "2026-07-31 09:30:00",
                "open": 10.0,
                "high": 10.2,
                "low": 9.9,
                "close": 10.1,
                "vol": 100.0,
                "amount": 1_000.0,
            },
            {
                "ts_code": "600000.SH",
                "trade_time": "2026-07-31 09:31:00",
                "open": 10.1,
                "high": 10.3,
                "low": 10.0,
                "close": 10.2,
                "vol": 100.0,
                "amount": 1_000.0,
            },
            {
                "ts_code": "600001.SH",
                "trade_time": "2026-07-31 09:31:00",
                "open": 20.0,
                "high": 20.4,
                "low": 19.8,
                "close": 20.3,
                "vol": 200.0,
                "amount": 4_000.0,
            },
        ]
    )
    normalized = MarketMinuteGateway.normalize_frame(frame)
    payload = MarketMinuteGateway.encode_payload(normalized)
    event_start = normalized["trade_time"].min().to_pydatetime()
    event_end = normalized["trade_time"].max().to_pydatetime()
    spool.publish(
        BatchEnvelope(
            schema_version=1,
            channel=LiveChannel.MARKET_MINUTE,
            dataset_id="market_minute",
            source="test.degraded",
            source_request_id="degraded-request-0",
            batch_id="degraded-market-minute-0",
            sequence=0,
            revision=1,
            event_time_start=event_start,
            event_time_end=event_end,
            source_time=event_end,
            received_at=RECEIVED,
            available_at=RECEIVED,
            row_count=len(normalized),
            content_sha256=hashlib.sha256(payload).hexdigest(),
            quality_status=BatchQualityStatus.DEGRADED,
            degraded_reasons=("partial_source",),
            producer_version="market-minute-v1",
            producer_commit="a" * 40,
        ),
        payload,
    )


def _config() -> IntradayFeatureConfig:
    return IntradayFeatureConfig(
        lookback_sessions=2,
        opening_acceleration_block_minutes=3,
        producer_commit="b" * 40,
    )


def test_service_consumes_raw_once_and_publishes_pit_feature_batch(tmp_path: Path) -> None:
    gateway = _gateway(tmp_path, [_raw_frame()])
    gateway.capture_once(received_at=RECEIVED)
    features = FeatureBatchSpool(tmp_path / "features")

    summary = run_feature_live_batch(
        raw_spool=gateway.spool,
        feature_spool=features,
        historical_minutes=_history(),
        historical_snapshot_id="history-20260730",
        config=_config(),
        observed_at=RECEIVED,
        limit=10,
    )

    assert summary.processed_count == 1
    assert summary.last_raw_sequence == 0
    record = features.list_after(sequence=-1, through_sequence=0, limit=1)[0]
    result = features.read_result(record)
    assert result.frame.iloc[0]["rel_same_minute"] == pytest.approx(2.0)
    assert result.envelope.input_batch_ids == tuple(
        sorted(("history-20260730", gateway.spool.current(LiveChannel.MARKET_MINUTE).batch_id))
    )


def test_feature_service_schema_dual_write_is_fail_closed_before_publish(
    tmp_path: Path,
) -> None:
    gateway = _gateway(tmp_path, [_raw_frame()])
    gateway.capture_once(received_at=RECEIVED)
    features = FeatureBatchSpool(tmp_path / "features")

    class _RejectingDualWriter:
        def prepare_payload(self, _values: object, *, observed_at: datetime) -> object:
            assert observed_at == RECEIVED
            raise ValueError("shared field content_hash differs")

        def commit_payload(self, _prepared: object, *, operation_id: str) -> object:
            raise AssertionError(f"must not commit {operation_id}")

    with pytest.raises(ValueError, match="shared field content_hash"):
        run_feature_live_batch(
            raw_spool=gateway.spool,
            feature_spool=features,
            historical_minutes=_history(),
            historical_snapshot_id="history-20260730",
            config=_config(),
            observed_at=RECEIVED,
            limit=10,
            schema_dual_writer=_RejectingDualWriter(),
        )

    assert features.current() is None
    assert gateway.spool.load_cursor("feature-live", LiveChannel.MARKET_MINUTE) is None


def test_crash_after_feature_publish_replays_without_duplicate_batch(
    tmp_path: Path,
) -> None:
    gateway = _gateway(tmp_path, [_raw_frame()])
    gateway.capture_once(received_at=RECEIVED)
    features = FeatureBatchSpool(tmp_path / "features")

    def fail(stage: str) -> None:
        if stage == "after_feature_publish":
            raise RuntimeError("injected crash")

    with pytest.raises(RuntimeError, match="injected crash"):
        run_feature_live_batch(
            raw_spool=gateway.spool,
            feature_spool=features,
            historical_minutes=_history(),
            historical_snapshot_id="history-20260730",
            config=_config(),
            observed_at=RECEIVED,
            limit=10,
            fault_hook=fail,
        )

    assert gateway.spool.load_cursor("feature-live", LiveChannel.MARKET_MINUTE) is None
    recovered = run_feature_live_batch(
        raw_spool=gateway.spool,
        feature_spool=features,
        historical_minutes=_history(),
        historical_snapshot_id="history-20260730",
        config=_config(),
        observed_at=RECEIVED + timedelta(seconds=1),
        limit=10,
    )
    assert recovered.processed_count == 1
    assert len(features.list_after(sequence=-1, through_sequence=0, limit=10)) == 1


def test_feature_producer_publishes_close_marker_only_for_complete_open_session(
    tmp_path: Path,
) -> None:
    gateway = _gateway(tmp_path, [_close_frame()])
    received_at = SESSION_CLOSE + timedelta(seconds=2)
    capture = gateway.capture_once(received_at=received_at)
    features = FeatureBatchSpool(tmp_path / "features")

    run_feature_live_batch(
        raw_spool=gateway.spool,
        feature_spool=features,
        historical_minutes=_history(),
        historical_snapshot_id="history-20260730",
        config=_config(),
        observed_at=received_at,
        limit=10,
        calendar=_calendar(),
    )

    marker = features.session_close_marker(date(2026, 7, 31))
    assert marker is not None
    assert marker.calendar_generation_id == _calendar().content_sha256
    assert marker.complete_through == SESSION_CLOSE
    assert (
        marker.upstream_source_generation_id
        == gateway.spool.source_descriptor(LiveChannel.MARKET_MINUTE).generation_id
    )
    assert marker.upstream_final_sequence == capture.pointer.sequence
    assert marker.upstream_final_batch_id == capture.pointer.batch_id
    assert marker.upstream_final_content_hash == capture.pointer.content_sha256


@pytest.mark.parametrize(
    ("frame", "open_dates"),
    [
        (_close_frame(minute=-1), (date(2026, 7, 31),)),
        (_close_frame(), ()),
    ],
)
def test_feature_producer_refuses_incomplete_or_closed_session_marker(
    tmp_path: Path,
    frame: pd.DataFrame,
    open_dates: tuple[date, ...],
) -> None:
    gateway = _gateway(tmp_path, [frame])
    gateway.capture_once(received_at=SESSION_CLOSE + timedelta(seconds=2))
    features = FeatureBatchSpool(tmp_path / "features")

    run_feature_live_batch(
        raw_spool=gateway.spool,
        feature_spool=features,
        historical_minutes=_history(),
        historical_snapshot_id="history-20260730",
        config=_config(),
        observed_at=SESSION_CLOSE + timedelta(seconds=2),
        limit=10,
        calendar=_calendar(open_dates=open_dates),
    )

    assert features.session_close_marker(date(2026, 7, 31)) is None


def test_service_does_not_consume_future_raw_batch(tmp_path: Path) -> None:
    gateway = _gateway(tmp_path, [_raw_frame()])
    gateway.capture_once(received_at=RECEIVED + timedelta(minutes=1))

    summary = run_feature_live_batch(
        raw_spool=gateway.spool,
        feature_spool=FeatureBatchSpool(tmp_path / "features"),
        historical_minutes=_history(),
        historical_snapshot_id="history-20260730",
        config=_config(),
        observed_at=RECEIVED,
        limit=10,
    )

    assert summary.processed_count == 0
    assert summary.has_deferred_batches is True


def test_stale_raw_batch_becomes_explicit_stale_empty_feature_batch(tmp_path: Path) -> None:
    gateway = MarketMinuteGateway(
        spool=LiveBatchSpool(tmp_path / "live"),
        fetcher=lambda: (_ for _ in ()).throw(TimeoutError("source down")),
        config=MarketMinuteGatewayConfig(
            producer_version="market-minute-v1",
            producer_commit="a" * 40,
        ),
    )
    capture = gateway.capture_once(received_at=RECEIVED)
    assert capture.pointer.quality_status is BatchQualityStatus.STALE
    features = FeatureBatchSpool(tmp_path / "features")

    run_feature_live_batch(
        raw_spool=gateway.spool,
        feature_spool=features,
        historical_minutes=_history(),
        historical_snapshot_id="history-20260730",
        config=_config(),
        observed_at=RECEIVED,
        limit=10,
    )

    result = features.read_result(features.list_after(sequence=-1, through_sequence=0, limit=1)[0])
    assert result.frame.empty
    assert result.envelope.row_count == 0
    assert all(
        status.status is FeatureAvailability.STALE for status in result.envelope.field_statuses
    )
    statuses = {status.name: status for status in result.envelope.field_statuses}
    assert statuses.keys() >= GEOMETRY_FIELDS
    assert all(statuses[name].reason.startswith("source_stale:") for name in GEOMETRY_FIELDS)


def test_published_empty_raw_batch_is_consumed_as_unavailable_feature_batch(
    tmp_path: Path,
) -> None:
    gateway = _gateway(tmp_path, [pd.DataFrame(columns=_raw_frame().columns)])
    capture = gateway.capture_once(received_at=RECEIVED)
    assert capture.pointer.quality_status is BatchQualityStatus.PUBLISHED
    features = FeatureBatchSpool(tmp_path / "features")

    summary = run_feature_live_batch(
        raw_spool=gateway.spool,
        feature_spool=features,
        historical_minutes=_history(),
        historical_snapshot_id="history-20260730",
        config=_config(),
        observed_at=RECEIVED,
        limit=10,
    )

    result = features.read_result(features.list_after(sequence=-1, through_sequence=0, limit=1)[0])
    assert summary.processed_count == 1
    assert result.frame.empty
    assert all(
        status.status is FeatureAvailability.UNAVAILABLE and status.reason == "source_empty"
        for status in result.envelope.field_statuses
    )
    statuses = {status.name: status for status in result.envelope.field_statuses}
    assert statuses.keys() >= GEOMETRY_FIELDS
    assert all(statuses[name].reason == "source_empty" for name in GEOMETRY_FIELDS)


def test_source_degradation_preserves_feature_availability_lattice(tmp_path: Path) -> None:
    raw_spool = LiveBatchSpool(tmp_path / "live")
    _publish_degraded_raw_batch(raw_spool)
    features = FeatureBatchSpool(tmp_path / "features")

    run_feature_live_batch(
        raw_spool=raw_spool,
        feature_spool=features,
        historical_minutes=_history(),
        historical_snapshot_id="history-20260730",
        config=_config(),
        observed_at=RECEIVED,
        limit=10,
    )

    result = features.read_result(features.list_after(sequence=-1, through_sequence=0, limit=1)[0])
    latest = result.envelope.field_status("latest_close", candidate_id="600000.SH")
    opening = result.envelope.field_status("session_open", candidate_id="600001.SH")
    acceleration = result.envelope.field_status(
        "amount_accel_5m",
        candidate_id="600000.SH",
    )
    assert latest is not None and latest.status is FeatureAvailability.DEGRADED
    assert latest.reason == "source_degraded:partial_source"
    assert opening is not None and opening.status is FeatureAvailability.UNAVAILABLE
    assert "missing_opening_bar" in opening.reason
    assert "source_degraded:partial_source" in opening.reason
    assert acceleration is not None
    assert acceleration.status is FeatureAvailability.UNAVAILABLE
    assert "opening_segment" in acceleration.reason
    assert "source_degraded:partial_source" in acceleration.reason

    stale_statuses = tuple(
        FeatureFieldStatus(
            candidate_id=status.candidate_id,
            name=status.name,
            status=FeatureAvailability.STALE,
            source_event_time=status.source_event_time,
            available_at=status.available_at,
            decision_cutoff=status.decision_cutoff,
            actual_delay_seconds=status.actual_delay_seconds,
            reason="prior_stale",
        )
        if status.name == "latest_close"
        else status
        for status in result.envelope.field_statuses
    )
    stale_input = FeatureComputationResult(
        mode=FeatureComputationMode.LIVE,
        payload_json=result.payload_json,
        envelope=result.envelope.model_copy(update={"field_statuses": stale_statuses}),
    )
    degraded_again = _degrade_result(stale_input, reasons=("second_source_reason",))
    stale = degraded_again.envelope.field_status(
        "latest_close",
        candidate_id="600000.SH",
    )
    assert stale is not None and stale.status is FeatureAvailability.STALE
    assert "prior_stale" in stale.reason
    assert "source_degraded:second_source_reason" in stale.reason
