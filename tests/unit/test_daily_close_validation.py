from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from rquant.daily_close_gateway import (
    DailyCloseGateway,
    DailyCloseGatewayConfig,
)
from rquant.daily_close_validation import (
    DailyCloseValidationError,
    DailyCloseValidationPolicy,
    DailyCloseValidator,
    DailyMinuteAggregate,
    DailyMinuteSnapshot,
)
from rquant.live_contracts import LiveChannel
from rquant.live_spool import LiveBatchRecord, LiveBatchSpool
from rquant.runtime_market_session import MarketCalendarAuthority

TRADE_DATE = date(2026, 7, 31)
OBSERVED_AT = datetime(2026, 7, 31, 9, 5, tzinfo=UTC)
AVAILABLE_AT = OBSERVED_AT + timedelta(seconds=2)


def _calendar(
    *,
    open_dates: tuple[date, ...] = (TRADE_DATE,),
    generated_at: datetime = OBSERVED_AT - timedelta(seconds=1),
) -> MarketCalendarAuthority:
    return MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit="a" * 40,
        coverage_start=TRADE_DATE - timedelta(days=7),
        coverage_end=TRADE_DATE + timedelta(days=7),
        open_dates=open_dates,
        generated_at=generated_at,
    )


def _bar(ts_code: str = "600000.SH", *, close: float = 10.2) -> dict[str, object]:
    open_price = close - 0.2
    pre_close = close - 0.25
    return {
        "ts_code": ts_code,
        "trade_date": TRADE_DATE,
        "open": open_price,
        "high": close + 0.2,
        "low": close - 0.3,
        "close": close,
        "pre_close": pre_close,
        "change": close - pre_close,
        "pct_chg": (close - pre_close) / pre_close * 100,
        "vol": 1_000.0,
        "amount": close * 1_000.0,
    }


def _snapshot(
    *,
    close: float = 10.2,
    second_daily_without_coverage: bool = False,
    status_is_st: bool | None = False,
    partial: tuple[str, ...] = (),
) -> dict[str, object]:
    daily_bar = (_bar(close=close),)
    if second_daily_without_coverage:
        daily_bar += (_bar("000001.SZ", close=8.2),)
    return {
        "daily_bar": daily_bar,
        "daily_basic": (
            {
                "ts_code": "600000.SH",
                "trade_date": TRADE_DATE,
                "turnover_rate": 0.5,
                "volume_ratio": 1.2,
                "total_mv": 200_000.0,
                "circ_mv": 180_000.0,
            },
        ),
        "adj_factor": (
            {
                "ts_code": "600000.SH",
                "trade_date": TRADE_DATE,
                "adj_factor": 1.01,
            },
        ),
        "index_daily": (
            {
                "ts_code": "000001.SH",
                "trade_date": TRADE_DATE,
                "open": 3200.0,
                "high": 3230.0,
                "low": 3190.0,
                "close": 3220.0,
                "pre_close": 3198.0,
                "change": 22.0,
                "pct_chg": 0.688,
                "vol": 2_000.0,
                "amount": 30_000.0,
            },
        ),
        "security_status": (
            {
                "ts_code": "600000.SH",
                "trade_date": TRADE_DATE,
                "name": "浦发银行",
                "is_st": status_is_st,
                "listing_status": "L",
            },
        ),
        "suspension_status": (),
        "partial_datasets": partial,
    }


def _published(
    tmp_path: Path,
    snapshots: list[dict[str, object]] | None = None,
) -> tuple[DailyCloseGateway, object]:
    responses = iter(snapshots or [_snapshot()])
    gateway = DailyCloseGateway(
        spool=LiveBatchSpool(tmp_path / "live"),
        fetcher=lambda _request: next(responses),
        config=DailyCloseGatewayConfig(
            producer_version="validation-test-v1",
            producer_commit="a" * 40,
        ),
        completion_clock=lambda: AVAILABLE_AT,
    )
    gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)
    record = gateway.spool.list_after(LiveChannel.DAILY_CLOSE, sequence=-1)[-1]
    return gateway, record


def _policy() -> DailyCloseValidationPolicy:
    return DailyCloseValidationPolicy(
        expected_schema_version=1,
        min_daily_rows=1,
        max_daily_rows=10,
        required_index_codes=("000001.SH",),
    )


def test_validator_binds_exact_published_typed_batch_identity(tmp_path: Path) -> None:
    gateway, record = _published(tmp_path)

    verified = DailyCloseValidator(
        spool=gateway.spool,
        policy=_policy(),
        calendar=_calendar(),
    ).validate(record)

    descriptor = gateway.spool.source_descriptor(LiveChannel.DAILY_CLOSE)
    assert verified.trade_date == TRADE_DATE
    assert verified.source_generation_id == descriptor.generation_id
    assert verified.source_sequence == record.envelope.sequence == 0
    assert verified.source_batch_id == record.envelope.batch_id
    assert verified.envelope_sha256 == record.envelope.identity_sha256
    assert verified.payload_sha256 == record.envelope.content_sha256
    assert verified.raw_content_sha256 == verified.facts.identity_sha256
    assert verified.available_at == AVAILABLE_AT
    assert verified.revision == 1
    assert verified.validation_sha256


def test_validator_binds_immutable_calendar_authority_evidence(tmp_path: Path) -> None:
    gateway, record = _published(tmp_path)
    calendar = _calendar()

    verified = DailyCloseValidator(
        spool=gateway.spool,
        policy=_policy(),
        calendar=calendar,
    ).validate(record)

    assert verified.calendar_generation_id == calendar.content_sha256
    assert verified.calendar_producer_commit == calendar.producer_commit
    assert verified.calendar_content_sha256 == calendar.content_sha256
    assert verified.calendar_as_of == calendar.generated_at


def test_validator_rejects_a_calendar_generated_after_raw_observation(tmp_path: Path) -> None:
    gateway, record = _published(tmp_path)

    with pytest.raises(DailyCloseValidationError, match="not available"):
        DailyCloseValidator(
            spool=gateway.spool,
            policy=_policy(),
            calendar=_calendar(generated_at=OBSERVED_AT + timedelta(microseconds=1)),
        ).validate(record)


def test_validator_rejects_a_payload_path_not_owned_by_the_published_spool_record(
    tmp_path: Path,
) -> None:
    gateway, record = _published(tmp_path)
    forged_payload = tmp_path / "outside-spool.payload"
    forged_payload.write_bytes(gateway.spool.read_payload(record))
    forged_payload.chmod(0o600)
    forged = LiveBatchRecord(
        envelope=record.envelope,
        manifest_path=record.manifest_path,
        payload_path=forged_payload,
    )

    with pytest.raises(DailyCloseValidationError, match="immutable spool record"):
        DailyCloseValidator(
            spool=gateway.spool,
            policy=_policy(),
            calendar=_calendar(),
        ).validate(forged)


def test_validator_rejects_non_published_or_noncurrent_revision(tmp_path: Path) -> None:
    degraded, degraded_record = _published(
        tmp_path / "degraded",
        [_snapshot(partial=("daily_basic",))],
    )
    validator = DailyCloseValidator(
        spool=degraded.spool,
        policy=_policy(),
        calendar=_calendar(),
    )
    with pytest.raises(DailyCloseValidationError, match="PUBLISHED"):
        validator.validate(degraded_record)

    gateway, original = _published(
        tmp_path / "revision",
        [_snapshot(), _snapshot(close=10.3)],
    )
    gateway.capture_once(
        trade_date=TRADE_DATE,
        observed_at=OBSERVED_AT + timedelta(seconds=1),
        refresh=True,
    )
    current_validator = DailyCloseValidator(
        spool=gateway.spool,
        policy=_policy(),
        calendar=_calendar(),
    )
    with pytest.raises(DailyCloseValidationError, match="current"):
        current_validator.validate(original)


def test_validator_fails_closed_on_calendar_status_and_key_coverage(tmp_path: Path) -> None:
    gateway, record = _published(tmp_path / "calendar")
    with pytest.raises(DailyCloseValidationError, match="open trading day"):
        DailyCloseValidator(
            spool=gateway.spool,
            policy=_policy(),
            calendar=_calendar(open_dates=()),
        ).validate(record)

    unknown, unknown_record = _published(
        tmp_path / "unknown",
        [_snapshot(status_is_st=None)],
    )
    with pytest.raises(DailyCloseValidationError, match="security status"):
        DailyCloseValidator(
            spool=unknown.spool,
            policy=_policy(),
            calendar=_calendar(),
        ).validate(unknown_record)

    incomplete, incomplete_record = _published(
        tmp_path / "coverage",
        [_snapshot(second_daily_without_coverage=True)],
    )
    with pytest.raises(DailyCloseValidationError, match="key coverage"):
        DailyCloseValidator(
            spool=incomplete.spool,
            policy=_policy(),
            calendar=_calendar(),
        ).validate(incomplete_record)


def test_minute_consistency_extends_point_in_time_availability(tmp_path: Path) -> None:
    gateway, record = _published(tmp_path)
    minute_available_at = AVAILABLE_AT + timedelta(minutes=3)
    matching = DailyMinuteSnapshot(
        trade_date=TRADE_DATE,
        available_at=minute_available_at,
        rows=(
            DailyMinuteAggregate(
                ts_code="600000.SH",
                trade_date=TRADE_DATE,
                open=10.0,
                high=10.4,
                low=9.9,
                close=10.2,
                vol=1_000.0,
                amount=10_200.0,
            ),
        ),
    )
    validator = DailyCloseValidator(
        spool=gateway.spool,
        policy=_policy(),
        calendar=_calendar(),
        minute_source=lambda _trade_date: matching,
    )

    verified = validator.validate(record)

    assert verified.available_at == minute_available_at
    assert verified.minute_content_sha256 == matching.content_sha256

    mismatched = matching.model_copy(
        update={
            "rows": (matching.rows[0].model_copy(update={"close": 10.1}),),
        }
    )
    with pytest.raises(DailyCloseValidationError, match="minute consistency"):
        DailyCloseValidator(
            spool=gateway.spool,
            policy=_policy(),
            calendar=_calendar(),
            minute_source=lambda _trade_date: mismatched,
        ).validate(record)
