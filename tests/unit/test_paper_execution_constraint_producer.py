from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from rquant.live_contracts import BatchEnvelope, BatchQualityStatus, LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.market_minute_gateway import (
    MarketMinuteGateway,
    MarketMinuteGatewayConfig,
)
from rquant.paper_execution_constraint_producer import (
    PaperExecutionConstraintEvidenceError,
    PaperExecutionConstraintProducer,
    PaperExecutionConstraintProductionRequest,
)
from rquant.paper_execution_constraints import (
    PaperExecutionConstraintAuthority,
    PaperExecutionConstraintPublisher,
)
from rquant.reference_data_registry import (
    ReferenceDataset,
    ReferenceRecord,
    ReferenceRegistry,
)
from rquant.signal_contracts import SignalAction

COMMIT = "a" * 40
DAY = date(2026, 7, 21)
CODE = "600000.SH"
_SHANGHAI = ZoneInfo("Asia/Shanghai")


def _cn(hour: int, minute: int, second: int = 0) -> datetime:
    return datetime(2026, 7, 21, hour, minute, second, tzinfo=_SHANGHAI).astimezone(UTC)


DEFAULT_EFFECTIVE_FROM = _cn(9, 25)
DEFAULT_EFFECTIVE_TO = _cn(15, 5)
DEFAULT_FIRST_AVAILABLE_AT = _cn(9, 20)
DEFAULT_REFERENCE_PUBLISHED_AT = _cn(9, 24)


def _append_reference(
    registry: ReferenceRegistry,
    *,
    dataset: ReferenceDataset,
    payload: dict[str, object],
    effective_from: datetime = DEFAULT_EFFECTIVE_FROM,
    effective_to: datetime | None = DEFAULT_EFFECTIVE_TO,
    first_available_at: datetime = DEFAULT_FIRST_AVAILABLE_AT,
    key: str = CODE,
) -> ReferenceRecord:
    record = ReferenceRecord(
        dataset_id=dataset,
        key=key,
        effective_from=effective_from,
        effective_to=effective_to,
        revision=1,
        source="test.reference",
        first_available_at=first_available_at,
        payload=payload,
    )
    registry.append(record)
    return record


def _complete_reference_registry(
    root: Path,
    *,
    is_st: bool = False,
    is_suspended: bool = False,
    price_payload: dict[str, object] | None = None,
    published_at: datetime = DEFAULT_REFERENCE_PUBLISHED_AT,
) -> tuple[ReferenceRegistry, str]:
    registry = ReferenceRegistry(root / "reference.sqlite3")
    _append_reference(
        registry,
        dataset=ReferenceDataset.ST_STATUS,
        payload={"is_st": is_st},
    )
    _append_reference(
        registry,
        dataset=ReferenceDataset.SUSPENSION_STATUS,
        payload={"is_suspended": is_suspended},
    )
    _append_reference(
        registry,
        dataset=ReferenceDataset.PRICE_LIMIT_REGIME,
        payload=price_payload
        if price_payload is not None
        else {"limit_up_price": 11.0, "limit_down_price": 9.0},
    )
    generation = registry.publish(published_at=published_at)
    return registry, generation.generation_id


def _minute_frame(*, minute: int, close: float, code: str = CODE) -> pd.DataFrame:
    trade_time = datetime(2026, 7, 21, 9, minute, tzinfo=_SHANGHAI)
    return pd.DataFrame(
        [
            {
                "ts_code": code,
                "trade_time": trade_time,
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "vol": 10_000.0,
                "amount": close * 10_000.0,
            }
        ]
    )


def _minute_spool(
    root: Path,
    observations: tuple[tuple[int, float], ...],
) -> LiveBatchSpool:
    spool = LiveBatchSpool(root / "minute-spool")
    for minute, close in observations:
        gateway = MarketMinuteGateway(
            spool=spool,
            fetcher=lambda minute=minute, close=close: _minute_frame(
                minute=minute,
                close=close,
            ),
            config=MarketMinuteGatewayConfig(
                producer_version="test-v1",
                producer_commit=COMMIT,
            ),
        )
        gateway.capture_once(received_at=_cn(9, minute, 5))
    return spool


def _producer(
    root: Path,
    *,
    registry: ReferenceRegistry,
    spool: LiveBatchSpool,
    published_at: datetime,
) -> PaperExecutionConstraintProducer:
    publisher = PaperExecutionConstraintPublisher(
        root=root / "constraint-authority",
        producer_commit=COMMIT,
        clock=lambda: published_at,
    )
    return PaperExecutionConstraintProducer(
        reference_registry=registry,
        minute_spool=spool,
        publisher=publisher,
        producer_commit=COMMIT,
        quote_ttl=timedelta(minutes=2),
    )


def _request(
    *,
    generation_id: str,
    observed_at: datetime,
    sequence: int = 7,
) -> PaperExecutionConstraintProductionRequest:
    return PaperExecutionConstraintProductionRequest(
        trade_date=DAY,
        ts_codes=(CODE,),
        observed_at=observed_at,
        reference_generation_id=generation_id,
        sequence=sequence,
    )


def test_produces_pit_intervals_and_publishes_existing_authority(tmp_path: Path) -> None:
    registry, generation_id = _complete_reference_registry(tmp_path)
    spool = _minute_spool(
        tmp_path,
        ((31, 10.0), (32, 11.0), (33, 10.5), (34, 9.0)),
    )
    producer = _producer(
        tmp_path,
        registry=registry,
        spool=spool,
        published_at=_cn(9, 34, 30),
    )

    publication = producer.produce(
        _request(generation_id=generation_id, observed_at=_cn(9, 34, 30))
    )

    assert publication.batch.sequence == 7
    assert publication.pointer.batch_hash == publication.batch.content_hash
    assert [record.available_at for record in publication.batch.records] == [
        _cn(9, minute, 5) for minute in (31, 32, 33, 34)
    ]
    assert [
        (record.buy_limit_locked, record.sell_limit_locked) for record in publication.batch.records
    ] == [(False, False), (True, False), (False, False), (False, True)]
    assert all(
        record.source_snapshot_ids["reference_slow"] == generation_id
        for record in publication.batch.records
    )
    assert all(
        set(record.source_snapshot_ids) == {"market_minute", "reference_slow"}
        for record in publication.batch.records
    )
    for first, second in zip(
        publication.batch.records,
        publication.batch.records[1:],
        strict=False,
    ):
        assert first.expires_at == second.available_at

    authority = PaperExecutionConstraintAuthority(
        root=tmp_path / "constraint-authority",
        expected_producer_commit=COMMIT,
    )
    assert (
        authority.resolve(
            ts_code=CODE,
            trade_date=DAY,
            action=SignalAction.S_INTENT,
            observed_at=_cn(9, 34, 30),
        ).limit_locked
        is True
    )


@pytest.mark.parametrize(
    ("is_st", "is_suspended", "expected_risk", "expected_suspended"),
    [
        (True, False, True, False),
        (False, True, False, True),
    ],
)
def test_maps_visible_slow_status_to_risk_and_suspension(
    tmp_path: Path,
    is_st: bool,
    is_suspended: bool,
    expected_risk: bool,
    expected_suspended: bool,
) -> None:
    registry, generation_id = _complete_reference_registry(
        tmp_path,
        is_st=is_st,
        is_suspended=is_suspended,
    )
    spool = _minute_spool(tmp_path, ((31, 10.0),))
    publication = _producer(
        tmp_path,
        registry=registry,
        spool=spool,
        published_at=_cn(9, 31, 30),
    ).produce(_request(generation_id=generation_id, observed_at=_cn(9, 31, 30)))

    record = publication.batch.records[0]
    assert record.risk_rejected is expected_risk
    assert record.suspended is expected_suspended


def test_excludes_future_minute_evidence_without_advancing_the_cutoff(
    tmp_path: Path,
) -> None:
    registry, generation_id = _complete_reference_registry(tmp_path)
    spool = _minute_spool(tmp_path, ((31, 10.0), (32, 11.0)))

    publication = _producer(
        tmp_path,
        registry=registry,
        spool=spool,
        published_at=_cn(9, 31, 30),
    ).produce(_request(generation_id=generation_id, observed_at=_cn(9, 31, 30)))

    assert len(publication.batch.records) == 1
    assert publication.batch.records[0].available_at == _cn(9, 31, 5)
    assert publication.batch.records[0].buy_limit_locked is False


def test_rejects_minute_event_time_later_than_its_batch_availability(
    tmp_path: Path,
) -> None:
    registry, generation_id = _complete_reference_registry(tmp_path)
    spool = LiveBatchSpool(tmp_path / "minute-spool")
    frame = MarketMinuteGateway.normalize_frame(_minute_frame(minute=32, close=10.0))
    payload = MarketMinuteGateway.encode_payload(frame)
    content_hash = hashlib.sha256(payload).hexdigest()
    event_time = _cn(9, 32)
    spool.publish(
        BatchEnvelope(
            schema_version=1,
            channel=LiveChannel.MARKET_MINUTE,
            dataset_id="market_minute",
            source="test.market-minute",
            source_request_id="request-0",
            batch_id="batch-0",
            sequence=0,
            revision=1,
            event_time_start=event_time,
            event_time_end=event_time,
            source_time=event_time,
            received_at=_cn(9, 31, 5),
            available_at=_cn(9, 31, 5),
            row_count=1,
            content_sha256=content_hash,
            quality_status=BatchQualityStatus.PUBLISHED,
            producer_version="test-v1",
            producer_commit=COMMIT,
        ),
        payload,
    )

    with pytest.raises(PaperExecutionConstraintEvidenceError, match="future.*batch"):
        _producer(
            tmp_path,
            registry=registry,
            spool=spool,
            published_at=_cn(9, 32, 30),
        ).produce(_request(generation_id=generation_id, observed_at=_cn(9, 32, 30)))


def test_rejects_reference_generation_published_after_observed_at(tmp_path: Path) -> None:
    registry, generation_id = _complete_reference_registry(
        tmp_path,
        published_at=_cn(9, 32),
    )
    spool = _minute_spool(tmp_path, ((31, 10.0),))
    producer = _producer(
        tmp_path,
        registry=registry,
        spool=spool,
        published_at=_cn(9, 31, 30),
    )

    with pytest.raises(PaperExecutionConstraintEvidenceError, match="reference.*future"):
        producer.produce(_request(generation_id=generation_id, observed_at=_cn(9, 31, 30)))


@pytest.mark.parametrize(
    "price_payload",
    [
        {"limit_percent": 10},
        {"limit_up_price": 11.0},
        {"limit_up_price": 9.0, "limit_down_price": 11.0},
    ],
)
def test_fails_closed_when_exact_price_limit_evidence_is_missing_or_invalid(
    tmp_path: Path,
    price_payload: dict[str, object],
) -> None:
    registry, generation_id = _complete_reference_registry(
        tmp_path,
        price_payload=price_payload,
    )
    spool = _minute_spool(tmp_path, ((31, 10.0),))

    with pytest.raises(PaperExecutionConstraintEvidenceError, match="price limit"):
        _producer(
            tmp_path,
            registry=registry,
            spool=spool,
            published_at=_cn(9, 31, 30),
        ).produce(_request(generation_id=generation_id, observed_at=_cn(9, 31, 30)))


def test_fails_closed_when_minute_price_exceeds_visible_limits(tmp_path: Path) -> None:
    registry, generation_id = _complete_reference_registry(tmp_path)
    spool = _minute_spool(tmp_path, ((31, 11.01),))

    with pytest.raises(PaperExecutionConstraintEvidenceError, match="outside.*price limit"):
        _producer(
            tmp_path,
            registry=registry,
            spool=spool,
            published_at=_cn(9, 31, 30),
        ).produce(_request(generation_id=generation_id, observed_at=_cn(9, 31, 30)))


def test_fails_closed_when_requested_code_has_no_visible_minute(tmp_path: Path) -> None:
    registry, generation_id = _complete_reference_registry(tmp_path)
    spool = _minute_spool(tmp_path, ((32, 10.0),))

    with pytest.raises(PaperExecutionConstraintEvidenceError, match="visible.*minute"):
        _producer(
            tmp_path,
            registry=registry,
            spool=spool,
            published_at=_cn(9, 31, 30),
        ).produce(_request(generation_id=generation_id, observed_at=_cn(9, 31, 30)))
