"""reference-slow publisher -> paper constraints: the listing classification (package AI, F1).

`paper_execution_constraint_producer._required_a_share_instrument_context` needs `market`,
`exchange`, `instrument_class` and `security_class` on LISTING_STATUS, and until package AI
`reference_slow_publisher` wrote none of them. The trading-day e2e never saw it because it
writes LISTING_STATUS by hand; the day replay of 2026-09-24 did: every code was refused,
so no paper-execution pointer, no `paper_accounts` authority, no serving generation.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from rquant.live_spool import LiveBatchSpool
from rquant.market_minute_gateway import MarketMinuteGateway, MarketMinuteGatewayConfig
from rquant.paper_execution_constraint_producer import (
    PaperExecutionConstraintEvidenceError,
    PaperExecutionConstraintProducer,
    PaperExecutionConstraintProductionRequest,
)
from rquant.paper_execution_constraints import PaperExecutionConstraintPublisher
from rquant.reference_data_registry import ReferenceDataset, ReferenceRecord, ReferenceRegistry
from rquant.reference_slow_publisher import (
    ReferenceDailyFact,
    ReferenceSecurityFact,
    ReferenceSlowSourceSnapshot,
    publish_reference_slow_snapshot,
)
from tests.unit.test_reference_slow_publisher import (
    AVAILABLE_AT,
    CAPTURED_AT,
    COMMIT,
    PRIOR_DATE,
    TARGET_DATE,
    VISIBLE_AT,
    _calendar,
)

_SHANGHAI = ZoneInfo("Asia/Shanghai")
#: one per exchange suffix, and each board segment stock_basic reports
SECURITIES = (
    ("300001.SZ", "创业板", "SZSE"),
    ("600000.SH", "主板", "SSE"),
    ("688001.SH", "科创板", "SSE"),
    ("920001.BJ", "北交所", "BSE"),
)


def _cn(hour: int, minute: int, second: int = 0) -> datetime:
    return datetime.combine(TARGET_DATE, datetime.min.time(), tzinfo=_SHANGHAI).replace(
        hour=hour, minute=minute, second=second
    )


def _snapshot() -> ReferenceSlowSourceSnapshot:
    return ReferenceSlowSourceSnapshot.create(
        target_trade_date=TARGET_DATE,
        captured_at=CAPTURED_AT,
        producer_commit=COMMIT,
        source_snapshot_ids={
            "daily": "1" * 64,
            "security": "2" * 64,
            "suspension": "3" * 64,
            "calendar": _calendar().content_sha256,
        },
        daily_facts=tuple(
            ReferenceDailyFact(
                ts_code=code,
                trade_date=PRIOR_DATE,
                close_raw=10.0,
                prior_adj_factor=1.0,
                adj_factor=1.0,
            )
            for code, _board, _exchange in SECURITIES
        ),
        security_facts=tuple(
            ReferenceSecurityFact(
                ts_code=code,
                name="样本",
                list_date=date(2020, 1, 2),
                market=board,
            )
            for code, board, _exchange in SECURITIES
        ),
    )


def _published(tmp_path: Path) -> tuple[ReferenceRegistry, str]:
    registry = ReferenceRegistry(tmp_path / "reference.sqlite3")
    receipt = publish_reference_slow_snapshot(
        registry=registry,
        calendar=_calendar(),
        snapshot=_snapshot(),
        completion_clock=lambda: VISIBLE_AT,
    )
    return registry, receipt.generation_id


def _minute_spool(root: Path, codes: tuple[str, ...]) -> LiveBatchSpool:
    spool = LiveBatchSpool(root / "minute-spool")
    trade_time = _cn(9, 31)
    frame = pd.DataFrame(
        [
            {
                "ts_code": code,
                "trade_time": trade_time,
                "open": 10.2,
                "high": 10.3,
                "low": 10.1,
                "close": 10.2,
                "vol": 10_000.0,
                "amount": 102_000.0,
            }
            for code in codes
        ]
    )
    MarketMinuteGateway(
        spool=spool,
        fetcher=lambda: frame,
        config=MarketMinuteGatewayConfig(producer_version="test-v1", producer_commit=COMMIT),
    ).capture_once(received_at=_cn(9, 31, 5))
    return spool


def test_the_publisher_writes_the_classification_paper_constraints_require(
    tmp_path: Path,
) -> None:
    registry, generation_id = _published(tmp_path)
    for code, board, exchange in SECURITIES:
        listing = registry.as_of(
            dataset_id=ReferenceDataset.LISTING_STATUS,
            key=code,
            event_time=_cn(9, 31),
            decision_time=_cn(9, 31, 5),
            generation_id=generation_id,
        )
        assert {
            name: listing.record.payload[name]
            for name in ("market", "exchange", "instrument_class", "security_class")
        } == {
            "market": "CN",
            "exchange": exchange,
            "instrument_class": "EQUITY",
            "security_class": "A_SHARE",
        }
        #: the board segment stock_basic calls `market` is still where it always was
        board_record = registry.as_of(
            dataset_id=ReferenceDataset.BOARD_MEMBERSHIP,
            key=code,
            event_time=_cn(9, 31),
            decision_time=_cn(9, 31, 5),
            generation_id=generation_id,
        )
        assert board_record.record.payload["market"] == board


def test_a_published_generation_carries_every_code_through_paper_constraints(
    tmp_path: Path,
) -> None:
    """End to end, no hand-written LISTING_STATUS: what the replay's stub used to derive."""

    registry, generation_id = _published(tmp_path)
    codes = tuple(code for code, _board, _exchange in SECURITIES)
    observed_at = _cn(9, 31, 30)
    producer = PaperExecutionConstraintProducer(
        reference_registry=registry,
        minute_spool=_minute_spool(tmp_path, codes),
        publisher=PaperExecutionConstraintPublisher(
            root=tmp_path / "constraint-authority",
            producer_commit=COMMIT,
            clock=lambda: observed_at,
        ),
        producer_commit=COMMIT,
        quote_ttl=timedelta(minutes=2),
    )

    publication = producer.produce(
        PaperExecutionConstraintProductionRequest(
            trade_date=TARGET_DATE,
            ts_codes=codes,
            observed_at=observed_at.astimezone(UTC),
            reference_generation_id=generation_id,
            sequence=0,
        )
    )

    contexts = {record.ts_code: record.instrument_context for record in publication.batch.records}
    assert set(contexts) == set(codes)
    for code, _board, exchange in SECURITIES:
        context = contexts[code]
        assert context.scope_key == ("CN", exchange, "EQUITY", "A_SHARE")
        assert context.classification_provenance is not None
        assert context.classification_provenance.reference_dataset == "security_listing_status"
        assert context.classification_provenance.reference_generation_id == generation_id


def test_a_record_written_before_package_ai_is_still_refused_as_before(tmp_path: Path) -> None:
    """Backward compatibility, stated: an older LISTING_STATUS without the four fields.

    Nothing guesses them for a record that does not carry them; such a generation keeps
    being refused exactly as v0.33.21 refused it. The host has no published generation yet
    (first publication Monday), and a new window re-publishes the record as a revision.
    """

    registry = ReferenceRegistry(tmp_path / "reference.sqlite3")
    snapshot = _snapshot()
    publish_reference_slow_snapshot(
        registry=registry,
        calendar=_calendar(),
        snapshot=snapshot,
        completion_clock=lambda: VISIBLE_AT,
    )
    legacy = tmp_path / "legacy"
    legacy.mkdir()
    old = ReferenceRegistry(legacy / "reference.sqlite3")
    code = SECURITIES[0][0]
    for dataset in (
        ReferenceDataset.ST_STATUS,
        ReferenceDataset.SUSPENSION_STATUS,
        ReferenceDataset.PRICE_LIMIT_REGIME,
        ReferenceDataset.LISTING_STATUS,
    ):
        record = registry.as_of(
            dataset_id=dataset,
            key=code,
            event_time=_cn(9, 31),
            decision_time=_cn(9, 31, 5),
        ).record
        payload = dict(record.payload)
        if dataset is ReferenceDataset.LISTING_STATUS:
            for name in ("market", "exchange", "instrument_class", "security_class"):
                payload.pop(name)
        old.append(
            ReferenceRecord(
                dataset_id=record.dataset_id,
                key=record.key,
                effective_from=record.effective_from,
                effective_to=record.effective_to,
                revision=record.revision,
                source=record.source,
                first_available_at=record.first_available_at,
                payload=payload,
            )
        )
    generation = old.publish(published_at=AVAILABLE_AT)
    observed_at = _cn(9, 31, 30)
    producer = PaperExecutionConstraintProducer(
        reference_registry=old,
        minute_spool=_minute_spool(tmp_path, (code,)),
        publisher=PaperExecutionConstraintPublisher(
            root=tmp_path / "constraint-authority",
            producer_commit=COMMIT,
            clock=lambda: observed_at,
        ),
        producer_commit=COMMIT,
    )

    with pytest.raises(
        PaperExecutionConstraintEvidenceError,
        match=f"{code} listing classification market is missing",
    ):
        producer.produce(
            PaperExecutionConstraintProductionRequest(
                trade_date=TARGET_DATE,
                ts_codes=(code,),
                observed_at=observed_at.astimezone(UTC),
                reference_generation_id=generation.generation_id,
                sequence=0,
            )
        )
