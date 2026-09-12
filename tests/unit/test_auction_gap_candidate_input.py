from __future__ import annotations

import os
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from rquant.auction_gap_candidate_input import (
    AuctionGapCandidateInputError,
    assemble_auction_gap_candidate_batch,
)
from rquant.auction_match_gateway import AuctionMatchGateway, AuctionMatchGatewayConfig
from rquant.live_spool import LiveBatchSpool
from rquant.readside_replica_gate import ReplicaReadGate
from rquant.reference_data_registry import (
    ReadonlyReferenceRegistry,
    ReferenceDataset,
    ReferenceRecord,
    ReferenceRegistry,
)
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.strategy_candidate_producers import produce_auction_gap_candidates

COMMIT = "a" * 40
CODE = "300001.SZ"
TRADE_DATE = date(2026, 7, 31)
OBSERVED_AT = datetime(2026, 7, 31, 1, 27, tzinfo=UTC)
AUCTION_AVAILABLE_AT = datetime(2026, 7, 31, 1, 26, 5, tzinfo=UTC)
PRIOR_DATES = (
    date(2026, 7, 24),
    date(2026, 7, 27),
    date(2026, 7, 28),
    date(2026, 7, 29),
    date(2026, 7, 30),
)


def _calendar() -> MarketCalendarAuthority:
    return MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=COMMIT,
        coverage_start=PRIOR_DATES[0],
        coverage_end=TRADE_DATE,
        open_dates=(*PRIOR_DATES, TRADE_DATE),
        generated_at=datetime(2026, 7, 23, tzinfo=UTC),
    )


def _auction_spool(tmp_path: Path) -> LiveBatchSpool:
    spool = LiveBatchSpool(tmp_path / "auction-spool")
    frame = pd.DataFrame(
        [
            {
                "ts_code": CODE,
                "trade_date": TRADE_DATE,
                "price": 10.5,
                "vol": 20_000.0,
                "amount": 210_000.0,
                "pre_close": 10.0,
                "turnover_rate": 0.2,
                "volume_ratio": 9.9,
            }
        ]
    )
    gateway = AuctionMatchGateway(
        spool=spool,
        fetcher=lambda _: frame,
        config=AuctionMatchGatewayConfig(
            producer_version="auction-match-v1",
            producer_commit=COMMIT,
            min_coverage_ratio=1.0,
        ),
    )
    capture = gateway.capture_once(
        trade_date=TRADE_DATE,
        received_at=AUCTION_AVAILABLE_AT,
        expected_codes=(CODE,),
    )
    assert capture.published is True
    return spool


def _daily_snapshot(path: Path) -> Path:
    with duckdb.connect(str(path)) as connection:
        connection.execute("CREATE TABLE daily_bar(ts_code VARCHAR, trade_date DATE, vol DOUBLE)")
        connection.executemany(
            "INSERT INTO daily_bar VALUES (?, ?, ?)",
            [(CODE, trade_date, 1_000.0) for trade_date in PRIOR_DATES],
        )
    path.chmod(0o600)
    snapshot_time = datetime(2026, 7, 31, 1, 0, tzinfo=UTC).timestamp()
    os.utime(path, (snapshot_time, snapshot_time))
    return path


def _reference_registry(
    tmp_path: Path,
    *,
    price_payload: dict[str, object] | None = None,
) -> ReadonlyReferenceRegistry:
    path = tmp_path / "reference.sqlite3"
    registry = ReferenceRegistry(path)
    effective_from = datetime(2026, 7, 31, 0, 0, tzinfo=UTC)
    first_available_at = datetime(2026, 7, 31, 1, 20, tzinfo=UTC)
    payloads = (
        (ReferenceDataset.ST_STATUS, {"is_st": False}),
        (ReferenceDataset.SUSPENSION_STATUS, {"is_suspended": False}),
        (ReferenceDataset.LISTING_STATUS, {"status": "listed"}),
        (
            ReferenceDataset.PRICE_LIMIT_REGIME,
            price_payload
            or {
                "limit_eligible": True,
                "limit_percent": 0.1,
                "limit_up_price": 11.0,
                "limit_down_price": 9.0,
            },
        ),
    )
    for dataset, payload in payloads:
        registry.append(
            ReferenceRecord(
                dataset_id=dataset,
                key=CODE,
                effective_from=effective_from,
                revision=1,
                source="test.reference",
                first_available_at=first_available_at,
                payload=payload,
            )
        )
    registry.publish(published_at=datetime(2026, 7, 31, 1, 24, tzinfo=UTC))
    return ReadonlyReferenceRegistry(path)


def test_assembles_only_point_in_time_evidence_into_candidate_batch(tmp_path: Path) -> None:
    calendar = _calendar()
    batch = assemble_auction_gap_candidate_batch(
        auction_spool=_auction_spool(tmp_path),
        daily_database_path=_daily_snapshot(tmp_path / "operational-ro.duckdb"),
        reference_registry=_reference_registry(tmp_path),
        calendar=calendar,
        trade_date=TRADE_DATE,
        observed_at=OBSERVED_AT,
        producer_commit=COMMIT,
    )

    assert batch.authority.trade_date == TRADE_DATE
    assert batch.authority.captured_at == AUCTION_AVAILABLE_AT
    assert len(batch.facts) == 1
    fact = batch.facts[0]
    assert fact.ts_code == CODE
    assert fact.expected_prior5_trade_dates == PRIOR_DATES
    assert tuple(item.daily_volume_lots for item in fact.prior5_daily_volumes) == (1_000.0,) * 5
    assert fact.reference_snapshot_ids["session"] == fact.source_snapshot_id
    assert fact.reference_snapshot_ids["status"]
    assert fact.reference_snapshot_ids["limit"]

    candidates = produce_auction_gap_candidates(
        authority=batch.authority,
        facts=batch.facts,
    )
    assert len(candidates) == 1
    assert candidates[0].candidate_id == CODE
    assert candidates[0].static_features["auction_vol_ratio_5d"] == 0.2


def test_rejects_daily_snapshot_beneath_symlinked_ancestor(tmp_path: Path) -> None:
    real_parent = tmp_path / "real"
    real_parent.mkdir(mode=0o700)
    database = _daily_snapshot(real_parent / "operational-ro.duckdb")
    linked_parent = tmp_path / "linked"
    linked_parent.symlink_to(real_parent, target_is_directory=True)

    with pytest.raises(AuctionGapCandidateInputError, match="unsafe"):
        assemble_auction_gap_candidate_batch(
            auction_spool=_auction_spool(tmp_path),
            daily_database_path=linked_parent / database.name,
            reference_registry=_reference_registry(tmp_path),
            calendar=_calendar(),
            trade_date=TRADE_DATE,
            observed_at=OBSERVED_AT,
            producer_commit=COMMIT,
        )


def test_rejects_unordered_point_in_time_price_limits(tmp_path: Path) -> None:
    with pytest.raises(AuctionGapCandidateInputError, match="price limit"):
        assemble_auction_gap_candidate_batch(
            auction_spool=_auction_spool(tmp_path),
            daily_database_path=_daily_snapshot(tmp_path / "operational-ro.duckdb"),
            reference_registry=_reference_registry(
                tmp_path,
                price_payload={
                    "limit_eligible": True,
                    "limit_percent": 0.1,
                    "limit_up_price": 11.0,
                    "limit_down_price": 12.0,
                },
            ),
            calendar=_calendar(),
            trade_date=TRADE_DATE,
            observed_at=OBSERVED_AT,
            producer_commit=COMMIT,
        )


def test_repeated_assembly_keeps_evidence_capture_identity_stable(tmp_path: Path) -> None:
    spool = _auction_spool(tmp_path)
    database = _daily_snapshot(tmp_path / "operational-ro.duckdb")
    registry = _reference_registry(tmp_path)
    common = {
        "auction_spool": spool,
        "daily_database_path": database,
        "reference_registry": registry,
        "calendar": _calendar(),
        "trade_date": TRADE_DATE,
        "producer_commit": COMMIT,
    }

    first = assemble_auction_gap_candidate_batch(observed_at=OBSERVED_AT, **common)
    repeated = assemble_auction_gap_candidate_batch(
        observed_at=datetime(2026, 7, 31, 1, 28, tzinfo=UTC),
        **common,
    )

    assert repeated == first


def _count_connects(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """How many times the assembler actually opened the replica (#256)."""

    import duckdb

    opens = [0]
    original = duckdb.connect

    def counted(*args: object, **kwargs: object) -> object:
        opens[0] += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(duckdb, "connect", counted)
    return opens


def test_an_unchanged_replica_is_read_once_across_the_auction_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#256: this publisher runs every five seconds through 09:26-09:30.

    Each pass used to query the replica's whole `daily_bar`. Over one generation of a
    file replaced every five minutes, the window's passes now open it once, and the
    batch they assemble is the same one.
    """

    database = _daily_snapshot(tmp_path / "operational-ro.duckdb")
    calendar = _calendar()
    spool = _auction_spool(tmp_path)
    registry = _reference_registry(tmp_path)
    gate: ReplicaReadGate[object] = ReplicaReadGate(database)
    opens = _count_connects(monkeypatch)

    batches = [
        assemble_auction_gap_candidate_batch(
            auction_spool=spool,
            daily_database_path=database,
            reference_registry=registry,
            calendar=calendar,
            trade_date=TRADE_DATE,
            observed_at=OBSERVED_AT,
            producer_commit=COMMIT,
            read_gate=gate,
        )
        for _ in range(3)
    ]

    assert opens[0] == 1
    assert {batch.facts[0].source_snapshot_id for batch in batches} == {
        batches[0].facts[0].source_snapshot_id
    }
    assert gate.last_read is not None and gate.last_read.opened is False


def test_a_replaced_replica_is_read_again_by_the_next_pass(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _daily_snapshot(tmp_path / "operational-ro.duckdb")
    replacement = _daily_snapshot(tmp_path / "operational-ro.duckdb.tmp.1")
    calendar = _calendar()
    spool = _auction_spool(tmp_path)
    registry = _reference_registry(tmp_path)
    gate: ReplicaReadGate[object] = ReplicaReadGate(database)
    opens = _count_connects(monkeypatch)

    def assemble() -> object:
        return assemble_auction_gap_candidate_batch(
            auction_spool=spool,
            daily_database_path=database,
            reference_registry=registry,
            calendar=calendar,
            trade_date=TRADE_DATE,
            observed_at=OBSERVED_AT,
            producer_commit=COMMIT,
            read_gate=gate,
        )

    assemble()
    assemble()
    os.replace(replacement, database)
    assemble()
    assemble()

    assert opens[0] == 2
