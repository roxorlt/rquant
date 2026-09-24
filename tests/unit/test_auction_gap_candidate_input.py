from __future__ import annotations

import math
import os
from datetime import UTC, date, datetime, timedelta
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
    ReferenceDataUnavailableError,
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


# #299 -- the round's reference evidence is one registry read, not four per row ------------

MARKET_EFFECTIVE_FROM = datetime(2026, 7, 30, 16, 0, tzinfo=UTC)
MARKET_EFFECTIVE_TO = datetime(2026, 7, 31, 16, 0, tzinfo=UTC)
MARKET_AVAILABLE_AT = datetime(2026, 7, 31, 1, 20, tzinfo=UTC)
MARKET_PUBLISHED_AT = datetime(2026, 7, 31, 1, 24, tzinfo=UTC)


def _market_codes(count: int) -> tuple[str, ...]:
    half = count // 2
    return tuple(
        sorted(
            [f"{index:06d}.SZ" for index in range(1, half + 1)]
            + [f"{600000 + index:06d}.SH" for index in range(count - half)]
        )
    )


def _market_payload(dataset: ReferenceDataset, index: int) -> dict[str, object]:
    """A per-code variation, so a lookup answered for the wrong code cannot pass."""

    if dataset is ReferenceDataset.ST_STATUS:
        return {"is_st": index % 7 == 0}
    if dataset is ReferenceDataset.SUSPENSION_STATUS:
        return {"is_suspended": index % 11 == 0}
    if dataset is ReferenceDataset.LISTING_STATUS:
        return {"status": "delisted" if index % 13 == 0 else "listed"}
    wide = index % 2 == 0
    return {
        "limit_eligible": index % 17 != 0,
        "limit_percent": 0.2 if wide else 0.1,
        "limit_up_price": 12.0 if wide else 11.0,
        "limit_down_price": 8.0 if wide else 9.0,
    }


def _market_record(
    dataset: ReferenceDataset,
    code: str,
    payload: dict[str, object],
    *,
    revision: int = 1,
    effective_from: datetime = MARKET_EFFECTIVE_FROM,
    effective_to: datetime | None = MARKET_EFFECTIVE_TO,
    first_available_at: datetime = MARKET_AVAILABLE_AT,
) -> ReferenceRecord:
    return ReferenceRecord(
        dataset_id=dataset,
        key=code,
        effective_from=effective_from,
        effective_to=effective_to,
        revision=revision,
        source="test.reference",
        first_available_at=first_available_at,
        replacement_reason=None if revision == 1 else "exchange correction",
        payload=payload,
    )


_DATASETS = (
    ReferenceDataset.ST_STATUS,
    ReferenceDataset.SUSPENSION_STATUS,
    ReferenceDataset.LISTING_STATUS,
    ReferenceDataset.PRICE_LIMIT_REGIME,
)


def _market_registry(
    path: Path,
    codes: tuple[str, ...],
    *,
    skip: frozenset[tuple[str, ReferenceDataset]] = frozenset(),
    generations: tuple[tuple[tuple[ReferenceRecord, ...], datetime], ...] = (),
) -> ReadonlyReferenceRegistry:
    """Every code's four records in one generation, then any further generations."""

    registry = ReferenceRegistry(path)
    registry.append_many_and_publish(
        tuple(
            _market_record(dataset, code, _market_payload(dataset, index))
            for index, code in enumerate(codes)
            for dataset in _DATASETS
            if (code, dataset) not in skip
        ),
        published_at=MARKET_PUBLISHED_AT,
    )
    for records, published_at in generations:
        registry.append_many_and_publish(records, published_at=published_at)
    return ReadonlyReferenceRegistry(path)


def _market_auction_spool(root: Path, codes: tuple[str, ...]) -> LiveBatchSpool:
    spool = LiveBatchSpool(root / "auction-spool")
    frame = pd.DataFrame(
        [
            {
                "ts_code": code,
                "trade_date": TRADE_DATE,
                "price": 10.5,
                "vol": 20_000.0,
                "amount": 210_000.0,
                "pre_close": 10.0,
                "turnover_rate": 0.2,
                "volume_ratio": 9.9,
            }
            for code in codes
        ]
    )
    capture = AuctionMatchGateway(
        spool=spool,
        fetcher=lambda _: frame,
        config=AuctionMatchGatewayConfig(
            producer_version="auction-match-v1",
            producer_commit=COMMIT,
            min_coverage_ratio=1.0,
        ),
    ).capture_once(trade_date=TRADE_DATE, received_at=AUCTION_AVAILABLE_AT, expected_codes=codes)
    assert capture.published is True
    return spool


def _market_daily_snapshot(path: Path, codes: tuple[str, ...]) -> Path:
    with duckdb.connect(str(path)) as connection:
        connection.execute("CREATE TABLE daily_bar(ts_code VARCHAR, trade_date DATE, vol DOUBLE)")
        connection.executemany(
            "INSERT INTO daily_bar VALUES (?, ?, ?)",
            [
                (code, trade_date, float(1_000 + 10 * index + offset))
                for index, code in enumerate(codes)
                for offset, trade_date in enumerate(PRIOR_DATES)
            ],
        )
    path.chmod(0o600)
    snapshot_time = datetime(2026, 7, 31, 1, 0, tzinfo=UTC).timestamp()
    os.utime(path, (snapshot_time, snapshot_time))
    return path


class _SingleKeyReads:
    """What the assembly did before #299: every lookup its own `as_of` round trip."""

    def __init__(self, registry: ReadonlyReferenceRegistry, generation_id: str | None) -> None:
        self._registry = registry
        self._generation_id = generation_id

    def as_of(self, **lookup: object) -> object:
        return self._registry.as_of(**lookup, generation_id=self._generation_id)


def _read_per_row(registry: ReadonlyReferenceRegistry) -> None:
    """Swap the bulk read for the pre-#299 per-row reads, on this registry object only."""

    def single_key_snapshot(
        *,
        dataset_ids: object,
        keys: object,
        generation_id: str | None = None,
    ) -> _SingleKeyReads:
        return _SingleKeyReads(registry, generation_id)

    registry.as_of_snapshot = single_key_snapshot  # type: ignore[method-assign]


def _assembly_outcome(
    tmp_path: Path,
    *,
    codes: tuple[str, ...],
    registry_path: Path,
    per_row: bool,
    observed_at: datetime = OBSERVED_AT,
) -> tuple[str, object, object]:
    registry = ReadonlyReferenceRegistry(registry_path)
    if per_row:
        _read_per_row(registry)
    try:
        batch = assemble_auction_gap_candidate_batch(
            auction_spool=LiveBatchSpool(tmp_path / "auction-spool"),
            daily_database_path=tmp_path / "operational-ro.duckdb",
            reference_registry=registry,
            calendar=_calendar(),
            trade_date=TRADE_DATE,
            observed_at=observed_at,
            producer_commit=COMMIT,
        )
    except AuctionGapCandidateInputError as exc:
        cause = exc.__cause__
        return (
            str(exc),
            type(cause).__name__ if cause is not None else None,
            str(cause) if cause is not None else None,
        )
    return ("batch", batch, None)


def test_a_full_market_assembly_reads_the_registry_four_times_not_four_per_row(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#299 on a real-size session: 5,556 registered securities, 5,475 auction rows.

    The host took 74 ms per `as_of` (~27 min for this round, window 20 min); this Mac 14 ms
    (326 s). Wall time is not asserted -- the number of registry connections and statements
    is, and it is what regressed: pointer + manifest + one snapshot, whatever the row count.
    """

    from tests.unit.test_reference_as_of_snapshot import count_registry_io

    registered = _market_codes(5_556)
    auctioned = tuple(code for index, code in enumerate(registered) if index % 68 != 67)
    assert len(auctioned) == 5_475
    registry = _market_registry(tmp_path / "reference.sqlite3", registered)
    _market_auction_spool(tmp_path, auctioned)
    daily = _market_daily_snapshot(tmp_path / "operational-ro.duckdb", auctioned)
    io = count_registry_io(monkeypatch, ReadonlyReferenceRegistry)

    batch = assemble_auction_gap_candidate_batch(
        auction_spool=LiveBatchSpool(tmp_path / "auction-spool"),
        daily_database_path=daily,
        reference_registry=registry,
        calendar=_calendar(),
        trade_date=TRADE_DATE,
        observed_at=OBSERVED_AT,
        producer_commit=COMMIT,
    )

    #: current_pointer, current_manifest (pointer + generation) and the one snapshot; the
    #: old path added 4 x 5,475 = 21,900 more
    assert io.connections == 4
    chunk_queries = [statement for statement in io.statements if "business_key IN" in statement]
    assert len(chunk_queries) == 4 * math.ceil(5_475 / 500)
    assert len(io.statements) < len(chunk_queries) + 20
    assert len(batch.facts) == 5_475
    index_of = {code: index for index, code in enumerate(registered)}
    for fact in batch.facts:
        index = index_of[fact.ts_code]
        wide = index % 2 == 0
        assert fact.is_st is (index % 7 == 0)
        assert fact.is_suspended is (index % 11 == 0)
        assert fact.is_listed is (index % 13 != 0)
        assert fact.limit_eligible is (index % 17 != 0)
        assert fact.limit_pct == (0.2 if wide else 0.1)
        assert fact.limit_up_price_session_raw == (12.0 if wide else 11.0)
        daily_index = auctioned.index(fact.ts_code) if index % 500 == 0 else None
        if daily_index is not None:
            assert tuple(item.trade_date for item in fact.prior5_daily_volumes) == PRIOR_DATES
            assert tuple(item.daily_volume_lots for item in fact.prior5_daily_volumes) == tuple(
                float(1_000 + 10 * daily_index + offset) for offset in range(5)
            )


def test_the_bulk_assembly_equals_the_per_row_single_key_assembly(tmp_path: Path) -> None:
    """Same batch, fact for fact, as four `as_of` calls per row -- across corrections.

    A second generation corrects some codes (the correction is used) and restates some
    listings in a lineage that only starts tomorrow (ignored at this event time); a third
    set of corrections becomes available only after observed_at, so no generation carries
    it and the originals are used.
    """

    codes = _market_codes(40)
    corrections = (
        tuple(
            _market_record(
                ReferenceDataset.PRICE_LIMIT_REGIME,
                code,
                {
                    "limit_eligible": True,
                    "limit_percent": 0.05,
                    "limit_up_price": 10.5,
                    "limit_down_price": 9.5,
                },
                revision=2,
                first_available_at=datetime(2026, 7, 31, 1, 25, tzinfo=UTC),
            )
            for code in codes[:5]
        )
        + tuple(
            _market_record(
                ReferenceDataset.ST_STATUS,
                code,
                {"is_st": True},
                revision=2,
                first_available_at=datetime(2026, 7, 31, 2, 0, tzinfo=UTC),
            )
            for code in codes[5:10]
        )
        + tuple(
            _market_record(
                ReferenceDataset.LISTING_STATUS,
                code,
                {"status": "delisted"},
                effective_from=MARKET_EFFECTIVE_TO,
                effective_to=None,
                first_available_at=datetime(2026, 7, 31, 1, 25, tzinfo=UTC),
            )
            for code in codes[10:15]
        )
    )
    _market_registry(
        tmp_path / "reference.sqlite3",
        codes,
        generations=((corrections, datetime(2026, 7, 31, 1, 26, tzinfo=UTC)),),
    )
    _market_auction_spool(tmp_path, codes)
    _market_daily_snapshot(tmp_path / "operational-ro.duckdb", codes)

    bulk = _assembly_outcome(
        tmp_path, codes=codes, registry_path=tmp_path / "reference.sqlite3", per_row=False
    )
    per_row = _assembly_outcome(
        tmp_path, codes=codes, registry_path=tmp_path / "reference.sqlite3", per_row=True
    )

    assert bulk[0] == "batch", bulk
    assert bulk == per_row
    facts = {fact.ts_code: fact for fact in bulk[1].facts}  # type: ignore[union-attr]
    assert {facts[code].limit_pct for code in codes[:5]} == {0.05}
    assert not any(facts[code].is_st for code in codes[5:10] if codes.index(code) % 7)
    assert all(facts[code].is_listed for code in codes[10:15] if codes.index(code) % 13)


@pytest.mark.parametrize(
    "fault",
    [
        "missing_suspension",
        "st_never_published",
        "listing_not_effective",
        "status_not_a_string_before_a_missing_code",
    ],
)
def test_every_refusal_names_the_same_row_as_the_per_row_reads(
    tmp_path: Path,
    fault: str,
) -> None:
    """A code the registry cannot answer for refuses the round as that code -- never skipped."""

    codes = _market_codes(12)
    faulty = codes[6]
    registry_path = tmp_path / "reference.sqlite3"
    registry = ReferenceRegistry(registry_path)
    records = []
    for index, code in enumerate(codes):
        for dataset in _DATASETS:
            payload = _market_payload(dataset, index)
            first_available_at = MARKET_AVAILABLE_AT
            effective_from = MARKET_EFFECTIVE_FROM
            if code == faulty:
                if fault == "missing_suspension" and dataset is ReferenceDataset.SUSPENSION_STATUS:
                    continue
                if fault == "st_never_published" and dataset is ReferenceDataset.ST_STATUS:
                    first_available_at = OBSERVED_AT + timedelta(minutes=1)
                if fault == "listing_not_effective" and dataset is ReferenceDataset.LISTING_STATUS:
                    effective_from = MARKET_EFFECTIVE_TO - timedelta(hours=1)
            if (
                fault == "status_not_a_string_before_a_missing_code"
                and dataset is ReferenceDataset.LISTING_STATUS
                and code == codes[3]
            ):
                payload = {"status": 1}
            if (
                fault == "status_not_a_string_before_a_missing_code"
                and code == faulty
                and dataset is ReferenceDataset.PRICE_LIMIT_REGIME
            ):
                continue
            records.append(
                _market_record(
                    dataset,
                    code,
                    payload,
                    effective_from=effective_from,
                    first_available_at=first_available_at,
                )
            )
    registry.append_many_and_publish(tuple(records), published_at=MARKET_PUBLISHED_AT)
    _market_auction_spool(tmp_path, codes)
    _market_daily_snapshot(tmp_path / "operational-ro.duckdb", codes)

    bulk = _assembly_outcome(tmp_path, codes=codes, registry_path=registry_path, per_row=False)
    per_row = _assembly_outcome(tmp_path, codes=codes, registry_path=registry_path, per_row=True)

    assert bulk == per_row
    if fault == "status_not_a_string_before_a_missing_code":
        assert bulk[0] == f"{codes[3]} listing status must be a string"
    else:
        assert bulk[0] == f"{faulty} required reference evidence is unavailable"
        assert bulk[1] == "ReferenceDataUnavailableError"


def test_a_registry_that_fails_after_the_pointer_refuses_as_the_first_row(
    tmp_path: Path,
) -> None:
    """The snapshot is taken where the first row's first lookup was, and refuses as it did."""

    codes = _market_codes(6)
    registry_path = tmp_path / "reference.sqlite3"
    _market_registry(registry_path, codes)
    _market_auction_spool(tmp_path, codes)
    _market_daily_snapshot(tmp_path / "operational-ro.duckdb", codes)
    pending = ReferenceDataUnavailableError(
        "reference publication is pending a durable completion receipt"
    )
    outcomes = []
    for per_row in (False, True):
        registry = ReadonlyReferenceRegistry(registry_path)
        if per_row:
            _read_per_row(registry)

            def refuse_single(**_lookup: object) -> object:
                raise pending

            registry.as_of = refuse_single  # type: ignore[method-assign]
        else:

            def refuse_bulk(**_lookup: object) -> object:
                raise pending

            registry.as_of_snapshot = refuse_bulk  # type: ignore[method-assign]
        with pytest.raises(AuctionGapCandidateInputError) as refused:
            assemble_auction_gap_candidate_batch(
                auction_spool=LiveBatchSpool(tmp_path / "auction-spool"),
                daily_database_path=tmp_path / "operational-ro.duckdb",
                reference_registry=registry,
                calendar=_calendar(),
                trade_date=TRADE_DATE,
                observed_at=OBSERVED_AT,
                producer_commit=COMMIT,
            )
        outcomes.append(str(refused.value))

    assert outcomes[0] == outcomes[1]
    assert outcomes[0].endswith(" required reference evidence is unavailable")
    assert outcomes[0].split(" ")[0] in codes


def test_a_future_generation_is_refused_before_any_snapshot_is_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.unit.test_reference_as_of_snapshot import count_registry_io

    codes = _market_codes(4)
    registry = ReferenceRegistry(tmp_path / "reference.sqlite3")
    registry.append_many_and_publish(
        tuple(
            _market_record(dataset, code, _market_payload(dataset, index))
            for index, code in enumerate(codes)
            for dataset in _DATASETS
        ),
        published_at=OBSERVED_AT + timedelta(seconds=1),
    )
    readonly = ReadonlyReferenceRegistry(registry.path)
    _market_auction_spool(tmp_path, codes)
    _market_daily_snapshot(tmp_path / "operational-ro.duckdb", codes)
    io = count_registry_io(monkeypatch, ReadonlyReferenceRegistry)

    with pytest.raises(AuctionGapCandidateInputError, match="future evidence"):
        assemble_auction_gap_candidate_batch(
            auction_spool=LiveBatchSpool(tmp_path / "auction-spool"),
            daily_database_path=tmp_path / "operational-ro.duckdb",
            reference_registry=readonly,
            calendar=_calendar(),
            trade_date=TRADE_DATE,
            observed_at=OBSERVED_AT,
            producer_commit=COMMIT,
        )
    assert io.connections == 3
    assert not any("business_key IN" in statement for statement in io.statements)
