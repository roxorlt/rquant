"""auction_gap: codes without five prior daily rows are left out, not the whole day (pkg AI).

Host evidence, 2026-09-24: of 5,475 auction codes, 6 (new listings and suspensions) had
fewer than one `daily_bar` row on each of the prior five sessions, and the assembly refused
the batch for all of them, 20 rounds in a row. The rule now is per code, with a bound that
still refuses a snapshot that is itself incomplete.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from rquant.auction_gap_candidate_input import (
    PRIOR_FIVE_INCOMPLETE_MAX_FRACTION,
    PRIOR_FIVE_INCOMPLETE_OBSERVATION,
    AuctionGapCandidateAssembly,
    AuctionGapCandidateInputError,
    assemble_auction_gap_candidate_batch,
    assemble_auction_gap_candidate_input,
)
from rquant.auction_match_gateway import AuctionMatchGateway, AuctionMatchGatewayConfig
from rquant.live_spool import LiveBatchSpool
from rquant.reference_data_registry import (
    ReadonlyReferenceRegistry,
    ReferenceDataset,
    ReferenceRecord,
    ReferenceRegistry,
)
from rquant.runtime_builder_candidate import candidate_publisher_builder
from rquant.strategy_candidate_producers import produce_auction_gap_candidates
from tests.unit.test_auction_gap_candidate_input import (
    AUCTION_AVAILABLE_AT,
    COMMIT,
    OBSERVED_AT,
    PRIOR_DATES,
    TRADE_DATE,
    _calendar,
)
from tests.unit.test_runtime_builder_candidate import _auction_manifest

#: 120 codes so that 6 short ones are exactly the 5 % the bound allows
CODES = tuple(f"{600000 + index:06d}.SH" for index in range(120))
#: the host's six, by how many of the five prior sessions they had a row for
SHORT = {CODES[3]: 1, CODES[17]: 1, CODES[40]: 2, CODES[41]: 2, CODES[88]: 3, CODES[119]: 3}


def _auction_spool(tmp_path: Path, codes: tuple[str, ...]) -> LiveBatchSpool:
    spool = LiveBatchSpool(tmp_path / "auction-spool")
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


def _daily_snapshot(
    path: Path,
    codes: tuple[str, ...],
    *,
    sessions_per_code: dict[str, int] | None = None,
    duplicated: tuple[str, ...] = (),
) -> Path:
    """Every code gets the last `sessions_per_code[code]` prior sessions (default five)."""

    counts = sessions_per_code or {}
    rows = []
    for code in codes:
        dates = PRIOR_DATES[5 - counts.get(code, 5) :]
        rows.extend((code, day, 1_000.0) for day in dates)
        if code in duplicated:
            rows.append((code, PRIOR_DATES[-1], 1_000.0))
    with duckdb.connect(str(path)) as connection:
        connection.execute("CREATE TABLE daily_bar(ts_code VARCHAR, trade_date DATE, vol DOUBLE)")
        connection.executemany("INSERT INTO daily_bar VALUES (?, ?, ?)", rows)
    path.chmod(0o600)
    snapshot_time = datetime(2026, 7, 31, 1, 0, tzinfo=UTC).timestamp()
    os.utime(path, (snapshot_time, snapshot_time))
    return path


def _registry(tmp_path: Path, codes: tuple[str, ...]) -> ReadonlyReferenceRegistry:
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
            {
                "limit_eligible": True,
                "limit_percent": 0.1,
                "limit_up_price": 11.0,
                "limit_down_price": 9.0,
            },
        ),
    )
    for code in codes:
        for dataset, payload in payloads:
            registry.append(
                ReferenceRecord(
                    dataset_id=dataset,
                    key=code,
                    effective_from=effective_from,
                    revision=1,
                    source="test.reference",
                    first_available_at=first_available_at,
                    payload=payload,
                )
            )
    registry.publish(published_at=datetime(2026, 7, 31, 1, 24, tzinfo=UTC))
    return ReadonlyReferenceRegistry(path)


def _assemble(
    tmp_path: Path,
    *,
    codes: tuple[str, ...] = CODES,
    sessions_per_code: dict[str, int] | None = None,
    duplicated: tuple[str, ...] = (),
) -> AuctionGapCandidateAssembly:
    return assemble_auction_gap_candidate_input(
        auction_spool=_auction_spool(tmp_path, codes),
        daily_database_path=_daily_snapshot(
            tmp_path / "operational-ro.duckdb",
            codes,
            sessions_per_code=sessions_per_code,
            duplicated=duplicated,
        ),
        reference_registry=_registry(tmp_path, codes),
        calendar=_calendar(),
        trade_date=TRADE_DATE,
        observed_at=OBSERVED_AT,
        producer_commit=COMMIT,
    )


def test_the_host_s_six_short_codes_are_left_out_and_the_rest_assembled(tmp_path: Path) -> None:
    assembly = _assemble(tmp_path, sessions_per_code=SHORT)

    assert assembly.auction_code_count == len(CODES)
    assert assembly.prior_five_incomplete_codes == tuple(sorted(SHORT))
    facts = {fact.ts_code: fact for fact in assembly.batch.facts}
    assert set(facts) == set(CODES) - set(SHORT)
    assert all(
        tuple(item.trade_date for item in fact.prior5_daily_volumes) == PRIOR_DATES
        for fact in facts.values()
    )
    #: and the batch is a real one: the others become candidates as they always did
    candidates = produce_auction_gap_candidates(
        authority=assembly.batch.authority, facts=assembly.batch.facts
    )
    assert len(candidates) == len(CODES) - len(SHORT)


def test_a_duplicated_prior_session_row_leaves_only_that_code_out(tmp_path: Path) -> None:
    assembly = _assemble(tmp_path, duplicated=(CODES[5],))

    assert assembly.prior_five_incomplete_codes == (CODES[5],)
    assert CODES[5] not in {fact.ts_code for fact in assembly.batch.facts}


def test_the_bound_allows_exactly_five_percent_and_refuses_one_more(tmp_path: Path) -> None:
    assert PRIOR_FIVE_INCOMPLETE_MAX_FRACTION == 0.05
    assert len(SHORT) == PRIOR_FIVE_INCOMPLETE_MAX_FRACTION * len(CODES)
    allowed = _assemble(tmp_path / "allowed", sessions_per_code=SHORT)
    assert len(allowed.prior_five_incomplete_codes) == 6

    one_more = {**SHORT, CODES[60]: 4}
    with pytest.raises(AuctionGapCandidateInputError) as refused:
        _assemble(tmp_path / "refused", sessions_per_code=one_more)
    message = str(refused.value)
    assert "exactly one row for every prior-five session" in message
    assert "7 of 120 auction codes" in message
    assert CODES[3] in message


def test_a_snapshot_missing_a_whole_prior_session_is_still_refused(tmp_path: Path) -> None:
    with pytest.raises(AuctionGapCandidateInputError, match="120 of 120 auction codes"):
        _assemble(tmp_path, sessions_per_code=dict.fromkeys(CODES, 4))


def test_a_single_code_day_still_refuses_its_one_short_code(tmp_path: Path) -> None:
    """One code of one is 100 %: the per-code rule never turns an empty batch into a day."""

    with pytest.raises(AuctionGapCandidateInputError, match="1 of 1 auction codes"):
        _assemble(tmp_path, codes=CODES[:1], sessions_per_code={CODES[0]: 4})


def test_the_daily_snapshot_identity_names_what_was_left_out(tmp_path: Path) -> None:
    complete = _assemble(tmp_path / "complete").batch
    short = _assemble(tmp_path / "short", sessions_per_code=SHORT).batch

    complete_ids = {fact.daily_snapshot_id for fact in complete.facts}
    short_ids = {fact.daily_snapshot_id for fact in short.facts}
    assert len(complete_ids) == len(short_ids) == 1
    assert complete_ids != short_ids
    assert complete.authority.authority_snapshot_id != short.authority.authority_snapshot_id


def test_the_batch_function_keeps_returning_the_batch(tmp_path: Path) -> None:
    codes = CODES[:20]
    batch = assemble_auction_gap_candidate_batch(
        auction_spool=_auction_spool(tmp_path, codes),
        daily_database_path=_daily_snapshot(tmp_path / "operational-ro.duckdb", codes),
        reference_registry=_registry(tmp_path, codes),
        calendar=_calendar(),
        trade_date=TRADE_DATE,
        observed_at=OBSERVED_AT,
        producer_commit=COMMIT,
    )

    assert {fact.ts_code for fact in batch.facts} == set(codes)


def test_the_publisher_reports_the_count_as_an_observation_not_a_degradation(
    tmp_path: Path,
) -> None:
    """The count is on the heartbeat all day, and it never makes the role `degraded` (#290)."""

    assembly = _assemble(tmp_path / "world", sessions_per_code=SHORT)
    clock = {"now": datetime(2026, 7, 31, 1, 40, tzinfo=UTC)}
    step = candidate_publisher_builder(
        auction_input_loader=lambda **_: assembly,
        clock=lambda: clock["now"],
    )(_auction_manifest(tmp_path / "publisher"))

    published = step()
    #: after the window: nothing is assembled, and the day's count is still what it said
    clock["now"] = datetime(2026, 7, 31, 2, 15, tzinfo=UTC)
    idled = step()

    for result in (published, idled):
        assert result.degraded_reasons == ()
        assert result.degraded_detail is None
        assert dict(result.observations) == {PRIOR_FIVE_INCOMPLETE_OBSERVATION: 6}
    assert published.processed_count == len(CODES) - len(SHORT)
    assert idled.output_sequence == published.output_sequence


def test_a_refused_round_names_the_class_and_carries_the_message(tmp_path: Path) -> None:
    def refusing(**_: object) -> object:
        try:
            raise OSError("replica went away")
        except OSError as cause:
            raise AuctionGapCandidateInputError("daily snapshot query failed") from cause

    step = candidate_publisher_builder(
        auction_input_loader=refusing,
        clock=lambda: datetime(2026, 7, 31, 1, 40, tzinfo=UTC),
    )(_auction_manifest(tmp_path))

    result = step()

    assert result.degraded_reasons == (
        "auction_gap_input_unavailable:AuctionGapCandidateInputError",
    )
    assert result.degraded_detail == (
        "AuctionGapCandidateInputError: daily snapshot query failed "
        "(caused by OSError: replica went away)"
    )
    assert dict(result.observations) == {}


def test_the_carried_count_is_cleared_when_the_local_trade_date_changes(tmp_path: Path) -> None:
    """Review S4: a publisher that survives the night must not show yesterday's count."""

    assembly = _assemble(tmp_path / "world", sessions_per_code=SHORT)
    clock = {"now": datetime(2026, 7, 31, 1, 40, tzinfo=UTC)}
    step = candidate_publisher_builder(
        auction_input_loader=lambda **_: assembly,
        clock=lambda: clock["now"],
    )(_auction_manifest(tmp_path / "publisher"))

    published = step()
    #: 23:59 the same local day: still that day's count
    clock["now"] = datetime(2026, 7, 31, 15, 59, tzinfo=UTC)
    late_same_day = step()
    #: 00:01 the next local day, before its window: nothing measured yet today
    clock["now"] = datetime(2026, 7, 31, 16, 1, tzinfo=UTC)
    next_day = step()

    assert dict(published.observations) == {PRIOR_FIVE_INCOMPLETE_OBSERVATION: 6}
    assert dict(late_same_day.observations) == {PRIOR_FIVE_INCOMPLETE_OBSERVATION: 6}
    assert dict(next_day.observations) == {}
    assert next_day.output_sequence == published.output_sequence
