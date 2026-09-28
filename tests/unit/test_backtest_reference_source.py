"""A frozen four-domain reference slice for the 09:25 backtest decision."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError

from rquant.backtest.reference_source import (
    MAX_REFERENCE_CODES,
    BacktestReferenceSnapshot,
    BacktestReferenceSourceError,
    select_backtest_reference_facts,
)
from rquant.reference_data_registry import (
    ReadonlyReferenceRegistry,
    ReferenceAsOfSnapshot,
    ReferenceDataset,
    ReferenceRecord,
    ReferenceRegistry,
)

DAY = date(2026, 7, 21)
CODE = "600000.SH"
OTHER_CODE = "300001.SZ"
CN = ZoneInfo("Asia/Shanghai")
DECISION = datetime(2026, 7, 21, 9, 25, tzinfo=CN)
EARLY = DECISION - timedelta(minutes=10)
EFFECTIVE = DECISION - timedelta(minutes=20)
DOMAINS = (
    ReferenceDataset.LISTING_STATUS,
    ReferenceDataset.ST_STATUS,
    ReferenceDataset.SUSPENSION_STATUS,
    ReferenceDataset.PRICE_LIMIT_REGIME,
)


def _payloads(code: str) -> dict[ReferenceDataset, dict[str, object]]:
    exchange = "SSE" if code.endswith(".SH") else "SZSE"
    return {
        ReferenceDataset.LISTING_STATUS: {
            "status": "listed",
            "market": "CN",
            "exchange": exchange,
            "instrument_class": "EQUITY",
            "security_class": "A_SHARE",
        },
        ReferenceDataset.ST_STATUS: {"is_st": False},
        ReferenceDataset.SUSPENSION_STATUS: {"is_suspended": False},
        ReferenceDataset.PRICE_LIMIT_REGIME: {
            "limit_up_price": 11.0,
            "limit_down_price": 9.0,
        },
    }


def _frozen(
    root: Path,
    *,
    codes: tuple[str, ...] = (CODE,),
    replacements: dict[ReferenceDataset, dict[str, object]] | None = None,
    missing: ReferenceDataset | None = None,
    future_available: ReferenceDataset | None = None,
    future_effective: ReferenceDataset | None = None,
    available_at: datetime | None = None,
) -> tuple[ReferenceAsOfSnapshot, dict[tuple[str, ReferenceDataset], ReferenceRecord]]:
    root.mkdir(parents=True, exist_ok=True)
    registry = ReferenceRegistry(root / "reference.sqlite3")
    written: dict[tuple[str, ReferenceDataset], ReferenceRecord] = {}
    for code in codes:
        payloads = _payloads(code)
        for dataset in DOMAINS:
            if dataset == missing:
                continue
            record = ReferenceRecord(
                dataset_id=dataset,
                key=code,
                effective_from=(
                    DECISION + timedelta(minutes=1) if dataset == future_effective else EFFECTIVE
                ),
                first_available_at=(
                    DECISION + timedelta(minutes=1)
                    if dataset == future_available
                    else available_at or EARLY
                ),
                revision=1,
                source="test.reference",
                payload=(replacements or {}).get(dataset, payloads[dataset]),
            )
            registry.append(record)
            written[(code, dataset)] = record
    generation = registry.publish(
        published_at=(
            DECISION + timedelta(minutes=2)
            if future_available is not None
            else DECISION
            if available_at == DECISION
            else DECISION - timedelta(minutes=1)
        )
    )
    readonly = ReadonlyReferenceRegistry(registry.path)
    snapshot = readonly.as_of_snapshot(
        dataset_ids=DOMAINS,
        keys=codes,
        generation_id=generation.generation_id,
    )
    return snapshot, written


def _select(
    snapshot: ReferenceAsOfSnapshot,
    *,
    ts_codes: tuple[str, ...] = (CODE,),
    decision_time: datetime = DECISION,
) -> BacktestReferenceSnapshot:
    return select_backtest_reference_facts(
        snapshot,
        trade_date=DAY,
        decision_time=decision_time,
        ts_codes=ts_codes,
    )


def test_frozen_registry_selects_complete_typed_reference_and_stable_digest(tmp_path: Path) -> None:
    snapshot, written = _frozen(tmp_path, codes=(CODE, OTHER_CODE))
    result = _select(snapshot, ts_codes=(OTHER_CODE, CODE))
    again = _select(snapshot, ts_codes=(CODE, OTHER_CODE), decision_time=DECISION.astimezone(UTC))

    assert result.source_identity == again.source_identity
    assert result.generation_id == snapshot.generation_id
    assert result.decision_time == DECISION.astimezone(UTC)
    assert tuple(item.ts_code for item in result.facts) == (OTHER_CODE, CODE)
    fact = result.facts[1]
    assert fact.listing_status == "listed"
    assert fact.instrument_context.scope_key == ("CN", "SSE", "EQUITY", "A_SHARE")
    assert fact.instrument_context.classification_provenance is not None
    assert (
        fact.instrument_context.classification_provenance.reference_record_id
        == written[(CODE, ReferenceDataset.LISTING_STATUS)].record_id
    )
    assert fact.instrument_context.classification_provenance.reference_generation_id == (
        snapshot.generation_id
    )
    assert fact.is_st is False and fact.is_suspended is False
    assert str(fact.limit_down_price) == "9.0"
    assert str(fact.limit_up_price) == "11.0"
    assert fact.last_reference_available_at == EARLY.astimezone(UTC)
    for field, dataset in (
        ("listing", ReferenceDataset.LISTING_STATUS),
        ("st", ReferenceDataset.ST_STATUS),
        ("suspension", ReferenceDataset.SUSPENSION_STATUS),
        ("price_limit", ReferenceDataset.PRICE_LIMIT_REGIME),
    ):
        evidence = getattr(fact, field)
        assert evidence.reference_dataset == dataset.value
        assert evidence.reference_record_id == written[(CODE, dataset)].record_id
        assert evidence.reference_generation_id == snapshot.generation_id
        assert evidence.first_available_at == EARLY.astimezone(UTC)
    with pytest.raises(ValidationError):
        fact.is_st = True


def test_record_first_available_at_exactly_0925_is_preserved(tmp_path: Path) -> None:
    snapshot, _ = _frozen(tmp_path, available_at=DECISION)
    fact = _select(snapshot).facts[0]
    assert fact.listing.first_available_at == DECISION.astimezone(UTC)
    assert fact.last_reference_available_at == DECISION.astimezone(UTC)


def test_source_identity_changes_with_a_selected_record(tmp_path: Path) -> None:
    ordinary, _ = _frozen(tmp_path / "ordinary")
    st, _ = _frozen(tmp_path / "st", replacements={ReferenceDataset.ST_STATUS: {"is_st": True}})
    assert _select(ordinary).source_identity != _select(st).source_identity


@pytest.mark.parametrize("dataset", DOMAINS)
def test_missing_or_not_yet_visible_reference_fails_closed(
    tmp_path: Path, dataset: ReferenceDataset
) -> None:
    missing, _ = _frozen(tmp_path / "missing", missing=dataset)
    with pytest.raises(BacktestReferenceSourceError, match="reference evidence"):
        _select(missing)
    future, _ = _frozen(tmp_path / "future", future_available=dataset)
    with pytest.raises(BacktestReferenceSourceError, match="reference evidence"):
        _select(future)
    ineffective, _ = _frozen(tmp_path / "ineffective", future_effective=dataset)
    with pytest.raises(BacktestReferenceSourceError, match="reference evidence"):
        _select(ineffective)


@pytest.mark.parametrize(
    ("dataset", "payload"),
    (
        (ReferenceDataset.LISTING_STATUS, {"status": "listed"}),
        (
            ReferenceDataset.LISTING_STATUS,
            {**_payloads(CODE)[ReferenceDataset.LISTING_STATUS], "status": True},
        ),
        (
            ReferenceDataset.LISTING_STATUS,
            {**_payloads(CODE)[ReferenceDataset.LISTING_STATUS], "status": "delisted"},
        ),
        (
            ReferenceDataset.LISTING_STATUS,
            {**_payloads(CODE)[ReferenceDataset.LISTING_STATUS], "market": "US"},
        ),
        (
            ReferenceDataset.LISTING_STATUS,
            {**_payloads(CODE)[ReferenceDataset.LISTING_STATUS], "security_class": "ETF"},
        ),
        (
            ReferenceDataset.LISTING_STATUS,
            {**_payloads(CODE)[ReferenceDataset.LISTING_STATUS], "exchange": "SZSE"},
        ),
        (ReferenceDataset.ST_STATUS, {"is_st": 1}),
        (ReferenceDataset.SUSPENSION_STATUS, {"is_suspended": "false"}),
        (ReferenceDataset.PRICE_LIMIT_REGIME, {"limit_up_price": 0, "limit_down_price": 9}),
        (ReferenceDataset.PRICE_LIMIT_REGIME, {"limit_up_price": 9, "limit_down_price": 9}),
        (ReferenceDataset.PRICE_LIMIT_REGIME, {"limit_up_price": True, "limit_down_price": 9}),
        (ReferenceDataset.PRICE_LIMIT_REGIME, {"limit_up_price": "nan", "limit_down_price": 9}),
    ),
)
def test_malformed_or_non_a_share_reference_fails_closed(
    tmp_path: Path, dataset: ReferenceDataset, payload: dict[str, object]
) -> None:
    snapshot, _ = _frozen(tmp_path, replacements={dataset: payload})
    with pytest.raises(BacktestReferenceSourceError):
        _select(snapshot)


def test_batch_rejects_duplicate_unbounded_or_missing_prepared_codes(tmp_path: Path) -> None:
    snapshot, _ = _frozen(tmp_path)
    with pytest.raises(BacktestReferenceSourceError, match="duplicate"):
        _select(snapshot, ts_codes=(CODE, CODE))
    too_many = tuple(f"{index:06d}.SH" for index in range(MAX_REFERENCE_CODES + 1))
    with pytest.raises(BacktestReferenceSourceError, match="maximum"):
        _select(snapshot, ts_codes=too_many)
    with pytest.raises(BacktestReferenceSourceError, match="snapshot"):
        _select(snapshot, ts_codes=(OTHER_CODE,))


@pytest.mark.parametrize(
    "decision_time",
    (
        datetime(2026, 7, 21, 9, 24, tzinfo=CN),
        datetime(2026, 7, 21, 9, 25, 1, tzinfo=CN),
        datetime(2026, 7, 22, 9, 25, tzinfo=CN),
        datetime(2026, 7, 21, 9, 25),
    ),
)
def test_decision_time_must_be_exact_local_0925(tmp_path: Path, decision_time: datetime) -> None:
    snapshot, _ = _frozen(tmp_path)
    with pytest.raises(BacktestReferenceSourceError, match="09:25"):
        _select(snapshot, decision_time=decision_time)
