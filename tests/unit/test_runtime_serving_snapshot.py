from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from rquant.paper_contracts import PaperAccountSnapshot
from rquant.runtime_service_control import (
    RuntimeServiceHealth,
    RuntimeServicePlane,
    RuntimeServiceStatus,
)
from rquant.runtime_serving_snapshot import (
    LAB_JOBS_DATASET_ID,
    PAPER_ACCOUNTS_DATASET_ID,
    PROMOTIONS_DATASET_ID,
    REFERENCE_SLOW_AUTHORITY_DATASET_ID,
    REFERENCE_SLOW_CONTRACT_DATASET_ID,
    REFERENCE_SLOW_DATASET_ID,
    RUNTIME_HEALTH_DATASET_ID,
    SIGNALS_DATASET_ID,
    LabJobsPayload,
    PaperAccountsPayload,
    PromotionsPayload,
    ReferenceSlowPayload,
    RuntimeHealthPayload,
    ServingSnapshotAssembler,
    SignalDeliveryPayload,
    SourceReadResult,
)
from rquant.serving_contracts import FreshnessStatus
from rquant.serving_read_models import ServingProjectionPayload, ServingSignalRecord
from rquant.signal_contracts import SignalAction, SignalEnvelope

NOW = datetime(2026, 7, 31, 2, 31, tzinfo=UTC)


def _signal_record(sequence: int, candidate_id: str) -> ServingSignalRecord:
    signal = SignalEnvelope(
        schema_version=1,
        strategy_id="n-shape",
        strategy_version="1",
        parameter_fingerprint="a" * 64,
        dataset_snapshot_id="b" * 64,
        feature_snapshot_id="c" * 64,
        event_time=NOW - timedelta(seconds=5),
        available_at=NOW,
        candidate_id=candidate_id,
        action=SignalAction.B_INTENT,
        reason_codes=("strong_support",),
        evidence={"score": 0.8},
        expires_at=NOW + timedelta(minutes=5),
        producer_commit="d" * 40,
    )
    return ServingSignalRecord(global_sequence=sequence, signal=signal)


def _paper_account(account_id: str) -> PaperAccountSnapshot:
    return PaperAccountSnapshot(
        account_id=account_id,
        as_of_time=NOW,
        cash=Decimal("100000"),
        available_cash=Decimal("100000"),
        frozen_cash=Decimal("0"),
        realized_pnl=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        nav=Decimal("100000"),
    )


def _runtime_health(service_id: str) -> RuntimeServiceHealth:
    return RuntimeServiceHealth(
        service_id=service_id,
        plane=RuntimeServicePlane.LIVE,
        status=RuntimeServiceStatus.RUNNING,
        stale=False,
        observed_at=NOW,
    )


def _result(
    dataset_id: str,
    payload: object,
    *,
    generation_character: str,
    sequence: int = 1,
    status: FreshnessStatus = FreshnessStatus.FRESH,
    reason: str | None = None,
    event_time: datetime = NOW,
    published_at: datetime = NOW,
) -> SourceReadResult:
    return SourceReadResult(
        dataset_id=dataset_id,
        generation_id=generation_character * 64,
        sequence=sequence,
        event_time=event_time,
        published_at=published_at,
        status=status,
        reason=reason,
        payload=payload,
    )


def _assembler(
    *,
    signal_result: SourceReadResult | None = None,
    paper_result: SourceReadResult | None = None,
    runtime_result: SourceReadResult | None = None,
    fail_closed: bool = True,
) -> ServingSnapshotAssembler:
    return ServingSnapshotAssembler(
        signal_reader=lambda _as_of: (
            signal_result
            or _result(
                SIGNALS_DATASET_ID,
                SignalDeliveryPayload(),
                generation_character="1",
            )
        ),
        paper_accounts_reader=lambda _as_of: (
            paper_result
            or _result(
                PAPER_ACCOUNTS_DATASET_ID,
                PaperAccountsPayload(),
                generation_character="2",
            )
        ),
        runtime_health_reader=lambda _as_of: (
            runtime_result
            or _result(
                RUNTIME_HEALTH_DATASET_ID,
                RuntimeHealthPayload(),
                generation_character="3",
            )
        ),
        lab_jobs_reader=lambda _as_of: _result(
            LAB_JOBS_DATASET_ID,
            LabJobsPayload(),
            generation_character="4",
        ),
        promotions_reader=lambda _as_of: _result(
            PROMOTIONS_DATASET_ID,
            PromotionsPayload(),
            generation_character="5",
        ),
        reference_slow_reader=lambda _as_of: _result(
            REFERENCE_SLOW_AUTHORITY_DATASET_ID,
            ReferenceSlowPayload(
                reference_generation_id="6" * 64,
                revision=1,
                price_basis="raw_session",
                adjustment_basis="tushare_adj_factor",
                available_at=NOW,
            ),
            generation_character="7",
        ),
        fail_closed=fail_closed,
    )


def test_assembles_all_owner_readers_into_one_coherent_snapshot() -> None:
    calls: list[tuple[str, datetime]] = []

    def read(dataset_id: str, result: SourceReadResult):
        def reader(as_of: datetime) -> SourceReadResult:
            calls.append((dataset_id, as_of))
            return result

        return reader

    signal_result = _result(
        SIGNALS_DATASET_ID,
        SignalDeliveryPayload(
            signals=(
                _signal_record(2, "600001.SH"),
                _signal_record(1, "600000.SH"),
            )
        ),
        generation_character="1",
        sequence=2,
    )
    paper_result = _result(
        PAPER_ACCOUNTS_DATASET_ID,
        PaperAccountsPayload(paper_accounts=(_paper_account("paper-b"), _paper_account("paper-a"))),
        generation_character="2",
    )
    runtime_result = _result(
        RUNTIME_HEALTH_DATASET_ID,
        RuntimeHealthPayload(
            runtime_services=(_runtime_health("router"), _runtime_health("feature"))
        ),
        generation_character="3",
    )
    assembler = ServingSnapshotAssembler(
        signal_reader=read(SIGNALS_DATASET_ID, signal_result),
        paper_accounts_reader=read(PAPER_ACCOUNTS_DATASET_ID, paper_result),
        runtime_health_reader=read(RUNTIME_HEALTH_DATASET_ID, runtime_result),
        lab_jobs_reader=read(
            LAB_JOBS_DATASET_ID,
            _result(
                LAB_JOBS_DATASET_ID,
                LabJobsPayload(),
                generation_character="4",
            ),
        ),
        promotions_reader=read(
            PROMOTIONS_DATASET_ID,
            _result(
                PROMOTIONS_DATASET_ID,
                PromotionsPayload(),
                generation_character="5",
            ),
        ),
        reference_slow_reader=read(
            REFERENCE_SLOW_AUTHORITY_DATASET_ID,
            _result(
                REFERENCE_SLOW_AUTHORITY_DATASET_ID,
                ReferenceSlowPayload(
                    reference_generation_id="6" * 64,
                    revision=1,
                    price_basis="raw_session",
                    adjustment_basis="tushare_adj_factor",
                    available_at=NOW,
                ),
                generation_character="7",
            ),
        ),
    )

    snapshot = assembler.assemble(NOW)

    assert calls == [
        (SIGNALS_DATASET_ID, NOW),
        (PAPER_ACCOUNTS_DATASET_ID, NOW),
        (RUNTIME_HEALTH_DATASET_ID, NOW),
        (LAB_JOBS_DATASET_ID, NOW),
        (PROMOTIONS_DATASET_ID, NOW),
        (REFERENCE_SLOW_AUTHORITY_DATASET_ID, NOW),
    ]
    assert snapshot.read_model.observed_at == NOW
    assert [record.global_sequence for record in snapshot.read_model.signals] == [1, 2]
    assert [record.account_id for record in snapshot.read_model.paper_accounts] == [
        "paper-a",
        "paper-b",
    ]
    assert [record.service_id for record in snapshot.read_model.runtime_services] == [
        "feature",
        "router",
    ]
    assert [watermark.dataset_id for watermark in snapshot.watermarks] == sorted(
        snapshot.source_generations
    )
    assert snapshot.source_generations[SIGNALS_DATASET_ID] == "1" * 64
    assert snapshot.source_generations[REFERENCE_SLOW_DATASET_ID] == "6" * 64
    assert snapshot.source_generations[REFERENCE_SLOW_AUTHORITY_DATASET_ID] == "7" * 64
    assert len(snapshot.source_generations[REFERENCE_SLOW_CONTRACT_DATASET_ID]) == 64
    assert snapshot.reference_slow.revision == 1


def test_reference_slow_evidence_is_required_and_cannot_be_future() -> None:
    assembler = _assembler()
    object.__setattr__(
        assembler,
        "reference_slow_reader",
        lambda _as_of: _result(
            REFERENCE_SLOW_AUTHORITY_DATASET_ID,
            ReferenceSlowPayload(
                reference_generation_id="8" * 64,
                revision=2,
                price_basis="raw_session",
                adjustment_basis="tushare_adj_factor",
                available_at=NOW + timedelta(microseconds=1),
            ),
            generation_character="9",
            published_at=NOW,
        ),
    )

    with pytest.raises(ValueError, match="reference.*future|future.*reference"):
        assembler.assemble(NOW)


def test_page_projection_is_bound_to_the_verified_owner_generation() -> None:
    assembler = _assembler()
    object.__setattr__(
        assembler,
        "reference_slow_reader",
        lambda _as_of: _result(
            REFERENCE_SLOW_AUTHORITY_DATASET_ID,
            ReferenceSlowPayload(
                reference_generation_id="6" * 64,
                revision=1,
                price_basis="raw_session",
                adjustment_basis="tushare_adj_factor",
                available_at=NOW,
                projections=(
                    ServingProjectionPayload(
                        table_name="stock_basic",
                        available_at=NOW,
                        rows=(
                            {
                                "ts_code": "600000.SH",
                                "name": "浦发银行",
                                "industry": "银行",
                            },
                        ),
                    ),
                ),
            ),
            generation_character="7",
            published_at=NOW,
        ),
    )

    snapshot = assembler.assemble(NOW)

    assert len(snapshot.read_model.projections) == 1
    projection = snapshot.read_model.projections[0]
    assert projection.table_name == "stock_basic"
    assert projection.owner_dataset_id == REFERENCE_SLOW_AUTHORITY_DATASET_ID
    assert projection.owner_generation_id == "7" * 64


def test_reader_failure_is_fail_closed_or_explicitly_unavailable() -> None:
    def failed_reader(_as_of: datetime) -> SourceReadResult:
        raise OSError("runtime heartbeat unavailable")

    closed = _assembler()
    object.__setattr__(closed, "runtime_health_reader", failed_reader)
    with pytest.raises(RuntimeError, match="runtime_health reader failed"):
        closed.assemble(NOW)

    open_assembler = _assembler(fail_closed=False)
    object.__setattr__(open_assembler, "runtime_health_reader", failed_reader)
    snapshot = open_assembler.assemble(NOW)
    watermark = next(
        item for item in snapshot.watermarks if item.dataset_id == RUNTIME_HEALTH_DATASET_ID
    )

    assert watermark.status is FreshnessStatus.UNAVAILABLE
    assert watermark.reason == "OSError: runtime heartbeat unavailable"
    assert watermark.event_time == NOW
    assert watermark.published_at == NOW
    assert snapshot.read_model.runtime_services == ()


def test_nonfresh_source_requires_reason_and_is_preserved() -> None:
    with pytest.raises(ValidationError, match="requires reason"):
        _result(
            PAPER_ACCOUNTS_DATASET_ID,
            PaperAccountsPayload(),
            generation_character="2",
            status=FreshnessStatus.STALE,
        )

    stale = _result(
        PAPER_ACCOUNTS_DATASET_ID,
        PaperAccountsPayload(),
        generation_character="2",
        status=FreshnessStatus.STALE,
        reason="source generation is 90 seconds old",
    )
    snapshot = _assembler(paper_result=stale).assemble(NOW)

    watermark = next(
        item for item in snapshot.watermarks if item.dataset_id == PAPER_ACCOUNTS_DATASET_ID
    )
    assert watermark.status is FreshnessStatus.STALE
    assert watermark.reason == "source generation is 90 seconds old"


@pytest.mark.parametrize("field", ["event_time", "published_at"])
def test_rejects_future_source_evidence(field: str) -> None:
    future_time = NOW + timedelta(microseconds=1)
    values = {field: future_time}
    if field == "event_time":
        values["published_at"] = future_time
    future = _result(
        SIGNALS_DATASET_ID,
        SignalDeliveryPayload(),
        generation_character="1",
        **values,
    )

    with pytest.raises(ValueError, match="future evidence"):
        _assembler(signal_result=future).assemble(NOW)


def test_rejects_duplicate_dataset_and_owner_payload_type_mismatch() -> None:
    duplicate = _result(
        PAPER_ACCOUNTS_DATASET_ID,
        SignalDeliveryPayload(),
        generation_character="1",
    )
    with pytest.raises(ValueError, match="duplicate dataset"):
        _assembler(signal_result=duplicate).assemble(NOW)

    wrong_type = _result(
        SIGNALS_DATASET_ID,
        PaperAccountsPayload(),
        generation_character="1",
    )
    with pytest.raises(TypeError, match="signals payload"):
        _assembler(signal_result=wrong_type).assemble(NOW)


def test_assembly_is_deterministic_for_equivalent_reader_ordering() -> None:
    first = _assembler(
        paper_result=_result(
            PAPER_ACCOUNTS_DATASET_ID,
            PaperAccountsPayload(
                paper_accounts=(_paper_account("paper-b"), _paper_account("paper-a"))
            ),
            generation_character="2",
        ),
        runtime_result=_result(
            RUNTIME_HEALTH_DATASET_ID,
            RuntimeHealthPayload(
                runtime_services=(_runtime_health("router"), _runtime_health("feature"))
            ),
            generation_character="3",
        ),
    ).assemble(NOW)
    second = _assembler(
        paper_result=_result(
            PAPER_ACCOUNTS_DATASET_ID,
            PaperAccountsPayload(
                paper_accounts=(_paper_account("paper-a"), _paper_account("paper-b"))
            ),
            generation_character="2",
        ),
        runtime_result=_result(
            RUNTIME_HEALTH_DATASET_ID,
            RuntimeHealthPayload(
                runtime_services=(_runtime_health("feature"), _runtime_health("router"))
            ),
            generation_character="3",
        ),
    ).assemble(NOW)

    assert first.model_dump(mode="json") == second.model_dump(mode="json")
