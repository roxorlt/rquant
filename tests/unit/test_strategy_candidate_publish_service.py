from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

import rquant.strategy_candidate_publish_service as service_module
from rquant.live_contracts import BatchQualityStatus
from rquant.strategy_candidate_producers import (
    AuctionMatchFact,
    GrowthBoardFact,
    NShapePoolFact,
    PublishedCandidateInputAuthority,
)
from rquant.strategy_candidate_publish_service import (
    AuctionGapCandidateBatch,
    GrowthBoardCandidateBatch,
    NShapeCandidateBatch,
    publish_candidate_batch,
)
from rquant.strategy_candidate_snapshot import (
    StrategyCandidatePriceBasis,
    StrategyCandidateRecord,
    StrategyCandidateSnapshotSpool,
    strategy_candidate_schema_fingerprint,
)

TRADE_DATE = date(2026, 7, 31)
CAPTURED_AT = datetime(2026, 7, 31, 1, 28, tzinfo=UTC)
COMMIT = "a" * 40
AUTHORITY_ID = "1" * 64
DEFINITION_FINGERPRINT = "2" * 64
EXECUTABLE_FINGERPRINT = "3" * 64
STATIC_FEATURE_SCHEMA = {"source": {"dtype": "string", "semantic": "candidate source strategy"}}


def _exact_fingerprints(strategy_id: str) -> dict[str, object]:
    return {
        "definition_fingerprint": DEFINITION_FINGERPRINT,
        "executable_fingerprint": EXECUTABLE_FINGERPRINT,
        "candidate_schema_fingerprint": strategy_candidate_schema_fingerprint(
            strategy_id=strategy_id,
            strategy_version="1",
            static_feature_schema=STATIC_FEATURE_SCHEMA,
        ),
        "static_feature_schema": STATIC_FEATURE_SCHEMA,
    }


def _authority(
    *,
    quality_status: BatchQualityStatus = BatchQualityStatus.PUBLISHED,
    producer_commit: str = COMMIT,
    authority_snapshot_id: str = AUTHORITY_ID,
) -> PublishedCandidateInputAuthority:
    return PublishedCandidateInputAuthority(
        trade_date=TRADE_DATE,
        captured_at=CAPTURED_AT,
        quality_status=quality_status,
        authority_snapshot_id=authority_snapshot_id,
        producer_commit=producer_commit,
    )


def _record(strategy_id: str) -> StrategyCandidateRecord:
    variant = {
        "n_shape": "pool1",
        "growth_board_surge": "gem",
        "auction_gap": "auction_gap",
    }[strategy_id]
    return StrategyCandidateRecord(
        strategy_id=strategy_id,
        strategy_version="1",
        candidate_id="300001.SZ",
        variant=variant,
        decision_at=CAPTURED_AT,
        available_at=CAPTURED_AT,
        effective_trade_date=TRADE_DATE,
        reference_trade_date=TRADE_DATE,
        price_basis=StrategyCandidatePriceBasis.RAW,
        static_features={"source": strategy_id},
        reference_snapshot_ids={"candidate_authority": AUTHORITY_ID},
    )


@pytest.mark.parametrize(
    ("batch", "producer_name", "strategy_id"),
    [
        (
            NShapeCandidateBatch(authority=_authority(), facts=()),
            "produce_n_shape_candidates",
            "n_shape",
        ),
        (
            GrowthBoardCandidateBatch(authority=_authority(), facts=()),
            "produce_growth_board_surge_candidates",
            "growth_board_surge",
        ),
        (
            AuctionGapCandidateBatch(authority=_authority(), facts=()),
            "produce_auction_gap_candidates",
            "auction_gap",
        ),
    ],
)
def test_publish_candidate_batch_dispatches_typed_batch_and_publishes_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    batch: NShapeCandidateBatch | GrowthBoardCandidateBatch | AuctionGapCandidateBatch,
    producer_name: str,
    strategy_id: str,
) -> None:
    calls: list[tuple[PublishedCandidateInputAuthority, object]] = []

    def producer(
        *, authority: PublishedCandidateInputAuthority, facts: object
    ) -> tuple[StrategyCandidateRecord, ...]:
        calls.append((authority, facts))
        return (_record(strategy_id),)

    monkeypatch.setattr(service_module, producer_name, producer)
    root = (tmp_path / strategy_id).resolve()

    summary = publish_candidate_batch(
        snapshot_root=root,
        expected_commit=COMMIT,
        batch=batch,
        **_exact_fingerprints(strategy_id),
    )

    assert calls == [(batch.authority, batch.facts)]
    assert summary.strategy_id == strategy_id
    assert summary.strategy_version == "1"
    assert summary.trade_date == TRADE_DATE
    assert summary.captured_at == CAPTURED_AT
    assert summary.authority_snapshot_id == AUTHORITY_ID
    assert summary.candidate_count == 1
    assert summary.snapshot_sequence == 0
    assert summary.published is True
    observed = StrategyCandidateSnapshotSpool(root).read_strategy_as_of(
        CAPTURED_AT,
        strategy_id=strategy_id,
        strategy_version="1",
        definition_fingerprint=DEFINITION_FINGERPRINT,
        executable_fingerprint=EXECUTABLE_FINGERPRINT,
        candidate_schema_fingerprint=_exact_fingerprints(strategy_id)[
            "candidate_schema_fingerprint"
        ],
        static_feature_schema=STATIC_FEATURE_SCHEMA,
    )
    assert observed is not None
    assert observed.content_sha256 == summary.snapshot_content_sha256
    assert observed.rows == (_record(strategy_id),)
    assert observed.authority_binding is not None
    assert observed.authority_binding.definition_fingerprint == DEFINITION_FINGERPRINT
    assert observed.authority_binding.executable_fingerprint == EXECUTABLE_FINGERPRINT
    assert (
        observed.authority_binding.candidate_schema_fingerprint
        == (_exact_fingerprints(strategy_id)["candidate_schema_fingerprint"])
    )


def test_publish_candidate_batch_suppresses_an_identical_retry(tmp_path: Path) -> None:
    root = (tmp_path / "n-shape").resolve()
    batch = NShapeCandidateBatch(authority=_authority(), facts=())

    first = publish_candidate_batch(
        snapshot_root=root,
        expected_commit=COMMIT,
        batch=batch,
        **_exact_fingerprints("n_shape"),
    )
    duplicate = publish_candidate_batch(
        snapshot_root=root,
        expected_commit=COMMIT,
        batch=batch,
        **_exact_fingerprints("n_shape"),
    )

    assert first.published is True
    assert duplicate.published is False
    assert duplicate.snapshot_sequence == first.snapshot_sequence == 0
    assert duplicate.snapshot_content_sha256 == first.snapshot_content_sha256
    assert len(list((root / "generations").glob("*.json"))) == 1


def test_publish_candidate_batch_rejects_reusing_another_strategy_root(
    tmp_path: Path,
) -> None:
    root = (tmp_path / "shared").resolve()
    first = publish_candidate_batch(
        snapshot_root=root,
        expected_commit=COMMIT,
        batch=NShapeCandidateBatch(authority=_authority(), facts=()),
        **_exact_fingerprints("n_shape"),
    )
    before = tuple(path.read_bytes() for path in sorted(root.rglob("*.json")))

    with pytest.raises(RuntimeError, match="authority.*identity|bound"):
        publish_candidate_batch(
            snapshot_root=root,
            expected_commit=COMMIT,
            batch=GrowthBoardCandidateBatch(authority=_authority(), facts=()),
            **_exact_fingerprints("growth_board_surge"),
        )

    assert first.strategy_id == "n_shape"
    assert tuple(path.read_bytes() for path in sorted(root.rglob("*.json"))) == before
    assert len(list((root / "generations").glob("*.json"))) == 1


def test_empty_candidate_batch_publishes_new_generation_for_new_input_authority(
    tmp_path: Path,
) -> None:
    root = (tmp_path / "n-shape").resolve()
    first = publish_candidate_batch(
        snapshot_root=root,
        expected_commit=COMMIT,
        batch=NShapeCandidateBatch(authority=_authority(), facts=()),
        **_exact_fingerprints("n_shape"),
    )
    changed = publish_candidate_batch(
        snapshot_root=root,
        expected_commit=COMMIT,
        batch=NShapeCandidateBatch(
            authority=_authority(authority_snapshot_id="2" * 64),
            facts=(),
        ),
        **_exact_fingerprints("n_shape"),
    )

    assert first.candidate_count == changed.candidate_count == 0
    assert first.authority_snapshot_id == AUTHORITY_ID
    assert changed.authority_snapshot_id == "2" * 64
    assert changed.published is True
    assert changed.snapshot_sequence == first.snapshot_sequence + 1
    assert changed.snapshot_content_sha256 != first.snapshot_content_sha256


@pytest.mark.parametrize(
    ("batch", "expected_commit", "match"),
    [
        (
            NShapeCandidateBatch(
                authority=_authority(quality_status=BatchQualityStatus.DEGRADED),
                facts=(),
            ),
            COMMIT,
            "published",
        ),
        (
            NShapeCandidateBatch(authority=_authority(), facts=()),
            "b" * 40,
            "commit",
        ),
    ],
)
def test_publish_candidate_batch_fails_closed_before_creating_authority(
    tmp_path: Path,
    batch: NShapeCandidateBatch,
    expected_commit: str,
    match: str,
) -> None:
    root = (tmp_path / "candidate").resolve()

    with pytest.raises((RuntimeError, ValueError), match=match):
        publish_candidate_batch(
            snapshot_root=root,
            expected_commit=expected_commit,
            batch=batch,
            **_exact_fingerprints("n_shape"),
        )

    assert not root.exists()


def test_publish_candidate_batch_rejects_commit_before_running_producer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    producer_called = False

    def producer(**_: object) -> tuple[StrategyCandidateRecord, ...]:
        nonlocal producer_called
        producer_called = True
        return ()

    monkeypatch.setattr(service_module, "produce_n_shape_candidates", producer)

    with pytest.raises(ValueError, match="commit"):
        publish_candidate_batch(
            snapshot_root=(tmp_path / "candidate").resolve(),
            expected_commit="b" * 40,
            batch=NShapeCandidateBatch(authority=_authority(), facts=()),
            **_exact_fingerprints("n_shape"),
        )

    assert producer_called is False


@pytest.mark.parametrize("missing", tuple(_exact_fingerprints("n_shape")))
def test_publish_candidate_batch_requires_complete_exact_identity(
    tmp_path: Path,
    missing: str,
) -> None:
    identity = dict(_exact_fingerprints("n_shape"))
    identity.pop(missing)

    with pytest.raises(TypeError, match=missing):
        publish_candidate_batch(
            snapshot_root=(tmp_path / "candidate").resolve(),
            expected_commit=COMMIT,
            batch=NShapeCandidateBatch(authority=_authority(), facts=()),
            **identity,
        )

    assert not (tmp_path / "candidate").exists()


def test_candidate_batch_models_reject_cross_strategy_facts() -> None:
    with pytest.raises(ValueError):
        NShapeCandidateBatch(
            authority=_authority(),
            facts=(GrowthBoardFact.model_construct(),),  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError):
        GrowthBoardCandidateBatch(
            authority=_authority(),
            facts=(AuctionMatchFact.model_construct(),),  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError):
        AuctionGapCandidateBatch(
            authority=_authority(),
            facts=(NShapePoolFact.model_construct(),),  # type: ignore[arg-type]
        )
