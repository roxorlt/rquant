from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.runtime_candidate_universe import (
    CandidateUniverseAuthority,
    CandidateUniverseAuthorityEvidence,
    CandidateUniverseCodeEvidence,
    CandidateUniverseDegradedAuthority,
    CandidateUniverseHitEvidence,
    RuntimeCandidateUniverseConfig,
    RuntimeCandidateUniverseIntegrityError,
    RuntimeCandidateUniverseLoader,
    RuntimeCandidateUniverseResult,
)
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.strategy_candidate_snapshot import (
    StrategyCandidatePriceBasis,
    StrategyCandidateRecord,
    StrategyCandidateSnapshot,
    StrategyCandidateSnapshotSpool,
)

COMMIT = "a" * 40
OTHER_COMMIT = "b" * 40
TRADE_DATE = date(2026, 7, 31)
AS_OF = datetime(2026, 7, 31, 1, 30, tzinfo=UTC)
REFERENCE_HASH = "1" * 64


def _row(
    code: str,
    *,
    strategy_id: str = "n_shape",
    strategy_version: str = "v1",
    decision_at: datetime | None = None,
    available_at: datetime | None = None,
    variant: str = "default",
) -> StrategyCandidateRecord:
    resolved_decision_at = decision_at or AS_OF - timedelta(minutes=5)
    return StrategyCandidateRecord(
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        candidate_id=code,
        variant=variant,
        decision_at=resolved_decision_at,
        available_at=available_at or resolved_decision_at + timedelta(minutes=1),
        reference_trade_date=resolved_decision_at.date() - timedelta(days=1),
        price_basis=StrategyCandidatePriceBasis.QFQ_PIT,
        static_features={"score": 0.8},
        reference_snapshot_ids={"daily": REFERENCE_HASH},
    )


def _publish(
    root: Path,
    *,
    strategy_id: str = "n_shape",
    strategy_version: str = "v1",
    codes: tuple[str, ...] = ("000001.SZ",),
    sequence: int = 0,
    captured_at: datetime | None = None,
    producer_commit: str = COMMIT,
    trade_date: date = TRADE_DATE,
    rows: tuple[StrategyCandidateRecord, ...] | None = None,
) -> StrategyCandidateSnapshot:
    resolved_captured_at = captured_at or AS_OF - timedelta(minutes=2)
    resolved_rows = (
        rows
        if rows is not None
        else tuple(
            _row(
                code,
                strategy_id=strategy_id,
                strategy_version=strategy_version,
                decision_at=datetime.combine(
                    trade_date,
                    resolved_captured_at.timetz(),
                )
                - timedelta(minutes=2),
            )
            for code in codes
        )
    )
    snapshot = StrategyCandidateSnapshot.build(
        sequence=sequence,
        trade_date=trade_date,
        captured_at=resolved_captured_at,
        producer_commit=producer_commit,
        rows=resolved_rows,
    )
    StrategyCandidateSnapshotSpool(root.resolve()).publish(snapshot)
    return snapshot


def _authority(
    root: Path,
    *,
    strategy_id: str = "n_shape",
    strategy_version: str = "v1",
    required: bool = True,
    max_age_seconds: int = 600,
) -> CandidateUniverseAuthority:
    return CandidateUniverseAuthority(
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        snapshot_root=root,
        required=required,
        max_age_seconds=max_age_seconds,
    )


def _loader(*authorities: CandidateUniverseAuthority) -> RuntimeCandidateUniverseLoader:
    return RuntimeCandidateUniverseLoader(
        RuntimeCandidateUniverseConfig(
            expected_commit=COMMIT,
            authorities=authorities,
        )
    )


def _tree_state(root: Path) -> tuple[tuple[str, int, int, int, str], ...]:
    return tuple(
        (
            str(path.relative_to(root)),
            path.lstat().st_mode,
            path.lstat().st_mtime_ns,
            path.lstat().st_size,
            canonical_sha256(path.read_bytes().hex())
            if path.is_file() and not path.is_symlink()
            else "",
        )
        for path in sorted(root.rglob("*"))
    )


def test_cross_layer_models_are_frozen_runtime_contracts(tmp_path: Path) -> None:
    models = (
        CandidateUniverseAuthority,
        CandidateUniverseAuthorityEvidence,
        CandidateUniverseHitEvidence,
        CandidateUniverseCodeEvidence,
        CandidateUniverseDegradedAuthority,
        RuntimeCandidateUniverseConfig,
        RuntimeCandidateUniverseResult,
    )

    assert all(issubclass(model, RuntimeContractModel) for model in models)
    authority = _authority(tmp_path / "n")
    with pytest.raises(ValidationError):
        authority.required = False  # type: ignore[misc]


@pytest.mark.parametrize(
    ("root", "message"),
    [
        (Path("relative/spool"), "absolute"),
        (Path("/tmp/rquant/a/../b"), "normalized"),
    ],
)
def test_authority_rejects_relative_and_non_normalized_roots(
    root: Path,
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        CandidateUniverseAuthority(
            strategy_id="n_shape",
            strategy_version="v1",
            snapshot_root=root,
            required=True,
            max_age_seconds=60,
        )


def test_config_rejects_duplicate_strategy_authority(tmp_path: Path) -> None:
    first = _authority(tmp_path / "one")
    duplicate = _authority(tmp_path / "two")

    with pytest.raises(ValidationError, match="duplicate"):
        RuntimeCandidateUniverseConfig(
            expected_commit=COMMIT,
            authorities=(first, duplicate),
        )


def test_load_unions_codes_and_preserves_all_authority_evidence(tmp_path: Path) -> None:
    n_root = tmp_path / "n"
    growth_root = tmp_path / "growth"
    n_snapshot = _publish(n_root, codes=("000001.SZ", "600000.SH"))
    growth_snapshot = _publish(
        growth_root,
        strategy_id="growth_board_surge",
        strategy_version="v2",
        codes=("000001.SZ", "300001.SZ"),
    )
    loader = _loader(
        _authority(n_root),
        _authority(
            growth_root,
            strategy_id="growth_board_surge",
            strategy_version="v2",
        ),
    )

    result = loader.load(as_of=AS_OF, required_trade_date=TRADE_DATE)

    assert result.codes == ("000001.SZ", "300001.SZ", "600000.SH")
    assert result.degraded_optional_authorities == ()
    assert tuple(
        (item.strategy_id, item.strategy_version, item.generation_sha256, item.row_count)
        for item in result.authorities
    ) == (
        ("growth_board_surge", "v2", growth_snapshot.content_sha256, 2),
        ("n_shape", "v1", n_snapshot.content_sha256, 2),
    )
    repeated = next(item for item in result.code_evidence if item.code == "000001.SZ")
    assert tuple((hit.strategy_id, hit.strategy_version) for hit in repeated.hits) == (
        ("growth_board_surge", "v2"),
        ("n_shape", "v1"),
    )
    assert result.content_fingerprint == canonical_sha256(
        result.model_dump(mode="python", exclude={"content_fingerprint"})
    )


def test_optional_missing_is_degraded_but_required_missing_fails(tmp_path: Path) -> None:
    required_root = tmp_path / "required"
    optional_root = tmp_path / "optional-missing"
    _publish(required_root)
    loader = _loader(
        _authority(required_root),
        _authority(
            optional_root,
            strategy_id="auction_gap",
            strategy_version="v3",
            required=False,
        ),
    )

    result = loader.load(as_of=AS_OF, required_trade_date=TRADE_DATE)

    assert result.codes == ("000001.SZ",)
    assert result.degraded_optional_authorities == (
        CandidateUniverseDegradedAuthority(
            strategy_id="auction_gap",
            strategy_version="v3",
            reason="missing",
        ),
    )
    assert not optional_root.exists()

    with pytest.raises(RuntimeCandidateUniverseIntegrityError, match="required.*missing"):
        _loader(_authority(tmp_path / "required-missing")).load(
            as_of=AS_OF,
            required_trade_date=TRADE_DATE,
        )


def test_optional_future_generation_is_degraded_as_not_visible(tmp_path: Path) -> None:
    required_root = tmp_path / "required"
    optional_root = tmp_path / "optional-future"
    _publish(required_root)
    _publish(
        optional_root,
        strategy_id="auction_gap",
        strategy_version="v3",
        captured_at=AS_OF + timedelta(minutes=5),
        rows=(
            _row(
                "600000.SH",
                strategy_id="auction_gap",
                strategy_version="v3",
                decision_at=AS_OF + timedelta(minutes=1),
                available_at=AS_OF + timedelta(minutes=2),
            ),
        ),
    )

    result = _loader(
        _authority(required_root),
        _authority(
            optional_root,
            strategy_id="auction_gap",
            strategy_version="v3",
            required=False,
        ),
    ).load(as_of=AS_OF, required_trade_date=TRADE_DATE)

    assert result.degraded_optional_authorities[0].reason == "not_visible"


def test_optional_corruption_is_never_skipped(tmp_path: Path) -> None:
    required_root = tmp_path / "required"
    damaged_root = tmp_path / "damaged"
    _publish(required_root)
    damaged = _publish(
        damaged_root,
        strategy_id="auction_gap",
        strategy_version="v3",
    )
    generation = damaged_root / "generations" / f"{damaged.content_sha256}.json"
    payload = json.loads(generation.read_text())
    payload["rows"][0]["variant"] = "forged"
    generation.write_text(json.dumps(payload, separators=(",", ":"), sort_keys=True))
    os.chmod(generation, 0o600)
    loader = _loader(
        _authority(required_root),
        _authority(
            damaged_root,
            strategy_id="auction_gap",
            strategy_version="v3",
            required=False,
        ),
    )

    with pytest.raises(RuntimeCandidateUniverseIntegrityError, match="auction_gap"):
        loader.load(as_of=AS_OF, required_trade_date=TRADE_DATE)


@pytest.mark.parametrize(
    ("case", "message"),
    [
        ("commit", "commit"),
        ("identity", "identity"),
        ("trade_date", "trade date"),
        ("freshness", "stale"),
        ("code", "code"),
    ],
)
def test_snapshot_contract_mismatches_fail_closed(
    tmp_path: Path,
    case: str,
    message: str,
) -> None:
    root = tmp_path / case
    authority = _authority(root, max_age_seconds=60 if case == "freshness" else 600)
    if case == "commit":
        _publish(root, producer_commit=OTHER_COMMIT)
    elif case == "identity":
        _publish(
            root,
            rows=(_row("000001.SZ", strategy_id="wrong", strategy_version="v1"),),
        )
    elif case == "trade_date":
        prior = TRADE_DATE - timedelta(days=1)
        captured_at = AS_OF - timedelta(days=1)
        _publish(root, trade_date=prior, captured_at=captured_at)
    elif case == "freshness":
        _publish(root, captured_at=AS_OF - timedelta(minutes=10))
    else:
        _publish(root, codes=("NOT-A-CODE",))

    with pytest.raises(RuntimeCandidateUniverseIntegrityError, match=message):
        _loader(authority).load(as_of=AS_OF, required_trade_date=TRADE_DATE)


def test_required_future_generation_is_not_visible(tmp_path: Path) -> None:
    root = tmp_path / "future"
    future = AS_OF + timedelta(minutes=5)
    _publish(
        root,
        captured_at=future,
        rows=(
            _row(
                "000001.SZ",
                decision_at=AS_OF + timedelta(minutes=1),
                available_at=AS_OF + timedelta(minutes=2),
            ),
        ),
    )

    with pytest.raises(RuntimeCandidateUniverseIntegrityError, match="visible"):
        _loader(_authority(root)).load(as_of=AS_OF, required_trade_date=TRADE_DATE)


def test_empty_universe_fails_closed(tmp_path: Path) -> None:
    root = tmp_path / "empty"
    _publish(root, rows=())

    with pytest.raises(RuntimeCandidateUniverseIntegrityError, match="empty"):
        _loader(_authority(root)).load(as_of=AS_OF, required_trade_date=TRADE_DATE)


def test_symlink_authority_fails_closed(tmp_path: Path) -> None:
    real_root = tmp_path / "real"
    _publish(real_root)
    linked_root = tmp_path / "linked"
    linked_root.symlink_to(real_root, target_is_directory=True)

    with pytest.raises(RuntimeCandidateUniverseIntegrityError, match="symlink"):
        _loader(_authority(linked_root)).load(
            as_of=AS_OF,
            required_trade_date=TRADE_DATE,
        )


def test_optional_missing_below_symlink_ancestor_is_corruption(tmp_path: Path) -> None:
    required_root = tmp_path / "required"
    _publish(required_root)
    real_parent = tmp_path / "real-parent"
    real_parent.mkdir()
    linked_parent = tmp_path / "linked-parent"
    linked_parent.symlink_to(real_parent, target_is_directory=True)
    loader = _loader(
        _authority(required_root),
        _authority(
            linked_parent / "missing",
            strategy_id="auction_gap",
            strategy_version="v3",
            required=False,
        ),
    )

    with pytest.raises(RuntimeCandidateUniverseIntegrityError, match="symlink"):
        loader.load(as_of=AS_OF, required_trade_date=TRADE_DATE)


def test_each_load_resolves_current_generation_again(tmp_path: Path) -> None:
    root = tmp_path / "dynamic"
    _publish(root)
    loader = _loader(_authority(root))

    first = loader.load(as_of=AS_OF, required_trade_date=TRADE_DATE)
    _publish(
        root,
        sequence=1,
        captured_at=AS_OF + timedelta(minutes=1),
        codes=("000001.SZ", "600000.SH"),
    )
    second = loader.load(
        as_of=AS_OF + timedelta(minutes=2),
        required_trade_date=TRADE_DATE,
    )

    assert first.codes == ("000001.SZ",)
    assert second.codes == ("000001.SZ", "600000.SH")
    assert first.authorities[0].generation_sha256 != second.authorities[0].generation_sha256


def test_complete_reader_lifecycle_does_not_write(tmp_path: Path) -> None:
    root = tmp_path / "readonly"
    _publish(root)
    before = _tree_state(tmp_path)

    loader = _loader(_authority(root))
    result = loader.load(as_of=AS_OF, required_trade_date=TRADE_DATE)

    assert result.codes == ("000001.SZ",)
    assert _tree_state(tmp_path) == before


def test_result_rejects_internally_inconsistent_hashed_evidence(tmp_path: Path) -> None:
    root = tmp_path / "authority"
    _publish(root)
    result = _loader(_authority(root)).load(
        as_of=AS_OF,
        required_trade_date=TRADE_DATE,
    )
    payload = result.model_dump(mode="python")
    payload["authorities"][0]["row_count"] = 99
    payload["content_fingerprint"] = canonical_sha256(
        {key: value for key, value in payload.items() if key != "content_fingerprint"}
    )

    with pytest.raises(ValidationError, match="row_count"):
        RuntimeCandidateUniverseResult.model_validate(payload)


@pytest.mark.parametrize("mutation", ["captured", "decision", "available", "capture_before_hit"])
def test_result_rejects_rehashed_future_pit_evidence(
    tmp_path: Path,
    mutation: str,
) -> None:
    root = tmp_path / mutation
    _publish(root)
    result = _loader(_authority(root)).load(
        as_of=AS_OF,
        required_trade_date=TRADE_DATE,
    )
    payload = result.model_dump(mode="python")
    if mutation == "captured":
        payload["authorities"][0]["captured_at"] = AS_OF + timedelta(seconds=1)
    elif mutation == "decision":
        payload["code_evidence"][0]["hits"][0]["decision_at"] = AS_OF + timedelta(seconds=1)
    elif mutation == "available":
        payload["code_evidence"][0]["hits"][0]["available_at"] = AS_OF + timedelta(seconds=1)
    else:
        available_at = payload["code_evidence"][0]["hits"][0]["available_at"]
        payload["authorities"][0]["captured_at"] = available_at - timedelta(seconds=1)
    payload["content_fingerprint"] = canonical_sha256(
        {key: value for key, value in payload.items() if key != "content_fingerprint"}
    )

    with pytest.raises(ValidationError, match="PIT|captured|available_at|authority capture"):
        RuntimeCandidateUniverseResult.model_validate(payload)
