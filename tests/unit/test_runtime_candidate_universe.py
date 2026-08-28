from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime, timedelta, timezone
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
    StrategyCandidateAuthorityBinding,
    StrategyCandidatePriceBasis,
    StrategyCandidateRecord,
    StrategyCandidateSnapshot,
    StrategyCandidateSnapshotSpool,
    candidate_occurrence_id,
    strategy_candidate_schema_fingerprint,
    strategy_candidate_snapshot_content_sha256,
)

COMMIT = "a" * 40
OTHER_COMMIT = "b" * 40
TRADE_DATE = date(2026, 7, 31)
AS_OF = datetime(2026, 7, 31, 1, 30, tzinfo=UTC)
REFERENCE_HASH = "1" * 64
DEFINITION_FINGERPRINT = "4" * 64
EXECUTABLE_FINGERPRINT = "5" * 64


def _static_feature_schema(*names: str) -> dict[str, dict[str, str]]:
    return {
        name: {
            "dtype": (
                "string"
                if "basis" in name or name == "pool"
                else "object"
                if name in {"levels", "nested"}
                else "number"
            ),
            "semantic": f"candidate static feature {name}",
        }
        for name in sorted(names)
    }


STATIC_FEATURE_SCHEMA = _static_feature_schema("score")
CANDIDATE_SCHEMA_FINGERPRINT = strategy_candidate_schema_fingerprint(
    strategy_id="n_shape",
    strategy_version="1",
    static_feature_schema=STATIC_FEATURE_SCHEMA,
)


def _row(
    code: str,
    *,
    strategy_id: str = "n_shape",
    strategy_version: str = "1",
    decision_at: datetime | None = None,
    available_at: datetime | None = None,
    effective_trade_date: date = TRADE_DATE,
    variant: str = "default",
    static_features: dict[str, object] | None = None,
) -> StrategyCandidateRecord:
    resolved_decision_at = decision_at or AS_OF - timedelta(minutes=5)
    return StrategyCandidateRecord(
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        candidate_id=code,
        variant=variant,
        decision_at=resolved_decision_at,
        available_at=available_at or resolved_decision_at + timedelta(minutes=1),
        effective_trade_date=effective_trade_date,
        reference_trade_date=resolved_decision_at.date() - timedelta(days=1),
        price_basis=StrategyCandidatePriceBasis.QFQ_PIT,
        static_features=static_features or {"score": 0.8},
        reference_snapshot_ids={"daily": REFERENCE_HASH},
    )


def _publish(
    root: Path,
    *,
    strategy_id: str = "n_shape",
    strategy_version: str = "1",
    codes: tuple[str, ...] = ("000001.SZ",),
    sequence: int = 0,
    captured_at: datetime | None = None,
    producer_commit: str = COMMIT,
    trade_date: date = TRADE_DATE,
    rows: tuple[StrategyCandidateRecord, ...] | None = None,
    definition_fingerprint: str = DEFINITION_FINGERPRINT,
    executable_fingerprint: str = EXECUTABLE_FINGERPRINT,
    candidate_schema_fingerprint: str | None = None,
    static_feature_schema: dict[str, dict[str, str]] | None = None,
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
                effective_trade_date=trade_date,
                decision_at=datetime.combine(
                    trade_date,
                    resolved_captured_at.timetz(),
                )
                - timedelta(minutes=2),
            )
            for code in codes
        )
    )
    schema = static_feature_schema or _static_feature_schema(
        *(resolved_rows[0].static_features if resolved_rows else ("score",))
    )
    fingerprint = candidate_schema_fingerprint or strategy_candidate_schema_fingerprint(
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        static_feature_schema=schema,
    )
    result = StrategyCandidateSnapshotSpool(root.resolve()).publish_strategy_records(
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        definition_fingerprint=definition_fingerprint,
        executable_fingerprint=executable_fingerprint,
        candidate_schema_fingerprint=fingerprint,
        static_feature_schema=schema,
        source_snapshot_ids={
            "candidate_input": canonical_sha256(
                {
                    "sequence": sequence,
                    "trade_date": trade_date,
                    "captured_at": resolved_captured_at,
                    "rows": resolved_rows,
                }
            )
        },
        trade_date=trade_date,
        captured_at=resolved_captured_at,
        producer_commit=producer_commit,
        rows=resolved_rows,
    )
    return result.snapshot


def _authority(
    root: Path,
    *,
    strategy_id: str = "n_shape",
    strategy_version: str = "1",
    required: bool = True,
    max_age_seconds: int = 600,
    definition_fingerprint: str = DEFINITION_FINGERPRINT,
    executable_fingerprint: str = EXECUTABLE_FINGERPRINT,
    candidate_schema_fingerprint: str | None = None,
    static_feature_names: tuple[str, ...] = ("score",),
    static_feature_schema: dict[str, dict[str, str]] | None = None,
) -> CandidateUniverseAuthority:
    schema = static_feature_schema or _static_feature_schema(*static_feature_names)
    fingerprint = candidate_schema_fingerprint or strategy_candidate_schema_fingerprint(
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        static_feature_schema=schema,
    )
    return CandidateUniverseAuthority(
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        snapshot_root=root,
        required=required,
        max_age_seconds=max_age_seconds,
        definition_fingerprint=definition_fingerprint,
        executable_fingerprint=executable_fingerprint,
        candidate_schema_fingerprint=fingerprint,
        static_feature_names=static_feature_names,
        static_feature_schema=schema,
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


def test_runtime_strategy_authority_requires_exact_semantic_fingerprints(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValidationError, match="fingerprint|required"):
        CandidateUniverseAuthority(
            strategy_id="n_shape",
            strategy_version="1",
            snapshot_root=(tmp_path / "legacy").resolve(),
            required=True,
            max_age_seconds=60,
        )

    authority = CandidateUniverseAuthority(
        strategy_id="n_shape",
        strategy_version="1",
        snapshot_root=(tmp_path / "bound").resolve(),
        required=True,
        max_age_seconds=60,
        definition_fingerprint=DEFINITION_FINGERPRINT,
        executable_fingerprint=EXECUTABLE_FINGERPRINT,
        candidate_schema_fingerprint=CANDIDATE_SCHEMA_FINGERPRINT,
        static_feature_names=("score",),
        static_feature_schema=STATIC_FEATURE_SCHEMA,
    )

    assert authority.executable_fingerprint == EXECUTABLE_FINGERPRINT


@pytest.mark.parametrize(
    "field",
    ["definition_fingerprint", "executable_fingerprint", "candidate_schema_fingerprint"],
)
def test_runtime_loader_rejects_any_semantic_fingerprint_mismatch(
    tmp_path: Path,
    field: str,
) -> None:
    root = (tmp_path / field).resolve()
    _publish(root)
    authority_payload = _authority(root).model_dump(mode="python")
    authority_payload[field] = "f" * 64

    with pytest.raises(
        (ValidationError, RuntimeCandidateUniverseIntegrityError),
        match="fingerprint|identity|schema",
    ):
        _loader(CandidateUniverseAuthority.model_validate(authority_payload)).load(
            as_of=AS_OF,
            required_trade_date=TRADE_DATE,
        )


def test_runtime_result_preserves_exact_strategy_semantic_fingerprints(
    tmp_path: Path,
) -> None:
    root = (tmp_path / "exact-lineage").resolve()
    snapshot = _publish(root, codes=("000001.SZ", "000002.SZ"))

    result = _loader(_authority(root)).load(
        as_of=AS_OF,
        required_trade_date=TRADE_DATE,
    )

    evidence = result.authorities[0]
    assert evidence.authority_binding_sha256 == snapshot.authority_binding.content_sha256
    assert evidence.definition_fingerprint == DEFINITION_FINGERPRINT
    assert evidence.executable_fingerprint == EXECUTABLE_FINGERPRINT
    assert evidence.candidate_schema_fingerprint == CANDIDATE_SCHEMA_FINGERPRINT


def test_legacy_runtime_authority_payload_requires_explicit_republish() -> None:
    with pytest.raises(ValidationError, match="legacy|republish"):
        CandidateUniverseAuthorityEvidence(
            strategy_id="n_shape",
            strategy_version="1",
            schema_version=2,
            generation_sha256="2" * 64,
            authority_binding_sha256="3" * 64,
            definition_fingerprint=DEFINITION_FINGERPRINT,
            executable_fingerprint=EXECUTABLE_FINGERPRINT,
            candidate_schema_fingerprint=CANDIDATE_SCHEMA_FINGERPRINT,
            static_feature_names=("score",),
            static_feature_schema=STATIC_FEATURE_SCHEMA,
            source_snapshot_ids={"candidate_input": "7" * 64},
            sequence=0,
            row_count=0,
            captured_at=AS_OF,
            codes=(),
        )


def test_schema_v3_authority_evidence_rejects_empty_source_snapshot_name() -> None:
    with pytest.raises(ValidationError, match="non-empty"):
        CandidateUniverseAuthorityEvidence(
            strategy_id="n_shape",
            strategy_version="1",
            schema_version=3,
            generation_sha256="2" * 64,
            authority_binding_sha256="3" * 64,
            definition_fingerprint=DEFINITION_FINGERPRINT,
            executable_fingerprint=EXECUTABLE_FINGERPRINT,
            candidate_schema_fingerprint=CANDIDATE_SCHEMA_FINGERPRINT,
            static_feature_names=("score",),
            static_feature_schema=STATIC_FEATURE_SCHEMA,
            source_snapshot_ids={"": "4" * 64},
            sequence=0,
            row_count=0,
            captured_at=AS_OF,
            codes=(),
        )


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
            strategy_version="1",
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
        strategy_version="2",
        codes=("000001.SZ", "300001.SZ"),
    )
    loader = _loader(
        _authority(n_root),
        _authority(
            growth_root,
            strategy_id="growth_board_surge",
            strategy_version="2",
        ),
    )

    result = loader.load(as_of=AS_OF, required_trade_date=TRADE_DATE)

    assert result.codes == ("000001.SZ", "300001.SZ", "600000.SH")
    assert result.degraded_optional_authorities == ()
    assert tuple(
        (item.strategy_id, item.strategy_version, item.generation_sha256, item.row_count)
        for item in result.authorities
    ) == (
        ("growth_board_surge", "2", growth_snapshot.content_sha256, 2),
        ("n_shape", "1", n_snapshot.content_sha256, 2),
    )
    repeated = next(item for item in result.code_evidence if item.code == "000001.SZ")
    assert tuple((hit.strategy_id, hit.strategy_version) for hit in repeated.hits) == (
        ("growth_board_surge", "2"),
        ("n_shape", "1"),
    )
    assert result.content_fingerprint == canonical_sha256(
        result.model_dump(mode="python", exclude={"content_fingerprint"})
    )


def test_runtime_loader_rejects_bound_empty_authority_under_wrong_strategy(
    tmp_path: Path,
) -> None:
    root = (tmp_path / "bound-empty").resolve()
    StrategyCandidateSnapshotSpool(root).publish_strategy_records(
        strategy_id="n_shape",
        strategy_version="1",
        definition_fingerprint=DEFINITION_FINGERPRINT,
        executable_fingerprint=EXECUTABLE_FINGERPRINT,
        candidate_schema_fingerprint=CANDIDATE_SCHEMA_FINGERPRINT,
        static_feature_schema=STATIC_FEATURE_SCHEMA,
        source_snapshot_ids={"candidate_input": "3" * 64},
        trade_date=TRADE_DATE,
        captured_at=AS_OF - timedelta(minutes=2),
        producer_commit=COMMIT,
        rows=(),
    )

    with pytest.raises(RuntimeCandidateUniverseIntegrityError, match="identity|bound"):
        _loader(
            _authority(
                root,
                strategy_id="growth_board_surge",
                strategy_version="1",
            )
        ).load(as_of=AS_OF, required_trade_date=TRADE_DATE)


def test_runtime_loader_rejects_candidate_static_semantic_binding_mismatch(
    tmp_path: Path,
) -> None:
    root = (tmp_path / "static-binding").resolve()
    definition_fingerprint = "4" * 64
    static_feature_schema = _static_feature_schema("candidate_price_basis")
    candidate_schema_fingerprint = strategy_candidate_schema_fingerprint(
        strategy_id="n_shape",
        strategy_version="1",
        static_feature_schema=static_feature_schema,
    )
    _publish(
        root,
        strategy_version="1",
        rows=(
            _row(
                "000001.SZ",
                strategy_version="1",
                static_features={"candidate_price_basis": "raw_session"},
            ),
        ),
        definition_fingerprint=definition_fingerprint,
        candidate_schema_fingerprint=candidate_schema_fingerprint,
        static_feature_schema=static_feature_schema,
    )

    authority = CandidateUniverseAuthority(
        strategy_id="n_shape",
        strategy_version="1",
        snapshot_root=root,
        required=True,
        max_age_seconds=600,
        definition_fingerprint="6" * 64,
        executable_fingerprint=EXECUTABLE_FINGERPRINT,
        candidate_schema_fingerprint=candidate_schema_fingerprint,
        static_feature_names=("candidate_price_basis",),
        static_feature_schema=static_feature_schema,
    )
    with pytest.raises(
        RuntimeCandidateUniverseIntegrityError, match="identity|definition fingerprint"
    ):
        _loader(authority).load(as_of=AS_OF, required_trade_date=TRADE_DATE)


def test_runtime_loader_rejects_candidate_static_feature_shape_mismatch(
    tmp_path: Path,
) -> None:
    root = (tmp_path / "static-shape").resolve()
    definition_fingerprint = "4" * 64
    published_schema = _static_feature_schema("wrong")
    candidate_schema_fingerprint = strategy_candidate_schema_fingerprint(
        strategy_id="n_shape",
        strategy_version="1",
        static_feature_schema=published_schema,
    )
    _publish(
        root,
        strategy_version="1",
        rows=(_row("000001.SZ", strategy_version="1", static_features={"wrong": 1}),),
        definition_fingerprint=definition_fingerprint,
        executable_fingerprint=EXECUTABLE_FINGERPRINT,
        candidate_schema_fingerprint=candidate_schema_fingerprint,
        static_feature_schema=published_schema,
    )

    authority = CandidateUniverseAuthority(
        strategy_id="n_shape",
        strategy_version="1",
        snapshot_root=root,
        required=True,
        max_age_seconds=600,
        definition_fingerprint=definition_fingerprint,
        executable_fingerprint=EXECUTABLE_FINGERPRINT,
        candidate_schema_fingerprint=strategy_candidate_schema_fingerprint(
            strategy_id="n_shape",
            strategy_version="1",
            static_feature_schema=_static_feature_schema("candidate_price_basis"),
        ),
        static_feature_names=("candidate_price_basis",),
        static_feature_schema=_static_feature_schema("candidate_price_basis"),
    )
    with pytest.raises(
        RuntimeCandidateUniverseIntegrityError, match="identity|static feature schema"
    ):
        _loader(authority).load(as_of=AS_OF, required_trade_date=TRADE_DATE)


def test_load_accepts_prior_day_decision_and_preserves_immutable_pit_features(
    tmp_path: Path,
) -> None:
    root = tmp_path / "next-session"
    features = {"score": 0.91, "levels": {"support": [10.1, 10.2]}}
    cst = timezone(timedelta(hours=8))
    decision_at = datetime(2026, 7, 30, 17, 0, tzinfo=cst)
    available_at = datetime(2026, 7, 30, 17, 1, tzinfo=cst)
    captured_at = datetime(2026, 7, 30, 17, 2, tzinfo=cst)
    next_open = datetime(2026, 7, 31, 9, 30, tzinfo=cst)
    row = _row(
        "000001.SZ",
        decision_at=decision_at,
        available_at=available_at,
        effective_trade_date=TRADE_DATE,
        static_features=features,
    )
    snapshot = _publish(root, captured_at=captured_at, rows=(row,))
    features["score"] = 0.01
    features["levels"]["support"].append(99.0)  # type: ignore[index, union-attr]

    result = _loader(
        _authority(
            root,
            max_age_seconds=24 * 60 * 60,
            static_feature_names=("levels", "score"),
        )
    ).load(
        as_of=next_open,
        required_trade_date=TRADE_DATE,
    )
    hit = result.code_evidence[0].hits[0]

    assert snapshot.captured_at == datetime(2026, 7, 30, 9, 2, tzinfo=UTC)
    assert result.as_of == AS_OF
    assert hit.decision_at.date() == TRADE_DATE - timedelta(days=1)
    assert hit.effective_trade_date == TRADE_DATE
    assert hit.occurrence_id == row.occurrence_id
    assert hit.static_features == {
        "levels": {"support": (10.1, 10.2)},
        "score": 0.91,
    }
    assert dict(hit.reference_snapshot_ids) == {"daily": REFERENCE_HASH}
    with pytest.raises(TypeError):
        hit.static_features["score"] = 0.5  # type: ignore[index]
    with pytest.raises(TypeError):
        hit.static_features["levels"]["support"] = ()  # type: ignore[index]
    with pytest.raises(TypeError):
        hit.reference_snapshot_ids["daily"] = "2" * 64  # type: ignore[index]


@pytest.mark.parametrize("field", ["effective_trade_date", "occurrence_id"])
def test_result_rejects_rehashed_occurrence_identity_tampering(
    tmp_path: Path,
    field: str,
) -> None:
    root = tmp_path / field
    _publish(root)
    result = _loader(_authority(root)).load(
        as_of=AS_OF,
        required_trade_date=TRADE_DATE,
    )
    payload = result.model_dump(mode="python")
    hit = payload["code_evidence"][0]["hits"][0]
    if field == "effective_trade_date":
        hit[field] = TRADE_DATE + timedelta(days=1)
        hit["occurrence_id"] = candidate_occurrence_id(
            strategy_id=hit["strategy_id"],
            strategy_version=hit["strategy_version"],
            candidate_id=hit["candidate_id"],
            variant=hit["variant"],
            effective_trade_date=hit[field],
        )
    else:
        hit[field] = "f" * 64
    payload["content_fingerprint"] = canonical_sha256(
        {key: value for key, value in payload.items() if key != "content_fingerprint"}
    )

    with pytest.raises(ValidationError, match="effective|occurrence"):
        RuntimeCandidateUniverseResult.model_validate(payload)


def test_result_generation_hash_binds_static_features(tmp_path: Path) -> None:
    root = tmp_path / "features"
    _publish(root)
    result = _loader(_authority(root)).load(
        as_of=AS_OF,
        required_trade_date=TRADE_DATE,
    )
    payload = result.model_dump(mode="python")
    payload["code_evidence"][0]["hits"][0]["static_features"]["score"] = 0.99
    payload["content_fingerprint"] = canonical_sha256(
        {key: value for key, value in payload.items() if key != "content_fingerprint"}
    )

    with pytest.raises(ValidationError, match="generation"):
        RuntimeCandidateUniverseResult.model_validate(payload)


def test_runtime_result_rejects_rehashed_static_value_with_wrong_declared_dtype(
    tmp_path: Path,
) -> None:
    root = (tmp_path / "dtype-forgery").resolve()
    _publish(root, strategy_version="1")
    result = _loader(_authority(root, strategy_version="1")).load(
        as_of=AS_OF,
        required_trade_date=TRADE_DATE,
    )
    authority = result.authorities[0]
    hit = result.code_evidence[0].hits[0]
    forged_hit = hit.model_copy(update={"static_features": {"score": True}})
    forged_row = StrategyCandidateRecord(
        strategy_id=forged_hit.strategy_id,
        strategy_version=forged_hit.strategy_version,
        candidate_id=forged_hit.candidate_id,
        variant=forged_hit.variant,
        decision_at=forged_hit.decision_at,
        available_at=forged_hit.available_at,
        effective_trade_date=forged_hit.effective_trade_date,
        reference_trade_date=forged_hit.reference_trade_date,
        price_basis=forged_hit.price_basis,
        static_features=forged_hit.static_features,
        reference_snapshot_ids=forged_hit.reference_snapshot_ids,
    )
    binding = StrategyCandidateAuthorityBinding.create(
        strategy_id=authority.strategy_id,
        strategy_version=authority.strategy_version,
        definition_fingerprint=authority.definition_fingerprint,
        executable_fingerprint=authority.executable_fingerprint,
        candidate_schema_fingerprint=authority.candidate_schema_fingerprint,
        static_feature_schema=authority.static_feature_schema,
    )
    generation_sha256 = strategy_candidate_snapshot_content_sha256(
        schema_version=3,
        sequence=authority.sequence,
        trade_date=result.required_trade_date,
        captured_at=authority.captured_at,
        producer_commit=result.expected_commit,
        rows=(forged_row,),
        authority_binding=binding,
        source_snapshot_ids=authority.source_snapshot_ids,
    )
    forged_authority = authority.model_copy(update={"generation_sha256": generation_sha256})
    forged_hit = forged_hit.model_copy(update={"generation_sha256": generation_sha256})
    forged_code = result.code_evidence[0].model_copy(update={"hits": (forged_hit,)})

    with pytest.raises(ValueError, match="static feature.*dtype"):
        RuntimeCandidateUniverseResult.build(
            as_of=result.as_of,
            required_trade_date=result.required_trade_date,
            expected_commit=result.expected_commit,
            codes=result.codes,
            authorities=(forged_authority,),
            degraded_optional_authorities=(),
            code_evidence=(forged_code,),
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("reference_trade_date", TRADE_DATE - timedelta(days=2)),
        ("price_basis", StrategyCandidatePriceBasis.RAW),
        ("reference_snapshot_ids", {"daily": "2" * 64}),
    ],
)
def test_result_generation_hash_binds_remaining_row_contract(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    root = tmp_path / field
    _publish(root)
    result = _loader(_authority(root)).load(
        as_of=AS_OF,
        required_trade_date=TRADE_DATE,
    )
    payload = result.model_dump(mode="python")
    payload["code_evidence"][0]["hits"][0][field] = value
    payload["content_fingerprint"] = canonical_sha256(
        {key: item for key, item in payload.items() if key != "content_fingerprint"}
    )

    with pytest.raises(ValidationError, match="generation"):
        RuntimeCandidateUniverseResult.model_validate(payload)


def test_result_rejects_legacy_v1_generation_before_reconstruction() -> None:
    row = _row("000001.SZ")
    captured_at = AS_OF - timedelta(minutes=2)
    generation_sha256 = strategy_candidate_snapshot_content_sha256(
        schema_version=1,
        sequence=0,
        trade_date=TRADE_DATE,
        captured_at=captured_at,
        producer_commit=COMMIT,
        rows=(row,),
    )

    with pytest.raises(ValidationError, match="legacy|republish"):
        CandidateUniverseAuthorityEvidence(
            strategy_id=row.strategy_id,
            strategy_version=row.strategy_version,
            schema_version=1,
            generation_sha256=generation_sha256,
            authority_binding_sha256="3" * 64,
            definition_fingerprint=DEFINITION_FINGERPRINT,
            executable_fingerprint=EXECUTABLE_FINGERPRINT,
            candidate_schema_fingerprint=CANDIDATE_SCHEMA_FINGERPRINT,
            static_feature_names=("score",),
            static_feature_schema=STATIC_FEATURE_SCHEMA,
            source_snapshot_ids={"candidate_input": "7" * 64},
            sequence=0,
            row_count=1,
            captured_at=captured_at,
            codes=(row.candidate_id,),
        )


def test_candidate_hit_dates_use_asia_shanghai_calendar_day(tmp_path: Path) -> None:
    root = tmp_path / "shanghai-date"
    _publish(root)
    hit = (
        _loader(_authority(root))
        .load(
            as_of=AS_OF,
            required_trade_date=TRADE_DATE,
        )
        .code_evidence[0]
        .hits[0]
    )
    shanghai_midnight = datetime(
        2026,
        7,
        31,
        0,
        30,
        tzinfo=timezone(timedelta(hours=8)),
    )
    payload = hit.model_dump(mode="python")
    payload["decision_at"] = shanghai_midnight
    payload["available_at"] = shanghai_midnight
    payload["effective_trade_date"] = date(2026, 7, 30)
    payload["reference_trade_date"] = date(2026, 7, 30)
    payload["occurrence_id"] = candidate_occurrence_id(
        strategy_id=payload["strategy_id"],
        strategy_version=payload["strategy_version"],
        candidate_id=payload["candidate_id"],
        variant=payload["variant"],
        effective_trade_date=payload["effective_trade_date"],
    )
    with pytest.raises(ValidationError, match="effective_trade_date"):
        CandidateUniverseHitEvidence.model_validate(payload)

    payload["effective_trade_date"] = TRADE_DATE
    payload["reference_trade_date"] = TRADE_DATE
    payload["occurrence_id"] = candidate_occurrence_id(
        strategy_id=payload["strategy_id"],
        strategy_version=payload["strategy_version"],
        candidate_id=payload["candidate_id"],
        variant=payload["variant"],
        effective_trade_date=payload["effective_trade_date"],
    )
    assert CandidateUniverseHitEvidence.model_validate(payload).effective_trade_date == TRADE_DATE


def test_candidate_hit_legacy_schema_requires_explicit_republish() -> None:
    decision_at = datetime(2026, 7, 31, 16, 30, tzinfo=UTC)
    effective_trade_date = date(2026, 7, 31)
    occurrence_id = candidate_occurrence_id(
        strategy_id="n_shape",
        strategy_version="1",
        candidate_id="000001.SZ",
        variant="default",
        effective_trade_date=effective_trade_date,
    )
    payload = {
        "schema_version": 1,
        "strategy_id": "n_shape",
        "strategy_version": "1",
        "generation_sha256": "2" * 64,
        "candidate_id": "000001.SZ",
        "variant": "default",
        "decision_at": decision_at,
        "available_at": decision_at + timedelta(minutes=1),
        "effective_trade_date": effective_trade_date,
        "occurrence_id": occurrence_id,
        "static_features": {"score": 0.8},
        "reference_trade_date": effective_trade_date,
        "price_basis": StrategyCandidatePriceBasis.QFQ_PIT,
        "reference_snapshot_ids": {"daily": REFERENCE_HASH},
    }

    with pytest.raises(ValidationError, match="legacy|republish"):
        CandidateUniverseHitEvidence.model_validate(payload)


def test_candidate_hit_v1_and_v2_are_both_rejected_as_legacy() -> None:
    decision_at = datetime(2026, 7, 30, 9, 0, tzinfo=UTC)
    effective_trade_date = TRADE_DATE
    payload = {
        "schema_version": 1,
        "strategy_id": "n_shape",
        "strategy_version": "1",
        "generation_sha256": "2" * 64,
        "candidate_id": "000001.SZ",
        "variant": "default",
        "decision_at": decision_at,
        "available_at": decision_at + timedelta(minutes=1),
        "effective_trade_date": effective_trade_date,
        "occurrence_id": candidate_occurrence_id(
            strategy_id="n_shape",
            strategy_version="1",
            candidate_id="000001.SZ",
            variant="default",
            effective_trade_date=effective_trade_date,
        ),
        "static_features": {"score": 0.8},
        "reference_trade_date": date(2026, 7, 30),
        "price_basis": StrategyCandidatePriceBasis.QFQ_PIT,
        "reference_snapshot_ids": {"daily": REFERENCE_HASH},
    }

    for schema_version in (1, 2):
        payload["schema_version"] = schema_version
        with pytest.raises(ValidationError, match="legacy|republish"):
            CandidateUniverseHitEvidence.model_validate(payload)


def test_result_requires_hit_and_authority_schema_to_match(tmp_path: Path) -> None:
    root = tmp_path / "schema-mismatch"
    _publish(root)
    result = _loader(_authority(root)).load(
        as_of=AS_OF,
        required_trade_date=TRADE_DATE,
    )
    payload = result.model_dump(mode="python")
    payload["code_evidence"][0]["hits"][0]["schema_version"] = 1
    payload["content_fingerprint"] = canonical_sha256(
        {key: value for key, value in payload.items() if key != "content_fingerprint"}
    )

    with pytest.raises(ValidationError, match="schema"):
        RuntimeCandidateUniverseResult.model_validate(payload)


def test_optional_missing_is_degraded_but_required_missing_fails(tmp_path: Path) -> None:
    required_root = tmp_path / "required"
    optional_root = tmp_path / "optional-missing"
    _publish(required_root)
    loader = _loader(
        _authority(required_root),
        _authority(
            optional_root,
            strategy_id="auction_gap",
            strategy_version="3",
            required=False,
        ),
    )

    result = loader.load(as_of=AS_OF, required_trade_date=TRADE_DATE)

    assert result.codes == ("000001.SZ",)
    assert result.degraded_optional_authorities == (
        CandidateUniverseDegradedAuthority(
            strategy_id="auction_gap",
            strategy_version="3",
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
        strategy_version="3",
        captured_at=AS_OF + timedelta(minutes=5),
        rows=(
            _row(
                "600000.SH",
                strategy_id="auction_gap",
                strategy_version="3",
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
            strategy_version="3",
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
        strategy_version="3",
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
            strategy_version="3",
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
        _publish(root)
        authority = _authority(
            root,
            strategy_id="wrong",
            strategy_version="1",
            max_age_seconds=600,
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


def test_successful_required_authority_may_publish_empty_universe(tmp_path: Path) -> None:
    root = tmp_path / "empty"
    _publish(root, rows=())

    result = _loader(_authority(root)).load(
        as_of=AS_OF,
        required_trade_date=TRADE_DATE,
    )

    assert result.codes == ()
    assert result.code_evidence == ()
    assert len(result.authorities) == 1
    assert result.authorities[0].row_count == 0


def test_empty_universe_without_any_successful_authority_fails_closed(tmp_path: Path) -> None:
    missing = _authority(
        tmp_path / "missing",
        strategy_id="optional",
        strategy_version="1",
        required=False,
    )

    with pytest.raises(RuntimeCandidateUniverseIntegrityError, match="empty"):
        _loader(missing).load(as_of=AS_OF, required_trade_date=TRADE_DATE)


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
            strategy_version="3",
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
