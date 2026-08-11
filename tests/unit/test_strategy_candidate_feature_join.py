from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

from rquant.feature_contracts import (
    FeatureAvailability,
    FeatureBatchEnvelope,
    FeatureFieldStatus,
)
from rquant.runtime_candidate_universe import (
    CandidateUniverseAuthority,
    RuntimeCandidateUniverseConfig,
    RuntimeCandidateUniverseIntegrityError,
    RuntimeCandidateUniverseLoader,
    RuntimeCandidateUniverseResult,
)
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.strategy_candidate_feature_join import (
    StrategyCandidateFeatureBatch,
    StrategyCandidateFeatureJoinError,
    join_strategy_candidate_features,
)
from rquant.strategy_candidate_snapshot import (
    StrategyCandidatePriceBasis,
    StrategyCandidateRecord,
    StrategyCandidateSnapshot,
    StrategyCandidateSnapshotSpool,
    strategy_candidate_schema_fingerprint,
)
from rquant.strategy_runner import canonical_feature_payload

COMMIT = "a" * 40
TRADE_DATE = date(2026, 7, 31)
SHANGHAI = timezone(timedelta(hours=8))
EVENT_TIME = datetime(2026, 7, 31, 9, 31, tzinfo=SHANGHAI)
AVAILABLE_AT = EVENT_TIME + timedelta(seconds=2)
REFERENCE_HASH = "1" * 64
DEFINITION_FINGERPRINT = "4" * 64
EXECUTABLE_FINGERPRINT = "5" * 64


def _static_feature_schema(*names: str) -> dict[str, dict[str, str]]:
    return {
        name: {
            "dtype": (
                "string"
                if "basis" in name or name in {"pool", "candidate_occurrence_id"}
                else "object"
                if name in {"levels", "nested"}
                else "number"
            ),
            "semantic": f"candidate static feature {name}",
        }
        for name in sorted(names)
    }


def _payload_hash(frame: pd.DataFrame, *, schema_version: int = 3) -> str:
    return hashlib.sha256(
        canonical_feature_payload(frame, schema_version=schema_version)
    ).hexdigest()


def _common_frame(*codes: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": list(codes),
            "rel_same_minute": [2.0 + index for index in range(len(codes))],
            "vwap": [10.0 + index for index in range(len(codes))],
        }
    )


def _common_envelope(
    frame: pd.DataFrame,
    *,
    event_time: datetime = EVENT_TIME,
    available_at: datetime = AVAILABLE_AT,
    content_hash: str | None = None,
) -> FeatureBatchEnvelope:
    normalized_available_at = available_at.astimezone(UTC)
    return FeatureBatchEnvelope(
        schema_version=3,
        batch_id="common-minute-20260731-0931",
        contract_id="intraday-common-pit",
        contract_version=2,
        input_batch_ids=("minute-raw-1", "minute-history-7"),
        sequence=31,
        event_time=event_time,
        available_at=available_at,
        decision_cutoff=available_at,
        actual_delay_seconds=(available_at - event_time).total_seconds(),
        row_count=len(frame),
        content_hash=content_hash or _payload_hash(frame),
        field_statuses=(
            FeatureFieldStatus(
                name="rel_same_minute",
                status=FeatureAvailability.AVAILABLE,
                source_event_time=event_time,
                available_at=normalized_available_at,
                decision_cutoff=normalized_available_at,
                actual_delay_seconds=(normalized_available_at - event_time).total_seconds(),
            ),
            FeatureFieldStatus(
                name="vwap",
                status=FeatureAvailability.AVAILABLE,
                source_event_time=event_time,
                available_at=normalized_available_at,
                decision_cutoff=normalized_available_at,
                actual_delay_seconds=(normalized_available_at - event_time).total_seconds(),
            ),
        ),
        producer_commit=COMMIT,
    )


def _candidate(
    code: str,
    *,
    strategy_id: str,
    strategy_version: str,
    static_features: dict[str, object],
    variant: str = "default",
    decision_at: datetime | None = None,
    available_at: datetime | None = None,
) -> StrategyCandidateRecord:
    resolved_decision_at = decision_at or datetime(
        2026,
        7,
        30,
        17,
        0,
        tzinfo=SHANGHAI,
    )
    return StrategyCandidateRecord(
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        candidate_id=code,
        variant=variant,
        decision_at=resolved_decision_at,
        available_at=available_at or resolved_decision_at + timedelta(minutes=1),
        effective_trade_date=TRADE_DATE,
        reference_trade_date=date(2026, 7, 30),
        price_basis=StrategyCandidatePriceBasis.QFQ_PIT,
        static_features=static_features,
        reference_snapshot_ids={"daily": REFERENCE_HASH},
    )


def _publish(
    root: Path,
    *,
    strategy_id: str,
    strategy_version: str,
    rows: tuple[StrategyCandidateRecord, ...],
    sequence: int = 0,
    captured_at: datetime | None = None,
    static_feature_schema: dict[str, dict[str, str]] | None = None,
) -> StrategyCandidateSnapshot:
    schema = static_feature_schema or _static_feature_schema(
        *(rows[0].static_features if rows else ("n_score",))
    )
    candidate_schema_fingerprint = strategy_candidate_schema_fingerprint(
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        static_feature_schema=schema,
    )
    result = StrategyCandidateSnapshotSpool(root.resolve()).publish_strategy_records(
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        definition_fingerprint=DEFINITION_FINGERPRINT,
        executable_fingerprint=EXECUTABLE_FINGERPRINT,
        candidate_schema_fingerprint=candidate_schema_fingerprint,
        static_feature_schema=schema,
        source_snapshot_ids={
            "candidate_input": canonical_sha256(
                {
                    "strategy_id": strategy_id,
                    "strategy_version": strategy_version,
                    "sequence": sequence,
                    "captured_at": captured_at or AVAILABLE_AT - timedelta(seconds=1),
                }
            )
        },
        trade_date=TRADE_DATE,
        captured_at=captured_at or AVAILABLE_AT - timedelta(seconds=1),
        producer_commit=COMMIT,
        rows=rows,
    )
    assert all(
        (row.strategy_id, row.strategy_version) == (strategy_id, strategy_version) for row in rows
    )
    return result.snapshot


def _universe(
    tmp_path: Path,
    authorities: tuple[
        tuple[str, str, tuple[StrategyCandidateRecord, ...], int, datetime | None], ...
    ],
    *,
    as_of: datetime = AVAILABLE_AT,
) -> tuple[RuntimeCandidateUniverseResult, dict[tuple[str, str], StrategyCandidateSnapshot]]:
    configs: list[CandidateUniverseAuthority] = []
    snapshots: dict[tuple[str, str], StrategyCandidateSnapshot] = {}
    for index, (strategy_id, strategy_version, rows, sequence, captured_at) in enumerate(
        authorities
    ):
        root = tmp_path / f"authority-{index}"
        snapshot = _publish(
            root,
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            rows=rows,
            sequence=sequence,
            captured_at=captured_at,
        )
        snapshots[(strategy_id, strategy_version)] = snapshot
        configs.append(
            CandidateUniverseAuthority(
                strategy_id=strategy_id,
                strategy_version=strategy_version,
                snapshot_root=root,
                required=True,
                max_age_seconds=86_400,
                definition_fingerprint=DEFINITION_FINGERPRINT,
                executable_fingerprint=EXECUTABLE_FINGERPRINT,
                candidate_schema_fingerprint=(
                    snapshot.authority_binding.candidate_schema_fingerprint
                    if snapshot.authority_binding is not None
                    else ""
                ),
                static_feature_names=(
                    snapshot.authority_binding.static_feature_names
                    if snapshot.authority_binding is not None
                    else ()
                ),
                static_feature_schema=(
                    snapshot.authority_binding.static_feature_schema
                    if snapshot.authority_binding is not None
                    else {}
                ),
            )
        )
    result = RuntimeCandidateUniverseLoader(
        RuntimeCandidateUniverseConfig(
            expected_commit=COMMIT,
            authorities=tuple(configs),
        )
    ).load(as_of=as_of, required_trade_date=TRADE_DATE)
    return result, snapshots


def _join(
    frame: pd.DataFrame,
    universe: RuntimeCandidateUniverseResult,
    *,
    strategy_id: str = "n_shape",
    strategy_version: str = "1",
    envelope: FeatureBatchEnvelope | None = None,
) -> StrategyCandidateFeatureBatch:
    return join_strategy_candidate_features(
        envelope or _common_envelope(frame),
        frame,
        universe,
        strategy_id,
        strategy_version,
    )


def test_cross_layer_output_is_frozen_and_selects_only_requested_strategy(
    tmp_path: Path,
) -> None:
    n_row = _candidate(
        "000001.SZ",
        strategy_id="n_shape",
        strategy_version="1",
        static_features={"n_score": 0.91},
    )
    growth_row = _candidate(
        "300001.SZ",
        strategy_id="growth_board_surge",
        strategy_version="1",
        static_features={"surge_score": 0.88},
    )
    universe, snapshots = _universe(
        tmp_path,
        (
            ("n_shape", "1", (n_row,), 0, None),
            ("growth_board_surge", "1", (growth_row,), 0, None),
        ),
    )
    common = _common_frame("300001.SZ", "000001.SZ", "600000.SH")

    result = _join(common, universe)

    assert isinstance(result, RuntimeContractModel)
    assert result.envelope.row_count == 1
    assert result.candidate_authority.model_dump(mode="json") == {
        "strategy_id": "n_shape",
        "strategy_version": "1",
        "schema_version": 3,
        "generation_sha256": snapshots[("n_shape", "1")].content_sha256,
        "authority_binding_sha256": snapshots[("n_shape", "1")].authority_binding.content_sha256,
        "definition_fingerprint": DEFINITION_FINGERPRINT,
        "executable_fingerprint": EXECUTABLE_FINGERPRINT,
        "candidate_schema_fingerprint": (
            snapshots[("n_shape", "1")].authority_binding.candidate_schema_fingerprint
        ),
        "static_feature_names": ["n_score"],
        "static_feature_schema": _static_feature_schema("n_score"),
        "captured_at": snapshots[("n_shape", "1")].captured_at.isoformat().replace("+00:00", "Z"),
    }
    assert result.static_feature_names == ("n_score",)
    assert result.common_batch_id in result.envelope.input_batch_ids
    assert result.candidate_authority.input_id in result.envelope.input_batch_ids
    assert set(json.loads(result.payload_json)) == {
        "candidate_authority",
        "columns",
        "common_batch_id",
        "rows",
        "schema_version",
        "static_feature_names",
    }
    assert result.frame.to_dict(orient="records") == [
        {
            "candidate_effective_trade_date": "2026-07-31",
            "candidate_generation_sha256": snapshots[("n_shape", "1")].content_sha256,
            "candidate_occurrence_id": n_row.occurrence_id,
            "candidate_snapshot_schema_version": 3,
            "candidate_variant": "default",
            "n_score": 0.91,
            "rel_same_minute": 3.0,
            "ts_code": "000001.SZ",
            "vwap": 11.0,
        }
    ]
    assert "surge_score" not in result.frame.columns
    with pytest.raises(ValidationError):
        result.envelope = result.envelope  # type: ignore[misc]


def test_prior_day_n_shape_candidate_joins_on_effective_trade_date(tmp_path: Path) -> None:
    prior_day_decision = datetime(2026, 7, 30, 17, 0, tzinfo=SHANGHAI)
    row = _candidate(
        "000001.SZ",
        strategy_id="n_shape",
        strategy_version="1",
        decision_at=prior_day_decision,
        static_features={"t_close": 10.2},
    )
    universe, _ = _universe(
        tmp_path,
        (("n_shape", "1", (row,), 0, None),),
    )

    result = _join(_common_frame("000001.SZ"), universe)

    assert row.decision_at.astimezone(SHANGHAI).date() == date(2026, 7, 30)
    assert result.frame.loc[0, "candidate_effective_trade_date"] == "2026-07-31"
    assert result.frame.loc[0, "t_close"] == 10.2


def test_candidate_generation_changes_lineage_and_batch_id(tmp_path: Path) -> None:
    row = _candidate(
        "000001.SZ",
        strategy_id="n_shape",
        strategy_version="1",
        static_features={"n_score": 0.91},
    )
    common = _common_frame("000001.SZ")
    first_universe, _ = _universe(
        tmp_path / "first",
        (("n_shape", "1", (row,), 0, AVAILABLE_AT - timedelta(seconds=2)),),
    )
    second_universe, _ = _universe(
        tmp_path / "second",
        (("n_shape", "1", (row,), 0, AVAILABLE_AT - timedelta(seconds=1)),),
    )

    first = _join(common, first_universe)
    second = _join(common, second_universe)

    assert first.envelope.content_hash != second.envelope.content_hash
    assert first.envelope.input_fingerprint != second.envelope.input_fingerprint
    assert first.envelope.batch_id != second.envelope.batch_id


def test_rejects_universe_resolved_after_common_batch_available_at(tmp_path: Path) -> None:
    row = _candidate(
        "000001.SZ",
        strategy_id="n_shape",
        strategy_version="1",
        static_features={"n_score": 0.91},
    )
    later = AVAILABLE_AT + timedelta(microseconds=1)
    universe, _ = _universe(
        tmp_path,
        (("n_shape", "1", (row,), 0, None),),
        as_of=later,
    )

    with pytest.raises(StrategyCandidateFeatureJoinError, match="as_of"):
        _join(_common_frame("000001.SZ"), universe)


def test_revalidates_model_copy_universe_before_using_static_features(tmp_path: Path) -> None:
    row = _candidate(
        "000001.SZ",
        strategy_id="n_shape",
        strategy_version="1",
        static_features={"n_score": 0.91},
    )
    universe, _ = _universe(
        tmp_path,
        (("n_shape", "1", (row,), 0, None),),
    )
    hit = universe.code_evidence[0].hits[0]
    forged_hit = hit.model_copy(update={"static_features": {"n_score": 999.0}})
    forged_code = universe.code_evidence[0].model_copy(update={"hits": (forged_hit,)})
    forged_universe = universe.model_copy(update={"code_evidence": (forged_code,)})

    with pytest.raises(StrategyCandidateFeatureJoinError, match="revalidation"):
        _join(_common_frame("000001.SZ"), forged_universe)


def test_revalidates_model_copy_common_envelope_before_join(tmp_path: Path) -> None:
    row = _candidate(
        "000001.SZ",
        strategy_id="n_shape",
        strategy_version="1",
        static_features={"n_score": 0.91},
    )
    universe, _ = _universe(
        tmp_path,
        (("n_shape", "1", (row,), 0, None),),
    )
    common = _common_frame("000001.SZ")
    envelope = _common_envelope(common).model_copy(
        update={"input_batch_ids": ("duplicate", "duplicate")}
    )

    with pytest.raises(StrategyCandidateFeatureJoinError, match="revalidation"):
        _join(common, universe, envelope=envelope)


@pytest.mark.parametrize(
    ("first", "second", "message"),
    [
        ({"score": 0.8}, {"score": 0.8, "rank": 2}, "key set"),
        ({"score": 0.8}, {"score": 0.7}, "revalidation"),
        ({"vwap": 10.2}, {"vwap": 10.3}, "collides"),
        (
            {"candidate_occurrence_id": "forged"},
            {"candidate_occurrence_id": "forged"},
            "reserved",
        ),
    ],
)
def test_rejects_unsafe_static_feature_shapes(
    tmp_path: Path,
    first: dict[str, object],
    second: dict[str, object],
    message: str,
) -> None:
    first_row = _candidate(
        "000001.SZ",
        strategy_id="n_shape",
        strategy_version="1",
        static_features=first,
    )
    second_row = _candidate(
        "000002.SZ",
        strategy_id="n_shape",
        strategy_version="1",
        static_features=second,
    )
    if tuple(sorted(first)) != tuple(sorted(second)):
        with pytest.raises(
            (ValueError, ValidationError, RuntimeCandidateUniverseIntegrityError),
            match="static feature (?:names|schema)|row static features",
        ):
            _universe(
                tmp_path,
                (("n_shape", "1", (first_row, second_row), 0, None),),
            )
        return
    universe, _ = _universe(
        tmp_path,
        (("n_shape", "1", (first_row, second_row), 0, None),),
    )
    if message == "revalidation":
        hit = universe.code_evidence[1].hits[0]
        forged_hit = hit.model_copy(update={"static_features": {"score": float("nan")}})
        forged_code = universe.code_evidence[1].model_copy(update={"hits": (forged_hit,)})
        universe = universe.model_copy(
            update={"code_evidence": (universe.code_evidence[0], forged_code)}
        )

    with pytest.raises(StrategyCandidateFeatureJoinError, match=message):
        _join(_common_frame("000001.SZ", "000002.SZ"), universe)


def test_rejects_common_payload_mismatch_duplicate_codes_and_missing_authority(
    tmp_path: Path,
) -> None:
    row = _candidate(
        "000001.SZ",
        strategy_id="n_shape",
        strategy_version="1",
        static_features={"n_score": 0.91},
    )
    universe, _ = _universe(
        tmp_path,
        (("n_shape", "1", (row,), 0, None),),
    )
    common = _common_frame("000001.SZ")

    with pytest.raises(StrategyCandidateFeatureJoinError, match="content_hash"):
        _join(
            common,
            universe,
            envelope=_common_envelope(common, content_hash="f" * 64),
        )
    with pytest.raises(StrategyCandidateFeatureJoinError, match="unique"):
        _join(
            pd.concat([common, common], ignore_index=True),
            universe,
            envelope=_common_envelope(common),
        )
    with pytest.raises(StrategyCandidateFeatureJoinError, match="authority"):
        _join(common, universe, strategy_id="auction_gap")


def test_rejects_common_column_that_collides_with_reserved_metadata(tmp_path: Path) -> None:
    row = _candidate(
        "000001.SZ",
        strategy_id="n_shape",
        strategy_version="1",
        static_features={"n_score": 0.91},
    )
    universe, _ = _universe(
        tmp_path,
        (("n_shape", "1", (row,), 0, None),),
    )
    common = _common_frame("000001.SZ")
    common["candidate_variant"] = "forged-common-value"

    with pytest.raises(StrategyCandidateFeatureJoinError, match="reserved"):
        _join(common, universe)


def test_rejects_trade_date_and_commit_mismatch(tmp_path: Path) -> None:
    row = _candidate(
        "000001.SZ",
        strategy_id="n_shape",
        strategy_version="1",
        static_features={"n_score": 0.91},
    )
    universe, _ = _universe(
        tmp_path,
        (("n_shape", "1", (row,), 0, None),),
    )
    common = _common_frame("000001.SZ")

    wrong_date = universe.model_copy(update={"required_trade_date": date(2026, 7, 30)})
    with pytest.raises(StrategyCandidateFeatureJoinError, match="trade date"):
        _join(common, wrong_date)

    wrong_commit = universe.model_copy(update={"expected_commit": "b" * 40})
    with pytest.raises(StrategyCandidateFeatureJoinError, match="revalidation"):
        _join(common, wrong_commit)


def test_rejects_common_parent_lineage_collision(tmp_path: Path) -> None:
    row = _candidate(
        "000001.SZ",
        strategy_id="n_shape",
        strategy_version="1",
        static_features={"n_score": 0.91},
    )
    universe, _ = _universe(
        tmp_path,
        (("n_shape", "1", (row,), 0, None),),
    )
    common = _common_frame("000001.SZ")
    colliding_envelope = _common_envelope(common).model_copy(update={"batch_id": "minute-raw-1"})

    with pytest.raises(StrategyCandidateFeatureJoinError, match="collid"):
        _join(common, universe, envelope=colliding_envelope)


def test_result_detaches_from_mutated_common_and_static_inputs(tmp_path: Path) -> None:
    static_features: dict[str, object] = {"n_score": 0.91}
    row = _candidate(
        "000001.SZ",
        strategy_id="n_shape",
        strategy_version="1",
        static_features=static_features,
    )
    universe, _ = _universe(
        tmp_path,
        (("n_shape", "1", (row,), 0, None),),
    )
    common = _common_frame("000001.SZ")
    result = _join(common, universe)
    expected_bytes = result.payload_bytes

    common.loc[0, "vwap"] = 999.0
    static_features["n_score"] = 0.01
    restored = result.frame
    restored.loc[0, "n_score"] = 0.0

    assert result.payload_bytes == expected_bytes
    assert result.frame.loc[0, "vwap"] == 10.0
    assert result.frame.loc[0, "n_score"] == 0.91


def test_join_preserves_json_scalar_types_without_dataframe_coercion(tmp_path: Path) -> None:
    rows = tuple(
        _candidate(
            code,
            strategy_id="n_shape",
            strategy_version="1",
            static_features={"rank": rank},
        )
        for code, rank in (("000001.SZ", 1), ("000002.SZ", 2))
    )
    universe, _ = _universe(
        tmp_path,
        (("n_shape", "1", rows, 0, None),),
    )
    common = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000002.SZ"],
            "rel_same_minute": [2.0, 3.0],
            "vwap": [10.0, 11.0],
            "integer_or_null": pd.Series([1, None], dtype="object"),
        }
    )

    result = _join(common, universe)
    payload_rows = json.loads(result.payload_json)["rows"]

    assert payload_rows[0]["integer_or_null"] == 1
    assert isinstance(payload_rows[0]["integer_or_null"], int)
    assert payload_rows[1]["integer_or_null"] is None
    assert payload_rows[0]["rank"] == 1
    assert isinstance(payload_rows[0]["rank"], int)


def test_join_round_trips_declared_object_static_feature(tmp_path: Path) -> None:
    rows = tuple(
        _candidate(
            code,
            strategy_id="n_shape",
            strategy_version="1",
            static_features={"nested": {"rank": rank, "tags": ["a", "b"]}},
        )
        for code, rank in (("000001.SZ", 1), ("000002.SZ", 2))
    )
    universe, _ = _universe(
        tmp_path,
        (("n_shape", "1", rows, 0, None),),
    )

    result = _join(_common_frame("000001.SZ", "000002.SZ"), universe)

    assert result.frame.loc[0, "nested"] == {"rank": 1, "tags": ["a", "b"]}
    assert result.frame.loc[1, "nested"] == {"rank": 2, "tags": ["a", "b"]}


def test_pooled_static_feature_evidence_is_scoped_to_every_candidate(tmp_path: Path) -> None:
    rows = tuple(
        _candidate(
            code,
            strategy_id="n_shape",
            strategy_version="1",
            static_features={"rank": rank, "score": score},
        )
        for code, rank, score in (
            ("000001.SZ", 1, 0.91),
            ("000002.SZ", 2, 0.88),
        )
    )
    universe, _ = _universe(
        tmp_path,
        (("n_shape", "1", rows, 0, None),),
    )

    result = _join(_common_frame("000001.SZ", "000002.SZ"), universe)

    static_evidence = {
        (status.candidate_id, status.name)
        for status in result.envelope.field_statuses
        if status.name in result.static_feature_names
    }
    assert static_evidence == {
        ("000001.SZ", "rank"),
        ("000001.SZ", "score"),
        ("000002.SZ", "rank"),
        ("000002.SZ", "score"),
    }
    assert result.candidate_authority.definition_fingerprint == DEFINITION_FINGERPRINT
    assert result.candidate_authority.executable_fingerprint == EXECUTABLE_FINGERPRINT
    assert result.candidate_authority.candidate_schema_fingerprint == (
        strategy_candidate_schema_fingerprint(
            strategy_id="n_shape",
            strategy_version="1",
            static_feature_schema=_static_feature_schema("rank", "score"),
        )
    )


@pytest.mark.parametrize("tamper", ["missing", "wrong_candidate", "late"])
def test_pooled_static_feature_evidence_tampering_fails_closed(
    tmp_path: Path,
    tamper: str,
) -> None:
    rows = tuple(
        _candidate(
            code,
            strategy_id="n_shape",
            strategy_version="1",
            static_features={"score": score},
        )
        for code, score in (("000001.SZ", 0.91), ("000002.SZ", 0.88))
    )
    universe, _ = _universe(
        tmp_path,
        (("n_shape", "1", rows, 0, None),),
    )
    result = _join(_common_frame("000001.SZ", "000002.SZ"), universe)
    payload = result.model_dump(mode="json")
    statuses = payload["envelope"]["field_statuses"]
    target = next(
        status
        for status in statuses
        if status["candidate_id"] == "000002.SZ" and status["name"] == "score"
    )
    if tamper == "missing":
        statuses.remove(target)
    elif tamper == "wrong_candidate":
        target["candidate_id"] = "000003.SZ"
    else:
        target["available_at"] = (AVAILABLE_AT + timedelta(seconds=1)).isoformat()
        target["actual_delay_seconds"] = 2.0

    with pytest.raises(ValidationError, match="static feature|decision_cutoff|available_at"):
        StrategyCandidateFeatureBatch.model_validate(payload)


@pytest.mark.parametrize(
    "field",
    ["definition_fingerprint", "executable_fingerprint", "candidate_schema_fingerprint"],
)
def test_joined_authority_fingerprint_tampering_fails_closed(
    tmp_path: Path,
    field: str,
) -> None:
    row = _candidate(
        "000001.SZ",
        strategy_id="n_shape",
        strategy_version="1",
        static_features={"score": 0.91},
    )
    universe, _ = _universe(tmp_path, (("n_shape", "1", (row,), 0, None),))
    result = _join(_common_frame("000001.SZ"), universe)
    payload = result.model_dump(mode="json")
    payload["candidate_authority"][field] = "f" * 64

    with pytest.raises(ValidationError, match="candidate authority"):
        StrategyCandidateFeatureBatch.model_validate(payload)


def test_empty_intersection_is_valid_canonical_and_uses_authority_capture_time(
    tmp_path: Path,
) -> None:
    captured_at = AVAILABLE_AT - timedelta(seconds=1)
    row = _candidate(
        "000001.SZ",
        strategy_id="n_shape",
        strategy_version="1",
        static_features={"n_score": 0.91},
    )
    universe, _ = _universe(
        tmp_path,
        (("n_shape", "1", (row,), 0, captured_at),),
    )

    result = _join(_common_frame("600000.SH"), universe)

    assert result.envelope.row_count == 0
    assert result.frame.empty
    assert tuple(result.frame.columns) == (
        "candidate_effective_trade_date",
        "candidate_generation_sha256",
        "candidate_occurrence_id",
        "candidate_snapshot_schema_version",
        "candidate_variant",
        "n_score",
        "rel_same_minute",
        "ts_code",
        "vwap",
    )
    assert result.envelope.field_status("n_score") is None
    assert hashlib.sha256(result.payload_bytes).hexdigest() == result.envelope.content_hash


def test_empty_candidate_authority_preserves_complete_static_schema(tmp_path: Path) -> None:
    universe, _ = _universe(
        tmp_path,
        (("n_shape", "1", (), 0, AVAILABLE_AT - timedelta(seconds=1)),),
    )

    result = _join(_common_frame("000001.SZ"), universe)

    assert result.envelope.row_count == 0
    assert result.static_feature_names == ("n_score",)
    assert result.candidate_authority.static_feature_names == ("n_score",)
    assert set(result.candidate_authority.static_feature_schema) == {"n_score"}
    assert "n_score" in result.columns


def test_output_rejects_static_name_subset_with_column_and_status_retained(
    tmp_path: Path,
) -> None:
    row = _candidate(
        "000001.SZ",
        strategy_id="n_shape",
        strategy_version="1",
        static_features={"n_score": 0.91},
    )
    universe, _ = _universe(tmp_path, (("n_shape", "1", (row,), 0, None),))
    result = _join(_common_frame("000001.SZ"), universe)
    payload = result.model_dump(mode="json")
    joined = json.loads(payload["payload_json"])
    joined["static_feature_names"] = []
    payload["static_feature_names"] = []
    payload["payload_json"] = json.dumps(
        joined,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )

    with pytest.raises(ValidationError, match="authority schema"):
        StrategyCandidateFeatureBatch.model_validate(payload)


def test_static_status_uses_authority_capture_for_nonempty_intersection(tmp_path: Path) -> None:
    row_available_at = datetime(2026, 7, 30, 17, 1, tzinfo=SHANGHAI)
    captured_at = AVAILABLE_AT - timedelta(seconds=1)
    row = _candidate(
        "000001.SZ",
        strategy_id="n_shape",
        strategy_version="1",
        static_features={"n_score": 0.91},
        available_at=row_available_at,
    )
    universe, _ = _universe(
        tmp_path,
        (("n_shape", "1", (row,), 0, captured_at),),
    )

    result = _join(_common_frame("000001.SZ"), universe)

    assert row.available_at < captured_at
    assert result.envelope.field_status("n_score") == FeatureFieldStatus(
        candidate_id="000001.SZ",
        name="n_score",
        status=FeatureAvailability.AVAILABLE,
        source_event_time=captured_at,
        available_at=captured_at,
        decision_cutoff=AVAILABLE_AT,
        actual_delay_seconds=0.0,
    )


def test_empty_intersection_payload_binds_candidate_authority_generation(
    tmp_path: Path,
) -> None:
    row = _candidate(
        "000001.SZ",
        strategy_id="n_shape",
        strategy_version="1",
        static_features={"n_score": 0.91},
    )
    common = _common_frame("600000.SH")
    first_universe, first_snapshots = _universe(
        tmp_path / "first",
        (("n_shape", "1", (row,), 0, AVAILABLE_AT - timedelta(seconds=2)),),
    )
    second_universe, second_snapshots = _universe(
        tmp_path / "second",
        (("n_shape", "1", (row,), 0, AVAILABLE_AT - timedelta(seconds=1)),),
    )

    first = _join(common, first_universe)
    second = _join(common, second_universe)

    assert first.envelope.row_count == second.envelope.row_count == 0
    assert (
        first_snapshots[("n_shape", "1")].content_sha256
        != second_snapshots[("n_shape", "1")].content_sha256
    )
    assert first.envelope.content_hash != second.envelope.content_hash
    assert first.envelope.input_fingerprint != second.envelope.input_fingerprint
    assert first.envelope.batch_id != second.envelope.batch_id
    assert (
        first.candidate_authority.generation_sha256
        == first_snapshots[("n_shape", "1")].content_sha256
    )
    assert (
        second.candidate_authority.generation_sha256
        == second_snapshots[("n_shape", "1")].content_sha256
    )


def test_output_model_rejects_authority_lineage_and_batch_tampering(tmp_path: Path) -> None:
    row = _candidate(
        "000001.SZ",
        strategy_id="n_shape",
        strategy_version="1",
        static_features={"n_score": 0.91},
    )
    universe, _ = _universe(
        tmp_path,
        (("n_shape", "1", (row,), 0, None),),
    )
    result = _join(_common_frame("000001.SZ"), universe)
    payload = result.model_dump(mode="json")

    forged_authority = {**payload}
    forged_authority["candidate_authority"] = {
        **payload["candidate_authority"],
        "generation_sha256": "f" * 64,
    }
    with pytest.raises(ValidationError, match="candidate authority"):
        StrategyCandidateFeatureBatch.model_validate(forged_authority)

    missing_lineage = {**payload}
    missing_lineage["envelope"] = {
        **payload["envelope"],
        "input_batch_ids": ["minute-history-7", "minute-raw-1"],
    }
    with pytest.raises(ValidationError, match="authority input"):
        StrategyCandidateFeatureBatch.model_validate(missing_lineage)

    missing_common_parent = {**payload}
    missing_common_parent["envelope"] = {
        **payload["envelope"],
        "input_batch_ids": [
            item
            for item in payload["envelope"]["input_batch_ids"]
            if item != payload["common_batch_id"]
        ],
    }
    with pytest.raises(ValidationError, match="common parent"):
        StrategyCandidateFeatureBatch.model_validate(missing_common_parent)

    forged_common_parent = {**payload, "common_batch_id": "forged-common-parent"}
    with pytest.raises(ValidationError, match="common_batch_id"):
        StrategyCandidateFeatureBatch.model_validate(forged_common_parent)

    forged_batch = {**payload}
    forged_batch["envelope"] = {
        **payload["envelope"],
        "batch_id": "f" * 64,
    }
    with pytest.raises(ValidationError, match="batch_id"):
        StrategyCandidateFeatureBatch.model_validate(forged_batch)

    missing_reserved = {**payload}
    missing_reserved["columns"] = [
        column for column in payload["columns"] if column != "candidate_variant"
    ]
    with pytest.raises(ValidationError, match="reserved"):
        StrategyCandidateFeatureBatch.model_validate(missing_reserved)

    forged_row_payload = json.loads(payload["payload_json"])
    forged_row_payload["rows"][0]["candidate_generation_sha256"] = "f" * 64
    forged_row = {
        **payload,
        "payload_json": json.dumps(
            forged_row_payload,
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ),
    }
    with pytest.raises(ValidationError, match="row candidate authority"):
        StrategyCandidateFeatureBatch.model_validate(forged_row)

    unordered_lineage = {**payload}
    unordered_lineage["envelope"] = {
        **payload["envelope"],
        "input_batch_ids": list(reversed(payload["envelope"]["input_batch_ids"])),
    }
    with pytest.raises(ValidationError, match="sorted"):
        StrategyCandidateFeatureBatch.model_validate(unordered_lineage)


@pytest.mark.parametrize("offset_seconds", (-1, 1))
def test_output_model_rejects_static_status_time_tampering(
    tmp_path: Path,
    offset_seconds: int,
) -> None:
    captured_at = AVAILABLE_AT - timedelta(seconds=1)
    row = _candidate(
        "000001.SZ",
        strategy_id="n_shape",
        strategy_version="1",
        static_features={"n_score": 0.91},
    )
    universe, _ = _universe(
        tmp_path,
        (("n_shape", "1", (row,), 0, captured_at),),
    )
    result = _join(_common_frame("000001.SZ"), universe)
    payload = result.model_dump(mode="json")
    payload["envelope"]["field_statuses"] = [
        {
            **status,
            "available_at": (captured_at + timedelta(seconds=offset_seconds)).isoformat(),
        }
        if status["name"] == "n_score"
        else status
        for status in payload["envelope"]["field_statuses"]
    ]

    with pytest.raises(
        ValidationError,
        match="static feature status|source_event_time|actual_delay_seconds",
    ):
        StrategyCandidateFeatureBatch.model_validate(payload)


def test_live_and_replay_identical_inputs_produce_identical_bytes(tmp_path: Path) -> None:
    row = _candidate(
        "000001.SZ",
        strategy_id="n_shape",
        strategy_version="1",
        static_features={"n_score": 0.91},
    )
    universe, _ = _universe(
        tmp_path,
        (("n_shape", "1", (row,), 0, None),),
    )
    common = _common_frame("000001.SZ")
    envelope = _common_envelope(common)

    live = join_strategy_candidate_features(
        envelope,
        common,
        universe,
        "n_shape",
        "1",
    )
    replay = join_strategy_candidate_features(
        envelope,
        common.copy(deep=True),
        RuntimeCandidateUniverseResult.model_validate(universe.model_dump(mode="json")),
        "n_shape",
        "1",
    )

    assert live.payload_bytes == replay.payload_bytes
    assert live.model_dump_json() == replay.model_dump_json()
