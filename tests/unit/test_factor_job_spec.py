"""A factor evaluation job has its own bounded, content-addressed input."""

from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.factor.definition import FactorDefinition, build_factor_definition
from rquant.factor.expression import FeatureCatalog
from rquant.factor.historical_adapter import HistoricalFactorAdapterRequest
from rquant.factor.job_spec import FactorEvaluationJobSpec
from rquant.factor.registry import FactorDefinitionRegistry, SaveFactorDefinitionRequest
from rquant.factor_snapshot_admission import FactorSnapshotAdmissionRequest
from rquant.strict_json import canonical_json_bytes

START = date(2026, 7, 13)
END = date(2026, 7, 30)
AS_OF = datetime(2026, 7, 31, 8, tzinfo=UTC)
DEADLINE = AS_OF + timedelta(days=1)


def _definition(*, version: int = 1, name: str = "历史价格因子") -> FactorDefinition:
    return build_factor_definition(
        factor_id="historical_price",
        name_zh=name,
        category="technical",
        direction="higher_is_better",
        version=version,
        earliest_available_date=START,
        expression="ts_mean(close, 3)",
        feature_catalog=FeatureCatalog(columns=("close",)),
    )


def _definition_sha256(definition: FactorDefinition) -> str:
    return hashlib.sha256(
        canonical_json_bytes(definition.model_dump(mode="json", round_trip=True))
    ).hexdigest()


def _admission(**changes: object) -> FactorSnapshotAdmissionRequest:
    fields: dict[str, object] = {
        "snapshot_id": "a" * 64,
        "binding_hash": "b" * 64,
        "start_date": START,
        "end_date": END,
        "source_mode": "historical_retrospective",
    }
    return FactorSnapshotAdmissionRequest.model_validate({**fields, **changes})


def _adapter(**changes: object) -> HistoricalFactorAdapterRequest:
    fields: dict[str, object] = {
        "definition": _definition(),
        "stock_codes": ("000001.SZ", "000002.SZ"),
        "pool_basis": "explicit_fixed_list",
        "evaluation_days": (date(2026, 7, 16), date(2026, 7, 23)),
        "query_start_date": START,
        "query_end_date": END,
        "holding_sessions": 5,
        "as_of": AS_OF,
    }
    return HistoricalFactorAdapterRequest.model_validate({**fields, **changes})


def _spec(
    *,
    admission_request: FactorSnapshotAdmissionRequest | None = None,
    adapter_request: HistoricalFactorAdapterRequest | None = None,
    definition_content_sha256: str | None = None,
    code_revision: str = "c" * 40,
    deadline: datetime = DEADLINE,
) -> FactorEvaluationJobSpec:
    adapter = adapter_request or _adapter()
    return FactorEvaluationJobSpec(
        admission_request=admission_request or _admission(),
        adapter_request=adapter,
        definition_content_sha256=(
            definition_content_sha256 or _definition_sha256(adapter.definition)
        ),
        code_revision=code_revision,
        deadline=deadline,
    )


def test_factor_job_spec_round_trips_with_registry_definition_digest(tmp_path: Path) -> None:
    spec = _spec(adapter_request=_adapter(stock_codes=("000002.SZ", "000001.SZ")))
    same = _spec()
    decoded = FactorEvaluationJobSpec.model_validate_json(spec.model_dump_json())
    registry = FactorDefinitionRegistry(tmp_path / "factor.sqlite3")
    identity = registry.initialize()
    receipt = registry.save(
        SaveFactorDefinitionRequest(
            command_id="factor-eval-spec-test",
            definition=spec.adapter_request.definition,
            expected_head=None,
        ),
        expected_identity=identity,
    )

    assert spec.job_type == "factor_eval"
    assert spec.schema_version == 1
    assert spec.research_status == "exploratory"
    assert spec.result_kind == "research_diagnostic"
    assert spec.definition_content_sha256 == receipt.content_sha256
    assert "definition" not in spec.model_dump(mode="json")
    assert decoded == spec
    assert spec.spec_sha256 == same.spec_sha256 == decoded.spec_sha256
    assert (
        spec.spec_sha256
        == hashlib.sha256(
            canonical_json_bytes(spec.model_dump(mode="json", round_trip=True))
        ).hexdigest()
    )


def test_equivalent_timestamp_offsets_have_one_canonical_spec() -> None:
    same_instant_timezone = timezone(timedelta(hours=8))
    local = _spec(
        adapter_request=_adapter(as_of=AS_OF.astimezone(same_instant_timezone)),
        deadline=DEADLINE.astimezone(same_instant_timezone),
    )
    utc = _spec()
    assert local.model_dump_json() == utc.model_dump_json()
    assert local.spec_sha256 == utc.spec_sha256


def test_factor_job_digest_changes_with_every_bound_input() -> None:
    baseline = _spec()
    changed_definition = _adapter(definition=_definition(version=2, name="新版历史价格因子"))
    variants = (
        _spec(
            adapter_request=changed_definition,
            definition_content_sha256=_definition_sha256(changed_definition.definition),
        ),
        _spec(admission_request=_admission(snapshot_id="d" * 64)),
        _spec(admission_request=_admission(binding_hash="e" * 64)),
        _spec(adapter_request=_adapter(stock_codes=("000001.SZ",))),
        _spec(adapter_request=_adapter(evaluation_days=(date(2026, 7, 17),))),
        _spec(adapter_request=_adapter(holding_sessions=10)),
        _spec(adapter_request=_adapter(as_of=AS_OF + timedelta(hours=1))),
        _spec(adapter_request=_adapter(query_start_date=START + timedelta(days=1))),
        _spec(code_revision="d" * 40),
        _spec(deadline=DEADLINE + timedelta(days=1)),
    )
    assert len({baseline.spec_sha256, *(variant.spec_sha256 for variant in variants)}) == (
        len(variants) + 1
    )


@pytest.mark.parametrize(
    "admission",
    (
        _admission(start_date=START + timedelta(days=1)),
        _admission(end_date=END - timedelta(days=1)),
    ),
)
def test_factor_job_rejects_admission_that_does_not_cover_query(
    admission: FactorSnapshotAdmissionRequest,
) -> None:
    with pytest.raises(ValidationError, match="admission"):
        _spec(admission_request=admission)


@pytest.mark.parametrize("deadline", (AS_OF, AS_OF - timedelta(seconds=1)))
def test_factor_job_rejects_deadline_at_or_before_research_as_of(deadline: datetime) -> None:
    with pytest.raises(ValidationError, match="deadline"):
        _spec(deadline=deadline)


def test_factor_job_rejects_naive_deadline_and_false_definition_digest() -> None:
    with pytest.raises(ValidationError):
        _spec(deadline=DEADLINE.replace(tzinfo=None))
    with pytest.raises(ValidationError, match="definition"):
        _spec(definition_content_sha256="0" * 64)


@pytest.mark.parametrize(
    "extra",
    (
        {"strategy_execution": {}},
        {"execution_costs": {}},
        {"path": "/tmp/forbidden"},
        {"sql": "SELECT 1"},
        {"python_code": "pass"},
    ),
)
def test_factor_job_rejects_strategy_and_arbitrary_execution_fields(
    extra: dict[str, object],
) -> None:
    with pytest.raises(ValidationError, match="Extra inputs"):
        FactorEvaluationJobSpec.model_validate({**_spec().model_dump(mode="python"), **extra})


@pytest.mark.parametrize(
    "change",
    (
        {"job_type": "strategy"},
        {"schema_version": 2},
        {"code_revision": "A" * 40},
        {"code_revision": "a" * 39},
        {"research_status": "validated"},
        {"result_kind": "tradable"},
    ),
)
def test_factor_job_rejects_other_workflow_or_identity(change: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        FactorEvaluationJobSpec.model_validate({**_spec().model_dump(mode="python"), **change})


def test_factor_job_requires_existing_retrospective_source_contracts() -> None:
    with pytest.raises(ValidationError):
        _admission(source_mode="point_in_time")
    with pytest.raises(ValidationError):
        _adapter(source_mode="point_in_time")
    with pytest.raises(ValidationError):
        _adapter(pool_basis="dynamic_universe")
    with pytest.raises(ValidationError):
        _adapter(holding_sessions=999)


def test_model_copy_validates_changed_inputs_before_building_new_digest() -> None:
    original = _spec()
    with pytest.raises(ValidationError):
        original.model_copy(update={"strategy_execution": {}})
    with pytest.raises(ValidationError):
        original.model_copy(update={"definition_content_sha256": "0" * 64})
    updated = original.model_copy(update={"code_revision": "f" * 40})
    assert updated.spec_sha256 != original.spec_sha256
