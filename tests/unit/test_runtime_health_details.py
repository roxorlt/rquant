"""Rule fixtures for health detail integrity; these are not installed source evidence."""

from __future__ import annotations

import importlib
import importlib.util
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import ModuleType

import pytest
from pydantic import ValidationError

from rquant.ops_status import (
    STATIC_TIMER_STEMS,
    OpsResourceEvidence,
    OpsSnapshot,
    OpsUnitEvidence,
)
from rquant.ops_status_serving import ops_runtime_health_metrics, ops_status_source_result
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_service_control import (
    RuntimeServiceHealth,
    RuntimeServiceHeartbeat,
    RuntimeServicePlane,
    RuntimeServiceSpec,
    RuntimeServiceStatus,
    project_heartbeat,
)
from rquant.runtime_serving_snapshot import SourceReadResult
from rquant.serving_read_models import ServingProjectionInput, ServingReadModelInput
from rquant.strict_json import canonical_json_bytes

AT = datetime(2026, 10, 6, 3, 0, tzinfo=UTC)
START = AT - timedelta(seconds=60)
SHA = "1" * 64
GENERATION = "2" * 64


def core() -> ModuleType:
    assert importlib.util.find_spec("rquant.runtime_health_details") is not None, (
        "health detail core is not implemented"
    )
    return importlib.import_module("rquant.runtime_health_details")


def spec(service_id: str = "strategy.test.v1") -> RuntimeServiceSpec:
    return RuntimeServiceSpec(
        service_id=service_id,
        plane=RuntimeServicePlane.LIVE,
        stale_after=timedelta(seconds=120),
        producer_commit="a" * 40,
    )


def heartbeat(
    service: RuntimeServiceSpec | None = None, **changes: object
) -> RuntimeServiceHeartbeat:
    service = service or spec()
    values: dict[str, object] = {
        "service_id": service.service_id,
        "spec_fingerprint": service.identity,
        "run_id": "3" * 64,
        "generation": 1,
        "status": RuntimeServiceStatus.RUNNING,
        "started_at": START,
        "heartbeat_at": AT,
        "last_success_at": AT,
        "observations": {"processed_candidates": 7},
        "recent_step_durations_seconds": (0.2,),
        "last_step_duration_seconds": 0.2,
        "p95_step_duration_seconds": 0.2,
    }
    return RuntimeServiceHeartbeat.model_validate(values | changes)


def context(**changes: object) -> object:
    return core().RuntimeHealthOpsContext.model_validate(
        {
            "host_name": "fixture-host",
            "boot_id": "fixture-boot",
            "manifest_digest": "4" * 64,
            "ops_source_generation_id": "5" * 64,
            "source_identity": "6" * 64,
            "sampled_at": START,
        }
        | changes
    )


def witness(hb: RuntimeServiceHeartbeat, **changes: object) -> object:
    return core().startup_witness_for_run(
        context=context(**changes),
        spec=spec(hb.service_id),
        run_id=hb.run_id,
        generation=hb.generation,
        started_at=hb.started_at,
    )


def legacy(hb: RuntimeServiceHeartbeat | None, *, at: datetime = AT) -> RuntimeServiceHealth:
    stale = hb is None or at - hb.heartbeat_at > spec().stale_after
    return RuntimeServiceHealth(
        service_id=spec().service_id if hb is None else hb.service_id,
        plane=RuntimeServicePlane.LIVE,
        status=RuntimeServiceStatus.DEGRADED
        if stale and hb is not None
        else (RuntimeServiceStatus.MISSING if hb is None else hb.status),
        stale=stale,
        observed_at=at,
        heartbeat=project_heartbeat(hb),
    )


def material(hb: RuntimeServiceHeartbeat | None, **changes: object) -> object:
    if hb is not None:
        facts = tuple(
            item for item in changes.get("metrics", ()) if item.owner_dataset_id != "ops_status"
        )
        ids = tuple(
            (item.metric_id, item.owner_dataset_id, canonical_sha256(item.scope))
            for item in facts
        )
        fields = {"startup_witness": changes.get("startup_witness", hb.startup_witness)}
        if (
            "metrics" in changes
            and facts
            and len(ids) == len(set(ids))
            and all(item.observed_at <= hb.heartbeat_at for item in facts)
        ):
            fields["health_metrics"] = facts
        hb = RuntimeServiceHeartbeat.model_validate(hb.model_dump(mode="python") | fields)
    return core().RuntimeHealthHeartbeatMaterial.from_read(
        control_root=Path("/fixture/control"),
        spec=spec() if hb is None else spec(hb.service_id),
        heartbeat=hb,
        observed_at=AT,
        **changes,
    )


def publication(*, hb: RuntimeServiceHeartbeat | None = None, with_witness: bool = True) -> tuple:
    hb = hb or heartbeat()
    source = material(hb, startup_witness=witness(hb) if with_witness else None)
    inputs = {
        "legacy_services": (legacy(hb),),
        "source_receipts": {hb.service_id: source.source_receipt},
        "context": context(),
        "owner_generation_id": GENERATION,
        "observed_at": AT,
    }
    graph = core().build_runtime_health_detail_graph(materials=(source,), enabled=True, **inputs)
    return graph, inputs


def replace_material(graph: object, mutate: object) -> object:
    values = graph.model_dump(mode="json")
    table = next(p for p in values["projections"] if p["table_name"] == "runtime_service_detail")
    row = table["rows"][0]
    source = json.loads(row["source_material_json"])
    mutate(source)
    row["source_material_json"] = canonical_json_bytes(source).decode()
    return core().RuntimeHealthDetailGraph.model_validate(values)


def test_core_exists() -> None:
    assert core().__name__ == "rquant.runtime_health_details"


@pytest.mark.parametrize("original_witness", [False, True])
def test_same_read_rejects_witness_absent_from_or_different_to_original_heartbeat(
    original_witness: bool,
) -> None:
    h = heartbeat()
    original = witness(h)
    if original_witness:
        h = heartbeat(startup_witness=original)
    detached = witness(h, host_name="other-host") if original_witness else original
    with pytest.raises(ValueError, match="original heartbeat"):
        core().RuntimeHealthHeartbeatMaterial.from_read(
            control_root=Path("/fixture/control"),
            spec=spec(),
            heartbeat=h,
            observed_at=AT,
            startup_witness=detached,
        )


@pytest.mark.parametrize("mutation", ["changed", "missing", "added"])
def test_same_read_rejects_independent_producer_metrics_with_original_receipt(
    mutation: str,
) -> None:
    h = heartbeat()
    binding, fact = witness(h), metric()
    h = heartbeat(startup_witness=binding, health_metrics=(fact,))
    metrics = (
        (metric(value=999),)
        if mutation == "changed"
        else ()
        if mutation == "missing"
        else (
            fact,
            metric(scope={"kind": "strategy_batch", "batch_id": "9" * 64, "processed": True}),
        )
    )
    with pytest.raises(ValueError, match="original heartbeat"):
        core().RuntimeHealthHeartbeatMaterial.from_read(
            control_root=Path("/fixture/control"),
            spec=spec(),
            heartbeat=h,
            observed_at=AT,
            startup_witness=binding,
            metrics=metrics,
        )


def test_same_read_keeps_exact_producer_facts_and_current_ops_supplement() -> None:
    h = heartbeat()
    binding, fact = witness(h), metric()
    h = heartbeat(startup_witness=binding, health_metrics=(fact,))
    args = {
        "control_root": Path("/fixture/control"),
        "spec": spec(),
        "heartbeat": h,
        "observed_at": AT,
        "startup_witness": binding,
    }
    original = core().RuntimeHealthHeartbeatMaterial.from_read(**args, metrics=(fact,))
    owner = original_ops_source()
    ops = next(
        item for item in ops_runtime_health_metrics(owner) if item.metric_id == "host_memory"
    )
    supplemented = core().RuntimeHealthHeartbeatMaterial.from_read(
        **args, metrics=(fact, ops), ops_source=owner
    )
    assert supplemented.source_receipt == original.source_receipt
    inputs = {
        "legacy_services": (legacy(h),),
        "source_receipts": {h.service_id: original.source_receipt},
        "context": context(
            sampled_at=AT,
            ops_source_generation_id=owner.generation_id,
            source_identity=canonical_sha256(owner),
        ),
        "owner_generation_id": GENERATION,
        "observed_at": AT,
    }
    graph = core().build_runtime_health_detail_graph(
        materials=(supplemented,), enabled=True, **inputs
    )
    detail = core().validate_runtime_health_detail_graph(graph, **inputs).services[0]
    assert [item.metric for item in detail.metrics] == [fact, ops]


def original_ops_source() -> SourceReadResult:
    return ops_status_source_result(
        OpsSnapshot(
            sampled_at=AT,
            host_name="fixture-host",
            boot_id="fixture-boot",
            manifest_digest="4" * 64,
            host_memory_total_bytes=1000,
            host_memory_available_bytes=100,
            units=tuple(
                OpsUnitEvidence(
                    timer=f"rquant-{stem}.timer",
                    service=f"rquant-{stem}.service",
                    label="合成任务",
                    expected_enabled=True,
                    session="all",
                    resource_group="maintenance",
                )
                for stem in STATIC_TIMER_STEMS
            ),
            resources=tuple(
                OpsResourceEvidence(slice_name=name, memory_current_bytes=100)
                for name in (
                    "rquant.slice",
                    "rquant-live.slice",
                    "rquant-serving.slice",
                    "rquant-research.slice",
                    "rquant-maintenance.slice",
                )
            ),
        )
    )


def validate_ops_supplement(
    facts: tuple[object, ...],
    source: SourceReadResult,
    *,
    trusted_source: SourceReadResult | None = None,
) -> object:
    h = heartbeat()
    binding = witness(h)
    h = heartbeat(startup_witness=binding)
    original = core().RuntimeHealthHeartbeatMaterial.from_read(
        control_root=Path("/fixture/control"),
        spec=spec(),
        heartbeat=h,
        observed_at=AT,
        startup_witness=binding,
        metrics=facts,
        ops_source=source,
    )
    trusted = trusted_source or source
    inputs = {
        "legacy_services": (legacy(h),),
        "source_receipts": {h.service_id: original.source_receipt},
        "context": context(
            sampled_at=AT,
            ops_source_generation_id=trusted.generation_id,
            source_identity=canonical_sha256(trusted),
        ),
        "owner_generation_id": GENERATION,
        "observed_at": AT,
    }
    graph = core().build_runtime_health_detail_graph(
        materials=(original,), enabled=True, **inputs
    )
    return core().validate_runtime_health_detail_graph(graph, **inputs)


def test_ops_supplement_cannot_add_strategy_candidates() -> None:
    source = original_ops_source()
    spoof = metric(
        owner_dataset_id="ops_status",
        source_generation_id=source.generation_id,
        source_identity=canonical_sha256(source),
        value=999,
    )
    with pytest.raises(ValueError, match="Ops"):
        validate_ops_supplement((spoof,), source)


def test_ops_supplement_cannot_change_original_owner_memory_value() -> None:
    source = original_ops_source()
    original = next(
        fact for fact in ops_runtime_health_metrics(source) if fact.metric_id == "host_memory"
    )
    assert original.value == source.payload.snapshot.host_memory_available_bytes == 100
    altered = core().RuntimeHealthMetric.model_validate(
        original.model_dump(mode="python") | {"value": 999}
    )
    assert altered.source_generation_id == source.generation_id
    assert altered.source_identity == canonical_sha256(source)
    with pytest.raises(ValueError, match="Ops"):
        validate_ops_supplement((altered,), source)


@pytest.mark.parametrize(
    "changes",
    [
        {"reason_code": "replacement"},
        {"observed_at": AT + timedelta(microseconds=1)},
        {"event_time_start": AT - timedelta(seconds=1)},
        {"fresh_until": AT + timedelta(seconds=120)},
        {"validity": None},
        {"completeness": "unavailable", "value": None, "verdict": "unavailable"},
    ],
)
def test_ops_supplement_keeps_all_original_owner_fields(changes: dict) -> None:
    owner = original_ops_source()
    original = next(
        fact for fact in ops_runtime_health_metrics(owner) if fact.metric_id == "host_memory"
    )
    altered = core().RuntimeHealthMetric.model_validate(original.model_dump() | changes)
    with pytest.raises(ValueError, match="Ops|cutoff"):
        validate_ops_supplement((altered,), owner)


def test_ops_supplement_requires_its_actual_owner_record() -> None:
    owner = original_ops_source()
    facts = ops_runtime_health_metrics(owner)
    h = heartbeat()
    binding = witness(h)
    with pytest.raises(ValueError, match="Ops source.*record"):
        core().RuntimeHealthHeartbeatMaterial.from_read(
            control_root=Path("/fixture/control"),
            spec=spec(),
            heartbeat=heartbeat(startup_witness=binding),
            observed_at=AT,
            startup_witness=binding,
            metrics=facts,
        )


def test_ops_supplement_cannot_replace_the_trusted_current_owner_record() -> None:
    owner = original_ops_source()
    changed = ops_status_source_result(
        type(owner.payload.snapshot).model_validate(
            owner.payload.snapshot.model_dump() | {"host_memory_available_bytes": 999}
        )
    )
    with pytest.raises(ValueError, match="trusted current context"):
        validate_ops_supplement(ops_runtime_health_metrics(changed), changed, trusted_source=owner)


def test_ops_supplement_rejects_foreign_slice_scope() -> None:
    owner = original_ops_source()
    original = next(
        fact for fact in ops_runtime_health_metrics(owner) if fact.metric_id == "slice_memory"
    )
    altered = core().RuntimeHealthMetric.model_validate(
        original.model_dump()
        | {"scope": original.scope.model_dump() | {"slice_id": "foreign.slice"}}
    )
    with pytest.raises(ValueError, match="Ops source"):
        validate_ops_supplement((altered,), owner)


def test_ops_owner_record_cannot_bypass_original_health_cell_budget() -> None:
    owner = original_ops_source()
    sample = owner.payload.snapshot
    large_unit = type(sample.units[0]).model_validate(
        sample.units[0].model_dump() | {"label": "x" * (32 * 1024)}
    )
    large = ops_status_source_result(
        type(sample).model_validate(
            sample.model_dump() | {"units": (large_unit, *sample.units[1:])}
        )
    )
    assert len(canonical_json_bytes(large.model_dump(mode="json"))) < 512 * 1024
    with pytest.raises(ValueError, match="cell.*budget"):
        validate_ops_supplement(ops_runtime_health_metrics(large), large)


def test_ops_owner_record_requires_strict_canonical_json() -> None:
    source = ops_serving_input().runtime_health_details.services[0].material
    duplicate = '{"dataset_id":"ops_status",' + source.ops_source_json[1:]
    with pytest.raises(ValueError, match="duplicate"):
        type(source).model_validate(source.model_dump() | {"ops_source_json": duplicate})


def ops_serving_input() -> ServingReadModelInput:
    owner = original_ops_source()
    details = validate_ops_supplement(ops_runtime_health_metrics(owner), owner)
    h = heartbeat(startup_witness=witness(heartbeat()))
    inputs = {
        "legacy_services": (legacy(h),),
        "source_receipts": {h.service_id: details.services[0].source_receipt},
        "context": details.context,
        "owner_generation_id": GENERATION,
        "observed_at": AT,
    }
    graph = core().build_runtime_health_detail_graph(
        materials=(details.services[0].material,), enabled=True, **inputs
    )
    return ServingReadModelInput(
        observed_at=AT,
        runtime_services=(legacy(h),),
        runtime_health_details=details,
        projections=tuple(
            ServingProjectionInput(**projection.model_dump(mode="python"))
            for projection in graph.projections
        ),
    )


def test_original_ops_record_survives_serving_json_round_trip() -> None:
    original = ops_serving_input()
    decoded = ServingReadModelInput.model_validate_json(original.model_dump_json())
    assert decoded == original
    assert decoded.runtime_health_details.services[0].material.ops_source == original_ops_source()
    assert next(
        fact.value
        for fact in decoded.runtime_health_details.services[0].metrics
        if fact.metric.metric_id == "host_memory"
    ) == 100


@pytest.mark.parametrize("replacement", ["strategy_candidates", "memory_value"])
def test_serving_serialization_cannot_replace_original_ops_facts(replacement: str) -> None:
    raw = ops_serving_input().model_dump(mode="json")
    source = raw["runtime_health_details"]["services"][0]["material"]
    fact = next(item for item in source["metrics"] if item["metric_id"] == "host_memory")
    if replacement == "strategy_candidates":
        fact |= {
            "metric_id": "strategy_candidates",
            "scope": metric().scope.model_dump(),
            "unit": "count",
        }
    fact["value"] = 999
    for projection in raw["projections"]:
        if projection["table_name"] == "runtime_service_detail":
            projection["rows"][0]["source_material_json"] = canonical_json_bytes(source).decode()
    with pytest.raises(ValueError, match="Ops source"):
        ServingReadModelInput.model_validate(raw)


def test_optional_emission_is_off_and_legacy_absence_is_unavailable() -> None:
    graph, inputs = publication()
    source = material(heartbeat())
    assert "ops_source_json" not in source.model_dump(mode="json")
    assert core().build_runtime_health_detail_graph(materials=(source,), **inputs) is None
    assert core().validate_runtime_health_detail_graph(None, **inputs) is None
    old_graph, old_inputs = publication(with_witness=False)
    result = core().validate_runtime_health_detail_graph(old_graph, **old_inputs)
    assert result.services[0].availability == "unavailable"
    assert result.services[0].reason_code == "startup_witness_missing"
    assert result.services[0].heartbeat is None
    assert legacy(heartbeat()).status is RuntimeServiceStatus.RUNNING
    assert graph is not None


def test_startup_witness_needs_original_context_and_120_second_start_window() -> None:
    h = heartbeat()
    args = {"spec": spec(), "run_id": h.run_id, "generation": 1, "started_at": START}
    assert core().startup_witness_for_run(context=None, **args) is None
    assert (
        core().startup_witness_for_run(
            context=context(sampled_at=START - timedelta(seconds=121)), **args
        )
        is None
    )
    assert core().startup_witness_for_run(context=context(sampled_at=START), **args) is not None
    with pytest.raises(ValueError, match="future"):
        core().startup_witness_for_run(
            context=context(sampled_at=START + timedelta(seconds=1)), **args
        )


def test_same_read_projection_and_original_receipt_are_both_bound() -> None:
    graph, inputs = publication()
    result = core().validate_runtime_health_detail_graph(graph, **inputs)
    assert dict(result.services[0].heartbeat.observations) == {"processed_candidates": 7}
    source = material(heartbeat())
    expected = canonical_sha256(
        {
            "contract": "runtime-health-source-receipt/v1",
            "control_root": "/fixture/control",
            "spec": spec().model_dump(mode="json"),
            "heartbeat": heartbeat().model_dump(mode="json"),
            "observed_at": AT,
        }
    )
    assert source.source_receipt == expected
    wrong_raw = replace_material(
        graph,
        lambda value: value.update(
            heartbeat_json=canonical_json_bytes(
                heartbeat(
                    observations={"processed_candidates": 8},
                    startup_witness=witness(heartbeat()),
                ).model_dump(mode="json")
            ).decode()
        ),
    )
    with pytest.raises(ValueError, match="receipt"):
        core().validate_runtime_health_detail_graph(wrong_raw, **inputs)
    wrong_legacy = inputs | {"legacy_services": (legacy(heartbeat(processed_count=2)),)}
    with pytest.raises(ValueError, match="same read"):
        core().validate_runtime_health_detail_graph(graph, **wrong_legacy)


@pytest.mark.parametrize("field,value", [("run_id", "8" * 64), ("generation", 2)])
def test_witness_cannot_attach_to_another_run(field: str, value: object) -> None:
    graph, inputs = publication()
    with pytest.raises(ValueError, match="run"):
        wrong = replace_material(
            graph, lambda source: source["startup_witness"].update({field: value})
        )
        core().validate_runtime_health_detail_graph(wrong, **inputs)


@pytest.mark.parametrize(
    "changes,reason",
    [
        ({"host_name": "other-host"}, "host_mismatch"),
        ({"boot_id": "other-boot"}, "boot_mismatch"),
        ({"manifest_digest": "8" * 64}, "installation_mismatch"),
    ],
)
def test_current_collector_does_not_relabel_old_host_boot_or_install(
    changes: dict[str, object], reason: str
) -> None:
    graph, inputs = publication()
    values = inputs | {"context": context(**changes)}
    h = heartbeat()
    graph = core().build_runtime_health_detail_graph(
        materials=(material(h, startup_witness=witness(h)),), enabled=True, **values
    )
    result = core().validate_runtime_health_detail_graph(graph, **values)
    assert result.services[0].availability == "unavailable"
    assert result.services[0].reason_code == reason
    assert result.services[0].heartbeat is None


def test_startup_and_current_generation_may_differ_but_current_context_must_match() -> None:
    graph, inputs = publication()
    current = context(ops_source_generation_id="a" * 64, source_identity="b" * 64)
    h = heartbeat()
    graph = core().build_runtime_health_detail_graph(
        materials=(material(h, startup_witness=witness(h)),),
        enabled=True,
        **(inputs | {"context": current}),
    )
    result = core().validate_runtime_health_detail_graph(graph, **(inputs | {"context": current}))
    assert result.services[0].availability == "available"
    with pytest.raises(ValueError, match="context"):
        core().validate_runtime_health_detail_graph(graph, **inputs)


@pytest.mark.parametrize(
    "mutation", ["duplicate", "missing", "foreign", "wrong_owner", "wrong_cutoff"]
)
def test_detail_graph_is_complete_and_has_one_owner_and_cutoff(mutation: str) -> None:
    graph, inputs = publication()
    values = graph.model_dump(mode="json")
    detail = next(p for p in values["projections"] if p["table_name"] == "runtime_service_detail")
    if mutation == "duplicate":
        detail["rows"].append(detail["rows"][0])
    elif mutation == "missing":
        detail["rows"] = []
    elif mutation == "foreign":
        detail["rows"][0]["service_id"] = "foreign.test.v1"
    elif mutation == "wrong_owner":
        detail["owner_generation_id"] = "8" * 64
    else:
        detail["available_at"] = (AT + timedelta(seconds=1)).isoformat()
    with pytest.raises(ValueError):
        invalid = core().RuntimeHealthDetailGraph.model_validate(values)
        core().validate_runtime_health_detail_graph(invalid, **inputs)


@pytest.mark.parametrize("partial", [0, 1])
def test_present_partial_tables_cannot_succeed(partial: int) -> None:
    graph, _inputs = publication()
    values = graph.model_dump(mode="json")
    values["projections"] = values["projections"][:partial]
    with pytest.raises(ValueError, match="complete"):
        core().RuntimeHealthDetailGraph.model_validate(values)


@pytest.mark.parametrize("kind", ["missing", "superseded", "unreadable"])
def test_absent_source_preserves_original_receipt_and_status(kind: str) -> None:
    kwargs = {"read_kind": kind}
    if kind == "unreadable":
        kwargs["unreadable_error"] = "OSError"
    source = material(None, **kwargs)
    summary = (
        None
        if kind == "missing"
        else ({"superseded": True} if kind == "superseded" else {"unreadable": "OSError"})
    )
    assert source.source_receipt == canonical_sha256(
        {
            "contract": "runtime-health-source-receipt/v1",
            "control_root": "/fixture/control",
            "spec": spec().model_dump(mode="json"),
            "heartbeat": summary,
            "observed_at": AT,
        }
    )
    old = legacy(None)
    if kind == "unreadable":
        old = RuntimeServiceHealth.model_validate(
            old.model_dump(mode="python") | {"status": RuntimeServiceStatus.DEGRADED}
        )
    inputs = {
        "legacy_services": (old,),
        "source_receipts": {spec().service_id: source.source_receipt},
        "context": context(),
        "owner_generation_id": GENERATION,
        "observed_at": AT,
    }
    graph = core().build_runtime_health_detail_graph(materials=(source,), enabled=True, **inputs)
    result = core().validate_runtime_health_detail_graph(graph, **inputs)
    assert result.services[0].availability == "unavailable"
    assert result.services[0].reason_code == f"heartbeat_{kind}"


def test_strict_canonical_material_rejects_duplicate_json_and_future_raw_evidence() -> None:
    graph, inputs = publication()
    values = graph.model_dump(mode="json")
    row = next(p for p in values["projections"] if p["table_name"] == "runtime_service_detail")[
        "rows"
    ][0]
    row["source_material_json"] = row["source_material_json"].replace(
        '"read_kind":"readable"', '"read_kind":"readable","read_kind":"missing"'
    )
    with pytest.raises(ValueError):
        invalid = core().RuntimeHealthDetailGraph.model_validate(values)
        core().validate_runtime_health_detail_graph(invalid, **inputs)
    with pytest.raises(ValueError, match="future"):
        material(heartbeat(heartbeat_at=AT + timedelta(seconds=1)))


def metric(**changes: object) -> object:
    return core().RuntimeHealthMetric.model_validate(
        {
            "metric_id": "strategy_candidates",
            "owner_dataset_id": "strategy_features",
            "source_generation_id": SHA,
            "source_identity": "7" * 64,
            "scope": {"kind": "strategy_batch", "batch_id": "8" * 64, "processed": True},
            "event_time_start": START,
            "event_time_end": AT,
            "available_at": AT,
            "observed_at": AT,
            "unit": "count",
            "completeness": "complete",
            "value": 7,
            "verdict": "unassessed",
            "reason_code": "no_configured_threshold",
        }
        | changes
    )


def as_of_validity(fact: object, basis: str = "batch") -> dict[str, object]:
    basis_identity = (
        fact.scope.batch_id
        if basis == "batch"
        else fact.scope.baseline_identity
        if basis == "sealed_comparison"
        else fact.source_identity
    )
    return {
        "kind": "as_of",
        "basis": basis,
        "basis_identity": basis_identity,
        "as_of": fact.observed_at,
        "owner_dataset_id": fact.owner_dataset_id,
        "source_generation_id": fact.source_generation_id,
        "source_identity": fact.source_identity,
        "scope_identity": canonical_sha256(fact.scope),
        "event_time_start": fact.event_time_start,
        "event_time_end": fact.event_time_end,
        "available_at": fact.available_at,
    }


def service_with_fact(fact: object, *, with_witness: bool = True, stale: bool = False) -> object:
    h = (
        heartbeat(
            started_at=AT - timedelta(seconds=180),
            heartbeat_at=AT - timedelta(seconds=130),
            last_success_at=AT - timedelta(seconds=130),
        )
        if stale
        else heartbeat()
    )
    source = material(
        h,
        startup_witness=witness(h, sampled_at=h.started_at) if with_witness else None,
        metrics=(fact,),
    )
    inputs = {
        "legacy_services": (legacy(h),),
        "source_receipts": {h.service_id: source.source_receipt},
        "context": context(),
        "owner_generation_id": GENERATION,
        "observed_at": AT,
    }
    graph = core().build_runtime_health_detail_graph(materials=(source,), enabled=True, **inputs)
    return core().validate_runtime_health_detail_graph(graph, **inputs).services[0]


def test_complete_as_of_batch_survives_without_an_invented_expiry() -> None:
    original = metric()
    fact = metric(validity=as_of_validity(original))
    detail = service_with_fact(fact)
    visible = detail.metrics_at(AT + timedelta(days=10))[0]
    assert visible.value == 7
    assert visible.metric.observed_at == AT
    assert visible.metric.source_generation_id == original.source_generation_id
    assert visible.metric.fresh_until is None
    assert visible.metric.validity.basis_identity == original.scope.batch_id


@pytest.mark.parametrize("boundary,at_boundary", [("inclusive", 7), ("exclusive", None)])
def test_realtime_uses_declared_owner_boundary_at_consumer_read(
    boundary: str, at_boundary: int | None
) -> None:
    until = AT + timedelta(seconds=2)
    fact = metric(
        validity={
            "kind": "realtime",
            "rule_identity": SHA,
            "valid_until": until,
            "boundary": boundary,
        }
    )
    detail = service_with_fact(fact)
    assert detail.metrics_at(until - timedelta(microseconds=1))[0].value == 7
    assert detail.metrics_at(until)[0].value == at_boundary
    assert detail.metrics_at(until + timedelta(microseconds=1))[0].value is None
    assert detail.metrics_at(until + timedelta(microseconds=1))[0].reason_code == "metric_stale"


@pytest.mark.parametrize(
    "field",
    [
        "owner_dataset_id",
        "source_generation_id",
        "source_identity",
        "scope_identity",
        "basis_identity",
        "as_of",
        "event_time_start",
        "event_time_end",
        "available_at",
    ],
)
def test_as_of_declaration_rejects_detached_original_source_scope_or_window(field: str) -> None:
    fact = metric()
    declaration = as_of_validity(fact)
    declaration[field] = (
        declaration[field] + timedelta(microseconds=1)
        if isinstance(declaration[field], datetime)
        else "other_owner"
        if field == "owner_dataset_id"
        else "f" * 64
    )
    with pytest.raises(ValueError, match="as.of|basis"):
        metric(validity=declaration)


def test_as_of_never_bypasses_missing_startup_proof() -> None:
    fact = metric(validity=as_of_validity(metric()))
    detail = service_with_fact(fact, with_witness=False)
    assert detail.metrics_at(AT + timedelta(days=10))[0].value is None
    assert detail.metrics[0].reason_code == "startup_witness_missing"


def test_verified_as_of_survives_stale_liveness_without_making_service_available() -> None:
    args = {
        "event_time_start": AT - timedelta(seconds=180),
        "event_time_end": AT - timedelta(seconds=130),
        "available_at": AT - timedelta(seconds=130),
        "observed_at": AT - timedelta(seconds=130),
    }
    fact = metric(**args, validity=as_of_validity(metric(**args)))
    detail = service_with_fact(fact, stale=True)
    assert detail.availability == "unavailable"
    assert detail.heartbeat is None
    assert detail.reason_code == "heartbeat_stale"
    assert detail.metrics[0].value == 7


@pytest.mark.parametrize(
    "changes",
    [
        {
            "metric_id": "host_cpu",
            "owner_dataset_id": "ops_status",
            "unit": "ratio",
            "value": Decimal("0.5"),
            "scope": {"kind": "host", "host_name": "fixture-host", "boot_id": "fixture-boot"},
        },
        {
            "metric_id": "portfolio_exposure",
            "owner_dataset_id": "paper_portfolio",
            "unit": "fraction",
            "value": Decimal("0.6"),
            "scope": {
                "kind": "portfolio",
                "account_id": "fixture-account",
                "configuration_identity": SHA,
                "ledger_revision": 1,
            },
        },
    ],
)
def test_current_resources_cannot_be_relabeled_as_historical(changes: dict[str, object]) -> None:
    original = metric(**changes)
    with pytest.raises(ValueError, match="as.of|historical"):
        metric(**changes, validity=as_of_validity(original, "past_risk_decision"))


def test_as_of_orders_keep_the_exact_retained_window_and_empty_is_null() -> None:
    original = order_ratio()
    fact = order_ratio(validity=as_of_validity(original, "retained_window"))
    assert service_with_fact(fact).metrics_at(AT + timedelta(days=10))[0].value == Decimal("0.4")
    empty_scope = original.scope.model_dump() | {
        "retained_count": 0,
        "total_orders": 0,
        "has_more": False,
    }
    empty = order_ratio(
        scope=empty_scope,
        value=None,
        numerator=0,
        denominator=0,
        completeness="unavailable",
        verdict="unavailable",
        reason_code="empty_retained_window",
    )
    empty = order_ratio(
        **(empty.model_dump() | {"validity": as_of_validity(empty, "retained_window")})
    )
    assert service_with_fact(empty).metrics[0].value is None


def test_temporal_declarations_reject_conflicting_or_already_expired_rules() -> None:
    with pytest.raises(ValueError, match="expiry"):
        metric(validity=as_of_validity(metric()), fresh_until=AT)
    with pytest.raises(ValueError, match="expiry"):
        metric(
            fresh_until=AT + timedelta(seconds=1),
            validity={
                "kind": "realtime",
                "rule_identity": SHA,
                "valid_until": AT + timedelta(seconds=2),
                "boundary": "inclusive",
            },
        )
    with pytest.raises(ValueError, match="stale"):
        metric(
            validity={
                "kind": "realtime",
                "rule_identity": SHA,
                "valid_until": AT,
                "boundary": "exclusive",
            }
        )


def test_consumer_cutoff_cannot_precede_verified_owner_read() -> None:
    fact = metric(validity=as_of_validity(metric()))
    with pytest.raises(ValueError, match="cutoff"):
        service_with_fact(fact).metrics_at(AT - timedelta(microseconds=1))


def test_sealed_comparison_keeps_original_baseline_dates_as_of() -> None:
    scope = {
        "kind": "backtest_comparison",
        "account_id": "fixture-account",
        "strategy_version": "1.0",
        "parameter_fingerprint": SHA,
        "cost_identity": SHA,
        "calendar_identity": SHA,
        "baseline_identity": SHA,
        "comparison_dates": (AT.date(),),
        "baseline_dates": (AT.date(),),
        "complete": True,
    }
    original = metric(
        metric_id="return_comparison",
        owner_dataset_id="paper_portfolio",
        scope=scope,
        unit="band_position",
        value="inside",
        policy_identity=SHA,
        verdict="normal",
    )
    fact = core().RuntimeHealthMetric.model_validate(
        original.model_dump() | {"validity": as_of_validity(original, "sealed_comparison")}
    )
    assert service_with_fact(fact).metrics_at(AT + timedelta(days=10))[0].value == "inside"
    with pytest.raises(ValueError, match="comparison"):
        core().RuntimeHealthMetric.model_validate(
            fact.model_dump() | {"scope": scope | {"baseline_dates": ()}}
        )


def test_past_risk_fact_requires_original_policy_and_named_material() -> None:
    original = metric(
        metric_id="portfolio_risk",
        owner_dataset_id="paper_portfolio",
        scope={
            "kind": "portfolio",
            "account_id": "fixture-account",
            "configuration_identity": SHA,
            "ledger_revision": 1,
        },
        unit="risk_state",
        value="clear",
        policy_identity=SHA,
    )
    fact = core().RuntimeHealthMetric.model_validate(
        original.model_dump() | {"validity": as_of_validity(original, "past_risk_decision")}
    )
    assert service_with_fact(fact).metrics_at(AT + timedelta(days=10))[0].value == "clear"
    with pytest.raises(ValueError, match="as.of"):
        core().RuntimeHealthMetric.model_validate(fact.model_dump() | {"policy_identity": None})


@pytest.mark.parametrize("value", [True, -1, 7.0, "7", float("nan"), float("inf")])
def test_counts_reject_bool_negative_coercion_and_nonfinite(value: object) -> None:
    with pytest.raises((ValueError, ValidationError)):
        metric(value=value)


@pytest.mark.parametrize(
    "changes",
    [
        {"completeness": "unavailable", "value": 0, "verdict": "normal"},
        {"value": 7, "verdict": "normal"},
        {"scope": {"kind": "strategy_batch", "batch_id": SHA, "processed": False}},
        {"event_time_end": AT + timedelta(seconds=1)},
        {"available_at": START - timedelta(seconds=1)},
    ],
)
def test_unknown_idle_future_and_unconfigured_verdicts_cannot_be_green(changes: dict) -> None:
    with pytest.raises(ValueError):
        metric(**changes)


def test_unknown_is_null_and_actual_measured_zero_is_allowed() -> None:
    assert metric(value=0).value == 0
    unknown = metric(
        value=None,
        completeness="unavailable",
        verdict="unavailable",
        reason_code="no_processed_batch",
    )
    assert unknown.value is None


def test_exact_missing_codes_need_the_same_batch_expected_scope() -> None:
    args = {
        "metric_id": "minute_missing_codes",
        "owner_dataset_id": "market_minute",
        "scope": {
            "kind": "minute_batch",
            "batch_id": SHA,
            "expected_universe_identity": None,
            "scope_complete": False,
        },
        "value": 2,
    }
    with pytest.raises(ValueError, match="expected"):
        metric(**args)
    assert (
        metric(
            **(
                args
                | {
                    "value": None,
                    "completeness": "unavailable",
                    "verdict": "unavailable",
                    "reason_code": "expected_universe_missing",
                }
            )
        ).value
        is None
    )


def test_minute_delay_keeps_the_original_batch_clock_meaning() -> None:
    args = {
        "metric_id": "minute_delay",
        "owner_dataset_id": "market_minute",
        "scope": {"kind": "minute_batch", "batch_id": SHA, "scope_complete": True},
        "unit": "seconds",
        "event_time_end": AT - timedelta(seconds=2),
        "value": Decimal(2),
    }
    assert metric(**args).value == Decimal(2)
    with pytest.raises(ValueError, match="delay"):
        metric(**(args | {"value": Decimal(0)}))


def order_ratio(**changes: object) -> object:
    return metric(
        **(
            {
                "metric_id": "order_rejection_ratio",
                "owner_dataset_id": "paper_history",
                "scope": {
                    "kind": "retained_orders",
                    "account_id": "fixture-account",
                    "configuration_identity": SHA,
                    "ledger_revision": 3,
                    "retained_count": 5,
                    "total_orders": 20,
                    "has_more": True,
                },
                "unit": "ratio",
                "value": Decimal("0.4"),
                "numerator": 2,
                "denominator": 5,
            }
            | changes
        )
    )


def test_rejection_ratio_uses_retained_window_not_total_history() -> None:
    assert order_ratio().value == Decimal("0.4")
    with pytest.raises(ValueError, match="retained"):
        order_ratio(denominator=20, value=Decimal("0.1"))
    with pytest.raises(ValueError, match="ratio"):
        order_ratio(value=Decimal("0.5"))
    empty = {
        "kind": "retained_orders",
        "account_id": "fixture-account",
        "configuration_identity": SHA,
        "ledger_revision": 3,
        "retained_count": 0,
        "total_orders": 0,
        "has_more": False,
    }
    with pytest.raises(ValueError, match="empty"):
        order_ratio(scope=empty, value=Decimal(0), numerator=0, denominator=0)
    assert (
        order_ratio(
            scope=empty,
            value=None,
            numerator=0,
            denominator=0,
            completeness="unavailable",
            verdict="unavailable",
            reason_code="empty_retained_window",
        ).value
        is None
    )


def test_comparison_needs_exact_baseline_and_complete_matching_dates() -> None:
    scope = {
        "kind": "backtest_comparison",
        "account_id": "fixture-account",
        "strategy_version": "1.0",
        "parameter_fingerprint": SHA,
        "cost_identity": SHA,
        "calendar_identity": SHA,
        "baseline_identity": SHA,
        "comparison_dates": (AT.date(),),
        "baseline_dates": (AT.date(),),
        "complete": True,
    }
    args = {
        "metric_id": "return_comparison",
        "owner_dataset_id": "paper_portfolio",
        "scope": scope,
        "unit": "band_position",
        "value": "inside",
        "policy_identity": SHA,
        "verdict": "normal",
    }
    assert metric(**args).value == "inside"
    for wrong in ({"baseline_identity": None}, {"complete": False}, {"baseline_dates": ()}):
        with pytest.raises(ValueError, match="comparison"):
            metric(**(args | {"scope": scope | wrong}))


def test_metrics_are_bound_to_service_read_cutoff_and_unique() -> None:
    with pytest.raises(ValueError, match="unique"):
        material(heartbeat(), metrics=(metric(), metric()))
    with pytest.raises(ValueError, match="cutoff"):
        material(heartbeat(), metrics=(metric(observed_at=AT + timedelta(seconds=1)),))


def test_metric_age_uses_owner_expiry_and_keeps_unknown_null() -> None:
    h = heartbeat()
    old = metric(
        observed_at=AT - timedelta(seconds=1),
        available_at=AT - timedelta(seconds=1),
        event_time_end=AT - timedelta(seconds=1),
        fresh_until=AT - timedelta(seconds=1),
        verdict="normal",
        policy_identity=SHA,
    )
    source = material(h, startup_witness=witness(h), metrics=(old,))
    inputs = {
        "legacy_services": (legacy(h),),
        "source_receipts": {h.service_id: source.source_receipt},
        "context": context(),
        "owner_generation_id": GENERATION,
        "observed_at": AT,
    }
    graph = core().build_runtime_health_detail_graph(materials=(source,), enabled=True, **inputs)
    result = core().validate_runtime_health_detail_graph(graph, **inputs)
    visible = result.services[0].metrics[0]
    assert visible.value is None
    assert visible.verdict == "unavailable"
    assert visible.reason_code == "metric_stale"
    unknown = metric()
    source = material(h, startup_witness=witness(h), metrics=(unknown,))
    inputs["source_receipts"] = {h.service_id: source.source_receipt}
    graph = core().build_runtime_health_detail_graph(materials=(source,), enabled=True, **inputs)
    visible = core().validate_runtime_health_detail_graph(graph, **inputs).services[0].metrics[0]
    assert visible.value is None
    assert visible.reason_code == "metric_freshness_missing"


def test_ops_metric_must_match_the_real_current_ops_source() -> None:
    graph, inputs = publication()
    owner = original_ops_source()
    original = next(
        fact for fact in ops_runtime_health_metrics(owner) if fact.metric_id == "host_memory"
    )
    host_metric = metric(
        metric_id="host_memory",
        owner_dataset_id="ops_status",
        unit="bytes",
        source_generation_id="5" * 64,
        source_identity="6" * 64,
        scope={"kind": "host", "host_name": "fixture-host", "boot_id": "other-boot"},
        value=100,
        fresh_until=AT,
    )
    h = heartbeat()
    source = material(h, startup_witness=witness(h), metrics=(original,), ops_source=owner)
    source = source.model_copy(update={"metrics": (host_metric,)})
    inputs["source_receipts"] = {h.service_id: source.source_receipt}
    with pytest.raises(ValueError, match="Ops source"):
        core().build_runtime_health_detail_graph(materials=(source,), enabled=True, **inputs)


def test_distinct_slice_facts_keep_one_source_and_ignore_only_the_observer_clock() -> None:
    h = heartbeat()
    owner = original_ops_source()
    ops = context(
        sampled_at=AT,
        ops_source_generation_id=owner.generation_id,
        source_identity=canonical_sha256(owner),
    )
    facts = tuple(
        fact
        for fact in ops_runtime_health_metrics(owner)
        if fact.metric_id == "slice_memory"
        and fact.scope.slice_id in {"rquant-live.slice", "rquant-serving.slice"}
    )
    source = material(h, startup_witness=witness(h), metrics=facts, ops_source=owner)
    inputs = {
        "legacy_services": (legacy(h),),
        "source_receipts": {h.service_id: source.source_receipt},
        "context": ops,
        "owner_generation_id": GENERATION,
        "observed_at": AT,
    }
    graph = core().build_runtime_health_detail_graph(materials=(source,), enabled=True, **inputs)
    first = core().validate_runtime_health_detail_graph(graph, **inputs)
    assert len(first.services[0].metrics) == 2
    later = AT + timedelta(seconds=1)
    h = heartbeat(heartbeat_at=later, last_success_at=later)
    source = core().RuntimeHealthHeartbeatMaterial.from_read(
        control_root=Path("/fixture/control"),
        spec=spec(),
        heartbeat=RuntimeServiceHeartbeat.model_validate(
            h.model_dump(mode="python") | {"startup_witness": witness(h)}
        ),
        observed_at=later,
        startup_witness=witness(h),
        metrics=facts,
        ops_source=owner,
    )
    inputs |= {
        "legacy_services": (legacy(h, at=later),),
        "observed_at": later,
        "source_receipts": {h.service_id: source.source_receipt},
    }
    graph = core().build_runtime_health_detail_graph(materials=(source,), enabled=True, **inputs)
    second = core().validate_runtime_health_detail_graph(graph, **inputs)
    assert core().runtime_health_detail_state_identity(
        first
    ) == core().runtime_health_detail_state_identity(second)


def test_stale_current_context_and_stale_heartbeat_cannot_make_available_details() -> None:
    graph, inputs = publication()
    with pytest.raises(ValueError, match="context"):
        core().validate_runtime_health_detail_graph(graph, **(inputs | {"context": None}))
    with pytest.raises(ValueError, match="context"):
        core().validate_runtime_health_detail_graph(
            graph, **(inputs | {"context": context(sampled_at=AT - timedelta(seconds=121))})
        )
    later = AT + timedelta(seconds=121)
    h = heartbeat()
    source = core().RuntimeHealthHeartbeatMaterial.from_read(
        control_root=Path("/fixture/control"),
        spec=spec(),
        heartbeat=RuntimeServiceHeartbeat.model_validate(
            h.model_dump(mode="python") | {"startup_witness": witness(h)}
        ),
        observed_at=later,
        startup_witness=witness(h),
    )
    inputs |= {
        "context": context(sampled_at=later),
        "observed_at": later,
        "legacy_services": (legacy(h, at=later),),
        "source_receipts": {h.service_id: source.source_receipt},
    }
    graph = core().build_runtime_health_detail_graph(materials=(source,), enabled=True, **inputs)
    detail = core().validate_runtime_health_detail_graph(graph, **inputs).services[0]
    assert detail.heartbeat is None
    assert detail.reason_code == "heartbeat_stale"


def test_capacity_keeps_original_cell_context_service_and_combined_owner_limits() -> None:
    graph, _inputs = publication()
    existing = core().RuntimeHealthOwnerProjection(
        table_name="dashboard_summary",
        available_at=AT,
        rows=({"body": "x" * (7 * 1024 * 1024)},),
        owner_dataset_id="runtime_health",
        owner_generation_id=GENERATION,
    )
    with pytest.raises(ValueError, match="owner.*budget"):
        core().require_runtime_health_detail_capacity(graph, existing_projections=(existing,))
    values = graph.model_dump(mode="json")
    row = next(p for p in values["projections"] if p["table_name"] == "runtime_service_detail")[
        "rows"
    ][0]
    row["source_material_json"] = "x" * (64 * 1024)
    with pytest.raises(ValueError, match="cell.*budget"):
        core().RuntimeHealthDetailGraph.model_validate(values)
    values = graph.model_dump(mode="json")
    row = next(
        p for p in values["projections"] if p["table_name"] == "runtime_health_detail_context"
    )["rows"][0]
    row["boot_id"] = "中" * 700
    with pytest.raises(ValueError, match="context.*budget"):
        core().RuntimeHealthDetailGraph.model_validate(values)
    values = graph.model_dump(mode="json")
    detail = next(p for p in values["projections"] if p["table_name"] == "runtime_service_detail")
    row = detail["rows"][0]
    detail["rows"] = [row | {"service_id": f"service-{i}"} for i in range(501)]
    with pytest.raises(ValueError, match="row.*budget"):
        core().RuntimeHealthDetailGraph.model_validate(values)


def test_idle_clocks_counters_and_latency_do_not_change_material_state_identity() -> None:
    graph, inputs = publication()
    first = core().validate_runtime_health_detail_graph(graph, **inputs)
    later = AT + timedelta(seconds=1)
    h = heartbeat(
        heartbeat_at=later,
        last_success_at=later,
        total_successes=100,
        recent_step_durations_seconds=(0.3,),
        last_step_duration_seconds=0.3,
        p95_step_duration_seconds=0.3,
    )
    source = core().RuntimeHealthHeartbeatMaterial.from_read(
        control_root=Path("/fixture/control"),
        spec=spec(),
        heartbeat=RuntimeServiceHeartbeat.model_validate(
            h.model_dump(mode="python") | {"startup_witness": witness(h)}
        ),
        observed_at=later,
        startup_witness=witness(h),
    )
    second_inputs = inputs | {
        "observed_at": later,
        "legacy_services": (legacy(h, at=later),),
        "source_receipts": {h.service_id: source.source_receipt},
    }
    graph = core().build_runtime_health_detail_graph(
        materials=(source,), enabled=True, **second_inputs
    )
    second = core().validate_runtime_health_detail_graph(graph, **second_inputs)
    assert core().runtime_health_detail_state_identity(
        first
    ) == core().runtime_health_detail_state_identity(second)
    changed = heartbeat(observations={"processed_candidates": 8})
    source = material(changed, startup_witness=witness(changed))
    changed_inputs = inputs | {
        "legacy_services": (legacy(changed),),
        "source_receipts": {changed.service_id: source.source_receipt},
    }
    graph = core().build_runtime_health_detail_graph(
        materials=(source,), enabled=True, **changed_inputs
    )
    third = core().validate_runtime_health_detail_graph(graph, **changed_inputs)
    assert core().runtime_health_detail_state_identity(
        first
    ) != core().runtime_health_detail_state_identity(third)


def test_verified_service_view_observations_are_immutable() -> None:
    graph, values = publication()
    detail = core().validate_runtime_health_detail_graph(graph, **values)
    view = core().RuntimeHealthServiceView.from_verified(detail.services[0], detail.context)
    with pytest.raises(TypeError):
        view.observations["processed_candidates"] = 99
    assert view.observations["processed_candidates"] == 7
