"""Offline startup proof rules; no real Linux installation or live source claim."""

from __future__ import annotations

import json
from contextlib import nullcontext
from datetime import timedelta
from pathlib import Path
from threading import Event
from types import SimpleNamespace

import pytest

import rquant.runtime_health_authority as health
import rquant.runtime_service_main as main
from rquant.ops_status_serving import ops_status_source_result, publish_ops_status_snapshot
from rquant.runtime_builder_authority import RuntimeHealthPublisherSettings
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_deployment_profile import RuntimeDeploymentProfile
from rquant.runtime_service_control import (
    RuntimeServiceAlreadyRunningError,
    RuntimeServiceControl,
    RuntimeServicePlane,
    RuntimeStepResult,
    project_heartbeat,
)
from rquant.runtime_service_entrypoint import (
    RuntimeServiceKind,
    RuntimeServiceManifest,
    RuntimeServiceRegistry,
    run_runtime_service_manifest,
)
from tests.unit.test_ops_host_cpu import AT, BOOT, collector, signed_install
from tests.unit.test_ops_host_cpu import manifest as ops_manifest

COMMIT = "c" * 40


def ops_binding(tmp_path: Path, *, commit: str = COMMIT) -> tuple[object, object]:
    tmp_path.mkdir(mode=0o700, parents=True, exist_ok=True)
    tmp_path.chmod(0o700)
    public = signed_install(tmp_path)
    owner, _reads = collector(enabled=None)
    sample = owner.collect(ops_manifest())
    root = tmp_path / "ops-authority"
    publish_ops_status_snapshot(sample, root=root, producer_commit=commit, clock=lambda: AT)
    binding = health.RuntimeHealthOpsBinding(
        authority_root=root,
        install_manifest_path=tmp_path / "install.json",
        install_public_key_pem=public.decode(),
        producer_commit=commit,
    )
    return binding, sample


def trusted_provider(tmp_path: Path, **options: object) -> tuple[object, object]:
    binding, sample = ops_binding(tmp_path)
    provider = health.RuntimeHealthTrustedOpsProvider(
        binding,
        host_name=lambda: "fixture-host",
        proc_reader=lambda _path, limit: BOOT if limit == 128 else b"",
        **options,
    )
    return provider, sample


def service() -> RuntimeServiceManifest:
    return RuntimeServiceManifest(
        service_id="business-minute.source.v1",
        service_kind=RuntimeServiceKind.MARKET_MINUTE_SOURCE,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=0,
        stale_after_seconds=30,
        producer_commit=COMMIT,
        settings={},
    )


def health_manifest(binding: object | None) -> RuntimeServiceManifest:
    settings = {
        "authority_root": "/fixture/health",
        "sources": [
            {
                "control_root": "/fixture/control",
                "service_id": service().service_id,
                "plane": "live",
                "stale_after_seconds": 30,
                "producer_commit": COMMIT,
            }
        ],
    }
    if binding is not None:
        settings["ops_binding"] = binding.model_dump(mode="json")
    return RuntimeServiceManifest(
        service_id="runtime-health.all.v1",
        service_kind=RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER,
        plane=RuntimeServicePlane.SERVING,
        interval_seconds=10,
        stale_after_seconds=60,
        producer_commit=COMMIT,
        settings=settings,
    )


def profile(binding: object | None) -> RuntimeDeploymentProfile:
    manifests = (service(), health_manifest(binding))
    return RuntimeDeploymentProfile(
        producer_commit=COMMIT,
        manifests=manifests,
        capability_environment={
            service().service_id: ("TUSHARE_TOKEN_MAIN",),
            health_manifest(binding).service_id: (),
        },
    )


def test_publisher_settings_accepts_only_the_optional_typed_binding(tmp_path: Path) -> None:
    binding, _sample = ops_binding(tmp_path)
    settings = RuntimeHealthPublisherSettings.model_validate(
        dict(health_manifest(binding).settings)
    )
    assert settings.ops_binding == binding
    original = RuntimeHealthPublisherSettings.model_validate(dict(health_manifest(None).settings))
    assert original.ops_binding is None
    assert "ops_binding" not in original.model_dump(mode="json")
    raw = binding.model_dump(mode="json") | {"untrusted_extra": 1}
    with pytest.raises(ValueError):
        RuntimeHealthPublisherSettings.model_validate(
            dict(health_manifest(None).settings) | {"ops_binding": raw}
        )


def test_provider_reads_original_signed_source_once_and_binds_actual_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider, sample = trusted_provider(tmp_path)
    calls = []
    original = health.ServingSourceAuthorityReader.__call__

    def read(reader: object, cutoff: object) -> object:
        calls.append((reader.max_bytes, cutoff))
        return original(reader, cutoff)

    monkeypatch.setattr(health.ServingSourceAuthorityReader, "__call__", read)
    cutoff = AT + timedelta(seconds=1)
    context = provider(cutoff)
    source = ops_status_source_result(sample)
    assert calls == [(512 * 1024, cutoff)]
    assert context.host_name == sample.host_name
    assert context.boot_id == sample.boot_id
    assert context.manifest_digest == sample.manifest_digest
    assert context.ops_source_generation_id == source.generation_id
    assert context.source_identity == canonical_sha256(source)
    assert context.sampled_at == sample.sampled_at


@pytest.mark.parametrize(
    "age,available", [(0, True), (119.999, True), (120, False), (121, False), (-1, False)]
)
def test_trusted_adapter_enforces_strict_original_120_second_boundary(
    tmp_path: Path,
    age: float,
    available: bool,
) -> None:
    provider, _sample = trusted_provider(tmp_path)
    assert (provider(AT + timedelta(seconds=age)) is not None) is available


@pytest.mark.parametrize(
    "fault",
    ["signature", "host", "boot", "boot_changed", "host_changed", "producer", "missing", "unsafe"],
)
def test_unverified_or_unavailable_ops_cannot_become_a_context(tmp_path: Path, fault: str) -> None:
    binding, _sample = ops_binding(tmp_path)
    host = "other-host" if fault == "host" else "fixture-host"
    boots = iter((BOOT, b"87654321-1234-1234-1234-123456789abc\n"))
    hosts = iter(("fixture-host", "other-host"))
    if fault == "signature":
        path = binding.install_manifest_path
        document = json.loads(path.read_bytes())
        document["manifest"]["units"][0]["label"] = "tampered"
        path.write_text(json.dumps(document))
    if fault == "producer":
        binding = binding.model_copy(update={"producer_commit": "a" * 40})
    if fault == "missing":
        binding = binding.model_copy(update={"authority_root": tmp_path / "not-published"})
    if fault == "unsafe":
        binding.authority_root.chmod(0o777)
    provider = health.RuntimeHealthTrustedOpsProvider(
        binding,
        host_name=(lambda: next(hosts)) if fault == "host_changed" else lambda: host,
        proc_reader=(lambda *_args: next(boots))
        if fault == "boot_changed"
        else lambda *_args: b"87654321-1234-1234-1234-123456789abc\n" if fault == "boot" else BOOT,
    )
    assert provider(AT + timedelta(seconds=1)) is None


def test_entrypoint_captures_on_actual_business_start_and_carries_witness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider, _sample = trusted_provider(tmp_path / "source")
    capture_calls = []
    original = health.RuntimeHealthTrustedOpsProvider.__call__

    def capture(actual: object, at: object) -> object:
        capture_calls.append(at)
        return original(actual, at)

    monkeypatch.setattr(health.RuntimeHealthTrustedOpsProvider, "__call__", capture)
    registry = RuntimeServiceRegistry(ops_context_provider=provider)
    registry.register(
        RuntimeServiceKind.MARKET_MINUTE_SOURCE,
        lambda _manifest: lambda: RuntimeStepResult(processed_count=2),
    )
    result = run_runtime_service_manifest(
        service(),
        registry=registry,
        control_root=tmp_path / "control",
        stop_event=Event(),
        max_iterations=1,
        clock=lambda: AT + timedelta(seconds=1),
    )
    assert len(capture_calls) == 1
    witness = result.startup_witness
    assert witness.service_id == service().service_id
    assert witness.spec_fingerprint == service().service_spec.identity
    assert witness.run_id == result.run_id
    assert witness.generation == result.generation
    assert witness.started_at == result.started_at
    assert result.processed_count == 2
    raw = json.loads(
        RuntimeServiceControl._path_for(tmp_path / "control", service().service_spec).read_bytes()
    )
    assert raw["startup_witness"] == witness.model_dump(mode="json")
    assert "startup_witness" not in project_heartbeat(result).model_dump(mode="json")


def test_original_lock_prevents_any_second_capture_and_updates_keep_one_witness(
    tmp_path: Path,
) -> None:
    provider, _sample = trusted_provider(tmp_path / "source")
    root = tmp_path / "control"
    spec = service().service_spec
    first = RuntimeServiceControl(
        root, spec=spec, clock=lambda: AT + timedelta(seconds=1), ops_context_provider=provider
    )
    started = first.start()
    try:
        second = RuntimeServiceControl(
            root, spec=spec, clock=lambda: AT + timedelta(seconds=1), ops_context_provider=provider
        )
        with pytest.raises(RuntimeServiceAlreadyRunningError):
            second.start()
        assert first.record_success(RuntimeStepResult()).startup_witness == started.startup_witness
        assert (
            first.record_failure(ValueError("synthetic error")).startup_witness
            == started.startup_witness
        )
        assert first.stop(reason="fixture complete").startup_witness == started.startup_witness
    finally:
        if first._lock_descriptor >= 0:
            first.stop(reason="fixture cleanup")
    next_run = RuntimeServiceControl(
        root, spec=spec, clock=lambda: AT + timedelta(seconds=2), ops_context_provider=provider
    )
    try:
        restarted = next_run.start()
        assert restarted.generation == started.generation + 1
        assert restarted.startup_witness.run_id != started.run_id
    finally:
        next_run.stop(reason="fixture complete")


def test_default_off_never_repairs_an_old_heartbeat_or_adds_wire_fields(tmp_path: Path) -> None:
    control = RuntimeServiceControl(tmp_path, spec=service().service_spec, clock=lambda: AT)
    try:
        started = control.start()
        assert started.startup_witness is None
        assert "startup_witness" not in started.model_dump(mode="json")
        assert "startup_witness" not in project_heartbeat(started).model_dump(mode="json")
        assert control.record_success(RuntimeStepResult()).startup_witness is None
    finally:
        control.stop(reason="fixture complete")


def test_a_typed_context_callback_is_not_a_trusted_source(tmp_path: Path) -> None:
    with pytest.raises(TypeError, match="trusted Ops"):
        RuntimeServiceControl(
            tmp_path, spec=service().service_spec, ops_context_provider=lambda _at: None
        )


def test_profile_factory_checks_original_membership_before_any_capture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    binding, _sample = ops_binding(tmp_path / "source")
    observed = []
    frozen_profile = profile(binding)
    monkeypatch.setattr(
        main,
        "load_current_runtime_deployment_profile",
        lambda root: observed.append(root) or frozen_profile,
    )
    provider = main.build_runtime_health_ops_provider(tmp_path, manifest=service())
    assert isinstance(provider, health.RuntimeHealthTrustedOpsProvider)
    assert observed == [tmp_path]
    wrong = service().model_copy(update={"interval_seconds": 2})
    with pytest.raises(ValueError, match="profile.*manifest|manifest.*profile"):
        main.build_runtime_health_ops_provider(tmp_path, manifest=wrong)


def test_profile_factory_default_off_and_receipt_failure_do_not_restamp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        main, "load_current_runtime_deployment_profile", lambda _root: profile(None)
    )
    assert main.build_runtime_health_ops_provider(tmp_path, manifest=service()) is None

    def unsafe(_root: Path) -> object:
        raise ValueError("current profile receipt differs")

    monkeypatch.setattr(main, "load_current_runtime_deployment_profile", unsafe)
    assert main.build_runtime_health_ops_provider(tmp_path, manifest=service()) is None


def test_main_registry_wrapper_carries_the_same_lazy_provider(tmp_path: Path) -> None:
    provider, _sample = trusted_provider(tmp_path)
    registry = main.build_builtin_registry(
        runtime_capabilities={},
        ops_context_provider=provider,
        startup_degraded_reasons=("fixture_degraded",),
    )
    assert registry.ops_context_provider is provider


def test_main_runs_configured_business_service_through_original_factory_and_locked_start(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.runtime_service_builtin as builtin

    binding, _sample = ops_binding(tmp_path / "source")
    configured_profile = profile(binding)
    generation = "b" * 64
    directory = tmp_path / "generations" / generation / "manifests"
    directory.mkdir(parents=True)
    instance = "svc-" + canonical_sha256({"service_id": service().service_id})
    path = directory / (instance + ".json")
    path.write_text(service().model_dump_json())
    path.chmod(0o600)
    (tmp_path / "current").symlink_to(Path("generations") / generation)
    monkeypatch.setattr(main, "resolve_checkout_commit", lambda: COMMIT)
    monkeypatch.setattr(
        main, "load_current_runtime_deployment_profile", lambda _root: configured_profile
    )
    monkeypatch.setattr(main, "load_runtime_schema_service_bindings", lambda *_args, **_kwargs: ())
    monkeypatch.setattr(main, "load_systemd_runtime_capabilities", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(
        main,
        "StopSignalWatcher",
        lambda **_kwargs: nullcontext(SimpleNamespace(active=False, unarmed_signums=())),
    )
    monkeypatch.setattr(
        builtin,
        "market_minute_source_builder",
        lambda **_kwargs: lambda _manifest: lambda: RuntimeStepResult(processed_count=3),
    )
    original_provider_init = health.RuntimeHealthTrustedOpsProvider.__init__

    def fixture_physical_source(actual: object, selected: object) -> None:
        original_provider_init(
            actual, selected, host_name=lambda: "fixture-host", proc_reader=lambda *_args: BOOT
        )

    monkeypatch.setattr(health.RuntimeHealthTrustedOpsProvider, "__init__", fixture_physical_source)
    results = []

    def execute(manifest: RuntimeServiceManifest, **kwargs: object) -> object:
        result = run_runtime_service_manifest(
            manifest, **kwargs, clock=lambda: AT + timedelta(seconds=1)
        )
        results.append(result)
        return result

    monkeypatch.setattr(main, "run_runtime_service_manifest", execute)
    arguments = main.build_parser().parse_args(
        [
            "--manifest",
            str(tmp_path / "current" / "manifests" / path.name),
            "--control-root",
            str(tmp_path / "control"),
            "--expected-commit",
            COMMIT,
            "--expected-generation",
            generation,
            "--once",
        ]
    )
    assert main.run(arguments) == 0
    assert len(results) == 1
    assert results[0].processed_count == 3
    assert results[0].startup_witness.service_id == service().service_id
    assert results[0].startup_witness.run_id == results[0].run_id


def test_file_witness_must_name_exact_locked_run(tmp_path: Path) -> None:
    provider, _sample = trusted_provider(tmp_path / "source")
    control = RuntimeServiceControl(
        tmp_path / "control",
        spec=service().service_spec,
        clock=lambda: AT + timedelta(seconds=1),
        ops_context_provider=provider,
    )
    try:
        started = control.start()
        raw = started.model_dump(mode="json")
        raw["startup_witness"]["run_id"] = "a" * 64
        with pytest.raises(ValueError, match="witness.*run|run.*witness"):
            type(started).model_validate(raw)
    finally:
        control.stop(reason="fixture complete")


def test_profile_builder_adds_only_the_opted_in_binding(tmp_path: Path) -> None:
    from rquant.runtime_production_profile import (
        ProductionRuntimeProfileInputs,
        build_production_runtime_profile,
    )
    from tests.unit.test_runtime_production_profile import _inputs

    binding, _sample = ops_binding(tmp_path / "ops")
    original = _inputs(tmp_path / "profile")
    payload = original.model_dump(mode="python") | {
        "health_ops_binding": binding.model_dump(mode="json")
    }
    updated = ProductionRuntimeProfileInputs.model_validate(payload)
    candidate = build_production_runtime_profile(updated)
    configured = next(
        item
        for item in candidate.manifests
        if item.service_kind is RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER
    )
    assert configured.settings["ops_binding"] == binding.model_dump(mode="json")
    old = build_production_runtime_profile(original)
    legacy = next(
        item
        for item in old.manifests
        if item.service_kind is RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER
    )
    assert "ops_binding" not in legacy.settings


def test_health_owner_emits_same_read_details_and_references_signed_ops(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.runtime_health_details import (
        runtime_health_graph_from_projections,
        validate_runtime_health_detail_graph,
    )

    provider, _sample = trusted_provider(tmp_path / "source")
    at = AT + timedelta(seconds=1)
    control = RuntimeServiceControl(
        tmp_path / "control",
        spec=service().service_spec,
        clock=lambda: at,
        ops_context_provider=provider,
    )
    control.start()
    control.record_success(RuntimeStepResult(observations={"received": 7}))
    calls = []
    original = health._read_heartbeat

    def read(*args: object, **kwargs: object) -> object:
        calls.append(1)
        return original(*args, **kwargs)

    monkeypatch.setattr(health, "_read_heartbeat", read)
    try:
        result = health.RuntimeHealthSourceReader(
            sources=(
                health.RuntimeHealthControlSource(
                    control_root=tmp_path / "control", spec=service().service_spec
                ),
            ),
            serving_service_id="health.fixture.v1",
            details_enabled=True,
            ops_provider=provider,
        )(at)
        assert len(calls) == 1
        graph = runtime_health_graph_from_projections(
            result.payload.projections, owner_generation_id=result.generation_id
        )
        captured = provider.capture(at)
        verified = validate_runtime_health_detail_graph(
            graph,
            legacy_services=result.payload.runtime_services,
            source_receipts=result.payload.dashboard_summary_source_receipts,
            context=captured.context,
            owner_generation_id=result.generation_id,
            observed_at=at,
        )
        assert verified.services[0].heartbeat.observations == {"received": 7}
        assert verified.context.source_identity == canonical_sha256(captured.source)
        assert {m.metric.metric_id for m in verified.services[0].metrics} >= {
            "host_cpu",
            "host_memory",
        }
    finally:
        control.stop(reason="fixture complete")


def test_health_owner_never_repairs_old_heartbeat_witness(tmp_path: Path) -> None:
    from rquant.runtime_health_details import (
        runtime_health_graph_from_projections,
        validate_runtime_health_detail_graph,
    )

    provider, _sample = trusted_provider(tmp_path / "source")
    at = AT + timedelta(seconds=1)
    control = RuntimeServiceControl(
        tmp_path / "control", spec=service().service_spec, clock=lambda: at
    )
    control.start()
    try:
        result = health.RuntimeHealthSourceReader(
            sources=(
                health.RuntimeHealthControlSource(
                    control_root=tmp_path / "control", spec=service().service_spec
                ),
            ),
            serving_service_id="health.fixture.v1",
            details_enabled=True,
            ops_provider=provider,
        )(at)
        graph = runtime_health_graph_from_projections(
            result.payload.projections, owner_generation_id=result.generation_id
        )
        verified = validate_runtime_health_detail_graph(
            graph,
            legacy_services=result.payload.runtime_services,
            source_receipts=result.payload.dashboard_summary_source_receipts,
            context=provider(at),
            owner_generation_id=result.generation_id,
            observed_at=at,
        )
        assert verified.services[0].availability == "unavailable"
        assert verified.services[0].reason_code == "startup_witness_missing"
        assert (
            RuntimeServiceControl.read_heartbeat(
                tmp_path / "control", service().service_spec
            ).startup_witness
            is None
        )
    finally:
        control.stop(reason="fixture complete")


def test_complete_detail_profile_opts_in_real_owners_and_verified_ops(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.runtime_production_profile import (
        ProductionRuntimeProfileInputs,
        build_production_runtime_profile,
    )
    from tests.unit.test_runtime_production_profile import _inputs

    raw = _inputs(tmp_path / "profile").model_dump(mode="python")
    binding, _sample = ops_binding(tmp_path / "ops", commit=raw["producer_commit"])
    monkeypatch.setattr("socket.gethostname", lambda: "fixture-host")
    enabled = ProductionRuntimeProfileInputs.model_validate(
        raw | {"health_ops_binding": binding, "health_details_enabled": True}
    )
    result = build_production_runtime_profile(enabled)
    for item in result.manifests:
        if item.service_kind in {
            RuntimeServiceKind.MARKET_MINUTE_SOURCE,
            RuntimeServiceKind.STRATEGY_LIVE,
            RuntimeServiceKind.PAPER_BROKER,
        }:
            assert item.settings["health_metrics_enabled"] is True
        elif item.service_kind is RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER:
            assert item.settings["details_enabled"] is True
        elif item.service_kind is RuntimeServiceKind.SERVING_PUBLISHER:
            assert item.settings["health_ops_binding"] == binding.model_dump(mode="json")
            assert len(item.settings["source_authorities"]) == 6
            assert "ops_manifest_digest" not in item.settings
    with pytest.raises(ValueError):
        ProductionRuntimeProfileInputs.model_validate(raw | {"health_details_enabled": True})


def test_serving_uses_the_original_health_cutoff_and_signed_ops_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.serving_read_models import build_serving_read_models
    from tests.unit.test_runtime_serving_snapshot import _assembler

    provider, sample = trusted_provider(tmp_path / "source")
    at = AT + timedelta(seconds=1)
    control = RuntimeServiceControl(
        tmp_path / "control",
        spec=service().service_spec,
        clock=lambda: at,
        ops_context_provider=provider,
    )
    control.start()
    control.record_success(RuntimeStepResult(observations={"received": 7}))
    try:
        result = health.RuntimeHealthSourceReader(
            sources=(
                health.RuntimeHealthControlSource(
                    control_root=tmp_path / "control", spec=service().service_spec
                ),
            ),
            serving_service_id="health.fixture.v1",
            details_enabled=True,
            ops_provider=provider,
        )(at)
        # A later original owner generation must not replace the referenced read.
        publish_ops_status_snapshot(
            sample.model_copy(update={"sampled_at": AT + timedelta(seconds=10)}),
            root=provider.binding.authority_root,
            producer_commit=COMMIT,
            clock=lambda: AT + timedelta(seconds=10),
        )
        calls = []
        original = health.ServingSourceAuthorityReader.__call__

        def read(reader: object, cutoff: object) -> object:
            calls.append(cutoff)
            return original(reader, cutoff)

        monkeypatch.setattr(health.ServingSourceAuthorityReader, "__call__", read)
        assembler = _assembler(runtime_result=result)
        assembler.expected_ops_manifest_digest = None
        assembler.health_ops_reference_reader = provider.read_source
        assembled = assembler.assemble(at + timedelta(seconds=30))
        assert calls == [at]
        assert (
            assembled.read_model.runtime_health_details.context.ops_source_generation_id
            == ops_status_source_result(sample).generation_id
        )
        row = build_serving_read_models(assembled.read_model)["runtime_services"].iloc[0]
        assert json.loads(row["observations_json"]) == {"received": 7}
        assert (
            json.loads(row["detail_json"])["source_receipt"]
            == result.payload.dashboard_summary_source_receipts[service().service_id]
        )
        assembler.health_ops_reference_reader = lambda _cutoff: ops_status_source_result(sample)
        with pytest.raises(ValueError, match="original installed Ops verifier"):
            assembler.assemble(at + timedelta(seconds=31))
    finally:
        control.stop(reason="fixture complete")


def test_present_health_graph_refuses_a_different_ops_generation(tmp_path: Path) -> None:
    from tests.unit.test_runtime_serving_snapshot import _assembler

    provider, _sample = trusted_provider(tmp_path / "source")
    at = AT + timedelta(seconds=1)
    control = RuntimeServiceControl(
        tmp_path / "control",
        spec=service().service_spec,
        clock=lambda: at,
        ops_context_provider=provider,
    )
    control.start()
    try:
        result = health.RuntimeHealthSourceReader(
            sources=(
                health.RuntimeHealthControlSource(
                    control_root=tmp_path / "control", spec=service().service_spec
                ),
            ),
            serving_service_id="health.fixture.v1",
            details_enabled=True,
            ops_provider=provider,
        )(at)
        projections = []
        for projection in result.payload.projections:
            raw = projection.model_dump(mode="python")
            if projection.table_name == "runtime_health_detail_context":
                raw["rows"] = tuple(
                    dict(row) | {"ops_source_generation_id": "f" * 64} for row in projection.rows
                )
            projections.append(type(projection).model_validate(raw))
        changed = result.model_copy(
            update={
                "payload": result.payload.model_copy(update={"projections": tuple(projections)})
            }
        )
        assembler = _assembler(runtime_result=changed)
        assembler.expected_ops_manifest_digest = None
        assembler.health_ops_reference_reader = provider.read_source
        with pytest.raises(ValueError, match="context|current Ops"):
            assembler.assemble(at)
    finally:
        control.stop(reason="fixture complete")
