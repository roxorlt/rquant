"""Offline native domain fixture entry; Root owns API/process/browser execution.

Ops install keys and kernel inputs here are explicit synthetic fixtures. The
original signatures, authority read, startup file witness and business math run.
This is not Linux collection, a production installation or a trusted real host.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from threading import Event
from zoneinfo import ZoneInfo

import pytest

from rquant.feature_spool import FeatureBatchSpool
from rquant.ops_status_serving import publish_ops_status_snapshot
from rquant.runtime_builder_paper import paper_health_metrics_for_publication
from rquant.runtime_builder_serving import ServingRuntimeSnapshot
from rquant.runtime_builder_strategy import strategy_live_builder
from rquant.runtime_health_authority import (
    RuntimeHealthControlSource,
    RuntimeHealthOpsBinding,
    RuntimeHealthSourceReader,
    RuntimeHealthTrustedOpsProvider,
)
from rquant.runtime_service_control import RuntimeStepResult
from rquant.runtime_service_entrypoint import (
    RuntimeServiceKind,
    RuntimeServiceRegistry,
    run_runtime_service_manifest,
)
from rquant.runtime_serving_authority import (
    ServingSourceAuthorityPublisher,
    ServingSourceAuthorityReader,
)
from rquant.serving_contracts import ServingGenerationManifest
from rquant.serving_publisher import ServingPublisher
from rquant.serving_read_models import SERVING_TABLE_SPECS, build_serving_read_models
from tests.unit.test_ops_host_cpu import BOOT, collector, signed_install
from tests.unit.test_ops_host_cpu import manifest as ops_manifest
from tests.unit.test_runtime_builder_paper import _manifest as paper_manifest
from tests.unit.test_runtime_builder_strategy import (
    COMMIT,
    _publish,
    _publish_candidates,
)
from tests.unit.test_runtime_builder_strategy import (
    _manifest as strategy_manifest,
)
from tests.unit.test_runtime_serving_snapshot import NOW, _assembler
from tests.unit.test_web_health_layers import _multi_exposure_read


def build_native_health_input(work_root: Path, *, at: datetime = NOW) -> ServingRuntimeSnapshot:
    """Return actual assembled typed owner input; no process, socket or GUI."""
    work_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    work_root.chmod(0o700)
    ops_root = work_root / "ops"
    ops_root.mkdir(mode=0o700)
    public = signed_install(ops_root)
    original_collector, _ = collector()
    clocks = iter((at - timedelta(seconds=1), at, at))
    original_collector.clock = lambda: next(clocks)
    sample = original_collector.collect(ops_manifest())
    publish_ops_status_snapshot(
        sample, root=ops_root / "authority", producer_commit=COMMIT, clock=lambda: at
    )
    binding = RuntimeHealthOpsBinding(
        authority_root=ops_root / "authority",
        install_manifest_path=ops_root / "install.json",
        install_public_key_pem=public.decode(),
        producer_commit=COMMIT,
    )
    provider = RuntimeHealthTrustedOpsProvider(
        binding,
        host_name=lambda: "fixture-host",
        proc_reader=lambda _path, limit: BOOT if limit == 128 else b"",
    )

    with pytest.MonkeyPatch.context() as context:
        accounts, paper = _multi_exposure_read(work_root / "paper", context, publication_at=at)
    paper_authority = work_root / "paper-authority"
    ServingSourceAuthorityPublisher(
        root=paper_authority,
        producer_commit=COMMIT,
        dataset_id="paper_accounts",
        payload_kind="paper_accounts",
        clock=lambda: at,
    ).publish(paper)
    paper_reader = ServingSourceAuthorityReader(
        root=paper_authority,
        expected_producer_commit=COMMIT,
        expected_dataset_id="paper_accounts",
        expected_payload_kind="paper_accounts",
    )
    paper = paper_reader(at)
    paper_metrics = tuple(
        metric
        for account in accounts
        for metric in paper_health_metrics_for_publication(
            paper,
            account_id=account.configuration.binding.account_id,
            fallback_configuration_identity="f" * 64,
        )
    )

    (work_root / "strategy").mkdir(mode=0o700)
    strategy = strategy_manifest(work_root / "strategy")
    strategy = type(strategy).model_validate(
        strategy.model_dump(mode="python")
        | {"settings": dict(strategy.settings) | {"health_metrics_enabled": True}}
    )
    _publish(
        FeatureBatchSpool(work_root / "strategy/features"),
        sequence=0,
        available_at=at,
        source_event_time=at,
        decision_cutoff=at,
    )
    _publish_candidates(
        work_root / "strategy/candidates",
        trade_date=at.astimezone(ZoneInfo("Asia/Shanghai")).date(),
        captured_at=at,
        definition_fingerprint=strategy.settings["strategy_registration_fingerprint"],
        executable_fingerprint=strategy.settings["strategy_executable_fingerprint"],
        candidate_schema_fingerprint=strategy.settings["candidate_schema_fingerprint"],
    )
    registry = RuntimeServiceRegistry(ops_context_provider=provider)
    registry.register(RuntimeServiceKind.STRATEGY_LIVE, strategy_live_builder(clock=lambda: at))
    strategy_control = work_root / "strategy-control"
    run_runtime_service_manifest(
        strategy,
        registry=registry,
        control_root=strategy_control,
        stop_event=Event(),
        max_iterations=1,
        clock=lambda: at,
    )

    # Only transport the original complete publication's facts. No second
    # calculator or fake observations are added to the original business result.
    (work_root / "paper-service").mkdir(mode=0o700)
    paper_service = paper_manifest(work_root / "paper-service", RuntimeServiceKind.PAPER_BROKER)
    paper_step = RuntimeStepResult(
        source_generations={"paper_accounts": paper.generation_id}, health_metrics=paper_metrics
    )
    registry.register(RuntimeServiceKind.PAPER_BROKER, lambda _manifest: lambda: paper_step)
    paper_control = work_root / "paper-control"
    run_runtime_service_manifest(
        paper_service,
        registry=registry,
        control_root=paper_control,
        stop_event=Event(),
        max_iterations=1,
        clock=lambda: at,
    )
    health = RuntimeHealthSourceReader(
        sources=(
            RuntimeHealthControlSource(control_root=strategy_control, spec=strategy.service_spec),
            RuntimeHealthControlSource(control_root=paper_control, spec=paper_service.service_spec),
        ),
        serving_service_id="health.fixture.v1",
        details_enabled=True,
        ops_provider=provider,
    )(at)
    health_authority = work_root / "health-authority"
    ServingSourceAuthorityPublisher(
        root=health_authority,
        producer_commit=COMMIT,
        dataset_id="runtime_health",
        payload_kind="runtime_health",
        clock=lambda: at,
    ).publish(health)
    health_reader = ServingSourceAuthorityReader(
        root=health_authority,
        expected_producer_commit=COMMIT,
        expected_dataset_id="runtime_health",
        expected_payload_kind="runtime_health",
    )
    assembler = _assembler(paper_result=paper, runtime_result=health_reader(at))
    assembler.expected_ops_manifest_digest = None
    assembler.health_ops_reference_reader = provider.read_source
    # Root can substitute its genuine pinned readers for the unrelated baseline
    # owners before assembly when building a combined AI/collaboration fixture.
    assembler.paper_accounts_reader = paper_reader
    assembler.runtime_health_reader = health_reader
    return assembler.assemble(at)


def publish_native_health_fixture(
    serving_root: Path, work_root: Path, *, at: datetime = NOW
) -> ServingGenerationManifest:
    value = build_native_health_input(work_root, at=at)
    return ServingPublisher(
        serving_root, producer_commit=COMMIT, table_specs=SERVING_TABLE_SPECS
    ).publish(
        build_serving_read_models(value.read_model),
        watermarks=value.watermarks,
        source_generations=value.source_generations,
        built_at=at,
    )
