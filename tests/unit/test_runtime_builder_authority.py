from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from rquant.experiment_registry import ExperimentRegistry
from rquant.lab_jobs import LabJobStore
from rquant.live_contracts import LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.market_minute_gateway import MarketMinuteGateway, MarketMinuteGatewayConfig
from rquant.paper_execution_constraints import PaperExecutionConstraintAuthority
from rquant.reference_data_registry import ReferenceDataset, ReferenceRecord, ReferenceRegistry
from rquant.runtime_artifact_terminal_lifecycle import (
    ProductionArtifactTerminalLifecycle,
    build_production_artifact_terminal_lifecycle,
)
from rquant.runtime_builder_authority import (
    lab_jobs_publisher_builder,
    paper_execution_constraint_publisher_builder,
    promotions_publisher_builder,
    runtime_health_publisher_builder,
)
from rquant.runtime_service_control import (
    RuntimeServiceControl,
    RuntimeServicePlane,
    RuntimeServiceSpec,
    RuntimeServiceStatus,
    RuntimeStepResult,
)
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.runtime_serving_authority import ServingSourceAuthorityReader
from rquant.runtime_serving_snapshot import RuntimeHealthPayload
from rquant.signal_contracts import SignalAction
from rquant.storage.duckdb import DuckDBStore

COMMIT = "a" * 40
DAY = date(2026, 8, 3)
CN = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 8, 3, 9, 31, 30, tzinfo=CN).astimezone(UTC)


def _terminal_lifecycle(
    tmp_path: Path,
    *,
    experiment_registry_path: Path | None = None,
) -> ProductionArtifactTerminalLifecycle:
    research_root = tmp_path / "research"
    research_root.mkdir(parents=True, mode=0o700, exist_ok=True)
    research_root.chmod(0o700)
    return build_production_artifact_terminal_lifecycle(
        runtime_root=tmp_path,
        experiment_registry_path=(
            experiment_registry_path or tmp_path / "research" / "experiments.sqlite3"
        ),
        clock=lambda: NOW,
    )


def _reference(path: Path) -> str:
    registry = ReferenceRegistry(path)
    for dataset, payload in (
        (ReferenceDataset.ST_STATUS, {"is_st": False}),
        (ReferenceDataset.SUSPENSION_STATUS, {"is_suspended": False}),
        (
            ReferenceDataset.LISTING_STATUS,
            {
                "market": "CN",
                "exchange": "SSE",
                "instrument_class": "EQUITY",
                "security_class": "A_SHARE",
                "status": "listed",
            },
        ),
        (
            ReferenceDataset.PRICE_LIMIT_REGIME,
            {"limit_up_price": 11.0, "limit_down_price": 9.0},
        ),
    ):
        registry.append(
            ReferenceRecord(
                dataset_id=dataset,
                key="600000.SH",
                effective_from=datetime(2026, 8, 3, 9, 25, tzinfo=CN).astimezone(UTC),
                effective_to=datetime(2026, 8, 3, 15, 5, tzinfo=CN).astimezone(UTC),
                revision=1,
                source="fixture",
                first_available_at=datetime(2026, 8, 3, 9, 20, tzinfo=CN).astimezone(UTC),
                payload=payload,
            )
        )
    return registry.publish(
        published_at=datetime(2026, 8, 3, 9, 24, tzinfo=CN).astimezone(UTC)
    ).generation_id


def _minute_spool(root: Path) -> LiveBatchSpool:
    spool = LiveBatchSpool(root)
    frame = pd.DataFrame(
        [
            {
                "ts_code": "600000.SH",
                "trade_time": datetime(2026, 8, 3, 9, 31, tzinfo=CN),
                "open": 10.0,
                "high": 10.0,
                "low": 10.0,
                "close": 10.0,
                "vol": 1_000.0,
                "amount": 10_000.0,
            }
        ]
    )
    MarketMinuteGateway(
        spool=spool,
        fetcher=lambda: frame,
        config=MarketMinuteGatewayConfig(
            producer_version="fixture-v1",
            producer_commit=COMMIT,
        ),
    ).capture_once(received_at=datetime(2026, 8, 3, 9, 31, 5, tzinfo=CN).astimezone(UTC))
    return spool


def test_paper_constraint_builder_publishes_from_readonly_pit_inputs(
    tmp_path: Path,
) -> None:
    reference_path = tmp_path / "research" / "reference.sqlite3"
    generation = _reference(reference_path)
    minute_root = tmp_path / "live" / "market-minute"
    _minute_spool(minute_root)
    authority_root = tmp_path / "authorities" / "paper-execution"
    authority_root.parent.mkdir(parents=True)
    manifest = RuntimeServiceManifest(
        service_id="paper-constraints",
        service_kind=RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=5,
        stale_after_seconds=30,
        producer_commit=COMMIT,
        settings={
            "minute_spool_root": str(minute_root),
            "reference_registry_path": str(reference_path),
            "authority_root": str(authority_root),
            "quote_ttl_seconds": 120,
        },
    )
    step = paper_execution_constraint_publisher_builder(clock=lambda: NOW)(manifest)

    first = step()
    second = step()

    assert first.input_sequence == 0
    assert first.output_sequence == 0
    minute_generation = (
        LiveBatchSpool(minute_root)
        .list_after(
            LiveChannel.MARKET_MINUTE,
            sequence=-1,
        )[0]
        .envelope.identity_sha256
    )
    assert first.source_generations == {
        "market_minute": minute_generation,
        "reference_slow": generation,
        "paper_execution_constraints": first.source_generations["paper_execution_constraints"],
    }
    assert second.source_generations == first.source_generations
    authority = PaperExecutionConstraintAuthority(
        root=authority_root,
        expected_producer_commit=COMMIT,
    )
    decision = authority.resolve(
        ts_code="600000.SH",
        trade_date=DAY,
        observed_at=NOW,
        action=SignalAction.B_INTENT,
    )
    assert decision.limit_locked is False
    assert decision.source_snapshot_ids["reference_slow"] == generation


def _running_control(root: Path, spec: RuntimeServiceSpec) -> None:
    control = RuntimeServiceControl(root, spec=spec, clock=lambda: NOW)
    control.start()
    control.record_success(RuntimeStepResult(input_sequence=3, output_sequence=2))
    control.stop(reason="fixture")
    heartbeat = RuntimeServiceControl.read_heartbeat(root, spec)
    assert heartbeat is not None
    RuntimeServiceControl._path_for(root, spec).write_text(
        heartbeat.model_copy(
            update={"status": RuntimeServiceStatus.RUNNING, "stopped_at": None, "stop_reason": None}
        ).model_dump_json(),
        encoding="utf-8",
    )


def test_runtime_health_builder_publishes_real_control_heartbeats(tmp_path: Path) -> None:
    source_root = tmp_path / "control" / "features" / "feature"
    source_spec = RuntimeServiceSpec(
        service_id="feature",
        plane=RuntimeServicePlane.LIVE,
        stale_after=timedelta(seconds=20),
        producer_commit=COMMIT,
    )
    _running_control(source_root, source_spec)
    authority_root = tmp_path / "control" / "authority-runtime-health"
    manifest = RuntimeServiceManifest(
        service_id="runtime-health",
        service_kind=RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER,
        plane=RuntimeServicePlane.SERVING,
        interval_seconds=5,
        stale_after_seconds=20,
        producer_commit=COMMIT,
        settings={
            "authority_root": str(authority_root),
            "sources": [
                {
                    "control_root": str(source_root),
                    "service_id": source_spec.service_id,
                    "plane": source_spec.plane.value,
                    "stale_after_seconds": source_spec.stale_after.total_seconds(),
                    "producer_commit": COMMIT,
                }
            ],
        },
    )

    result = runtime_health_publisher_builder(clock=lambda: NOW)(manifest)()

    loaded = ServingSourceAuthorityReader(
        root=authority_root,
        expected_producer_commit=COMMIT,
        expected_dataset_id="runtime_health",
        expected_payload_kind="runtime_health",
    )(NOW)
    assert result.output_sequence == loaded.sequence
    assert result.source_generations["runtime_health"] == loaded.generation_id
    assert isinstance(loaded.payload, RuntimeHealthPayload)
    assert tuple(service.service_id for service in loaded.payload.runtime_services) == ("feature",)


def test_lab_jobs_builder_publishes_from_research_owned_ledger(tmp_path: Path) -> None:
    ledger_path = tmp_path / "research" / "lab_jobs.sqlite3"
    LabJobStore(ledger_path).initialize()
    ledger_path.chmod(0o600)
    metadata_path = tmp_path / "research" / "research_ro.duckdb"
    with DuckDBStore(metadata_path):
        pass
    authority_root = tmp_path / "research" / "serving-authorities" / "lab-jobs"
    authority_root.parent.mkdir(parents=True)
    manifest = RuntimeServiceManifest(
        service_id="lab-jobs-authority",
        service_kind=RuntimeServiceKind.LAB_JOBS_PUBLISHER,
        plane=RuntimeServicePlane.RESEARCH,
        interval_seconds=30,
        stale_after_seconds=90,
        producer_commit=COMMIT,
        settings={
            "lab_jobs_path": str(ledger_path),
            "research_metadata_path": str(metadata_path),
            "authority_root": str(authority_root),
            "max_jobs": 10,
        },
    )

    lifecycle = _terminal_lifecycle(tmp_path)
    step = lab_jobs_publisher_builder(
        clock=lambda: NOW,
        open_artifact_terminal_lifecycle=lambda: lifecycle,
    )(manifest)
    result = step()
    step.close()

    loaded = ServingSourceAuthorityReader(
        root=authority_root,
        expected_producer_commit=COMMIT,
        expected_dataset_id="lab_jobs",
        expected_payload_kind="lab_jobs",
    )(NOW)
    assert result.output_sequence == loaded.sequence
    assert result.source_generations["lab_jobs"] == loaded.generation_id


def test_lab_jobs_builder_auto_projects_research_gate_metadata(tmp_path: Path) -> None:
    ledger_path = tmp_path / "research" / "lab_jobs.sqlite3"
    LabJobStore(ledger_path).initialize()
    ledger_path.chmod(0o600)
    ledger_path.parent.chmod(0o700)
    metadata_path = tmp_path / "research" / "research_ro.duckdb"
    with DuckDBStore(metadata_path):
        pass
    authority_root = tmp_path / "research" / "serving-authorities" / "lab-jobs"
    authority_root.parent.mkdir(parents=True)
    manifest = RuntimeServiceManifest(
        service_id="lab-jobs-authority",
        service_kind=RuntimeServiceKind.LAB_JOBS_PUBLISHER,
        plane=RuntimeServicePlane.RESEARCH,
        interval_seconds=30,
        stale_after_seconds=90,
        producer_commit=COMMIT,
        settings={
            "lab_jobs_path": str(ledger_path),
            "research_metadata_path": str(metadata_path),
            "authority_root": str(authority_root),
            "max_jobs": 10,
        },
    )

    lifecycle = _terminal_lifecycle(tmp_path)
    step = lab_jobs_publisher_builder(
        clock=lambda: NOW,
        open_artifact_terminal_lifecycle=lambda: lifecycle,
    )(manifest)
    step()
    step.close()
    loaded = ServingSourceAuthorityReader(
        root=authority_root,
        expected_producer_commit=COMMIT,
        expected_dataset_id="lab_jobs",
        expected_payload_kind="lab_jobs",
    )(NOW)

    assert tuple(item.table_name for item in loaded.payload.projections) == (
        "research_gate_metadata",
    )


def test_promotions_builder_publishes_from_research_owned_registry(tmp_path: Path) -> None:
    registry_path = tmp_path / "research" / "experiments.sqlite3"
    ExperimentRegistry(registry_path, managed_trust_root=tmp_path)
    authority_root = tmp_path / "research" / "serving-authorities" / "promotions"
    authority_root.parent.mkdir(parents=True)
    manifest = RuntimeServiceManifest(
        service_id="promotions-authority",
        service_kind=RuntimeServiceKind.PROMOTIONS_PUBLISHER,
        plane=RuntimeServicePlane.RESEARCH,
        interval_seconds=30,
        stale_after_seconds=90,
        producer_commit=COMMIT,
        settings={
            "experiment_registry_path": str(registry_path),
            "experiment_registry_managed_trust_root": str(tmp_path),
            "authority_root": str(authority_root),
            "max_decisions": 10,
        },
    )

    lifecycle = _terminal_lifecycle(
        tmp_path,
        experiment_registry_path=registry_path,
    )
    step = promotions_publisher_builder(
        clock=lambda: NOW,
        open_artifact_terminal_lifecycle=lambda: lifecycle,
    )(manifest)
    result = step()
    step.close()

    loaded = ServingSourceAuthorityReader(
        root=authority_root,
        expected_producer_commit=COMMIT,
        expected_dataset_id="promotions",
        expected_payload_kind="promotions",
    )(NOW)
    assert result.output_sequence == loaded.sequence
    assert result.source_generations["promotions"] == loaded.generation_id


def test_terminal_owner_authority_builders_open_the_injected_lifecycle(
    tmp_path: Path,
) -> None:
    ledger_path = tmp_path / "research" / "lab_jobs.sqlite3"
    LabJobStore(ledger_path).initialize()
    ledger_path.chmod(0o600)
    ledger_path.parent.chmod(0o700)
    metadata_path = tmp_path / "research" / "research_ro.duckdb"
    with DuckDBStore(metadata_path):
        pass
    lab_manifest = RuntimeServiceManifest(
        service_id="lab-jobs-authority",
        service_kind=RuntimeServiceKind.LAB_JOBS_PUBLISHER,
        plane=RuntimeServicePlane.RESEARCH,
        interval_seconds=30,
        stale_after_seconds=90,
        producer_commit=COMMIT,
        settings={
            "lab_jobs_path": str(ledger_path),
            "research_metadata_path": str(metadata_path),
            "authority_root": str(tmp_path / "research" / "serving-authorities" / "lab-jobs"),
            "max_jobs": 10,
        },
    )
    experiment_path = tmp_path / "research" / "experiments.sqlite3"
    ExperimentRegistry(experiment_path, managed_trust_root=tmp_path)
    lifecycle = _terminal_lifecycle(
        tmp_path,
        experiment_registry_path=experiment_path,
    )
    opened: list[object] = []

    def open_artifact_terminal_lifecycle() -> object:
        opened.append(lifecycle)
        return lifecycle

    promotions_manifest = RuntimeServiceManifest(
        service_id="promotions-authority",
        service_kind=RuntimeServiceKind.PROMOTIONS_PUBLISHER,
        plane=RuntimeServicePlane.RESEARCH,
        interval_seconds=30,
        stale_after_seconds=90,
        producer_commit=COMMIT,
        settings={
            "experiment_registry_path": str(experiment_path),
            "experiment_registry_managed_trust_root": str(tmp_path),
            "authority_root": str(tmp_path / "research" / "serving-authorities" / "promotions"),
            "max_decisions": 10,
        },
    )

    lab_step = lab_jobs_publisher_builder(
        clock=lambda: NOW,
        open_artifact_terminal_lifecycle=open_artifact_terminal_lifecycle,
    )(lab_manifest)
    promotions_step = promotions_publisher_builder(
        clock=lambda: NOW,
        open_artifact_terminal_lifecycle=open_artifact_terminal_lifecycle,
    )(promotions_manifest)

    assert callable(lab_step)
    assert callable(promotions_step)
    assert opened == [lifecycle, lifecycle]
    lab_step.close()
    promotions_step.close()


def test_promotions_builder_republishes_unchanged_registry_after_code_upgrade(
    tmp_path: Path,
) -> None:
    registry_path = tmp_path / "research" / "experiments.sqlite3"
    ExperimentRegistry(registry_path, managed_trust_root=tmp_path)
    authority_root = tmp_path / "research" / "serving-authorities" / "promotions"
    authority_root.parent.mkdir(parents=True)
    settings = {
        "experiment_registry_path": str(registry_path),
        "experiment_registry_managed_trust_root": str(tmp_path),
        "authority_root": str(authority_root),
        "max_decisions": 10,
    }
    old_manifest = RuntimeServiceManifest(
        service_id="promotions-authority",
        service_kind=RuntimeServiceKind.PROMOTIONS_PUBLISHER,
        plane=RuntimeServicePlane.RESEARCH,
        interval_seconds=30,
        stale_after_seconds=90,
        producer_commit=COMMIT,
        settings=settings,
    )
    old_lifecycle = _terminal_lifecycle(
        tmp_path,
        experiment_registry_path=registry_path,
    )
    old_step = promotions_publisher_builder(
        clock=lambda: NOW,
        open_artifact_terminal_lifecycle=lambda: old_lifecycle,
    )(old_manifest)
    old_result = old_step()
    old_step.close()
    next_commit = "b" * 40
    next_manifest = old_manifest.model_copy(update={"producer_commit": next_commit})

    first_lifecycle = _terminal_lifecycle(
        tmp_path,
        experiment_registry_path=registry_path,
    )
    first_step = promotions_publisher_builder(
        clock=lambda: NOW + timedelta(seconds=1),
        open_artifact_terminal_lifecycle=lambda: first_lifecycle,
    )(next_manifest)
    first = first_step()
    first_step.close()
    second_lifecycle = _terminal_lifecycle(
        tmp_path,
        experiment_registry_path=registry_path,
    )
    second_step = promotions_publisher_builder(
        clock=lambda: NOW + timedelta(seconds=2),
        open_artifact_terminal_lifecycle=lambda: second_lifecycle,
    )(next_manifest)
    second = second_step()
    second_step.close()

    loaded = ServingSourceAuthorityReader(
        root=authority_root,
        expected_producer_commit=next_commit,
        expected_dataset_id="promotions",
        expected_payload_kind="promotions",
        trusted_historical_producer_commits=(COMMIT,),
    )(NOW + timedelta(seconds=2))
    assert first.output_sequence == old_result.output_sequence == loaded.sequence
    assert second.source_generations == first.source_generations
    assert loaded.payload.promotions == ()
