from __future__ import annotations

import json
import shutil
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from rquant.daily_pipeline_command_manifest import (
    DailyExternalReceiptKey,
    DailyPipelineCommandManifest,
    DailyPipelineCommandManifestError,
    DailyPipelineStageCommand,
    ExternalStageReceipt,
    load_daily_pipeline_command_manifest,
)
from rquant.daily_pipeline_control import (
    DailyPipelineControlPlan,
    resolve_production_daily_storage_profile,
)
from rquant.daily_pipeline_ledger import (
    DailyPipelineLedger,
    DailyPipelineLedgerError,
    DailyPipelineMode,
    DailyPipelineStorageBinding,
    DailyPipelineStorageProfile,
    DailyRunSpec,
    DailyStageSpec,
    StageResult,
)
from rquant.daily_pipeline_orchestrator import DailyStageBudget, DailyStageExecutionContext
from rquant.daily_pipeline_report_authority import (
    DailyPipelineReportAuthorityError,
    DailyPipelineReportStore,
    DailyPipelineRunReport,
)
from rquant.strict_json import canonical_model_json_bytes

NOW = datetime(2026, 8, 4, 9, 0, tzinfo=UTC)
PROFILE_HASH = "d" * 64


def _profile(root: Path, mode: DailyPipelineMode) -> DailyPipelineStorageProfile:
    return DailyPipelineStorageProfile.create(
        root=root.resolve(),
        mode=mode,
        profile_hash=PROFILE_HASH,
    )


def _spec(mode: DailyPipelineMode) -> DailyRunSpec:
    return DailyRunSpec(
        mode=mode,
        trade_date=date(2026, 8, 4),
        source_generation_id="a" * 64,
        source_content_hash="b" * 64,
        command_manifest_hash="c" * 64,
        code_commit="e" * 40,
        profile_hash=PROFILE_HASH,
        stages=(DailyStageSpec(stage_id="capture"),),
    )


def test_shadow_and_production_have_distinct_native_identity_and_physical_paths(
    tmp_path: Path,
) -> None:
    shadow_profile = _profile(tmp_path, DailyPipelineMode.SHADOW)
    production_profile = _profile(tmp_path, DailyPipelineMode.PRODUCTION)
    shadow_spec = _spec(DailyPipelineMode.SHADOW)
    production_spec = _spec(DailyPipelineMode.PRODUCTION)

    assert shadow_spec.run_id != production_spec.run_id
    assert shadow_spec.input_identity != production_spec.input_identity
    assert shadow_profile.namespace_id != production_profile.namespace_id
    assert shadow_profile.state_path != production_profile.state_path
    assert shadow_profile.receipt_root != production_profile.receipt_root
    assert shadow_profile.report_root != production_profile.report_root
    assert shadow_profile.command_manifest_path != production_profile.command_manifest_path

    shadow_plan = DailyPipelineControlPlan.create(
        mode=DailyPipelineMode.SHADOW,
        run_spec=shadow_spec,
        command_manifest_hash=shadow_spec.command_manifest_hash,
        storage_profile=shadow_profile,
    )
    production_plan = DailyPipelineControlPlan.create(
        mode=DailyPipelineMode.PRODUCTION,
        run_spec=production_spec,
        command_manifest_hash=production_spec.command_manifest_hash,
        storage_profile=production_profile,
    )
    assert shadow_plan.plan_hash != production_plan.plan_hash


def test_production_storage_root_comes_from_current_immutable_runtime_profile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import runtime_deployment_profile as deployment_module

    runtime_root = tmp_path / "runtime"
    runtime_root.mkdir(mode=0o700)
    monkeypatch.setattr(
        deployment_module,
        "LINUX_PRODUCTION_RUNTIME_ROOT",
        runtime_root,
    )
    observed_roots: list[Path] = []
    monkeypatch.setattr(
        deployment_module,
        "load_current_runtime_deployment_profile",
        lambda root: (
            observed_roots.append(Path(root))
            or SimpleNamespace(
                runtime_mode="linux-production",
                production_runtime_root=str(deployment_module.LINUX_PRODUCTION_RUNTIME_ROOT),
                producer_commit="e" * 40,
                profile_id=PROFILE_HASH,
            )
        ),
    )

    profile = resolve_production_daily_storage_profile(
        expected_code_commit="e" * 40,
        expected_profile_hash=PROFILE_HASH,
    )

    assert observed_roots == [deployment_module.LINUX_PRODUCTION_RUNTIME_ROOT]
    assert profile.root == deployment_module.LINUX_PRODUCTION_RUNTIME_ROOT
    assert profile.mode is DailyPipelineMode.PRODUCTION


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("runtime_mode", "local-test", "Linux production"),
        ("production_runtime_root", "/tmp/outside", "fixed runtime root"),
        ("producer_commit", "f" * 40, "code commit"),
        ("profile_id", "f" * 64, "profile hash"),
    ],
)
def test_production_storage_profile_rejects_untrusted_runtime_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: str,
    message: str,
) -> None:
    from rquant import runtime_deployment_profile as deployment_module

    runtime_root = tmp_path / "runtime"
    runtime_root.mkdir(mode=0o700)
    monkeypatch.setattr(
        deployment_module,
        "LINUX_PRODUCTION_RUNTIME_ROOT",
        runtime_root,
    )
    payload = {
        "runtime_mode": "linux-production",
        "production_runtime_root": str(deployment_module.LINUX_PRODUCTION_RUNTIME_ROOT),
        "producer_commit": "e" * 40,
        "profile_id": PROFILE_HASH,
    }
    payload[field] = value
    monkeypatch.setattr(
        deployment_module,
        "load_current_runtime_deployment_profile",
        lambda _root: SimpleNamespace(**payload),
    )

    with pytest.raises(ValueError, match=message):
        resolve_production_daily_storage_profile(
            expected_code_commit="e" * 40,
            expected_profile_hash=PROFILE_HASH,
        )


def test_production_profile_cannot_open_shadow_ledger_or_manifest(tmp_path: Path) -> None:
    shadow_profile = _profile(tmp_path, DailyPipelineMode.SHADOW)
    production_profile = _profile(tmp_path, DailyPipelineMode.PRODUCTION)
    shadow_ledger = DailyPipelineLedger(
        storage_profile=shadow_profile,
        service_owner="daily-close",
    )
    lease = shadow_ledger.acquire_writer(
        owner="daily-close",
        now=NOW,
        lease_for=timedelta(minutes=1),
    )
    shadow_ledger.create_run(lease, _spec(DailyPipelineMode.SHADOW), now=NOW)

    DailyPipelineStorageBinding.open(production_profile, leaf="state").close()
    shutil.copy2(shadow_profile.state_path, production_profile.state_path)
    with pytest.raises(DailyPipelineLedgerError, match="storage profile"):
        DailyPipelineLedger(
            storage_profile=production_profile,
            service_owner="daily-close",
        )

    shadow_manifest = DailyPipelineCommandManifest(
        mode=DailyPipelineMode.SHADOW,
        storage_profile=shadow_profile,
        stages=(
            DailyPipelineStageCommand(
                stage_id="capture",
                adapter_identity="mode-isolation/v1",
                argv=("/bin/true",),
                receipt_root=shadow_profile.receipt_root,
                receipt_key_id="daily-stage-1",
            ),
        ),
    )
    shadow_profile.command_manifest_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    shadow_profile.command_manifest_path.write_text(
        json.dumps(
            shadow_manifest.model_dump(mode="json"),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    shadow_profile.command_manifest_path.chmod(0o600)

    with pytest.raises(DailyPipelineCommandManifestError, match="storage profile"):
        load_daily_pipeline_command_manifest(
            shadow_profile.command_manifest_path,
            expected_storage_profile=production_profile,
        )


class _AcceptingAuthority:
    def compare_and_advance(self, _report: DailyPipelineRunReport) -> int:
        return 1


def test_production_report_store_rejects_shadow_report(tmp_path: Path) -> None:
    shadow_profile = _profile(tmp_path, DailyPipelineMode.SHADOW)
    production_profile = _profile(tmp_path, DailyPipelineMode.PRODUCTION)
    shadow_report = DailyPipelineRunReport.create(
        mode=DailyPipelineMode.SHADOW,
        profile_hash=PROFILE_HASH,
        namespace_id=shadow_profile.namespace_id,
        run_id=_spec(DailyPipelineMode.SHADOW).run_id,
        plan_hash="f" * 64,
        trade_date=date(2026, 8, 4),
        receipt_ids=("1" * 64,),
        generated_at=NOW,
    )

    with pytest.raises(DailyPipelineReportAuthorityError, match="storage profile"):
        DailyPipelineReportStore(
            storage_profile=production_profile,
            authority=_AcceptingAuthority(),
        ).publish(shadow_report)


def test_production_adapter_rejects_signed_shadow_receipt(tmp_path: Path) -> None:
    profile = _profile(tmp_path, DailyPipelineMode.PRODUCTION)
    key = DailyExternalReceiptKey(key_id="daily-stage-1", secret=b"k" * 32)
    manifest = DailyPipelineCommandManifest(
        mode=profile.mode,
        storage_profile=profile,
        stages=(
            DailyPipelineStageCommand(
                stage_id="capture",
                adapter_identity="mode-isolation/v2",
                argv=("/bin/true",),
                receipt_root=profile.receipt_root,
                receipt_key_id=key.key_id,
            ),
        ),
    )
    ledger = DailyPipelineLedger(storage_profile=profile, service_owner="daily-close")
    lease = ledger.acquire_writer(
        owner="daily-close",
        now=NOW,
        lease_for=timedelta(minutes=1),
    )
    run = ledger.create_run(
        lease,
        _spec(DailyPipelineMode.PRODUCTION).model_copy(
            update={"command_manifest_hash": manifest.manifest_hash}
        ),
        now=NOW,
    )
    attempt = ledger.claim_next_for_run(lease, run.run_id, now=NOW)
    assert attempt is not None
    context = DailyStageExecutionContext(
        run=run,
        lease=lease,
        attempt=attempt,
        dependency_receipts=(),
        budget=DailyStageBudget(max_wall_seconds=10),
        observed_at=NOW,
    )
    adapter = manifest.adapter_for(
        "capture",
        trusted_receipt_key_provider=lambda key_id: key if key_id == key.key_id else None,
    )
    effect = adapter.prepare(context)
    shadow_receipt = ExternalStageReceipt.signed(
        mode=DailyPipelineMode.SHADOW,
        run_id=run.run_id,
        stage_id=attempt.stage_id,
        idempotency_key=effect.idempotency_key,
        fencing_token=attempt.fencing_token,
        lease_expiry=attempt.lease_expires_at,
        source_generation_id=run.spec.source_generation_id,
        source_content_hash=run.spec.source_content_hash,
        command_manifest_hash=manifest.manifest_hash,
        effect_id=effect.effect_id,
        result=StageResult(content_hash="1" * 64, evidence_hash="2" * 64),
        issued_at=NOW,
        signing_key=key,
    )
    receipt_path = Path(effect.receipt_locator)
    receipt_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    receipt_path.write_bytes(canonical_model_json_bytes(shadow_receipt))
    receipt_path.chmod(0o600)

    with pytest.raises(DailyPipelineCommandManifestError, match="prepared effect"):
        adapter.reconcile(context, effect)


def test_mode_is_required_by_current_run_and_report_contracts() -> None:
    run_payload = _spec(DailyPipelineMode.SHADOW).model_dump(mode="json")
    run_payload.pop("mode")
    with pytest.raises(ValueError):
        DailyRunSpec.model_validate(run_payload)

    report_payload = DailyPipelineRunReport.create(
        mode=DailyPipelineMode.SHADOW,
        profile_hash=PROFILE_HASH,
        namespace_id="9" * 64,
        run_id=_spec(DailyPipelineMode.SHADOW).run_id,
        plan_hash="f" * 64,
        trade_date=date(2026, 8, 4),
        receipt_ids=("1" * 64,),
        generated_at=NOW,
    ).model_dump(mode="json")
    report_payload.pop("mode")
    with pytest.raises(ValueError):
        DailyPipelineRunReport.model_validate(report_payload)


def test_production_mode_component_symlink_cannot_escape_declared_root(tmp_path: Path) -> None:
    declared = tmp_path / "declared"
    outside = tmp_path / "outside"
    declared.mkdir(mode=0o700)
    outside.mkdir(mode=0o700)
    (declared / DailyPipelineMode.PRODUCTION.value).symlink_to(
        outside,
        target_is_directory=True,
    )
    profile = _profile(declared, DailyPipelineMode.PRODUCTION)

    with pytest.raises(DailyPipelineLedgerError, match="unsafe|symlink|binding"):
        DailyPipelineLedger(storage_profile=profile, service_owner="daily-close")

    assert list(outside.iterdir()) == []


def test_production_namespace_symlink_cannot_escape_declared_root(tmp_path: Path) -> None:
    declared = tmp_path / "declared"
    outside = tmp_path / "outside"
    mode_root = declared / DailyPipelineMode.PRODUCTION.value
    mode_root.mkdir(mode=0o700, parents=True)
    outside.mkdir(mode=0o700)
    profile = _profile(declared, DailyPipelineMode.PRODUCTION)
    (mode_root / str(profile.namespace_id)).symlink_to(outside, target_is_directory=True)

    with pytest.raises(DailyPipelineLedgerError, match="unsafe|symlink|binding"):
        DailyPipelineLedger(storage_profile=profile, service_owner="daily-close")

    assert list(outside.iterdir()) == []


@pytest.mark.parametrize("leaf", ("state", "receipts", "reports", "control"))
def test_profile_leaf_symlink_cannot_escape_declared_root(
    tmp_path: Path,
    leaf: str,
) -> None:
    declared = tmp_path / "declared"
    outside = tmp_path / "outside"
    profile = _profile(declared, DailyPipelineMode.PRODUCTION)
    namespace = profile.namespace_root
    namespace.mkdir(mode=0o700, parents=True)
    (declared / DailyPipelineMode.PRODUCTION.value).chmod(0o700)
    outside.mkdir(mode=0o700)
    (namespace / leaf).symlink_to(outside, target_is_directory=True)

    with pytest.raises(DailyPipelineLedgerError, match="unsafe|unavailable|binding"):
        DailyPipelineStorageBinding.open(profile, leaf=leaf)  # type: ignore[arg-type]

    assert list(outside.iterdir()) == []


def test_sqlite_final_symlink_is_rejected_without_touching_external_file(
    tmp_path: Path,
) -> None:
    declared = tmp_path / "declared"
    outside = tmp_path / "outside"
    declared.mkdir(mode=0o700)
    outside.mkdir(mode=0o700)
    profile = _profile(declared, DailyPipelineMode.PRODUCTION)
    DailyPipelineStorageBinding.open(profile, leaf="state").close()
    external_database = outside / "external.sqlite3"
    external_database.write_bytes(b"unchanged")
    external_database.chmod(0o600)
    profile.state_path.symlink_to(external_database)

    with pytest.raises(DailyPipelineLedgerError, match="unsafe|unavailable|identity"):
        DailyPipelineLedger(storage_profile=profile, service_owner="daily-close")

    assert external_database.read_bytes() == b"unchanged"
    assert tuple(outside.iterdir()) == (external_database,)


def test_directory_replacement_during_sqlite_open_fails_without_outside_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import daily_pipeline_ledger as ledger_module

    declared = tmp_path / "declared"
    declared.mkdir(mode=0o700)
    outside = tmp_path / "outside"
    outside.mkdir(mode=0o700)
    profile = _profile(declared, DailyPipelineMode.PRODUCTION)
    real_connect = ledger_module.sqlite3.connect
    attacked = False

    def replacing_connect(database: object, *args: object, **kwargs: object):
        nonlocal attacked
        if not attacked:
            attacked = True
            state_root = profile.state_path.parent
            displaced = state_root.with_name("state-displaced")
            state_root.rename(displaced)
            state_root.symlink_to(outside, target_is_directory=True)
        return real_connect(database, *args, **kwargs)

    monkeypatch.setattr(ledger_module.sqlite3, "connect", replacing_connect)

    with pytest.raises(DailyPipelineLedgerError, match="changed|replaced|unsafe|binding"):
        DailyPipelineLedger(storage_profile=profile, service_owner="daily-close")

    assert attacked is True
    assert list(outside.iterdir()) == []
    assert not any(path.name.startswith("daily-pipeline.sqlite3") for path in outside.iterdir())
