from __future__ import annotations

import os
import subprocess
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from rquant.daily_pipeline_ledger import (
    DailyPipelineLedger,
    DailyPipelineMode,
    DailyPipelineStorageProfile,
    DailyRunSpec,
    DailyStageSpec,
)
from rquant.daily_pipeline_orchestrator import DailyStageBudget, DailyStageExecutionContext
from rquant.strict_json import canonical_model_json_bytes

NOW = datetime(2026, 8, 3, 9, 0, tzinfo=UTC)


def _profile(tmp_path: Path) -> DailyPipelineStorageProfile:
    return DailyPipelineStorageProfile.create(
        root=tmp_path.resolve(),
        mode=DailyPipelineMode.SHADOW,
        profile_hash="d" * 64,
    )


def _context(
    command_manifest_hash: str,
    storage_profile: DailyPipelineStorageProfile,
):
    ledger = DailyPipelineLedger(
        storage_profile=storage_profile,
        service_owner="daily-close",
    )
    lease = ledger.acquire_writer(owner="daily-close", now=NOW, lease_for=timedelta(minutes=1))
    run = ledger.create_run(
        lease,
        DailyRunSpec(
            mode=storage_profile.mode,
            trade_date=date(2026, 8, 3),
            source_generation_id="a" * 64,
            source_content_hash="b" * 64,
            command_manifest_hash=command_manifest_hash,
            code_commit="c" * 40,
            profile_hash="d" * 64,
            stages=(DailyStageSpec(stage_id="capture"),),
        ),
        now=NOW,
    )
    attempt = ledger.claim_next_for_run(lease, run.run_id, now=NOW)
    assert attempt is not None
    return ledger, DailyStageExecutionContext(
        run=run,
        lease=lease,
        attempt=attempt,
        dependency_receipts=(),
        budget=DailyStageBudget(max_wall_seconds=10),
        observed_at=NOW,
    )


def test_reviewed_command_manifest_uses_env_bound_external_receipt(tmp_path: Path) -> None:
    from rquant.daily_pipeline_command_manifest import (
        DailyExternalReceiptKey,
        DailyPipelineCommandManifest,
        DailyPipelineStageCommand,
    )

    storage_profile = _profile(tmp_path)
    receipt_root = storage_profile.receipt_root
    child = (
        "from datetime import UTC, datetime; "
        "from rquant.daily_pipeline_command_manifest import "
        "publish_external_stage_receipt_from_environment; "
        "from rquant.daily_pipeline_ledger import StageResult; "
        "publish_external_stage_receipt_from_environment("
        "StageResult(content_hash='a'*64,evidence_hash='b'*64),"
        "issued_at=datetime(2026,8,3,9,0,tzinfo=UTC))"
    )
    key = DailyExternalReceiptKey(key_id="test-receipt", secret=b"s" * 32)
    manifest = DailyPipelineCommandManifest(
        mode=storage_profile.mode,
        storage_profile=storage_profile,
        stages=(
            DailyPipelineStageCommand(
                stage_id="capture",
                adapter_identity="test-command/v1",
                argv=(sys.executable, "-c", child),
                receipt_root=receipt_root,
                receipt_key_id=key.key_id,
            ),
        ),
    )
    adapter = manifest.adapter_for(
        "capture",
        trusted_receipt_key_provider=lambda key_id: key if key_id == key.key_id else None,
    )
    ledger, context = _context(manifest.manifest_hash, storage_profile)
    health = adapter.health(context)
    assert health.estimated_memory_mb == 64
    assert health.estimated_io_bytes == 0
    effect = adapter.prepare(context)
    ledger.prepare_effect(context.lease, context.attempt, effect, now=NOW)

    spec = adapter.command(context, effect)
    environment = {
        **os.environ,
        **spec.environment,
        "RQUANT_DAILY_EFFECT_ID": effect.effect_id,
        "RQUANT_DAILY_EFFECT_IDEMPOTENCY_KEY": effect.idempotency_key,
        "RQUANT_DAILY_EFFECT_RECEIPT_LOCATOR": effect.receipt_locator,
        "RQUANT_DAILY_RUN_ID": context.run.run_id,
        "RQUANT_DAILY_RUN_MODE": context.run.spec.mode.value,
        "RQUANT_DAILY_STAGE_ID": context.attempt.stage_id,
        "RQUANT_DAILY_LEDGER_PATH": str(ledger.path),
        "RQUANT_DAILY_STORAGE_ROOT": str(storage_profile.root),
        "RQUANT_DAILY_PROFILE_HASH": storage_profile.profile_hash,
        "RQUANT_DAILY_STORAGE_NAMESPACE_ID": str(storage_profile.namespace_id),
        "RQUANT_DAILY_SERVICE_OWNER": ledger.service_owner,
        "RQUANT_DAILY_COMMAND_MANIFEST_HASH": manifest.manifest_hash,
        "RQUANT_DAILY_FENCING_TOKEN": str(context.lease.fencing_token),
        "RQUANT_DAILY_EXTERNAL_RECEIPT_SECRET": key.secret.decode("ascii"),
    }
    completed = subprocess.run(spec.argv, env=environment, check=False)

    assert completed.returncode == 0
    assert adapter.reconcile(context, effect).model_dump() == {
        "content_hash": "a" * 64,
        "evidence_hash": "b" * 64,
    }


def test_manifest_loader_rejects_noncanonical_or_unsafe_input(tmp_path: Path) -> None:
    from rquant.daily_pipeline_command_manifest import (
        DailyPipelineCommandManifest,
        DailyPipelineCommandManifestError,
        DailyPipelineStageCommand,
        load_daily_pipeline_command_manifest,
    )
    from rquant.daily_pipeline_ledger import DailyPipelineStorageBinding

    storage_profile = _profile(tmp_path)
    manifest = DailyPipelineCommandManifest(
        mode=storage_profile.mode,
        storage_profile=storage_profile,
        stages=(
            DailyPipelineStageCommand(
                stage_id="capture",
                adapter_identity="test-command/v1",
                argv=("/bin/true",),
                receipt_root=storage_profile.receipt_root,
                receipt_key_id="test-receipt",
            ),
        ),
    )
    path = storage_profile.command_manifest_path
    DailyPipelineStorageBinding.open(storage_profile, leaf="control").close()
    path.write_bytes(canonical_model_json_bytes(manifest))
    path.chmod(0o600)

    assert (
        load_daily_pipeline_command_manifest(
            path.resolve(),
            expected_storage_profile=storage_profile,
        )
        == manifest
    )

    path.write_text('{"stages": []}', encoding="utf-8")
    path.chmod(0o600)
    with pytest.raises(DailyPipelineCommandManifestError, match="invalid"):
        load_daily_pipeline_command_manifest(
            path.resolve(),
            expected_storage_profile=storage_profile,
        )


def test_manifest_final_symlink_is_rejected_without_mutating_external_file(
    tmp_path: Path,
) -> None:
    from rquant.daily_pipeline_command_manifest import (
        DailyPipelineCommandManifestError,
        load_daily_pipeline_command_manifest,
    )
    from rquant.daily_pipeline_ledger import DailyPipelineStorageBinding

    storage_profile = _profile(tmp_path / "declared")
    storage_profile.root.mkdir(mode=0o700)
    outside = tmp_path / "outside"
    outside.mkdir(mode=0o700)
    external_manifest = outside / "manifest.json"
    external_manifest.write_bytes(b"external")
    external_manifest.chmod(0o600)
    DailyPipelineStorageBinding.open(storage_profile, leaf="control").close()
    storage_profile.command_manifest_path.symlink_to(external_manifest)

    with pytest.raises(DailyPipelineCommandManifestError, match="unsafe"):
        load_daily_pipeline_command_manifest(
            storage_profile.command_manifest_path,
            expected_storage_profile=storage_profile,
        )

    assert external_manifest.read_bytes() == b"external"
    assert tuple(outside.iterdir()) == (external_manifest,)
