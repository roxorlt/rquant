"""Process-bound exactly-once checks for the daily-close orchestrator.

These tests deliberately use an external receipt file rather than a Python
dictionary.  A new orchestrator instance must be able to finish a stage after
the previous owner died after the side effect was committed.
"""

from __future__ import annotations

import os
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from rquant.daily_pipeline_ledger import (
    DailyPipelineLedger,
    DailyPipelineMode,
    DailyPipelineStorageProfile,
    StageResult,
)

NOW = datetime(2026, 8, 3, 9, 0, tzinfo=UTC)


class _StaticSource:
    def resolve(self, run):
        from rquant.daily_pipeline_orchestrator import DailySourceIdentity

        return DailySourceIdentity(
            source_generation_id=run.spec.source_generation_id,
            source_content_hash=run.spec.source_content_hash,
        )


def _orchestrator(tmp_path: Path):
    from rquant.daily_pipeline_command_manifest import (
        DailyExternalReceiptKey,
        DailyPipelineCommandManifest,
        DailyPipelineStageCommand,
    )
    from rquant.daily_pipeline_orchestrator import (
        DailyPipelineDefinition,
        DailyPipelineOrchestrator,
        DailyStageBudget,
        DailyStageRuntimeSpec,
    )

    definition = DailyPipelineDefinition(
        stages=(
            DailyStageRuntimeSpec(
                stage_id="capture",
                budget=DailyStageBudget(max_wall_seconds=5),
            ),
        )
    )
    key = DailyExternalReceiptKey(key_id="effect-recovery", secret=b"s" * 32)
    storage_profile = DailyPipelineStorageProfile.create(
        root=tmp_path.resolve(),
        mode=DailyPipelineMode.SHADOW,
        profile_hash="0" * 64,
    )
    os.environ["RQUANT_DAILY_EXTERNAL_RECEIPT_KEY_ID"] = key.key_id
    os.environ["RQUANT_DAILY_EXTERNAL_RECEIPT_SECRET"] = key.secret.decode("ascii")
    child = (
        "from datetime import UTC,datetime; "
        "from rquant.daily_pipeline_command_manifest import "
        "publish_external_stage_receipt_from_environment; "
        "from rquant.daily_pipeline_ledger import StageResult; "
        "publish_external_stage_receipt_from_environment("
        "StageResult(content_hash='b'*64,evidence_hash='c'*64),"
        "issued_at=datetime(2026,8,3,9,0,tzinfo=UTC))"
    )
    manifest = DailyPipelineCommandManifest(
        mode=storage_profile.mode,
        storage_profile=storage_profile,
        stages=(
            DailyPipelineStageCommand(
                stage_id="capture",
                adapter_identity="receipt-process/v2",
                argv=(sys.executable, "-c", child),
                receipt_root=storage_profile.receipt_root,
                receipt_key_id=key.key_id,
            ),
        )
    )
    orchestrator = DailyPipelineOrchestrator(
        ledger=DailyPipelineLedger(
            storage_profile=storage_profile,
            service_owner="daily-close",
        ),
        service_owner="daily-close",
        definition=definition,
        adapters=(manifest.adapter_for("capture"),),
        source_resolver=_StaticSource(),
        clock=lambda: NOW,
        lease_for=timedelta(seconds=2),
    )
    return orchestrator, manifest.manifest_hash


def test_subprocess_stage_persists_receipt_then_finalizes(tmp_path: Path) -> None:
    orchestrator, manifest_hash = _orchestrator(tmp_path)
    run = orchestrator.create_run(
        mode=DailyPipelineMode.SHADOW,
        trade_date=date(2026, 8, 3),
        source_generation_id="d" * 64,
        source_content_hash="e" * 64,
        command_manifest_hash=manifest_hash,
        code_commit="f" * 40,
        profile_hash="0" * 64,
        now=NOW,
    )

    outcome = orchestrator.advance(run.run_id, now=NOW)

    assert outcome is not None
    assert outcome.disposition == "succeeded"
    assert orchestrator.status(run.run_id).state.value == "succeeded"


def test_immutable_external_receipt_recovers_after_ledger_gap_without_reexecution(
    tmp_path: Path,
) -> None:
    orchestrator, manifest_hash = _orchestrator(tmp_path)
    run = orchestrator.create_run(
        mode=DailyPipelineMode.SHADOW,
        trade_date=date(2026, 8, 3),
        source_generation_id="d" * 64,
        source_content_hash="e" * 64,
        command_manifest_hash=manifest_hash,
        code_commit="f" * 40,
        profile_hash="0" * 64,
        now=NOW,
    )
    lease = orchestrator.ledger.acquire_writer(
        owner="daily-close", now=NOW, lease_for=timedelta(seconds=2)
    )
    attempt = orchestrator.ledger.claim_next_for_run(lease, run.run_id, now=NOW)
    assert attempt is not None
    context = orchestrator._context_for(run, lease, attempt, NOW)
    effect = orchestrator.adapters["capture"].prepare(context)
    orchestrator.ledger.prepare_effect(lease, attempt, effect, now=NOW)
    from rquant.daily_pipeline_command_manifest import (
        publish_external_stage_receipt_from_environment,
    )

    environment = {
        "RQUANT_DAILY_EFFECT_ID": effect.effect_id,
        "RQUANT_DAILY_EFFECT_IDEMPOTENCY_KEY": effect.idempotency_key,
        "RQUANT_DAILY_EFFECT_RECEIPT_LOCATOR": effect.receipt_locator,
        "RQUANT_DAILY_RUN_ID": run.run_id,
        "RQUANT_DAILY_RUN_MODE": run.spec.mode.value,
        "RQUANT_DAILY_STAGE_ID": attempt.stage_id,
        "RQUANT_DAILY_LEDGER_PATH": str(orchestrator.ledger.path),
        "RQUANT_DAILY_STORAGE_ROOT": str(orchestrator.ledger.storage_profile.root),
        "RQUANT_DAILY_PROFILE_HASH": orchestrator.ledger.storage_profile.profile_hash,
        "RQUANT_DAILY_STORAGE_NAMESPACE_ID": str(
            orchestrator.ledger.storage_profile.namespace_id
        ),
        "RQUANT_DAILY_SERVICE_OWNER": orchestrator.ledger.service_owner,
        "RQUANT_DAILY_COMMAND_MANIFEST_HASH": manifest_hash,
        "RQUANT_DAILY_FENCING_TOKEN": str(lease.fencing_token),
    }
    previous = {key: os.environ.get(key) for key in environment}
    os.environ.update(environment)
    try:
        publish_external_stage_receipt_from_environment(
            StageResult(content_hash="b" * 64, evidence_hash="c" * 64),
            issued_at=NOW,
        )
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    resumed, resumed_manifest_hash = _orchestrator(tmp_path)
    assert resumed_manifest_hash == manifest_hash
    recovery = resumed.recover(now=NOW + timedelta(seconds=1))

    assert recovery.finalized_receipt_ids
    assert resumed.status(run.run_id).state.value == "succeeded"


def test_advance_rejects_source_resolver_revision_before_external_prepare(tmp_path: Path) -> None:
    orchestrator, manifest_hash = _orchestrator(tmp_path)
    run = orchestrator.create_run(
        mode=DailyPipelineMode.SHADOW,
        trade_date=date(2026, 8, 3),
        source_generation_id="d" * 64,
        source_content_hash="e" * 64,
        command_manifest_hash=manifest_hash,
        code_commit="f" * 40,
        profile_hash="0" * 64,
        now=NOW,
    )

    class _RevisedSource:
        def resolve(self, _run):
            from rquant.daily_pipeline_orchestrator import DailySourceIdentity

            return DailySourceIdentity(
                source_generation_id="1" * 64,
                source_content_hash="2" * 64,
            )

    object.__setattr__(orchestrator, "_source_resolver", _RevisedSource())

    try:
        orchestrator.advance(run.run_id, now=NOW)
    except ValueError as exc:
        assert "source identity" in str(exc)
    else:
        raise AssertionError("revised spool current identity was accepted")
