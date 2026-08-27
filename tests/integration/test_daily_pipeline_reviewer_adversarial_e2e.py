"""Reviewer adversarial scenarios for the daily-close trust boundaries."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from rquant.daily_pipeline_ledger import (
    DailyPipelineLedger,
    DailyPipelineLedgerError,
    DailyPipelineMode,
    DailyPipelineStorageProfile,
    DailyRunSpec,
    DailyStageEffectIntent,
    DailyStageSpec,
    StageResult,
)
from rquant.lab_highwater_authority import (
    LabHighWaterAuthorityClient,
    LabHighWaterAuthorityConfig,
)
from rquant.strict_json import canonical_model_json_bytes

NOW = datetime(2026, 8, 3, 9, 0, tzinfo=UTC)
AUTHORITY_KEY_ID = "daily-report-authority"


def _storage_profile(
    root: Path,
    *,
    mode: DailyPipelineMode = DailyPipelineMode.SHADOW,
    profile_hash: str = "d" * 64,
) -> DailyPipelineStorageProfile:
    root.mkdir(parents=True, exist_ok=True)
    return DailyPipelineStorageProfile.create(
        root=root.resolve(),
        mode=mode,
        profile_hash=profile_hash,
    )


def _pythonpath() -> str:
    project_root = Path(__file__).resolve().parents[2]
    return os.pathsep.join(filter(None, (str(project_root / "src"), os.environ.get("PYTHONPATH"))))


def test_prepositioned_forged_external_receipt_is_rejected(tmp_path: Path) -> None:
    from rquant.daily_pipeline_command_manifest import (
        DailyExternalReceiptKey,
        DailyPipelineCommandManifest,
        DailyPipelineCommandManifestError,
        DailyPipelineStageCommand,
        ExternalStageReceipt,
    )
    from rquant.daily_pipeline_orchestrator import (
        DailyStageBudget,
        DailyStageExecutionContext,
    )

    trusted_key = DailyExternalReceiptKey(key_id="daily-stage-1", secret=b"t" * 32)
    rogue_key = DailyExternalReceiptKey(key_id="daily-stage-1", secret=b"r" * 32)
    storage_profile = _storage_profile(tmp_path)
    manifest = DailyPipelineCommandManifest(
        mode=storage_profile.mode,
        storage_profile=storage_profile,
        stages=(
            DailyPipelineStageCommand(
                stage_id="capture",
                adapter_identity="reviewer-adversarial/v1",
                argv=("/bin/true",),
                receipt_root=storage_profile.receipt_root,
                receipt_key_id=trusted_key.key_id,
            ),
        ),
    )
    ledger = DailyPipelineLedger(
        storage_profile=storage_profile,
        service_owner="daily-close",
    )
    lease = ledger.acquire_writer(
        owner="daily-close",
        now=NOW,
        lease_for=timedelta(minutes=1),
    )
    run = ledger.create_run(
        lease,
        DailyRunSpec(
            mode=DailyPipelineMode.SHADOW,
            trade_date=date(2026, 8, 3),
            source_generation_id="a" * 64,
            source_content_hash="b" * 64,
            command_manifest_hash=manifest.manifest_hash,
            code_commit="c" * 40,
            profile_hash="d" * 64,
            stages=(DailyStageSpec(stage_id="capture"),),
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
        trusted_receipt_key_provider=lambda key_id: (
            trusted_key if key_id == trusted_key.key_id else None
        ),
    )
    effect = adapter.prepare(context)
    forged = ExternalStageReceipt.signed(
        mode=DailyPipelineMode.SHADOW,
        run_id=run.run_id,
        stage_id="capture",
        idempotency_key=effect.idempotency_key,
        fencing_token=lease.fencing_token,
        lease_expiry=lease.expires_at,
        source_generation_id=run.spec.source_generation_id,
        source_content_hash=run.spec.source_content_hash,
        command_manifest_hash=manifest.manifest_hash,
        effect_id=effect.effect_id,
        result=StageResult(content_hash="e" * 64, evidence_hash="f" * 64),
        issued_at=NOW,
        signing_key=rogue_key,
    )
    receipt_path = Path(effect.receipt_locator)
    receipt_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    receipt_path.write_bytes(canonical_model_json_bytes(forged))
    receipt_path.chmod(0o600)

    with pytest.raises(DailyPipelineCommandManifestError, match="signature"):
        adapter.reconcile(context, effect)

    valid_values = {
        "mode": DailyPipelineMode.SHADOW,
        "run_id": run.run_id,
        "stage_id": "capture",
        "idempotency_key": effect.idempotency_key,
        "fencing_token": lease.fencing_token,
        "lease_expiry": lease.expires_at,
        "source_generation_id": run.spec.source_generation_id,
        "source_content_hash": run.spec.source_content_hash,
        "command_manifest_hash": manifest.manifest_hash,
        "effect_id": effect.effect_id,
        "result": StageResult(content_hash="e" * 64, evidence_hash="f" * 64),
        "issued_at": NOW,
    }
    foreign_values = {
        "run_id": "daily-foreign-receipt",
        "stage_id": "foreign",
        "idempotency_key": "1" * 64,
        "fencing_token": lease.fencing_token + 1,
        "lease_expiry": lease.expires_at + timedelta(seconds=1),
        "source_generation_id": "2" * 64,
        "source_content_hash": "3" * 64,
        "command_manifest_hash": "4" * 64,
        "effect_id": "5" * 64,
        "mode": DailyPipelineMode.PRODUCTION,
    }
    for field, foreign_value in foreign_values.items():
        values = {**valid_values, field: foreign_value}
        foreign = ExternalStageReceipt.signed(**values, signing_key=trusted_key)
        receipt_path.unlink()
        receipt_path.write_bytes(canonical_model_json_bytes(foreign))
        receipt_path.chmod(0o600)
        with pytest.raises(DailyPipelineCommandManifestError):
            adapter.reconcile(context, effect)


def test_same_run_id_rejects_replaced_command_manifest(tmp_path: Path) -> None:
    storage_profile = _storage_profile(tmp_path)
    ledger = DailyPipelineLedger(
        storage_profile=storage_profile,
        service_owner="daily-close",
    )
    lease = ledger.acquire_writer(
        owner="daily-close",
        now=NOW,
        lease_for=timedelta(minutes=1),
    )
    baseline = DailyRunSpec(
        mode=DailyPipelineMode.SHADOW,
        run_id="daily-reviewer-manifest-binding",
        trade_date=date(2026, 8, 3),
        source_generation_id="a" * 64,
        source_content_hash="b" * 64,
        command_manifest_hash="1" * 64,
        code_commit="c" * 40,
        profile_hash="d" * 64,
        stages=(DailyStageSpec(stage_id="capture"),),
    )
    replaced = baseline.model_copy(update={"command_manifest_hash": "2" * 64})
    ledger.create_run(lease, baseline, now=NOW)

    with pytest.raises(DailyPipelineLedgerError, match="immutable input identity"):
        ledger.create_run(lease, replaced, now=NOW + timedelta(seconds=1))

    first_auto = DailyRunSpec.model_validate({**baseline.model_dump(mode="python"), "run_id": None})
    second_auto = DailyRunSpec.model_validate(
        {
            **first_auto.model_dump(mode="python"),
            "run_id": None,
            "command_manifest_hash": "2" * 64,
        }
    )
    assert first_auto.run_id != second_auto.run_id
    assert first_auto.input_identity != second_auto.input_identity
    first_effect = DailyStageEffectIntent(
        mode=DailyPipelineMode.SHADOW,
        idempotency_key="6" * 64,
        command_manifest_hash="1" * 64,
        adapter_identity="manifest-binding/v1",
        receipt_locator=str((tmp_path / "first.json").resolve()),
    )
    second_effect = first_effect.model_copy(
        update={"effect_id": None, "command_manifest_hash": "2" * 64}
    )
    second_effect = DailyStageEffectIntent.model_validate(second_effect.model_dump(mode="python"))
    assert first_effect.effect_id != second_effect.effect_id


def _write_expiring_fence_driver(path: Path) -> None:
    path.write_text(
        r"""
from __future__ import annotations

import json
import os
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from rquant.daily_pipeline_command_manifest import (
    DailyPipelineCommandManifest,
    DailyPipelineStageCommand,
)
from rquant.daily_pipeline_ledger import (
    DailyPipelineLedger,
    DailyPipelineMode,
    DailyPipelineStorageProfile,
)
from rquant.daily_pipeline_orchestrator import (
    DailyPipelineDefinition,
    DailyPipelineOrchestrator,
    DailySourceIdentity,
    DailyStageBudget,
    DailyStageRuntimeSpec,
)


class Source:
    def resolve(self, run):
        return DailySourceIdentity(
            source_generation_id=run.spec.source_generation_id,
            source_content_hash=run.spec.source_content_hash,
        )


root = Path(sys.argv[2]).resolve()
storage_profile = DailyPipelineStorageProfile.create(
    root=root,
    mode=DailyPipelineMode.SHADOW,
    profile_hash="d" * 64,
)
child = "\n".join((
    "from datetime import UTC, datetime",
    "from pathlib import Path",
    "import os, time",
    "from rquant.daily_pipeline_command_manifest import "
    "publish_external_stage_receipt_from_environment",
    "from rquant.daily_pipeline_ledger import StageResult",
    f"marker = Path({str(root / 'external-effect')!r})",
    f"go = Path({str(root / 'publish-go')!r})",
    f"attempted = Path({str(root / 'publish-attempted')!r})",
    "marker.parent.mkdir(mode=0o700, parents=True, exist_ok=True)",
    "try:",
    "    fd = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)",
    "except FileExistsError:",
    "    first = False",
    "else:",
    "    first = True",
    "    os.write(fd, b'one'); os.fsync(fd); os.close(fd)",
    # The orphan holds its receipt until the test has established that the fence
    # is expired, rather than sleeping a constant and leaving the ordering to
    # whichever of the two finishes first on the day.
    "if first:",
    "    deadline = time.monotonic() + 120",
    "    while not go.exists() and time.monotonic() < deadline:",
    "        time.sleep(0.02)",
    "try:",
    "    publish_external_stage_receipt_from_environment(",
    "        StageResult(content_hash='a' * 64, evidence_hash='b' * 64),",
    "        issued_at=datetime.now(UTC),",
    "    )",
    "finally:",
    "    if first:",
    # Reporting the attempt is what lets the test assert on a refusal that has
    # provably already happened.
    "        attempted.write_text('done')",
))
manifest = DailyPipelineCommandManifest(
    mode=storage_profile.mode,
    storage_profile=storage_profile,
    stages=(
    DailyPipelineStageCommand(
        stage_id="capture",
        adapter_identity="expiring-fence-e2e/v1",
        argv=(sys.executable, "-c", child),
        receipt_root=storage_profile.receipt_root,
        receipt_key_id="daily-stage-1",
    ),
    ),
)
definition = DailyPipelineDefinition(stages=(
    DailyStageRuntimeSpec(
        stage_id="capture",
        budget=DailyStageBudget(max_wall_seconds=5),
    ),
))
orchestrator = DailyPipelineOrchestrator(
    ledger=DailyPipelineLedger(
        storage_profile=storage_profile,
        service_owner="daily-close",
    ),
    service_owner="daily-close",
    definition=definition,
    adapters=(manifest.adapter_for("capture"),),
    source_resolver=Source(),
    clock=lambda: datetime.now(UTC),
    lease_for=timedelta(milliseconds=int(os.environ["RQUANT_TEST_LEASE_MS"])),
)
action = sys.argv[1]
if action == "start":
    run = orchestrator.create_run(
        mode=DailyPipelineMode.SHADOW,
        trade_date=date(2026, 8, 3),
        source_generation_id="a" * 64,
        source_content_hash="b" * 64,
        command_manifest_hash=manifest.manifest_hash,
        code_commit="c" * 40,
        profile_hash="d" * 64,
    )
    (root / "run-id").write_text(run.run_id, encoding="utf-8")
    orchestrator.advance(run.run_id)
elif action == "recover":
    run_id = (root / "run-id").read_text(encoding="utf-8").strip()
    orchestrator.recover(run_id=run_id)
    outcome = orchestrator.advance(run_id)
    print(json.dumps({
        "outcome": None if outcome is None else outcome.model_dump(mode="json"),
        "status": orchestrator.status(run_id).model_dump(mode="json"),
    }))
else:
    raise SystemExit("unknown action")
""".lstrip(),
        encoding="utf-8",
    )


def test_killed_parent_and_expired_fence_cannot_commit_receipt(tmp_path: Path) -> None:
    driver = tmp_path / "expiring_fence_driver.py"
    _write_expiring_fence_driver(driver)
    # One lease, one requirement: be long enough. It used to be two, pulling in
    # opposite directions - the parent's had to be short (0.25s) so the fence
    # expired before the orphan's 0.8s sleep ran out, the recovery's long enough
    # to re-run the whole stage - and the short one lost. A lease is wall clock,
    # and before the child is even spawned the parent has to get through a
    # ledger recover, a claim, an effect prepare and a process-spec build; on
    # this laptop that window is about 6ms, but on a loaded CI runner one commit
    # inside it can stall past 250ms, at which point the pre-spawn heartbeat
    # raises `LeaseLost: writer lease is stale`, no child is ever started, and
    # the case fails on a marker that could not have appeared. The ordering the
    # case actually needs - fence expired *before* the orphan publishes - is now
    # a handshake below rather than a race between two constants, which is what
    # frees this lease to be sized for the one thing left to get right.
    #
    # The cost that varies between hosts is an interpreter start plus this
    # import, so that is what gets measured rather than guessed.
    started = time.monotonic()
    subprocess.run(
        (sys.executable, "-c", "import rquant.daily_pipeline_orchestrator"),
        check=True,
        capture_output=True,
        timeout=120,
    )
    stage_start_seconds = time.monotonic() - started
    lease_seconds = max(1.5, 6 * stage_start_seconds)
    environment = {
        **os.environ,
        "PYTHONPATH": _pythonpath(),
        "RQUANT_DAILY_EXTERNAL_RECEIPT_KEY_ID": "daily-stage-1",
        "RQUANT_DAILY_EXTERNAL_RECEIPT_SECRET": "s" * 32,
        "RQUANT_TEST_LEASE_MS": str(int(lease_seconds * 1000)),
    }
    parent = subprocess.Popen(
        (sys.executable, str(driver), "start", str(tmp_path)),
        cwd=tmp_path,
        env=environment,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    marker = tmp_path / "external-effect"
    deadline = time.monotonic() + 60
    while not marker.exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert marker.exists(), parent.communicate(timeout=2)[1]
    parent.kill()
    parent.wait(timeout=30)
    killed_at = time.monotonic()
    # Every heartbeat sets the fence to its own clock plus the lease, and a
    # reaped process cannot heartbeat again, so once this much wall clock has
    # passed since the kill the fence is expired whatever the parent got up to
    # before it died. No estimate of where in its lease it was is needed.
    while time.monotonic() < killed_at + lease_seconds + 0.5:
        time.sleep(0.02)
    storage_profile = _storage_profile(tmp_path)
    assert not list(storage_profile.receipt_root.glob("*.json"))

    # Only now is the orphan allowed to try, and it says when it has, so the
    # assertion below is about a refusal that happened rather than one inferred
    # from a sleep being long enough.
    (tmp_path / "publish-go").write_text("go", encoding="utf-8")
    attempted = tmp_path / "publish-attempted"
    deadline = time.monotonic() + 120
    while not attempted.exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert attempted.exists(), "the orphaned stage child never attempted its publication"
    assert not list(storage_profile.receipt_root.glob("*.json"))

    recovered = subprocess.run(
        (sys.executable, str(driver), "recover", str(tmp_path)),
        cwd=tmp_path,
        env=environment,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    assert recovered.returncode == 0, recovered.stderr
    result = __import__("json").loads(recovered.stdout)
    assert result["outcome"]["disposition"] == "succeeded"
    assert result["status"]["state"] == "succeeded"
    assert marker.read_bytes() == b"one"


def _write_report_authority_capability(root: Path) -> tuple[Path, bytes]:
    helper = Path(__file__).resolve().parents[2] / "deploy/libexec/rquant-lab-highwater-authority"
    openssl = shutil.which("openssl")
    if openssl is None:
        pytest.skip("openssl is required for the real Ed25519 authority fixture")
    private_key = root / "authority-private.pem"
    public_key = root / "authority-public.pem"
    subprocess.run(
        [openssl, "genpkey", "-algorithm", "ED25519", "-out", str(private_key)],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        [openssl, "pkey", "-in", str(private_key), "-pubout", "-out", str(public_key)],
        check=True,
        capture_output=True,
    )
    private_key.chmod(0o600)
    public_key.chmod(0o600)
    keys = root / "authority-keys.json"
    keys.write_text(
        __import__("json").dumps(
            {
                "schema_version": 3,
                "generation": 1,
                "previous_manifest_hash": "0" * 64,
                "active_key_id": AUTHORITY_KEY_ID,
                "active_private_key_path": str(private_key),
                "previous_public_keys": {},
            }
        ),
        encoding="utf-8",
    )
    keys.chmod(0o600)
    wrapper = root / "daily-report-authority-capability"
    wrapper.write_text(
        "#!/bin/sh\n"
        f"exec {sys.executable} {helper} "
        f"--state-root {root / 'independent-authority-state'} "
        f"--keys-file {keys}\n",
        encoding="utf-8",
    )
    wrapper.chmod(0o700)
    return wrapper.resolve(), public_key.read_bytes()


def test_deleted_local_report_and_ledger_cannot_roll_back_external_authority(
    tmp_path: Path,
) -> None:
    from rquant.daily_pipeline_report_authority import (
        DailyPipelineReportAuthorityClient,
        DailyPipelineReportAuthorityError,
        DailyPipelineReportStore,
        DailyPipelineRunReport,
    )

    capability, public_key = _write_report_authority_capability(tmp_path)
    storage_profile = _storage_profile(tmp_path / "daily-profile", profile_hash="2" * 64)
    high_water = LabHighWaterAuthorityClient(
        LabHighWaterAuthorityConfig(
            command=(str(capability),),
            stable_identity=(
                f"daily-pipeline-report:daily-close:shadow:{storage_profile.namespace_id}:v2"
            ),
            code_identity="1" * 40,
            profile_identity="2" * 64,
            active_key_id=AUTHORITY_KEY_ID,
            trusted_key_provider=lambda key_id: public_key if key_id == AUTHORITY_KEY_ID else None,
            allow_identity_rotation=True,
        )
    )
    authority = DailyPipelineReportAuthorityClient(high_water)
    report_root = storage_profile.report_root
    local_ledger = storage_profile.state_path
    from rquant.daily_pipeline_ledger import DailyPipelineStorageBinding

    DailyPipelineStorageBinding.open(storage_profile, leaf="state").close()
    local_ledger.write_bytes(b"local-state")
    local_ledger.chmod(0o600)
    store = DailyPipelineReportStore(
        storage_profile=storage_profile,
        authority=authority,
    )
    first = DailyPipelineRunReport.create(
        mode=storage_profile.mode,
        profile_hash=storage_profile.profile_hash,
        namespace_id=storage_profile.namespace_id,
        run_id="daily-" + "a" * 40,
        plan_hash="b" * 64,
        trade_date=date(2026, 8, 3),
        receipt_ids=("c" * 64,),
        generated_at=NOW,
    )
    store.publish(first)

    shutil.rmtree(report_root)
    report_root.mkdir(mode=0o700)
    local_ledger.unlink()
    local_ledger.write_bytes(b"rebuilt-local-state")
    replacement = DailyPipelineRunReport.create(
        mode=storage_profile.mode,
        profile_hash=storage_profile.profile_hash,
        namespace_id=storage_profile.namespace_id,
        run_id="daily-" + "d" * 40,
        plan_hash="e" * 64,
        trade_date=date(2026, 8, 3),
        receipt_ids=("f" * 64,),
        generated_at=NOW + timedelta(minutes=1),
    )

    with pytest.raises(DailyPipelineReportAuthorityError, match="binding changed"):
        store.publish(replacement)
    assert list(report_root.iterdir()) == []

    reopened = DailyPipelineReportStore(
        storage_profile=storage_profile,
        authority=authority,
    )
    with pytest.raises(DailyPipelineReportAuthorityError, match="monotonic"):
        reopened.publish(replacement)
    assert high_water.config.command == (str(capability),)
    assert not hasattr(authority, "root")
    assert not hasattr(store, "authority_root")
