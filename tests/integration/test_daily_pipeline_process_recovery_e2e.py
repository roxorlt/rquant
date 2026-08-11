"""Real process crash/restart evidence for the daily-close external effect boundary."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

from rquant.daily_pipeline_ledger import DailyPipelineMode, DailyPipelineStorageProfile


def _write_driver(path: Path) -> None:
    path.write_text(
        """
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


def build(root):
    storage_profile = DailyPipelineStorageProfile.create(
        root=root,
        mode=DailyPipelineMode.SHADOW,
        profile_hash="d" * 64,
    )
    definition = DailyPipelineDefinition(stages=(
        DailyStageRuntimeSpec(stage_id="capture", budget=DailyStageBudget(max_wall_seconds=5)),
    ))
    marker = root / "external-effect-started"
    child = "\\n".join((
        "from datetime import UTC, datetime",
        "from pathlib import Path",
        "import os, time",
        "from rquant.daily_pipeline_command_manifest import "
        "publish_external_stage_receipt_from_environment",
        "from rquant.daily_pipeline_ledger import StageResult",
        "assert len(os.environ['RQUANT_DAILY_EFFECT_IDEMPOTENCY_KEY']) == 64",
        "assert int(os.environ['RQUANT_DAILY_FENCING_TOKEN']) >= 1",
        f"marker = Path({str(marker)!r})",
        "marker.parent.mkdir(mode=0o700, parents=True, exist_ok=True)",
        "fd = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)",
        "os.write(fd, b'one'); os.fsync(fd); os.close(fd)",
        "time.sleep(0.75)",
        "publish_external_stage_receipt_from_environment(",
        "    StageResult(content_hash='a' * 64, evidence_hash='b' * 64),",
        "    issued_at=datetime.now(UTC),",
        ")",
    ))
    manifest = DailyPipelineCommandManifest(
        mode=storage_profile.mode,
        storage_profile=storage_profile,
        stages=(
        DailyPipelineStageCommand(
            stage_id="capture",
            adapter_identity="process-e2e/v2",
            argv=(sys.executable, "-c", child),
            receipt_root=storage_profile.receipt_root,
            receipt_key_id="daily-stage-1",
        ),
        ),
    )
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
        lease_for=timedelta(seconds=2),
    )
    return orchestrator, manifest.manifest_hash


root = Path(sys.argv[2]).resolve()
action = sys.argv[1]
orchestrator, manifest_hash = build(root)
if action == "start":
    run = orchestrator.create_run(
        mode=DailyPipelineMode.SHADOW,
        trade_date=date(2026, 8, 3),
        source_generation_id="a" * 64,
        source_content_hash="b" * 64,
        command_manifest_hash=manifest_hash,
        code_commit="c" * 40,
        profile_hash="d" * 64,
    )
    (root / "run-id").write_text(run.run_id, encoding="utf-8")
    orchestrator.advance(run.run_id)
    print(json.dumps(orchestrator.status(run.run_id).model_dump(mode="json")))
elif action == "recover":
    recovery = orchestrator.recover()
    run_id = (root / "run-id").read_text(encoding="utf-8").strip()
    print(json.dumps({
        "recovery": recovery.model_dump(mode="json"),
        "status": orchestrator.status(run_id).model_dump(mode="json"),
    }))
else:
    raise SystemExit("unknown action")
""".lstrip(),
        encoding="utf-8",
    )


def test_killed_parent_recovers_external_receipt_without_second_side_effect(tmp_path: Path) -> None:
    driver = tmp_path / "daily_process_driver.py"
    _write_driver(driver)
    project_root = Path(__file__).resolve().parents[2]
    environment = {
        **os.environ,
        "RQUANT_DAILY_EXTERNAL_RECEIPT_KEY_ID": "daily-stage-1",
        "RQUANT_DAILY_EXTERNAL_RECEIPT_SECRET": "s" * 32,
        "PYTHONPATH": os.pathsep.join(
            filter(None, (str(project_root / "src"), os.environ.get("PYTHONPATH")))
        ),
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
    marker = tmp_path / "external-effect-started"
    deadline = time.monotonic() + 10
    while not marker.exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert marker.exists(), parent.communicate(timeout=2)[1]

    # Kill only the orchestrator parent.  The adapter child is in its own
    # process session and must finish the receipt independently.
    parent.kill()
    parent.wait(timeout=5)

    storage_profile = DailyPipelineStorageProfile.create(
        root=tmp_path.resolve(),
        mode=DailyPipelineMode.SHADOW,
        profile_hash="d" * 64,
    )
    receipt_dir = storage_profile.receipt_root
    receipt_deadline = time.monotonic() + 10
    while not list(receipt_dir.glob("*.json")) and time.monotonic() < receipt_deadline:
        time.sleep(0.02)
    assert list(receipt_dir.glob("*.json")), "orphaned child did not publish an immutable receipt"

    recovered = subprocess.run(
        (sys.executable, str(driver), "recover", str(tmp_path)),
        cwd=tmp_path,
        env=environment,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        check=False,
        timeout=15,
    )

    assert recovered.returncode == 0, recovered.stderr
    result = json.loads(recovered.stdout)
    assert result["recovery"]["finalized_receipt_ids"]
    assert result["status"]["state"] == "succeeded"
    assert marker.read_bytes() == b"one"
