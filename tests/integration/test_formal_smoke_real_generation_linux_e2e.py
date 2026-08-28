from __future__ import annotations

import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

from rquant.formal_smoke_protocol import (
    FormalSmokeExecutionReceipt,
    formal_smoke_receipt_digest,
)
from tests.formal_smoke_real_generation_support import (
    RealFormalSmokeGeneration,
    build_real_formal_smoke_generation,
    build_sealed_formal_smoke_data,
    invoke_outer_formal_smoke_cli_from_checkout_b,
    real_promotion_authority,
    write_redacted_exact_facts_if_requested,
)

_LINUX_EXACT_REASON = (
    "Linux-only real generation gate requires os.execve(fd) and Linux descriptor exec semantics"
)
pytestmark = [
    pytest.mark.integration,
    pytest.mark.linux_exact,
    pytest.mark.skipif(not sys.platform.startswith("linux"), reason=_LINUX_EXACT_REASON),
]


@pytest.fixture(scope="module")
def real_generation(
    tmp_path_factory: pytest.TempPathFactory,
) -> RealFormalSmokeGeneration:
    root = tmp_path_factory.mktemp("formal-smoke-real-generation-a")
    root.chmod(0o700)
    source_root = Path(__file__).resolve().parents[2]
    formal_data = build_sealed_formal_smoke_data(root / "sealed-data", source_root=source_root)
    return build_real_formal_smoke_generation(
        root / "generation-a",
        source_root=source_root,
        venv_root=Path(sys.prefix),
        formal_data=formal_data,
        now=datetime.now(UTC),
    )


def _assert_no_output_residue(output: Path) -> None:
    assert not list(output.glob(".formal-smoke-*"))
    assert not list(output.glob("strategy_lab_runs/*"))


@pytest.mark.exact_timeout(180)
def test_checkout_b_executes_real_generation_a_and_publishes_bound_artifacts(
    real_generation: RealFormalSmokeGeneration,
    tmp_path: Path,
) -> None:
    import rquant
    import rquant.cli

    generation = real_generation
    assert not Path(rquant.__file__).resolve().is_relative_to(generation.generation_root)
    assert not Path(rquant.cli.__file__).resolve().is_relative_to(generation.generation_root)
    output = tmp_path / "output"
    output.mkdir(mode=0o700)

    with real_promotion_authority(tmp_path / "authority", generation=generation) as bootstrap:
        invocation = invoke_outer_formal_smoke_cli_from_checkout_b(
            bootstrap_config=bootstrap,
            trusted_base=generation.trusted_base,
            output=output,
            formal_input=generation.formal_data.formal_input,
            child_environment=generation.child_environment,
            timeout_seconds=90,
        )

    assert invocation.exit_code == 0, invocation.stderr
    result = json.loads(invocation.stdout)
    receipt = FormalSmokeExecutionReceipt.model_validate(result["execution_receipt"])
    evidence = receipt.code_trust_evidence
    assert evidence == generation.code_trust_evidence
    assert evidence.generation_id == generation.package.receipt.generation_id
    assert evidence.content_root_sha256 == generation.package.receipt.content_root_sha256
    assert receipt.execution_identity.generation_root == generation.generation_root
    assert receipt.execution_identity.import_roots == (
        "release/runtime-site-packages",
        "release/src",
    )
    assert generation.import_roots == receipt.execution_identity.import_roots
    probe_paths = {
        module.name: module.relative_path for module in generation.provenance_probe.modules
    }
    assert probe_paths == {
        "rquant": "release/src/rquant/__init__.py",
        "rquant.cli": "release/src/rquant/cli.py",
        "rquant.formal_smoke_runtime_entry": ("release/src/rquant/formal_smoke_runtime_entry.py"),
        "rquant.formal_smoke_replay": "release/src/rquant/formal_smoke_replay.py",
        "rquant.strategy_compare": "release/src/rquant/strategy_compare.py",
    }
    attested_paths = {item.path for item in receipt.execution_identity.code_files}
    assert {
        "release/src/rquant/cli.py",
        "release/src/rquant/formal_smoke_runtime_entry.py",
        "release/src/rquant/formal_smoke_replay.py",
        "release/src/rquant/strategy_compare.py",
    } <= attested_paths
    assert result["fixed_spec_version"] == "stage1-smoke-v1"
    assert result["metrics"]["candidate_count"] == 0
    assert result["metrics"]["trade_count"] == 0
    assert result["sample_count"] == 0
    assert result["execution_receipt_digest"] == formal_smoke_receipt_digest(receipt)

    artifacts = sorted(receipt.artifacts, key=lambda item: item.kind)
    assert [item.kind for item in artifacts] == ["json", "markdown"]
    for artifact in artifacts:
        path = output.joinpath(*artifact.relative_path.split("/"))
        payload = path.read_bytes()
        assert len(payload) == artifact.size
        assert hashlib.sha256(payload).hexdigest() == artifact.sha256
    assert not list(output.glob(".formal-smoke-*"))
    assert len(list((output / "strategy_lab_runs").iterdir())) == 2

    saved = json.loads(Path(result["json_path"]).read_text(encoding="utf-8"))
    assert saved["manifest"]["code_trust_evidence"] == evidence.model_dump(mode="json")
    assert saved["manifest"]["dataset_binding_hash"] == receipt.result.dataset_binding_hash
    assert saved["manifest"]["result_hash"] == receipt.result.result_hash
    write_redacted_exact_facts_if_requested(
        generation=generation,
        receipt=receipt,
        receipt_digest=result["execution_receipt_digest"],
    )


@pytest.mark.exact_timeout(180)
def test_real_generation_business_gate_rejects_unknown_audit_and_snapshot(
    real_generation: RealFormalSmokeGeneration,
    tmp_path: Path,
) -> None:
    generation = real_generation
    output = tmp_path / "output"
    output.mkdir(mode=0o700)
    invalid = generation.formal_data.formal_input.model_copy(
        update={
            "audit_run_id": "f" * 64,
            "dataset_snapshot_id": "e" * 64,
            "dataset_binding_hash": "d" * 64,
        }
    )

    with real_promotion_authority(tmp_path / "authority", generation=generation) as bootstrap:
        invocation = invoke_outer_formal_smoke_cli_from_checkout_b(
            bootstrap_config=bootstrap,
            trusted_base=generation.trusted_base,
            output=output,
            formal_input=invalid,
            child_environment=generation.child_environment,
            timeout_seconds=90,
        )

    assert invocation.exit_code == 2
    assert invocation.stdout == ""
    _assert_no_output_residue(output)
