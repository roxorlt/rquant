from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import subprocess
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[2]
ENTRY = ROOT / "scripts" / "source_broker_v2_frozen.py"
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"


def _load_entry() -> ModuleType:
    spec = importlib.util.spec_from_file_location("source_broker_v2_frozen_entry", ENTRY)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _digest(nodeids: Sequence[str]) -> str:
    payload = ("\n".join(sorted(nodeids)) + "\n").encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _write_manifest(
    root: Path,
    *,
    modules: Sequence[str],
    expected_nodeids: Sequence[str],
    pressure_nodeids: Sequence[str] = (),
    pressure_rounds: int = 2,
) -> Path:
    effective_pressure_nodeids = tuple(pressure_nodeids) or (expected_nodeids[0],)
    manifest = root / "source_broker_v2_frozen.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "suite_id": "source_broker_v2_frozen",
                "expected_test_count": len(expected_nodeids),
                "expected_nodeids_sha256": _digest(expected_nodeids),
                "modules": [
                    {"path": path, "boundary": f"frozen boundary for {path}"} for path in modules
                ],
                "pressure_gate": {
                    "rounds": pressure_rounds,
                    "nodeids": list(effective_pressure_nodeids),
                },
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return manifest


def _fake_runner(
    collected_nodeids: Sequence[str],
    calls: list[tuple[str, ...]],
) -> Callable[..., subprocess.CompletedProcess[str]]:
    def run(command: Sequence[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        captured = tuple(command)
        calls.append(captured)
        stdout = ""
        if "--collect-only" in captured:
            stdout = "\n".join((*collected_nodeids, f"{len(collected_nodeids)} tests collected"))
        return subprocess.CompletedProcess(captured, 0, stdout=stdout, stderr="")

    return run


def _module(root: Path, relative: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("def test_placeholder():\n    assert True\n", encoding="utf-8")


def test_frozen_entry_collects_validates_executes_and_runs_pressure_separately(
    tmp_path: Path,
) -> None:
    entry = _load_entry()
    module = "tests/unit/test_one.py"
    nodeid = f"{module}::test_placeholder"
    _module(tmp_path, module)
    manifest = _write_manifest(
        tmp_path,
        modules=(module,),
        expected_nodeids=(nodeid,),
        pressure_nodeids=(nodeid,),
        pressure_rounds=2,
    )
    calls: list[tuple[str, ...]] = []

    result = entry.run_frozen_suite(
        repo_root=tmp_path,
        manifest_path=manifest,
        python_path=Path(sys.executable),
        subprocess_runner=_fake_runner((nodeid,), calls),
    )

    assert result.collected_count == 1
    assert result.pressure_rounds == 2
    assert sum("--collect-only" in call for call in calls) == 1
    assert len(calls) == 4
    assert module in calls[1]
    assert f"--deselect={nodeid}" in calls[1]
    assert calls[2][-1] == nodeid
    assert calls[3][-1] == nodeid


def test_frozen_entry_fails_closed_when_dependency_module_is_omitted(
    tmp_path: Path,
) -> None:
    entry = _load_entry()
    included = "tests/unit/test_one.py"
    omitted = "tests/unit/test_two.py"
    included_nodeid = f"{included}::test_placeholder"
    omitted_nodeid = f"{omitted}::test_placeholder"
    _module(tmp_path, included)
    _module(tmp_path, omitted)
    manifest = _write_manifest(
        tmp_path,
        modules=(included,),
        expected_nodeids=(included_nodeid, omitted_nodeid),
    )

    with pytest.raises(entry.FrozenSuiteError, match="count changed"):
        entry.run_frozen_suite(
            repo_root=tmp_path,
            manifest_path=manifest,
            python_path=Path(sys.executable),
            subprocess_runner=_fake_runner((included_nodeid,), []),
        )


def test_frozen_entry_rejects_duplicate_collected_nodeid(tmp_path: Path) -> None:
    entry = _load_entry()
    module = "tests/unit/test_one.py"
    nodeid = f"{module}::test_placeholder"
    _module(tmp_path, module)
    manifest = _write_manifest(
        tmp_path,
        modules=(module,),
        expected_nodeids=(nodeid,),
    )

    with pytest.raises(entry.FrozenSuiteError, match="duplicate nodeid"):
        entry.run_frozen_suite(
            repo_root=tmp_path,
            manifest_path=manifest,
            python_path=Path(sys.executable),
            subprocess_runner=_fake_runner((nodeid, nodeid), []),
        )


def test_frozen_entry_rejects_collect_digest_drift(tmp_path: Path) -> None:
    entry = _load_entry()
    module = "tests/unit/test_one.py"
    expected = f"{module}::test_expected"
    collected = f"{module}::test_changed"
    _module(tmp_path, module)
    manifest = _write_manifest(
        tmp_path,
        modules=(module,),
        expected_nodeids=(expected,),
    )

    with pytest.raises(entry.FrozenSuiteError, match="nodeid digest changed"):
        entry.run_frozen_suite(
            repo_root=tmp_path,
            manifest_path=manifest,
            python_path=Path(sys.executable),
            subprocess_runner=_fake_runner((collected,), []),
        )


def test_frozen_entry_rejects_unknown_module_path(tmp_path: Path) -> None:
    entry = _load_entry()
    module = "tests/unit/test_missing.py"
    nodeid = f"{module}::test_missing"
    manifest = _write_manifest(
        tmp_path,
        modules=(module,),
        expected_nodeids=(nodeid,),
    )

    with pytest.raises(entry.FrozenSuiteError, match="unknown module path"):
        entry.run_frozen_suite(
            repo_root=tmp_path,
            manifest_path=manifest,
            python_path=Path(sys.executable),
            subprocess_runner=_fake_runner((), []),
        )


def test_frozen_entry_rejects_symlink_parent_that_escapes_repo(tmp_path: Path) -> None:
    entry = _load_entry()
    module = "tests/unit/test_one.py"
    nodeid = f"{module}::test_placeholder"
    outside = tmp_path.parent / f"{tmp_path.name}-outside"
    _module(outside, "unit/test_one.py")
    (tmp_path / "tests").symlink_to(outside, target_is_directory=True)
    manifest = _write_manifest(
        tmp_path,
        modules=(module,),
        expected_nodeids=(nodeid,),
    )

    with pytest.raises(entry.FrozenSuiteError, match="symlink"):
        entry.run_frozen_suite(
            repo_root=tmp_path,
            manifest_path=manifest,
            python_path=Path(sys.executable),
            subprocess_runner=_fake_runner((nodeid,), []),
        )


def test_frozen_entry_rejects_symlink_module_file(tmp_path: Path) -> None:
    entry = _load_entry()
    module = "tests/unit/test_one.py"
    nodeid = f"{module}::test_placeholder"
    target = tmp_path / "real_test.py"
    target.write_text("def test_placeholder():\n    assert True\n", encoding="utf-8")
    path = tmp_path / module
    path.parent.mkdir(parents=True)
    path.symlink_to(target)
    manifest = _write_manifest(
        tmp_path,
        modules=(module,),
        expected_nodeids=(nodeid,),
    )

    with pytest.raises(entry.FrozenSuiteError, match="symlink"):
        entry.run_frozen_suite(
            repo_root=tmp_path,
            manifest_path=manifest,
            python_path=Path(sys.executable),
            subprocess_runner=_fake_runner((nodeid,), []),
        )


def test_frozen_entry_rejects_special_module_file(tmp_path: Path) -> None:
    entry = _load_entry()
    module = "tests/unit/test_fifo.py"
    nodeid = f"{module}::test_placeholder"
    path = tmp_path / module
    path.parent.mkdir(parents=True)
    os.mkfifo(path)
    manifest = _write_manifest(
        tmp_path,
        modules=(module,),
        expected_nodeids=(nodeid,),
    )

    with pytest.raises(entry.FrozenSuiteError, match="regular file"):
        entry.run_frozen_suite(
            repo_root=tmp_path,
            manifest_path=manifest,
            python_path=Path(sys.executable),
            subprocess_runner=_fake_runner((nodeid,), []),
        )


def test_frozen_entry_rejects_module_resolved_outside_repo(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entry = _load_entry()
    module = "tests/unit/test_one.py"
    nodeid = f"{module}::test_placeholder"
    _module(tmp_path, module)
    manifest_path = _write_manifest(
        tmp_path,
        modules=(module,),
        expected_nodeids=(nodeid,),
    )
    manifest = entry.load_manifest(manifest_path)
    candidate = tmp_path / module
    outside = tmp_path.parent / f"{tmp_path.name}-resolved-outside.py"
    outside.write_text("def test_placeholder():\n    assert True\n", encoding="utf-8")
    original_resolve = Path.resolve

    def resolve(path: Path, strict: bool = False) -> Path:
        if path == candidate:
            return outside
        return original_resolve(path, strict=strict)

    monkeypatch.setattr(Path, "resolve", resolve)

    with pytest.raises(entry.FrozenSuiteError, match="outside repository root"):
        entry._validate_paths(repo_root=tmp_path, manifest=manifest)


def test_frozen_entry_revalidates_paths_after_collect(tmp_path: Path) -> None:
    entry = _load_entry()
    module = "tests/unit/test_one.py"
    nodeid = f"{module}::test_placeholder"
    _module(tmp_path, module)
    candidate = tmp_path / module
    outside = tmp_path.parent / f"{tmp_path.name}-replacement.py"
    outside.write_text("def test_placeholder():\n    assert True\n", encoding="utf-8")
    manifest = _write_manifest(
        tmp_path,
        modules=(module,),
        expected_nodeids=(nodeid,),
    )
    calls: list[tuple[str, ...]] = []

    def replace_after_collect(
        command: Sequence[str], **_kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        captured = tuple(command)
        calls.append(captured)
        if "--collect-only" in captured:
            candidate.unlink()
            candidate.symlink_to(outside)
            stdout = f"{nodeid}\n1 test collected"
            return subprocess.CompletedProcess(captured, 0, stdout=stdout, stderr="")
        return subprocess.CompletedProcess(captured, 0, stdout="", stderr="")

    with pytest.raises(entry.FrozenSuiteError, match="symlink"):
        entry.run_frozen_suite(
            repo_root=tmp_path,
            manifest_path=manifest,
            python_path=Path(sys.executable),
            subprocess_runner=replace_after_collect,
        )

    assert len(calls) == 1


@pytest.mark.parametrize("interpreter_kind", ("missing", "not_executable"))
def test_frozen_entry_cli_reports_interpreter_exec_failure_without_traceback(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    interpreter_kind: str,
) -> None:
    entry = _load_entry()
    module = "tests/unit/test_source_broker_v2_frozen_entry.py"
    nodeid = f"{module}::test_placeholder"
    manifest = _write_manifest(
        tmp_path,
        modules=(module,),
        expected_nodeids=(nodeid,),
    )
    interpreter = tmp_path / "python"
    if interpreter_kind == "not_executable":
        interpreter.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        interpreter.chmod(0o600)

    exit_code = entry.main(
        (
            "--collect-only",
            "--manifest",
            str(manifest),
            "--python",
            str(interpreter),
        )
    )
    captured = capsys.readouterr()

    assert exit_code == 2
    assert "source_broker_v2_frozen: FAIL:" in captured.err
    assert "could not start" in captured.err
    assert "Traceback" not in captured.err
    assert "Traceback" not in captured.out


def test_ci_matrix_has_explicit_source_broker_v2_51_test_gate() -> None:
    workflow = CI_WORKFLOW.read_text(encoding="utf-8")
    explicit_command = "uv run pytest -q tests/unit/test_source_broker_v2.py"

    assert 'python-version: ["3.11", "3.12"]' in workflow
    assert "- name: SourceBroker V2 saga (51 tests)" in workflow
    assert workflow.count(explicit_command) == 1
    assert workflow.index(explicit_command) < workflow.index(
        "uv run python scripts/source_broker_v2_frozen.py"
    )
