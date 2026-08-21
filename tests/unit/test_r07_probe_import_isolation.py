"""Parent/child import isolation contracts for the executable R07 probe harness."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parents[2]


def test_child_environment_is_empty_derived_and_has_fixed_two_entry_pythonpath(
    monkeypatch,
    tmp_path: Path,
) -> None:
    from tests.r07_differential_probe_runner import _child_environment

    monkeypatch.setenv("PATH", "/poisoned/path")
    monkeypatch.setenv("UNKNOWN_SECRET", "must-not-cross")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "must-not-cross")
    monkeypatch.setenv("RQUANT_PRODUCTION_SENTINEL", "must-not-cross")
    monkeypatch.setenv("LANG", "poisoned-locale")
    candidate_root = (tmp_path / "candidate").resolve()
    (candidate_root / "src").mkdir(parents=True)
    environment_root = tmp_path / "child"

    environment = _child_environment(environment_root, candidate_root=candidate_root)

    assert "PATH" not in environment
    assert "UNKNOWN_SECRET" not in environment
    assert "AWS_SECRET_ACCESS_KEY" not in environment
    assert "RQUANT_PRODUCTION_SENTINEL" not in environment
    assert environment["LANG"] == "C"
    assert environment["LC_ALL"] == "C"
    assert environment["PYTHONPATH"] == os.pathsep.join(
        (str(candidate_root / "src"), str(candidate_root))
    )
    assert environment["PYTHONNOUSERSITE"] == "1"
    assert environment["PYTHONDONTWRITEBYTECODE"] == "1"


def test_probe_runner_disables_dotenv_before_any_rquant_import(
    tmp_path: Path,
) -> None:
    policy_path = ROOT / "tests/fixtures/r07_differential_gate/policy-v1.json"
    probe_path = tmp_path / "probe"
    facade_path = ROOT / "tests/r07_differential_probe_runner.py"
    environment = {
        "PATH": os.environ.get("PATH", ""),
        "HOME": str(tmp_path / "home"),
        "TMPDIR": str(tmp_path / "tmp"),
        "TMP": str(tmp_path / "tmp"),
        "TEMP": str(tmp_path / "tmp"),
        "PYTHONPATH": str(ROOT),
        "PYTHONNOUSERSITE": "1",
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        "DATA_DIR": str(tmp_path / "data"),
        "DUCKDB_PATH": str(tmp_path / "data" / "probe.duckdb"),
        "DUCKDB_READONLY_PATH": str(tmp_path / "data" / "probe-ro.duckdb"),
        "PARQUET_DIR": str(tmp_path / "parquet"),
        "LOG_DIR": str(tmp_path / "logs"),
        "TUSHARE_TOKEN_MAIN": "0" * 32,
        "NOTIFY_ENABLED": "false",
        "PUSHDEER_KEYS": "must-be-cleared-before-rquant-import",
    }

    def run_contract(facade_prefix: bytes) -> subprocess.CompletedProcess[str]:
        code = f"""
import os
import sys

runtime_dependencies = {{"atexit", "json", "pathlib", "subprocess", "tempfile"}}
assert runtime_dependencies.isdisjoint(sys.modules), sorted(
    runtime_dependencies.intersection(sys.modules)
)
before_environment = tuple(os.environ.items())
before_path = tuple(sys.path)
before_modules = tuple((name, id(module)) for name, module in sys.modules.items())
with open({str(facade_path)!r}, "rb") as facade_file:
    facade_source = {facade_prefix!r} + facade_file.read()
runner_namespace = {{
    "__file__": {str(facade_path)!r},
    "__name__": "tests.r07_differential_probe_runner",
}}
exec(compile(facade_source, {str(facade_path)!r}, "exec"), runner_namespace)

def assert_parent_unchanged(phase):
    assert tuple(os.environ.items()) == before_environment, phase + ": environment"
    assert tuple(sys.path) == before_path, phase + ": path"
    assert (
        tuple((name, id(module)) for name, module in sys.modules.items())
        == before_modules
    ), phase + ": ordered module identities"

assert_parent_unchanged("facade import")
run_boundary_probe_subprocess = runner_namespace["run_boundary_probe_subprocess"]
result = run_boundary_probe_subprocess(
    policy_path={str(policy_path)!r},
    inventory_id="R07-B01",
    tmp_path={str(probe_path)!r},
    fail_on_dotenv_read=True,
)
assert_parent_unchanged("B01 success")
assert result["inventory_id"] == "R07-B01"
assert result["passed"] is True
try:
    run_boundary_probe_subprocess(
        policy_path={str(policy_path)!r},
        inventory_id="R07-B99",
        tmp_path={str(probe_path)!r},
    )
except AssertionError:
    pass
else:
    raise AssertionError("invalid inventory must fail")
assert_parent_unchanged("B99 failure")
print("parent-unchanged-child-isolated")
"""
        return subprocess.run(
            [sys.executable, "-c", code],
            cwd=tmp_path,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )

    imported_dependency = run_contract(b"import json\n")
    assert imported_dependency.returncode != 0
    assert "facade import: ordered module identities" in imported_dependency.stderr

    replaced_module = run_contract(b'import sys\nsys.modules["os"] = type(sys)("os")\n')
    assert replaced_module.returncode != 0
    assert "facade import: ordered module identities" in replaced_module.stderr

    completed = run_contract(b"")
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert completed.stdout.strip() == "parent-unchanged-child-isolated"


def _isolated_child_environment(tmp_path: Path) -> dict[str, str]:
    site_packages = (
        Path(sys.prefix)
        / "lib"
        / f"python{sys.version_info.major}.{sys.version_info.minor}"
        / "site-packages"
    ).resolve(strict=True)
    for name in ("tmp", "data", "parquet", "logs", "home"):
        (tmp_path / name).mkdir(parents=True, exist_ok=True)
    return {
        "PATH": os.environ.get("PATH", ""),
        "LANG": "C",
        "LC_ALL": "C",
        "HOME": str(tmp_path / "home"),
        "TMPDIR": str(tmp_path / "tmp"),
        "TMP": str(tmp_path / "tmp"),
        "TEMP": str(tmp_path / "tmp"),
        "PYTHONPATH": os.pathsep.join((str(ROOT / "src"), str(site_packages))),
        "PYTHONNOUSERSITE": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        "DATA_DIR": str(tmp_path / "data"),
        "DUCKDB_PATH": str(tmp_path / "data" / "probe.duckdb"),
        "DUCKDB_READONLY_PATH": str(tmp_path / "data" / "probe-ro.duckdb"),
        "PARQUET_DIR": str(tmp_path / "parquet"),
        "LOG_DIR": str(tmp_path / "logs"),
        "RQUANT_DISABLE_DOTENV": "1",
        "TUSHARE_TOKEN_MAIN": "0" * 32,
        "NOTIFY_ENABLED": "false",
    }


def _head_commit() -> str:
    return subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "--verify", "HEAD^{commit}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def test_candidate_probe_facade_loads_from_git_object_not_dirty_worktree(
    tmp_path: Path,
) -> None:
    from rquant.signal_family_differential_gate import (
        _load_candidate_probe_runner,
        _materialize_candidate_tree,
    )

    repo = tmp_path / "repo"
    (repo / "tests").mkdir(parents=True)
    facade = repo / "tests" / "r07_differential_probe_runner.py"
    clean_source = (ROOT / "tests" / "r07_differential_probe_runner.py").read_bytes()
    facade.write_bytes(clean_source)
    for command in (
        ["git", "init", "--quiet", str(repo)],
        ["git", "-C", str(repo), "config", "user.email", "r07@example.invalid"],
        ["git", "-C", str(repo), "config", "user.name", "r07"],
        ["git", "-C", str(repo), "add", "-A"],
        ["git", "-C", str(repo), "commit", "--quiet", "-m", "candidate"],
    ):
        subprocess.run(command, check=True, capture_output=True)
    candidate = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "--verify", "HEAD^{commit}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    facade.write_text(
        'raise RuntimeError("dirty worktree facade must never be loaded")\n',
        encoding="utf-8",
    )

    with _materialize_candidate_tree(repo, candidate) as candidate_root:
        runner = _load_candidate_probe_runner(candidate_root)
        loaded = Path(runner.__code__.co_filename).resolve()
        assert loaded == (candidate_root / "tests" / "r07_differential_probe_runner.py").resolve()
        assert loaded.read_bytes() == clean_source
    assert facade.read_bytes() != clean_source


def test_candidate_probe_facade_loads_without_caller_sys_path_or_tests_package(
    tmp_path: Path,
) -> None:
    code = f"""
import importlib
import sys
from pathlib import Path

try:
    importlib.import_module("tests")
except ModuleNotFoundError:
    pass
else:
    raise SystemExit("the tests package must not be importable in this child")

from rquant.signal_family_differential_gate import (
    _load_candidate_probe_runner,
    _materialize_candidate_tree,
)

with _materialize_candidate_tree(Path({str(ROOT)!r}), {_head_commit()!r}) as candidate_root:
    runner = _load_candidate_probe_runner(candidate_root)
    loaded = Path(runner.__code__.co_filename).resolve()
    expected = (candidate_root / "tests" / "r07_differential_probe_runner.py").resolve()
    assert loaded == expected, (loaded, expected)

assert "tests" not in sys.modules
assert "tests.r07_differential_probe_runner" not in sys.modules
assert str(Path({str(ROOT)!r})) not in sys.path
print("candidate-facade-loaded")
"""
    completed = subprocess.run(
        [sys.executable, "-c", code],
        cwd=tmp_path,
        env=_isolated_child_environment(tmp_path),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert completed.stdout.strip() == "candidate-facade-loaded"


def test_verify_wire_rejects_forged_wire_without_caller_tests_import(
    tmp_path: Path,
) -> None:
    import rquant.signal_family_differential_gate as differential_gate
    from rquant.signal_family_differential_gate import (
        BASELINE_COMMIT_SHA,
        BASELINE_TREE_SHA,
        PythonRunEvidenceV1,
        R07DrGateEvidenceWireV1,
        python_run_result_digest,
    )

    commit = _head_commit()
    tree = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "--verify", f"{commit}^{{tree}}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    def _run(minor: str, job_id: str, job_run_id: int) -> PythonRunEvidenceV1:
        run = PythonRunEvidenceV1(
            python_minor=minor,
            job_id=job_id,
            job_run_id=job_run_id,
            workflow_run_id=100,
            run_attempt=1,
            candidate_commit_sha=commit,
            candidate_tree_sha=tree,
            collected=7,
            passed=7,
            skipped=0,
            deselected=0,
            result_digest="0" * 64,
            outcome="passed",
        )
        return run.model_copy(update={"result_digest": python_run_result_digest(run)})

    values: dict[str, object] = {
        "schema_version": 1,
        "repository": "roxorlt/rquant",
        "workflow_path": ".github/workflows/ci.yml",
        "event_name": "push",
        "ref": "refs/heads/main",
        "producer_job_id": "r07-differential-gate-evidence",
        "workflow_run_id": 100,
        "run_attempt": 1,
        "candidate_commit_sha": commit,
        "candidate_tree_sha": tree,
        "baseline_commit_sha": BASELINE_COMMIT_SHA,
        "baseline_tree_sha": BASELINE_TREE_SHA,
        "policy_digest": "1" * 64,
        "complete_diff_digest": "2" * 64,
        "candidate_binding_digest": differential_gate._candidate_binding_digest_values(
            baseline_commit_sha=BASELINE_COMMIT_SHA,
            baseline_tree_sha=BASELINE_TREE_SHA,
            candidate_commit_sha=commit,
            candidate_tree_sha=tree,
            complete_diff_digest="2" * 64,
        ),
        "boundary_manifest_digest": "3" * 64,
        "boundary_result_digest": "4" * 64,
        "root_snapshot_digest": "5" * 64,
        "forbidden_definition_digest": "6" * 64,
        "python_runs": (
            _run("3.11", "r07-differential-gate-py311", 311),
            _run("3.12", "r07-differential-gate-py312", 312),
        ),
        "artifact_name": f"r07-dr-gate-{commit}",
        "artifact_json_path": "r07-dr-gate/evidence-v1.json",
        "retention_days": 90,
        "outcome": "passed",
        "evidence_digest": "0" * 64,
    }
    provisional = R07DrGateEvidenceWireV1.model_construct(**values)
    values["evidence_digest"] = differential_gate._digest_without_field(
        provisional,
        "evidence_digest",
    )
    wire = R07DrGateEvidenceWireV1.model_validate(values)
    wire_path = tmp_path / "wire.json"
    wire_path.write_bytes(
        differential_gate.canonical_json_bytes(wire.model_dump(mode="json"))
    )

    policy_path = ROOT / "tests" / "fixtures" / "r07_differential_gate" / "policy-v1.json"
    code = f"""
import importlib
import sys
from pathlib import Path

try:
    importlib.import_module("tests")
except ModuleNotFoundError:
    pass
else:
    raise SystemExit("the tests package must not be importable in this child")

from rquant.signal_family_differential_gate import (
    R07DrGateEvidenceWireV1,
    load_policy,
    verify_wire,
)

policy = load_policy(Path({str(policy_path)!r}))
wire = R07DrGateEvidenceWireV1.from_canonical_json(Path({str(wire_path)!r}).read_bytes())
try:
    verify_wire(Path({str(ROOT)!r}), policy, wire)
except ValueError as exc:
    print("clean-reject")
else:
    raise SystemExit("the private verifier must reject a forged wire")
assert "tests" not in sys.modules
"""
    completed = subprocess.run(
        [sys.executable, "-c", code],
        cwd=tmp_path,
        env=_isolated_child_environment(tmp_path),
        capture_output=True,
        text=True,
        check=False,
    )

    assert "ModuleNotFoundError" not in completed.stderr, completed.stderr
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert completed.stdout.strip() == "clean-reject"
