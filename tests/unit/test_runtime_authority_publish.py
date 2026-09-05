"""TP1: the runtime authority publisher, end to end on a temporary root.

`acceptance-pra.md` §2 (A1-A23) and S1 §1.5 (T-1..T-20). Every world is a `tempfile`-derived
directory that stands in for `/`: `<root>/etc/rquant`, `<root>/var/lib/rquant/runtime-authority`,
a fake system interpreter, a git checkout carrying the role modules, a fake venv, and the two
pyz artifacts. `runtime_authority` is pointed at it through its module constants — the same
seam `tests/unit/test_runtime_authority.py` uses — and the wrapper through the keyword
arguments of `resolve_launch`. Nothing here needs root.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import platform
import shutil
import stat
import subprocess
import sys
import zipfile
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

import rquant.runtime_authority as authority_module
import rquant.runtime_authority_publish as publish_module
import rquant.runtime_authority_stage as stage_module
from rquant.runtime_authority import (
    PRODUCTION_ROLE_POLICY,
    RuntimeAncestorPolicy,
    RuntimeAuthorityError,
    RuntimeAuthorityPublishError,
    RuntimeAuthorityRollbackError,
    RuntimeFilePolicy,
    parse_runtime_authority_record,
    parse_runtime_closure_profile,
)
from rquant.runtime_authority_publish import RuntimeAuthorityStageError
from rquant.runtime_exec_wrapper import _verify
from rquant.strict_json import canonical_json_bytes, strict_json_loads

REPO_ROOT = Path(__file__).resolve().parents[2]
BUILDER = REPO_ROOT / "scripts" / "build-production-deploy-pyz.py"
UID = os.getuid()
GID = os.getgid()
ROLE_MODULES = tuple(sorted({entry.module for entry in PRODUCTION_ROLE_POLICY}))
ROLE_LEAVES = tuple(module.rsplit(".", 1)[-1] for module in ROLE_MODULES)
INSTANCED_ROLES = tuple(entry.name for entry in PRODUCTION_ROLE_POLICY if entry.instanced)
KIND_BACKED_ROLES = tuple(entry.name for entry in PRODUCTION_ROLE_POLICY if entry.service_kind)
POLICY = {entry.name: entry for entry in PRODUCTION_ROLE_POLICY}
RUNTIME_PYZ_BYTES = b"PK\x03\x04 runtime-exec pyz stand-in\n"

#: S1 §9.3: the 26 service ids Route B derives, frozen here as literals so a drift on either
#: side — the constant tables or the derivation — turns red (A18 regression lock).
BOOTSTRAP_SERVICE_IDS = {
    "reference_slow_source": ("reference-slow.source.v1",),
    "reference_slow_publisher": ("reference-slow.publisher.v1",),
    "auction_universe_publisher": ("auction-universe.publisher.v1",),
    "auction_match_source": ("auction-match.source.v1",),
    "market_minute_source": ("market-minute.source.v1",),
    "watchlist_quote_source": ("watchlist-quote.source.v1",),
    "daily_close_source": ("daily-close.source.v1",),
    "daily_pipeline_orchestrator": ("daily.pipeline.orchestrator.shadow.v1",),
    "shadow_session": ("shadow.session.production.v1",),
    "feature_live": ("feature.intraday-pit.v1",),
    "signal_router": ("signal-router.all-strategies.v1",),
    "notifier": ("notifier.admin.shadow.v1",),
    "paper_constraint_publisher": ("paper-constraint.market.v1",),
    "paper_broker": ("paper-broker.shadow-main.v1",),
    "runtime_health_publisher": ("runtime-health.all.v1",),
    "lab_jobs_publisher": ("lab-jobs.serving.v1",),
    "lab_artifact_catalog": ("artifact-catalog.primary.v1",),
    "artifact_retention": ("artifact-retention.primary.v1",),
    "promotions_publisher": ("promotions.serving.v1",),
    "serving_publisher": ("serving.publisher.v1",),
    "candidate_publisher": (
        "candidate.auction_gap.v1",
        "candidate.growth_board_surge.v1",
        "candidate.n_shape.v1",
    ),
    "strategy_live": (
        "strategy.auction_gap.v1",
        "strategy.growth_board_surge.v1",
        "strategy.n_shape.v1",
    ),
}
#: `acceptance-pra.md` §5: the first gate enables 16 of the 26 wrapper units.
FIRST_GATE_UNITS = (
    "rquant-lab-claim-finalizer.service",
    "rquant-runtime-artifact-catalog@.service",
    "rquant-runtime-auction-universe@.service",
    "rquant-runtime-candidate@.service",
    "rquant-runtime-daily-orchestrator@.service",
    "rquant-runtime-feature@.service",
    "rquant-runtime-lab-jobs@.service",
    "rquant-runtime-paper-broker@.service",
    "rquant-runtime-paper-constraint@.service",
    "rquant-runtime-promotions@.service",
    "rquant-runtime-runtime-health@.service",
    "rquant-runtime-serving@.service",
    "rquant-runtime-shadow@.service",
    "rquant-runtime-signal-router@.service",
    "rquant-runtime-strategy@.service",
    "rquant-runtime-watchlist-quote@.service",
)
DEFERRED_UNITS = (
    "rquant-artifact-retention",
    "rquant-runtime-auction-match@",
    "rquant-runtime-daily-close@",
    "rquant-runtime-market-minute@",
    "rquant-runtime-notifier@",
    "rquant-runtime-reference-slow-publisher@",
    "rquant-runtime-reference-slow-source@",
    "rquant-page-control",
    "rquant-runtime-recovery@",
    "rquant-runtime-recovery-rehearsal@",
)


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _digest(value: str) -> str:
    return _sha256(value.encode("ascii"))


def _load_builder() -> Any:
    spec = importlib.util.spec_from_file_location("build_production_deploy_pyz", BUILDER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _directory_policy(*directories: Path, extra: dict[Path, tuple[int, int]] | None = None) -> dict:
    policy: dict[Path, tuple[int, int]] = {}
    for directory in directories:
        current = Path("/")
        for component in (None, *directory.parts[1:]):
            if component is not None:
                current /= component
            observed = os.stat(current, follow_symlinks=False)
            policy[current] = (observed.st_uid, stat.S_IMODE(observed.st_mode))
    policy.update(extra or {})
    return policy


def _git(checkout: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", "-C", str(checkout), "-c", "user.name=pa2", "-c", "user.email=pa2@example.invalid",
         *arguments],
        capture_output=True,
        text=True,
        check=True,
    ).stdout


#: `World` replaces `stage_module.discover_interpreter_closure` with `_fake_closure`, so
#: this is the real one, bound at import time, for the single case that must run it (SF-1).
_REAL_INTERPRETER_CLOSURE = stage_module.discover_interpreter_closure


def _fake_closure(_system_python: Path) -> stage_module.InterpreterClosure:
    def policy(path: str, digest: str, mode: int) -> RuntimeFilePolicy:
        return RuntimeFilePolicy(path=Path(path), sha256=digest, owner_uid=0, mode=mode)

    return stage_module.InterpreterClosure(
        version="3.11.15",
        elf_loader=policy("/usr/lib64/ld-linux-x86-64.so.2", "1" * 64, 0o555),
        stdlib=(policy("/usr/lib64/python3.11/os.py", "2" * 64, 0o444),),
        shared_libraries=(policy("/usr/lib64/libpython3.11.so.1.0", "3" * 64, 0o555),),
    )


def _fake_ancestors(paths: Any) -> tuple[RuntimeAncestorPolicy, ...]:
    parents = sorted({parent for path in paths for parent in Path(path).parents}, key=str)
    return tuple(RuntimeAncestorPolicy(path=parent, owner_uid=0, mode=0o755) for parent in parents)


class World:
    """One temporary `/`: authority anchors, a checkout, a venv, the artifacts, the seams."""

    def __init__(self, root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self.root = root.resolve()
        self.monkeypatch = monkeypatch
        self.etc = self.root / "etc" / "rquant"
        self.var = self.root / "var" / "lib" / "rquant" / "runtime-authority"
        self.generations = self.var / "generations"
        self.inbox = self.var / "inbox"
        self.quarantine = self.var / "quarantine"
        self.profile_path = self.etc / "production-runtime-profile.json"
        self.keyring = self.etc / "daily-receipt-trusted-keys.json"
        self.authority_path = self.var / "current.json"
        self.system_python = self.root / "usr" / "bin" / "python3.11"
        self.runtime_pyz = self.root / "usr" / "local" / "libexec" / "rquant-runtime-exec.pyz"
        self.deploy_pyz = self.root / "usr" / "local" / "libexec" / "rquant-production-deploy.pyz"
        self.checkout = self.root / "checkout"
        self.venv = self.root / "venv"
        self.staging_root = self.root / "staging"
        self.commit = ""
        self.deploy_pyz_sha256 = ""

    # -- construction -------------------------------------------------------------

    def build(self, *, real_interpreter: bool = False) -> World:
        self.root.mkdir()
        self.root.chmod(0o755)
        for directory in (self.etc, self.var, self.system_python.parent, self.runtime_pyz.parent):
            directory.mkdir(parents=True)
        for directory in (
            self.root / "etc",
            self.etc,
            self.root / "var",
            self.root / "var" / "lib",
            self.var.parent,
            self.var,
            self.root / "usr",
            self.system_python.parent,
            self.root / "usr" / "local",
            self.runtime_pyz.parent,
        ):
            directory.chmod(0o755)
        if real_interpreter:
            base = Path(getattr(sys, "_base_executable", sys.executable)).resolve()
            shutil.copyfile(base, self.system_python)
        else:
            self.system_python.write_text("#!/bin/sh\nexec /usr/bin/true\n", encoding="utf-8")
        self.system_python.chmod(0o755)
        self.runtime_pyz.write_bytes(RUNTIME_PYZ_BYTES)
        self.runtime_pyz.chmod(0o555)
        self.write_keyring()
        self.deploy_pyz_sha256 = _load_builder().build_deploy_pyz(REPO_ROOT, self.deploy_pyz)
        self.staging_root.mkdir()
        (self.staging_root / ".keep").write_text("", encoding="utf-8")
        self._build_checkout()
        self._build_venv()
        self._patch()
        return self

    def _build_checkout(self) -> None:
        source = self.checkout / "src" / "rquant"
        source.mkdir(parents=True)
        for name in ("__init__.py", "strict_json.py", *(f"{leaf}.py" for leaf in ROLE_LEAVES)):
            shutil.copyfile(REPO_ROOT / "src" / "rquant" / name, source / name)
        (self.checkout / "scripts").mkdir()
        shutil.copyfile(
            REPO_ROOT / "scripts" / "strict_json.py", self.checkout / "scripts" / "strict_json.py"
        )
        units = self.checkout / "deploy" / "systemd"
        units.mkdir(parents=True)
        shutil.copyfile(
            REPO_ROOT / "deploy" / "systemd" / "rquant-page-control.service",
            units / "rquant-page-control.service",
        )
        (self.checkout / "README.md").write_text("synthetic checkout\n", encoding="utf-8")
        _git(self.checkout, "init", "-q")
        _git(self.checkout, "add", "-A")
        _git(self.checkout, "commit", "-q", "-m", "synthetic checkout")
        self.commit = _git(self.checkout, "rev-parse", "HEAD").strip()

    def _build_venv(self) -> None:
        site = self.venv / "lib" / "python3.11" / "site-packages"
        (site / "frozenpkg").mkdir(parents=True)
        (site / "frozenpkg" / "__init__.py").write_text("VALUE = 1\n", encoding="utf-8")
        (site / "frozenpkg" / "data.txt").write_text("payload\n", encoding="utf-8")
        (site / "_marker.py").write_text("MARKER = True\n", encoding="utf-8")
        (site / "_virtualenv.pth").write_text("import _virtualenv\n", encoding="utf-8")
        (site / "__pycache__").mkdir()
        (site / "__pycache__" / "_marker.cpython-311.pyc").write_bytes(b"\x00bytecode")
        (site / "frozenpkg" / "__pycache__").mkdir()
        (site / "frozenpkg" / "__pycache__" / "__init__.cpython-311.pyc").write_bytes(b"\x00")

    def _patch(self) -> None:
        patch = self.monkeypatch.setattr
        patch(authority_module, "PRODUCTION_PROFILE_ANCHOR", self.etc)
        patch(authority_module, "PRODUCTION_PROFILE_PATH", self.profile_path)
        patch(authority_module, "PRODUCTION_PROFILE_OWNER_UID", UID)
        patch(authority_module, "PRODUCTION_PROFILE_DIRECTORY_MODE", 0o755)
        patch(authority_module, "_PRODUCTION_PROFILE_DIRECTORY_POLICY", _directory_policy(self.etc))
        patch(authority_module, "RUNTIME_AUTHORITY_ANCHOR", self.var)
        patch(authority_module, "RUNTIME_AUTHORITY_PATH", self.authority_path)
        patch(authority_module, "RUNTIME_AUTHORITY_LOCK_PATH", self.var / "deployment.lock")
        patch(authority_module, "RUNTIME_AUTHORITY_OWNER_UID", UID)
        patch(authority_module, "RUNTIME_AUTHORITY_DIRECTORY_MODE", 0o755)
        patch(authority_module, "PRODUCTION_GENERATION_ROOT", self.generations)
        patch(authority_module, "PRODUCTION_INBOX_ROOT", self.inbox)
        patch(authority_module, "PRODUCTION_QUARANTINE_ROOT", self.quarantine)
        patch(
            authority_module,
            "_PRODUCTION_RUNTIME_DIRECTORY_POLICY",
            _directory_policy(self.var, extra={self.generations: (UID, 0o755)}),
        )
        patch(authority_module, "PRODUCTION_SYSTEM_PYTHON", self.system_python)
        patch(publish_module, "INSTALLED_RUNTIME_PYZ", self.runtime_pyz)
        patch(publish_module, "PUBLISH_OWNER_GID", GID)
        patch(publish_module, "WRAPPER_TRUSTED_ROOT", str(self.root))
        patch(stage_module, "DAILY_RECEIPT_KEYRING_PATH", self.keyring)
        patch(stage_module, "discover_interpreter_closure", _fake_closure)
        patch(stage_module, "ancestor_policies", _fake_ancestors)

    def write_keyring(self, *, mode: int = 0o444, **overrides: Any) -> Path:
        return _write_daily_receipt_keyring(self.keyring, mode=mode, **overrides)

    # -- driving ------------------------------------------------------------------

    def options(self, name: str, **overrides: Any) -> stage_module.StageOptions:
        values: dict[str, Any] = {
            "checkout_root": self.checkout,
            "commit": self.commit,
            "runtime_pyz": self.runtime_pyz,
            "deploy_pyz": self.deploy_pyz,
            "system_python": self.system_python,
            "venv_source": self.venv,
            "staging": self.staging_root / name,
            "operation_id": _digest(name)[:32],
            "bootstrap_from_checkout": True,
        }
        values.update(overrides)
        return stage_module.StageOptions(**values)

    def argv(self, name: str, *extra: str) -> list[str]:
        options = self.options(name)
        return [
            "--checkout-root", str(options.checkout_root),
            "--commit", options.commit,
            "--runtime-pyz", str(options.runtime_pyz),
            "--deploy-pyz", str(options.deploy_pyz),
            "--system-python", str(options.system_python),
            "--venv-source", str(options.venv_source),
            "--staging", str(options.staging),
            "--operation-id", options.operation_id,
            "--bootstrap-from-checkout",
            *extra,
        ]

    def stage(self, name: str, **overrides: Any) -> stage_module.StagePlan:
        plan = stage_module.build_stage_plan(self.options(name, **overrides))
        stage_module.apply_stage_plan(plan)
        return plan

    def publish(self, plan: stage_module.StagePlan, **overrides: Any) -> dict[str, object]:
        return publish_module.publish_staging(
            plan.options.staging,
            expect_plan_sha256=overrides.pop("expect_plan_sha256", plan.plan_sha256),
            **overrides,
        )

    def stage_and_publish(self, name: str = "first") -> stage_module.StagePlan:
        plan = self.stage(name)
        self.publish(plan)
        return plan

    def resolve(self, role: str, instance: str | None = None) -> dict[str, Any]:
        return _verify.resolve_launch(
            role,
            instance=instance,
            profile_path=str(self.profile_path),
            authority_path=str(self.authority_path),
            generation_root=str(self.generations),
            trusted_root=str(self.root),
            expected_owner_uid=UID,
            source_environment={"LANG": "C", "TZ": "UTC", "SECRET": "leak"},
        )

    def instances(self, plan: stage_module.StagePlan) -> dict[str, list[str]]:
        mapping = plan.plan["instance_mapping"]
        assert isinstance(mapping, dict)
        return {role: list(labels) for role, labels in mapping.items()}

    def combos(self, plan: stage_module.StagePlan) -> list[tuple[str, str | None]]:
        mapping = self.instances(plan)
        combos: list[tuple[str, str | None]] = []
        for entry in PRODUCTION_ROLE_POLICY:
            for label in mapping.get(entry.name, []) if entry.instanced else [None]:
                combos.append((entry.name, label))
        return combos

    def generation_path(self, plan: stage_module.StagePlan) -> Path:
        return self.generations / str(plan.plan["generation_id"])

    def record(self) -> dict[str, Any]:
        return strict_json_loads(self.authority_path.read_bytes())

    def bump_checkout(self, marker: str) -> None:
        """A second commit changing a mirrored source, so the next stage is a new generation."""

        target = self.checkout / "src" / "rquant" / "__init__.py"
        target.write_text(target.read_text(encoding="utf-8") + f"# {marker}\n", encoding="utf-8")
        _git(self.checkout, "commit", "-q", "-am", marker)
        self.commit = _git(self.checkout, "rev-parse", "HEAD").strip()


def _write_daily_receipt_keyring(path: Path, *, mode: int = 0o444, **overrides: Any) -> Path:
    """The `root:root 0444` daily receipt trusted keyring B-3 installs in `/etc/rquant`.

    Genesis shape (generation 1, no retired keys), which is what a first installation has.
    `manifest_hash` and `signature` are present because the real file has them; the stage
    does not verify the signature — the daily orchestrator does, at run time, against the
    keyring path the manifest names.
    """

    document: dict[str, Any] = {
        "schema_version": 2,
        "generation": 1,
        "previous_manifest_hash": "0" * 64,
        "active_key_id": "daily-receipt-v1",
        "active_public_key": (
            "-----BEGIN PUBLIC KEY-----\n"
            "MCowBQYDK2VwAyEAGb9ECWmEzf6FQbrBZ9w7lshQhqowtrbLDFw4rXAxZuE=\n"
            "-----END PUBLIC KEY-----\n"
        ),
        "previous_public_keys": {},
        "manifest_hash": "1" * 64,
        "signature": "pa2-test-signature",
    }
    document.update(overrides)
    if path.exists():
        path.chmod(0o644)
    path.write_bytes(canonical_json_bytes(document, trailing_newline=True))
    path.chmod(mode)
    return path


def _tamper(path: Path) -> None:
    mode = stat.S_IMODE(path.lstat().st_mode)
    path.chmod(0o644)
    payload = bytearray(path.read_bytes())
    if payload:
        payload[0] ^= 0x20
    else:
        payload.extend(b"#")
    path.write_bytes(bytes(payload))
    path.chmod(mode)


def _tree_snapshot(root: Path) -> set[str]:
    return {
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if ".git" not in path.parts
    }


@pytest.fixture
def world(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> World:
    return World(tmp_path / "root", monkeypatch).build()


@pytest.fixture
def published(world: World) -> tuple[World, stage_module.StagePlan]:
    return world, world.stage_and_publish()


# ---------------------------------------------------------------------------------------
# A1: the deploy pyz builder
# ---------------------------------------------------------------------------------------


def test_a1_deploy_pyz_builds_byte_identically_and_prints_its_digest(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    builder = _load_builder()
    first = tmp_path / "one.pyz"
    second = tmp_path / "two.pyz"
    assert builder.main(["--repository-root", str(REPO_ROOT), "--output", str(first)]) == 0
    assert builder.main(["--repository-root", str(REPO_ROOT), "--output", str(second)]) == 0
    assert first.read_bytes() == second.read_bytes()
    printed = capsys.readouterr().out.split()
    assert printed == [_sha256(first.read_bytes())] * 2
    assert stat.S_IMODE(first.stat().st_mode) == 0o555
    with zipfile.ZipFile(first) as archive:
        names = sorted(archive.namelist())
        assert names == [
            "__main__.py",
            "rquant/__init__.py",
            "rquant/runtime_authority.py",
            "rquant/runtime_authority_publish.py",
            "rquant/runtime_exec_wrapper/__init__.py",
            "rquant/runtime_exec_wrapper/_verify.py",
            "rquant/strict_json.py",
        ]
        for info in archive.infolist():
            assert info.date_time == (1980, 1, 1, 0, 0, 0)
            assert info.external_attr == builder.FROZEN_EXTERNAL_ATTR
        assert archive.read("rquant/__init__.py") == builder.PACKAGE_SHELL.encode()


def test_a1_deploy_pyz_runs_isolated_and_refuses_without_a_traceback(tmp_path: Path) -> None:
    artifact = tmp_path / "deploy.pyz"
    _load_builder().build_deploy_pyz(REPO_ROOT, artifact)
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    environment = {"PATH": "/usr/bin:/bin", "LANG": "C"}
    helped = subprocess.run(
        [sys.executable, "-I", "-S", str(artifact), "--help"],
        cwd=str(scratch), env=environment, capture_output=True, text=True, check=False,
    )
    assert helped.returncode == 0, helped.stderr
    assert "publish" in helped.stdout and "rollback" in helped.stdout
    refused = subprocess.run(
        [sys.executable, "-I", "-S", str(artifact), "publish", "--staging", str(scratch / "none"),
         "--expect-plan-sha256", "a" * 64],
        cwd=str(scratch), env=environment, capture_output=True, text=True, check=False,
    )
    assert refused.returncode == 1
    assert refused.stderr.startswith("refused: ")
    assert "Traceback" not in refused.stderr


def test_a1_deploy_pyz_builder_refuses_a_non_stdlib_import(tmp_path: Path) -> None:
    builder = _load_builder()
    with pytest.raises(SystemExit, match="cannot carry"):
        builder.assert_stdlib_only("import pydantic\n", "rquant/runtime_authority_publish.py")
    with pytest.raises(SystemExit, match="relative import"):
        builder.assert_stdlib_only("from . import x\n", "rquant/x.py")
    builder.assert_stdlib_only(
        "import os\nfrom rquant.strict_json import canonical_json_bytes\n", "rquant/ok.py"
    )
    # S-2: an import buried in a function body or a branch is caught as well.
    for nested in (
        "def later():\n    import rquant.config\n",
        "def later():\n    from rquant.runtime_production_profile import x\n",
        "if True:\n    import pydantic\n",
        "try:\n    import pandas\nexcept ImportError:\n    pass\n",
    ):
        with pytest.raises(SystemExit, match="cannot carry"):
            builder.assert_stdlib_only(nested, "rquant/runtime_authority_publish.py")
    builder.assert_stdlib_only("def later():\n    import argparse\n", "rquant/ok.py")


# ---------------------------------------------------------------------------------------
# A2 / A3 / T-1: dry run and determinism
# ---------------------------------------------------------------------------------------


def test_a2_dry_run_writes_nothing_and_prints_a_canonical_plan(
    world: World, capsysbinary: pytest.CaptureFixture[bytes]
) -> None:
    before = _tree_snapshot(world.root)
    assert stage_module.main(world.argv("dry")) == 0
    out = capsysbinary.readouterr().out
    assert _tree_snapshot(world.root) == before
    assert not (world.staging_root / "dry").exists()
    plan = publish_module.parse_plan(out)
    assert canonical_json_bytes(plan, trailing_newline=True) == out
    assert plan["mode"] == "bootstrap"
    assert plan["sequence"] == 1 and plan["previous_operation_id"] is None
    assert set(plan["instance_mapping"]) == set(INSTANCED_ROLES)
    assert sum(len(labels) for labels in plan["instance_mapping"].values()) == 29
    assert len(plan["service_manifests"]) == 28
    assert plan["runtime_pyz_sha256"] == _sha256(RUNTIME_PYZ_BYTES)
    assert plan["deploy_pyz_sha256"] == world.deploy_pyz_sha256
    assert plan["system_python_sha256"] == _sha256(world.system_python.read_bytes())
    staged = plan["staged_files"]
    assert {"production-runtime-profile.json", "current.json", "generation",
            "generation/full-manifest.json", "generation/pyvenv.cfg", "generation/bin/python",
            "generation/src/rquant/__init__.py", "generation/scripts/strict_json.py",
            "generation/lib/site-packages/_marker.py", "generation/cwd"} <= set(staged)
    assert "generation/lib/site-packages/_virtualenv.pth" not in staged
    assert not any("__pycache__" in path for path in staged)


def test_a3_t1_apply_is_deterministic(world: World) -> None:
    first = world.stage("one", operation_id="1" * 32)
    second = world.stage("two", operation_id="1" * 32)
    first_files = {
        path.relative_to(first.options.staging).as_posix(): path.read_bytes()
        for path in first.options.staging.rglob("*") if path.is_file()
    }
    second_files = {
        path.relative_to(second.options.staging).as_posix(): path.read_bytes()
        for path in second.options.staging.rglob("*") if path.is_file()
    }
    assert first_files == second_files
    assert first.plan_sha256 == second.plan_sha256

    third = world.stage("three", operation_id="3" * 32)
    third_plan = dict(strict_json_loads((third.options.staging / "plan.json").read_bytes()))
    first_plan = dict(strict_json_loads((first.options.staging / "plan.json").read_bytes()))
    assert third_plan.pop("operation_id") == "3" * 32
    assert first_plan.pop("operation_id") == "1" * 32
    third_plan["staged_files"].pop("current.json")
    first_plan["staged_files"].pop("current.json")
    assert third_plan == first_plan
    third_record = world_record(third)
    first_record = world_record(first)
    assert third_record.pop("operation_id") != first_record.pop("operation_id")
    assert third_record == first_record


def world_record(plan: stage_module.StagePlan) -> dict[str, Any]:
    return strict_json_loads((plan.options.staging / "current.json").read_bytes())


def test_a3_staging_is_frozen_and_named_by_its_temporary_rename(world: World) -> None:
    plan = world.stage("frozen")
    staging = plan.options.staging
    assert stat.S_IMODE(staging.lstat().st_mode) == 0o555
    assert not any(path.name.startswith(".frozen.tmp-") for path in world.staging_root.iterdir())
    for path in staging.rglob("*"):
        expected = 0o555 if path.is_dir() or path.name == "python" else 0o444
        assert stat.S_IMODE(path.lstat().st_mode) == expected, path
    with pytest.raises(RuntimeAuthorityStageError, match="already exists"):
        stage_module.build_stage_plan(world.options("frozen"))


# ---------------------------------------------------------------------------------------
# A4 / T-2: the profile
# ---------------------------------------------------------------------------------------


def test_a4_t2_staged_profile_passes_the_publish_side_parser(world: World) -> None:
    plan = world.stage("profile")
    payload = (plan.options.staging / "production-runtime-profile.json").read_bytes()
    profile = parse_runtime_closure_profile(payload)
    assert profile.profile_id == plan.plan["profile_id"]
    assert profile.platform == "linux"
    assert set(profile.roles) == set(POLICY) and len(profile.roles) == 28
    for name, role in profile.roles.items():
        entry = POLICY[name]
        assert (role.module, role.environment_allowlist, role.service_kind, role.control_root,
                role.once, role.module_arguments) == (
            entry.module, entry.environment_allowlist, entry.service_kind, entry.control_root,
            entry.once, entry.module_arguments)
        assert bool(role.instances) is entry.instanced
    assert profile.elf_loader.path not in {item.path for item in profile.shared_libraries}
    assert {item.path for item in profile.ancestors} == {
        parent for item in profile.files for parent in item.path.parents
    }
    assert len(profile.ancestors) <= authority_module.MAX_PROFILE_ANCESTORS
    assert len(profile.files) <= authority_module.MAX_CLOSURE_FILES
    assert len(payload) <= authority_module.MAX_PROFILE_BYTES
    assert profile.runtime_pyz.sha256 == _sha256(RUNTIME_PYZ_BYTES)
    summary = plan.plan["closure_summary"]
    assert summary["closure_files"] == len(profile.files)
    assert summary["ancestors"] == len(profile.ancestors)
    assert summary["profile_bytes"] == len(payload)


def test_a4_b5a_elf_loader_is_pt_interp_and_never_a_shared_library() -> None:
    readelf = (
        "Program Headers:\n  Type           Offset\n  PHDR           0x000040\n"
        "  INTERP         0x000318\n      [Requesting program interpreter: "
        "/lib64/ld-linux-x86-64.so.2]\n  LOAD           0x000000\n"
    )
    ldd = (
        "\tlinux-vdso.so.1 (0x00007ffd5f1f0000)\n"
        "\tlibpython3.11.so.1.0 => /lib64/libpython3.11.so.1.0 (0x00007f5a1c000000)\n"
        "\tlibc.so.6 => /lib64/libc.so.6 (0x00007f5a1bc00000)\n"
        "\tlibm.so.6 => /lib64/libm.so.6 (0x00007f5a1bb00000)\n"
        "\t/lib64/ld-linux-x86-64.so.2 (0x00007f5a1c400000)\n"
        "\t/lib64/ld-linux-x86-64.so.2 => /lib64/ld-linux-x86-64.so.2 (0x00007f5a1c400000)\n"
    )
    loader = stage_module.elf_loader_from_readelf(readelf)
    assert loader == "/lib64/ld-linux-x86-64.so.2"
    libraries = stage_module.shared_libraries_from_ldd(ldd, elf_loader=loader)
    # The members are declared under their real paths (#193 G-2), and on a usrmerge Linux
    # `/lib64` is a symlink to `usr/lib64`, so the expectation has to be resolved the same
    # way the stage resolves them. On macOS, where `/lib64` does not exist, `realpath` is the
    # identity and these are the literal paths the fixture prints.
    assert libraries == tuple(
        sorted(
            os.path.realpath(path)
            for path in (
                "/lib64/libc.so.6",
                "/lib64/libm.so.6",
                "/lib64/libpython3.11.so.1.0",
            )
        )
    )
    assert loader not in libraries
    assert os.path.realpath(loader) not in libraries
    with pytest.raises(RuntimeAuthorityStageError, match="exactly one"):
        stage_module.elf_loader_from_readelf("no interpreter here")


def test_a4_b5a_stdlib_walk_applies_the_runbook_exclusions(tmp_path: Path) -> None:
    stdlib = tmp_path / "python3.11"
    for relative in (
        "os.py", "json/__init__.py", "lib-dynload/_json.so", "test/test_os.py",
        "idlelib/idle.py", "tkinter/__init__.py", "lib2to3/main.py", "ensurepip/__init__.py",
        "site-packages/x.py", "__pycache__/os.cpython-311.pyc", "json/__pycache__/x.pyc",
    ):
        path = stdlib / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"x")
    (stdlib / "os.pyc").write_bytes(b"x")
    found = stage_module.stdlib_files([stdlib])
    assert [path.relative_to(stdlib).as_posix() for path in found] == [
        "json/__init__.py", "lib-dynload/_json.so", "os.py"
    ]


# ---------------------------------------------------------------------------------------
# A5 / T-4 / A21: the wrapper closes the loop
# ---------------------------------------------------------------------------------------


def test_a5_t4_a21_every_role_and_instance_resolves_with_the_policy_argv(
    published: tuple[World, stage_module.StagePlan],
) -> None:
    world, plan = published
    generation = world.generation_path(plan)
    combos = world.combos(plan)
    assert len(combos) == 32 and len({role for role, _ in combos}) == 28
    for role, instance in combos:
        launch = world.resolve(role, instance)
        entry = POLICY[role]
        argv = list(launch["module_argv"])
        if entry.control_root:
            expected = [
                "--manifest", str(generation / "manifests" / f"{instance}.json"),
                "--control-root", f"{entry.control_root}/{instance}",
                "--expected-commit", world.commit,
                "--expected-generation", plan.plan["generation_id"],
            ]
            if entry.service_kind:
                expected += ["--expected-kind", entry.service_kind]
            if entry.once:
                expected.append("--once")
            expected += list(entry.module_arguments)
        else:
            expected = list(entry.module_arguments)
        assert argv == expected, role
        assert launch["module_source"] == f"src/rquant/{entry.module.rsplit('.', 1)[-1]}.py"
        assert launch["python_path"] == str(generation / "bin" / "python")
        assert launch["app_source"] == str(generation / "src")
        assert launch["site_packages"] == [str(generation / "lib" / "site-packages")]
        assert launch["working_directory"] == str(generation / "cwd")
        assert "SECRET" not in launch["environment"] and launch["environment"]["LANG"] == "C"
    assert all(POLICY[r].module_arguments[-1:] == ("--authority-runtime",)
               for r in KIND_BACKED_ROLES)


def test_a5_receipt_and_record_after_the_first_publication(
    published: tuple[World, stage_module.StagePlan],
) -> None:
    world, plan = published
    record = world.record()
    assert record["sequence"] == 1 and record["state"] == "active"
    assert record["operation_id"] == plan.options.operation_id
    assert record["current_generation_id"] == plan.plan["generation_id"]
    assert record["current_commit"] == world.commit
    assert all(record[f"prior_{field}"] is None for field in _verify._SLOT_FIELDS)
    assert world.authority_path.read_bytes() == plan.record_payload
    assert world.profile_path.read_bytes() == plan.profile_payload
    for path in (world.authority_path, world.profile_path):
        info = path.lstat()
        assert stat.S_IMODE(info.st_mode) == 0o444 and info.st_nlink == 1 and info.st_uid == UID
    assert not (world.inbox / plan.options.operation_id).exists()
    lines = (world.var / "publications.jsonl").read_bytes().splitlines()
    receipt = strict_json_loads(lines[-1])
    assert receipt["result"] == "committed" and receipt["wrapper_preflight"] == 32
    assert stat.S_IMODE((world.var / "publications.jsonl").lstat().st_mode) == 0o600
    assert stat.S_IMODE(world.generation_path(plan).lstat().st_mode) == 0o555


# ---------------------------------------------------------------------------------------
# A6 / T-5..T-13: tampering is refused with typed errors
# ---------------------------------------------------------------------------------------


def test_t5_tampering_one_source_byte_is_refused_by_the_wrapper(
    published: tuple[World, stage_module.StagePlan],
) -> None:
    world, plan = published
    _tamper(world.generation_path(plan) / "src" / "rquant" / "runtime_service_main.py")
    with pytest.raises(_verify.RuntimeExecError, match="manifested generation node changed"):
        world.resolve("daily")


def test_t6_tampering_the_full_manifest_is_refused(
    published: tuple[World, stage_module.StagePlan],
) -> None:
    world, plan = published
    _tamper(world.generation_path(plan) / "full-manifest.json")
    with pytest.raises(_verify.RuntimeExecError, match="manifest hash does not match"):
        world.resolve("daily")


def test_t7_tampering_record_or_profile_is_refused(
    published: tuple[World, stage_module.StagePlan],
) -> None:
    world, plan = published
    _tamper(world.authority_path)
    with pytest.raises(_verify.RuntimeExecError, match="not canonical|not strict JSON"):
        world.resolve("daily")
    world.authority_path.chmod(0o644)
    world.authority_path.write_bytes(plan.record_payload)
    world.authority_path.chmod(0o444)
    world.resolve("daily")
    _tamper(world.profile_path)
    with pytest.raises(_verify.RuntimeExecError, match="not canonical|does not match|strict"):
        world.resolve("daily")


def test_t8_a_mode_change_is_refused(published: tuple[World, stage_module.StagePlan]) -> None:
    world, plan = published
    victim = world.generation_path(plan) / "scripts" / "strict_json.py"
    victim.chmod(0o644)
    with pytest.raises(_verify.RuntimeExecError, match="mode changed"):
        world.resolve("daily")


def test_t9_an_unmanifested_file_is_refused_by_the_publisher_but_not_the_wrapper(
    published: tuple[World, stage_module.StagePlan],
) -> None:
    world, plan = published
    generation = world.generation_path(plan)
    generation.chmod(0o755)
    (generation / "stray.txt").write_text("x", encoding="utf-8")
    (generation / "stray.txt").chmod(0o444)
    generation.chmod(0o555)
    world.resolve("daily")  # the wrapper checks manifested nodes only: asymmetric by design
    record = authority_module.load_runtime_authority()
    with pytest.raises(
        RuntimeAuthorityPublishError, match="absent from its manifest|does not exactly match"
    ):
        authority_module._revalidate_generation_slot(
            record.current, authority_module.load_production_runtime_profile()
        )


@pytest.mark.parametrize("hook", ("sitecustomize.py", "x.pth"))
def test_t10_an_import_hook_under_src_is_refused_on_both_sides(world: World, hook: str) -> None:
    hook_path = world.checkout / "src" / "rquant" / hook
    hook_path.write_text("", encoding="utf-8")
    _git(world.checkout, "add", "-A")
    _git(world.checkout, "commit", "-q", "-m", "hook")
    world.commit = _git(world.checkout, "rev-parse", "HEAD").strip()
    with pytest.raises(RuntimeAuthorityStageError, match="import hook"):
        stage_module.build_stage_plan(world.options("hooked"))
    # And the wrapper independently: a published world with the hook grafted in.
    hook_path.unlink()
    _git(world.checkout, "commit", "-q", "-am", "unhook")
    world.commit = _git(world.checkout, "rev-parse", "HEAD").strip()
    plan = world.stage_and_publish("clean")
    generation = world.generation_path(plan)
    manifest = strict_json_loads((generation / "full-manifest.json").read_bytes())
    entries = list(manifest["entries"])
    entries.append({
        "path": f"src/{hook}", "type": "file", "owner_uid": UID, "mode": 0o444,
        "nlink": 1, "size": 0, "sha256": _sha256(b""),
    })
    entries.sort(key=lambda entry: entry["path"])
    forged = canonical_json_bytes({**manifest, "entries": entries}, trailing_newline=True)
    (generation / "src").chmod(0o755)
    (generation / "src" / hook).write_bytes(b"")
    (generation / "src" / hook).chmod(0o444)
    (generation / "src").chmod(0o555)
    generation.chmod(0o755)
    (generation / "full-manifest.json").chmod(0o644)
    (generation / "full-manifest.json").write_bytes(forged)
    (generation / "full-manifest.json").chmod(0o444)
    generation.chmod(0o555)
    record = world.record()
    record["current_generation_id"] = record["current_full_manifest_hash"] = _sha256(forged)
    record["current_generation_path"] = str(generation)
    with pytest.raises(_verify.RuntimeExecError):
        _verify.load_generation_manifest(
            generation_path=str(generation), slot=_verify.current_slot(record),
            profile=strict_json_loads(world.profile_path.read_bytes()),
            trusted_root=str(world.root), expected_owner_uid=UID,
        ) and _verify.verify_code_identity(
            generation_path=str(generation),
            entries=tuple(entries),
            expected_owner_uid=UID,
        )


def test_t11_t12_t13_instance_and_commit_rules(
    published: tuple[World, stage_module.StagePlan],
) -> None:
    world, plan = published
    with pytest.raises(_verify.RuntimeExecError, match="not in the root-owned allowlist"):
        world.resolve("strategy_live", "svc-" + "f" * 64)
    with pytest.raises(_verify.RuntimeExecError, match="accepts no instance label"):
        world.resolve("daily", "svc-" + "f" * 64)
    with pytest.raises(_verify.RuntimeExecError, match="requires an instance label"):
        world.resolve("strategy_live")
    record = world.record()
    record["current_commit"] = "release-tag-not-a-sha"
    world.authority_path.chmod(0o644)
    world.authority_path.write_bytes(canonical_json_bytes(record, trailing_newline=True))
    world.authority_path.chmod(0o444)
    world.resolve("daily")  # no control root: the commit is never forwarded
    label = world.instances(plan)["strategy_live"][0]
    with pytest.raises(_verify.RuntimeExecError, match="not a forwardable commit sha"):
        world.resolve("strategy_live", label)


def test_a6_refusals_are_one_typed_line_on_the_command_line(
    world: World, capsys: pytest.CaptureFixture[str]
) -> None:
    plan = world.stage("cli")
    capsys.readouterr()
    assert publish_module.main(
        ["publish", "--staging", str(plan.options.staging), "--expect-plan-sha256", "0" * 64]
    ) == 1
    captured = capsys.readouterr()
    assert captured.err.startswith("refused: plan.json sha256") and "Traceback" not in captured.err
    assert stage_module.main(world.argv("cli")) == 1  # staging already exists
    assert capsys.readouterr().err.startswith("refused: --staging already exists")


# ---------------------------------------------------------------------------------------
# A7 / A8 / A20: the generation tree
# ---------------------------------------------------------------------------------------


def test_a7_full_manifest_roles_are_relative_and_pass_the_publish_side_check(
    published: tuple[World, stage_module.StagePlan],
) -> None:
    world, plan = published
    manifest = strict_json_loads(plan.manifest_payload)
    assert set(manifest) == {"schema_id", "profile_id", "roles", "entries"}
    assert manifest["roles"] == publish_module.relative_role_payloads()
    assert manifest["roles"]["daily"] == {
        "python_path": "bin/python", "module": "rquant.runtime_service_main",
        "working_directory": "cwd", "app_source": "src", "site_packages": ["lib/site-packages"],
    }
    profile = parse_runtime_closure_profile(plan.profile_payload)
    slot = publish_module.generation_slot(
        generation_id=str(plan.plan["generation_id"]), commit=world.commit,
        profile_id=profile.profile_id,
    )
    entries = authority_module._validate_generation_manifest(plan.manifest_payload, slot, profile)
    assert len(entries) == len(manifest["entries"])
    paths = {entry.path for entry in entries}
    assert {"src/rquant/__init__.py", "scripts/strict_json.py", "pyvenv.cfg", "bin/python", "cwd",
            "lib/site-packages", "manifests"} <= paths
    assert all(f"src/rquant/{leaf}.py" in paths for leaf in ROLE_LEAVES)


def test_a8_generation_tree_is_frozen_root_style_without_escapes(
    published: tuple[World, stage_module.StagePlan],
) -> None:
    world, plan = published
    generation = world.generation_path(plan)
    seen = 0
    for path in generation.rglob("*"):
        seen += 1
        info = path.lstat()
        assert not stat.S_ISLNK(info.st_mode), path
        assert path.name != "__pycache__" and not path.name.endswith((".pyc", ".pth")), path
        assert path.name not in ("sitecustomize.py", "usercustomize.py"), path
        assert info.st_uid == UID
        if path.is_dir():
            assert stat.S_IMODE(info.st_mode) == 0o555, path
        else:
            expected = 0o555 if path == generation / "bin" / "python" else 0o444
            assert stat.S_IMODE(info.st_mode) == expected, path
            assert info.st_nlink == 1
    assert seen > 10
    frozen = generation / "lib" / "site-packages" / "frozenpkg" / "data.txt"
    assert frozen.read_text() == "payload\n"
    assert not (generation / "lib" / "site-packages" / "_virtualenv.pth").exists()


def test_a20_kind_backed_manifests_validate_as_runtime_service_manifests(
    published: tuple[World, stage_module.StagePlan],
) -> None:
    from rquant.runtime_service_entrypoint import RuntimeServiceManifest

    world, plan = published
    generation = world.generation_path(plan)
    mapping = world.instances(plan)
    validated = 0
    for role in KIND_BACKED_ROLES:
        for label in mapping[role]:
            manifest = RuntimeServiceManifest.model_validate_json(
                (generation / "manifests" / f"{label}.json").read_bytes()
            )
            assert manifest.service_kind.value == role
            assert manifest.producer_commit == world.commit
            assert manifest.service_id == plan.plan["service_manifests"][label]
            validated += 1
    assert validated == 26 and len(KIND_BACKED_ROLES) == 22
    for role in ("page_control", "runtime_recovery", "runtime_recovery_rehearsal"):
        label = mapping[role][0]
        placeholder = strict_json_loads((generation / "manifests" / f"{label}.json").read_bytes())
        assert set(placeholder) == {"service_id", "producer_commit"}
    assert mapping["runtime_recovery"] == mapping["runtime_recovery_rehearsal"]
    assert len(list((generation / "manifests").iterdir())) == 28


# ---------------------------------------------------------------------------------------
# A9 / A10 / A12 / T-16..T-19: the root transaction fails closed
# ---------------------------------------------------------------------------------------


def _root_untouched(world: World) -> None:
    assert not world.profile_path.exists()
    assert not world.authority_path.exists()
    assert not world.generations.exists()
    assert not world.inbox.exists()
    assert not world.quarantine.exists()
    assert not (world.var / "deployment.lock").exists()


def test_t16_a9_wrong_plan_digest_writes_no_root_path(world: World) -> None:
    plan = world.stage("t16")
    with pytest.raises(RuntimeAuthorityPublishError, match="does not match the confirmed"):
        world.publish(plan, expect_plan_sha256="0" * 64)
    _root_untouched(world)
    with pytest.raises(RuntimeAuthorityPublishError, match="not a lowercase sha256"):
        world.publish(plan, expect_plan_sha256="not-a-digest")
    _root_untouched(world)


def test_t17_a9_a_staged_file_changed_after_staging_is_quarantined(world: World) -> None:
    plan = world.stage("t17")
    _tamper(plan.options.staging / "generation" / "src" / "rquant" / "workload_isolation.py")
    with pytest.raises(RuntimeAuthorityPublishError, match="changed after staging"):
        world.publish(plan)
    quarantined = world.quarantine / plan.options.operation_id
    assert quarantined.is_dir()
    assert (quarantined / "plan.json").exists()
    assert not (world.inbox / plan.options.operation_id).exists()
    assert not world.authority_path.exists()
    assert not world.generation_path(plan).exists()
    assert not world.profile_path.exists()


def test_t17_a_symlinked_staging_directory_is_refused(world: World) -> None:
    plan = world.stage("t17c")
    generation = plan.options.staging / "generation"
    generation.chmod(0o755)
    (generation / "scripts").chmod(0o755)  # Darwin: a directory must be writable to be renamed
    (generation / "scripts").rename(generation / "scripts.real")
    (generation / "scripts.real").chmod(0o555)
    os.symlink("scripts.real", generation / "scripts")
    generation.chmod(0o555)
    with pytest.raises(RuntimeAuthorityPublishError, match="is not a directory"):
        world.publish(plan)
    assert not world.authority_path.exists()
    assert not world.generation_path(plan).exists()
    assert (world.quarantine / plan.options.operation_id).is_dir()


def test_t17_a_tampered_plan_level_file_is_refused_before_the_inbox(world: World) -> None:
    plan = world.stage("t17b")
    _tamper(plan.options.staging / "production-runtime-profile.json")
    with pytest.raises(RuntimeAuthorityPublishError, match="does not match plan.json"):
        world.publish(plan)
    _root_untouched(world)


def test_t18_a10_installed_runtime_pyz_must_match_the_profile(world: World) -> None:
    plan = world.stage("t18")
    world.runtime_pyz.chmod(0o644)
    world.runtime_pyz.write_bytes(RUNTIME_PYZ_BYTES + b"tampered")
    world.runtime_pyz.chmod(0o555)
    with pytest.raises(RuntimeAuthorityPublishError, match="does not match the profile"):
        world.publish(plan)
    _root_untouched(world)


def test_t19_a12_a_failing_record_write_leaves_the_old_record_canonical(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = world.stage_and_publish("first")
    old_payload = world.authority_path.read_bytes()
    world.bump_checkout("second generation")
    second = world.stage("second")
    assert second.plan["sequence"] == 2
    assert second.plan["previous_operation_id"] == first.options.operation_id

    def fail_write(descriptor: int, payload: bytes) -> None:
        raise OSError("injected write crash")

    monkeypatch.setattr(authority_module, "_write_all", fail_write)
    with pytest.raises(RuntimeAuthorityPublishError):
        world.publish(second)
    assert world.authority_path.read_bytes() == old_payload
    visible = parse_runtime_authority_record(old_payload)
    assert visible.sequence == 1 and visible.operation_id == first.options.operation_id
    assert (world.quarantine / second.options.operation_id).is_dir()
    assert world.generation_path(second).is_dir()  # content-addressed, never deleted
    world.resolve("daily")


def test_a9_a_second_publisher_cannot_race_a_stale_plan(world: World) -> None:
    first = world.stage("first")
    world.bump_checkout("competing")
    competing = world.stage("competing")
    world.publish(first)
    with pytest.raises(RuntimeAuthorityPublishError, match="advanced since staging"):
        world.publish(competing)
    assert world.record()["operation_id"] == first.options.operation_id


def test_a9_a_profile_change_over_an_existing_record_is_refused_before_writing(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = world.stage_and_publish("first")
    profile_bytes = world.profile_path.read_bytes()
    world.bump_checkout("new closure")
    monkeypatch.setattr(
        stage_module,
        "discover_interpreter_closure",
        lambda python: stage_module.InterpreterClosure(
            version="3.11.16",
            elf_loader=_fake_closure(python).elf_loader,
            stdlib=(RuntimeFilePolicy(Path("/usr/lib64/python3.11/os.py"), "9" * 64, 0, 0o444),),
            shared_libraries=_fake_closure(python).shared_libraries,
        ),
    )
    with pytest.raises(RuntimeAuthorityError, match="profile id is not active"):
        stage_module.build_stage_plan(world.options("changed"))
    # The root side refuses the same transition on its own, before touching any path.
    monkeypatch.setattr(stage_module, "read_previous_record", lambda profile: None)
    plan = world.stage("changed", operation_id="9" * 32)
    with pytest.raises(RuntimeAuthorityPublishError, match="not a supported transition"):
        world.publish(plan)
    assert world.profile_path.read_bytes() == profile_bytes
    assert world.record()["operation_id"] == first.options.operation_id
    assert not (world.quarantine / ("9" * 32)).exists()


def _other_closure(python: Path) -> stage_module.InterpreterClosure:
    base = _fake_closure(python)
    return stage_module.InterpreterClosure(
        version="3.11.16",
        elf_loader=base.elf_loader,
        stdlib=(RuntimeFilePolicy(Path("/usr/lib64/python3.11/os.py"), "9" * 64, 0, 0o444),),
        shared_libraries=base.shared_libraries,
    )


def _stale_preflight_after(
    world: World, monkeypatch: pytest.MonkeyPatch, plan_b: stage_module.StagePlan
) -> Any:
    """B's read-only preflight, taken before A commits, then frozen: the D-3 window."""

    staged_b = publish_module._load_staging(plan_b.options.staging, plan_b.plan_sha256)
    stale = publish_module._preflight(staged_b)
    assert stale.installed is None and stale.previous is None
    monkeypatch.setattr(publish_module, "_preflight", lambda staged: stale)
    return stale


@pytest.mark.parametrize("same_profile", (False, True), ids=("different-profile", "same-profile"))
def test_s1_a_first_publisher_racing_through_the_lock_window_is_refused_inside_the_lock(
    world: World, monkeypatch: pytest.MonkeyPatch, same_profile: bool
) -> None:
    """S-1: A commits between B's preflight and B's lock. B must re-read the installed
    profile / record / runtime pyz inside the lock and refuse before touching any root path,
    so the live chain stays exactly A's — with another profile as much as with the same."""

    plan_a = world.stage("a")
    if not same_profile:
        monkeypatch.setattr(stage_module, "discover_interpreter_closure", _other_closure)
    plan_b = world.stage("b", operation_id="b" * 32)
    assert (plan_b.plan["profile_id"] == plan_a.plan["profile_id"]) is same_profile
    _stale_preflight_after(world, monkeypatch, plan_b)
    world.publish(plan_a)
    profile_a = world.profile_path.read_bytes()
    record_a = world.authority_path.read_bytes()
    with pytest.raises(RuntimeAuthorityPublishError, match="changed since preflight"):
        world.publish(plan_b)
    assert world.profile_path.read_bytes() == profile_a
    assert world.authority_path.read_bytes() == record_a
    assert not (world.inbox / ("b" * 32)).exists()
    assert not (world.quarantine / ("b" * 32)).exists()
    assert not world.generation_path(plan_b).exists() or same_profile
    assert world.resolve("daily")["operation_id"] == plan_a.options.operation_id
    assert len((world.var / "publications.jsonl").read_bytes().splitlines()) == 1


def test_s1_a_runtime_pyz_replaced_in_the_lock_window_is_refused(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan = world.stage("pyz")
    _stale_preflight_after(world, monkeypatch, plan)
    world.runtime_pyz.chmod(0o644)
    world.runtime_pyz.write_bytes(RUNTIME_PYZ_BYTES + b"swapped")
    world.runtime_pyz.chmod(0o555)
    with pytest.raises(RuntimeAuthorityPublishError, match="installed runtime pyz differs"):
        world.publish(plan)
    assert not world.profile_path.exists() and not world.authority_path.exists()
    assert not (world.inbox / plan.options.operation_id).exists()
    assert not (world.quarantine / plan.options.operation_id).exists()
    assert not world.generation_path(plan).exists()


def test_publish_is_idempotent_for_the_same_operation(
    published: tuple[World, stage_module.StagePlan],
) -> None:
    world, plan = published
    before = world.authority_path.read_bytes()
    receipt = world.publish(plan)
    assert receipt["result"] == "idempotent"
    assert receipt["generation_placed"] is False and receipt["profile_installed"] is False
    assert world.authority_path.read_bytes() == before
    assert not (world.inbox / plan.options.operation_id).exists()


def test_publish_dry_run_prints_steps_and_writes_nothing(
    world: World, capsys: pytest.CaptureFixture[str]
) -> None:
    plan = world.stage("dry")
    receipt = world.publish(plan, dry_run=True)
    assert receipt["dry_run"] is True and len(receipt["steps"]) == 6
    assert "dry-run:" in capsys.readouterr().err
    _root_untouched(world)


# ---------------------------------------------------------------------------------------
# A11 / T-14 / T-15: sequence, prior, rollback
# ---------------------------------------------------------------------------------------


def test_t14_t15_a11_second_publication_and_single_level_rollback(world: World) -> None:
    first = world.stage_and_publish("first")
    first_record = world.record()
    assert first_record["sequence"] == 1
    assert all(first_record[f"prior_{field}"] is None for field in _verify._SLOT_FIELDS)

    world.bump_checkout("second")
    second = world.stage("second")
    assert second.plan["generation_id"] != first.plan["generation_id"]
    receipt = world.publish(second)
    assert receipt["sequence"] == 2 and receipt["result"] == "committed"
    record = world.record()
    assert record["sequence"] == 2 and record["state"] == "active"
    assert record["current_generation_id"] == second.plan["generation_id"]
    assert record["prior_lifecycle"] == "rollback_ready"
    assert record["prior_generation_id"] == first.plan["generation_id"]
    for role, instance in world.combos(second):
        assert world.resolve(role, instance)["generation_id"] == second.plan["generation_id"]

    rolled = publish_module.rollback_authority(operation_id="c" * 32)
    assert rolled["state"] == "rolled_back" and rolled["sequence"] == 3
    record = world.record()
    assert record["state"] == "rolled_back"
    assert record["current_generation_id"] == first.plan["generation_id"]
    assert record["current_lifecycle"] == "active"
    assert record["prior_generation_id"] == second.plan["generation_id"]
    assert record["prior_lifecycle"] == "failed"
    assert world.resolve("daily")["generation_id"] == first.plan["generation_id"]
    with pytest.raises(RuntimeAuthorityRollbackError, match="single-level"):
        publish_module.rollback_authority(operation_id="d" * 32)


# ---------------------------------------------------------------------------------------
# A13 / T-20: pyvenv.cfg
# ---------------------------------------------------------------------------------------


def test_t20_a13_pyvenv_shape_passes_both_sides_and_only_that_shape(world: World) -> None:
    plan = world.stage("pyvenv")
    payload = (plan.options.staging / "generation" / "pyvenv.cfg").read_bytes()
    assert payload == (
        f"home = {world.system_python.parent}\ninclude-system-site-packages = false\n"
        "version = 3.11.15\n"
    ).encode()
    authority_module._validate_pyvenv_config(payload, system_python=world.system_python)
    _verify.verify_pyvenv_configuration(str(plan.options.staging / "generation"))
    with pytest.raises(RuntimeAuthorityPublishError, match="home is not"):
        authority_module._validate_pyvenv_config(
            payload.replace(str(world.system_python.parent).encode(), b"/opt/elsewhere"),
            system_python=world.system_python,
        )
    with pytest.raises(RuntimeAuthorityPublishError, match="must be false"):
        authority_module._validate_pyvenv_config(
            payload.replace(b"include-system-site-packages = false\n", b""),
            system_python=world.system_python,
        )
    # S-2: the publish side normalises spacing, the wrapper matches the literal line.
    squeezed = payload.replace(
        b"include-system-site-packages = false", b"include-system-site-packages=false"
    )
    authority_module._validate_pyvenv_config(squeezed, system_python=world.system_python)
    squeezed_dir = world.root / "squeezed"
    squeezed_dir.mkdir()
    (squeezed_dir / "pyvenv.cfg").write_bytes(squeezed)
    with pytest.raises(_verify.RuntimeExecError, match="does not disable"):
        _verify.verify_pyvenv_configuration(str(squeezed_dir))
    with pytest.raises(RuntimeAuthorityStageError, match="malformed"):
        stage_module.pyvenv_config("3.11")


def test_t20_a13_the_three_lines_boot_a_copied_interpreter_under_isolated_flags(
    tmp_path: Path,
) -> None:
    """Probe 1 scenario B as a test: `home` pointing at the base interpreter's directory lets a
    physical copy boot with `-I -S`. The cloud `§7 B-5` run on RHEL remains the merge gate."""

    base = Path(getattr(sys, "_base_executable", sys.executable)).resolve()
    generation = tmp_path / "generation"
    (generation / "bin").mkdir(parents=True)
    copied = generation / "bin" / "python"
    shutil.copyfile(base, copied)
    copied.chmod(0o555)
    (generation / "pyvenv.cfg").write_bytes(
        f"home = {base.parent}\ninclude-system-site-packages = false\n"
        f"version = {platform.python_version()}\n".encode()
    )
    # Relocatable dev interpreters (uv, actions/setup-python) find `libpython` through an
    # `$ORIGIN`-relative rpath, which a lone copy loses; the production `/usr/bin/python3.11`
    # links the soname from the loader path. The library path is test scaffolding only and
    # has no bearing on what is being proved: `home` deciding `sys.prefix`/`base_prefix`.
    library_dirs = os.pathsep.join(str(base.parents[1] / name) for name in ("lib", "lib64"))
    probe = subprocess.run(
        [str(copied), "-I", "-S", "-c",
         "import json, os, sys; print(json.dumps("
         "[sys.base_prefix, sys._base_executable, os.__file__, sys.flags.isolated]))"],
        capture_output=True, text=True, check=False, cwd=str(tmp_path),
        env={"PATH": "/usr/bin:/bin", "LANG": "C", "LD_LIBRARY_PATH": library_dirs,
             "DYLD_LIBRARY_PATH": library_dirs},
    )
    assert probe.returncode == 0, probe.stderr
    base_prefix, base_executable, os_file, isolated = json.loads(probe.stdout)
    assert isolated == 1
    # `home` was read: the interpreter names the profile's directory as its base, resolves
    # its standard library outside the generation, and the generation itself holds none.
    assert Path(base_executable).parent == base.parent
    assert Path(base_prefix).resolve() != generation.resolve()
    assert Path(os_file).is_file() and generation not in Path(os_file).resolve().parents


# ---------------------------------------------------------------------------------------
# A17 / A18 / A19 / A22: bootstrap from a checkout
# ---------------------------------------------------------------------------------------


def test_a17_bootstrap_never_touches_a_legacy_root(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    legacy = world.root / "home" / "lighthouse" / "rquant" / "data" / "runtime"
    assert not legacy.parent.exists()

    def forbidden(**kwargs: Any) -> Any:
        raise AssertionError("bootstrap mode consulted the legacy chain")

    monkeypatch.setattr(stage_module, "legacy_services", forbidden)
    plan = world.stage("a17", legacy_runtime_root=legacy)
    assert plan.plan["mode"] == "bootstrap"
    assert not legacy.exists() and not legacy.parent.exists()
    assert stage_module.main(world.argv("a17-cli", "--legacy-runtime-root", str(legacy))) == 0
    assert not legacy.exists()


def test_a18_derived_labels_match_the_spec_table_and_the_unit_literals(world: World) -> None:
    plan = world.stage("a18")
    mapping = world.instances(plan)
    derived = {
        role: [stage_module.instance_label(service_id) for service_id in service_ids]
        for role, service_ids in BOOTSTRAP_SERVICE_IDS.items()
    }
    assert sum(len(labels) for labels in derived.values()) == 26
    for role, labels in derived.items():
        assert mapping[role] == sorted(labels), role
    for label, service_id in plan.plan["service_manifests"].items():
        orphan_ids = (stage_module.RECOVERY_SERVICE_ID, stage_module.PAGE_CONTROL_SERVICE_ID)
        if service_id not in orphan_ids:
            assert stage_module.instance_label(service_id) == label
    retention_unit = (
        REPO_ROOT / "deploy" / "systemd" / "rquant-artifact-retention.service"
    ).read_text()
    assert mapping["artifact_retention"] == [
        stage_module.instance_label("artifact-retention.primary.v1")
    ]
    assert mapping["artifact_retention"][0] in retention_unit
    source_unit = (
        REPO_ROOT / "deploy" / "systemd" / "rquant-runtime-reference-slow-source@.service"
    ).read_text()
    assert mapping["reference_slow_publisher"] == [
        stage_module.instance_label("reference-slow.publisher.v1")
    ]
    assert mapping["reference_slow_publisher"][0] in source_unit
    from rquant.runtime_deployment_bundle import _instance_name

    assert stage_module.instance_label("x.y.v1") == _instance_name("x.y.v1")


def test_a19_page_control_label_is_the_unit_literal_and_must_be_unique(world: World) -> None:
    unit = world.checkout / "deploy" / "systemd" / "rquant-page-control.service"
    literal = "svc-981cb38218dd899500ee1592a504790a57d459c946bbc53c8e210f299cf1980b"
    assert literal in (REPO_ROOT / "deploy" / "systemd" / "rquant-page-control.service").read_text()
    plan = world.stage("a19")
    assert world.instances(plan)["page_control"] == [literal]
    text = unit.read_text(encoding="utf-8")
    for variant, expected in (
        (text.replace(literal, "svc-none"), "0 instance literals"),
        (text + f"\n# {'svc-' + 'e' * 64}\n", "2 instance literals"),
    ):
        unit.write_text(variant, encoding="utf-8")
        _git(world.checkout, "commit", "-q", "-am", expected)
        world.commit = _git(world.checkout, "rev-parse", "HEAD").strip()
        with pytest.raises(RuntimeAuthorityStageError, match=expected):
            stage_module.build_stage_plan(world.options("a19-" + expected[0]))
        assert stage_module.main(world.argv("a19-cli-" + expected[0])) == 1


def test_a22_stage_never_constructs_settings(world: World, monkeypatch: pytest.MonkeyPatch) -> None:
    import rquant.config as config

    def refuse() -> object:
        raise AssertionError("stage constructed Settings")

    monkeypatch.setattr(config, "get_settings", refuse)
    monkeypatch.setattr(config, "Settings", refuse, raising=False)
    world.stage("a22")


def test_a22_stage_runs_in_a_subprocess_with_no_configuration(world: World, tmp_path: Path) -> None:
    """The module entry (`python -m rquant.runtime_authority_stage`), an empty environment,
    no `.env`, the closure seams pinned inside the child. `rquant.config` is never imported.

    The `rquant` console script cannot offer this: `rquant.cli` imports `rquant.logging`, which
    constructs `Settings` at import time, so `rquant runtime-authority-stage` needs
    configuration like every other subcommand — the no-`.env` path is the module entry.
    """

    program = (
        "import os, sys, json\n"
        "from pathlib import Path\n"
        "import rquant.runtime_authority as authority\n"
        "import rquant.runtime_authority_stage as stage\n"
        "from rquant.runtime_authority import RuntimeAncestorPolicy, RuntimeFilePolicy\n"
        "world = json.loads(os.environ['PA2_WORLD'])\n"
        "authority.PRODUCTION_SYSTEM_PYTHON = Path(world['system_python'])\n"
        "authority.RUNTIME_AUTHORITY_OWNER_UID = os.getuid()\n"
        "authority.PRODUCTION_PROFILE_OWNER_UID = os.getuid()\n"
        "stage.DAILY_RECEIPT_KEYRING_PATH = Path(world['keyring'])\n"
        "def closure(python):\n"
        "    fp = lambda p, d, m: RuntimeFilePolicy(Path(p), d, 0, m)\n"
        "    return stage.InterpreterClosure('3.11.15',"
        " fp('/usr/lib64/ld-linux-x86-64.so.2', '1'*64, 0o555),"
        " (fp('/usr/lib64/python3.11/os.py', '2'*64, 0o444),),"
        " (fp('/usr/lib64/libpython3.11.so.1.0', '3'*64, 0o555),))\n"
        "stage.discover_interpreter_closure = closure\n"
        "stage.ancestor_policies = lambda paths: tuple(RuntimeAncestorPolicy(p, 0, 0o755)"
        " for p in sorted({q for x in paths for q in Path(x).parents}, key=str))\n"
        "code = stage.main(world['argv'])\n"
        "assert 'rquant.config' not in sys.modules, 'rquant.config was imported'\n"
        "sys.stderr.write('SETTINGS-UNTOUCHED\\n')\n"
        "raise SystemExit(code)\n"
    )
    environment = {
        "PATH": "/usr/bin:/bin",
        "LANG": "C",
        "RQUANT_DISABLE_DOTENV": "1",
        "PA2_WORLD": json.dumps({
            "system_python": str(world.system_python),
            "keyring": str(world.keyring),
            "argv": world.argv("a22-sub"),
        }),
    }
    result = subprocess.run(
        [sys.executable, "-c", program], cwd=str(tmp_path), env=environment,
        capture_output=True, check=False,
    )
    assert result.returncode == 0, result.stderr.decode()
    assert b"SETTINGS-UNTOUCHED" in result.stderr
    plan = publish_module.parse_plan(result.stdout)
    assert plan["mode"] == "bootstrap"
    assert not (world.checkout / ".env").exists()


def test_a23_deploy_notes_record_the_first_gate_set(world: World) -> None:
    deploy = (REPO_ROOT / "DEPLOY.md").read_text(encoding="utf-8")
    assert "16/26" in deploy
    for unit in FIRST_GATE_UNITS:
        assert unit in deploy, unit
    for unit in DEFERRED_UNITS:
        assert unit in deploy, unit
    assert "未启用" in deploy
    assert "首次安装场景无从执行" in deploy


# ---------------------------------------------------------------------------------------
# Legacy mode, the checkout gate, and the link-count convention
# ---------------------------------------------------------------------------------------


def test_legacy_mode_copies_manifests_verbatim_and_yields_the_same_generation(world: World) -> None:
    bootstrap = world.stage("bootstrap")
    generation = bootstrap.options.staging / "generation"
    legacy = world.root / "legacy-runtime"
    legacy_generation = legacy / "generations" / ("a" * 64)
    (legacy_generation / "manifests").mkdir(parents=True)
    mapping = world.instances(bootstrap)
    for role in KIND_BACKED_ROLES:
        for label in mapping[role]:
            shutil.copyfile(
                generation / "manifests" / f"{label}.json",
                legacy_generation / "manifests" / f"{label}.json",
            )
    os.symlink(f"generations/{'a' * 64}", legacy / "current")
    legacy_plan = world.stage(
        "legacy", bootstrap_from_checkout=False, legacy_runtime_root=legacy,
        operation_id=bootstrap.options.operation_id,
    )
    assert legacy_plan.plan["mode"] == "legacy"
    assert legacy_plan.plan["generation_id"] == bootstrap.plan["generation_id"]
    assert legacy_plan.plan["instance_mapping"] == bootstrap.plan["instance_mapping"]
    assert legacy_plan.manifest_payload == bootstrap.manifest_payload
    with pytest.raises(RuntimeAuthorityStageError, match="missing"):
        world.stage("legacy-missing", bootstrap_from_checkout=False,
                    legacy_runtime_root=world.root / "nowhere")


def test_checkout_gate_requires_head_and_a_clean_tree(world: World) -> None:
    with pytest.raises(RuntimeAuthorityStageError, match="is not --commit"):
        stage_module.build_stage_plan(world.options("head", commit="0" * 40))
    with pytest.raises(RuntimeAuthorityStageError, match="not a 40-hex"):
        stage_module.build_stage_plan(world.options("head", commit="HEAD"))
    (world.checkout / "src" / "rquant" / "stray.py").write_text("", encoding="utf-8")
    with pytest.raises(RuntimeAuthorityStageError, match="not clean"):
        stage_module.build_stage_plan(world.options("dirty"))
    (world.checkout / "src" / "rquant" / "stray.py").unlink()
    (world.checkout / "notes.txt").write_text("untracked outside the mirrored paths\n")
    stage_module.build_stage_plan(world.options("untracked-elsewhere"))
    (world.checkout / "README.md").write_text("modified\n", encoding="utf-8")
    with pytest.raises(RuntimeAuthorityStageError, match="not clean"):
        stage_module.build_stage_plan(world.options("modified"))


def test_venv_symlinks_and_import_hooks_are_refused(world: World) -> None:
    site = world.venv / "lib" / "python3.11" / "site-packages"
    os.symlink("_marker.py", site / "alias.py")
    with pytest.raises(RuntimeAuthorityStageError, match="symlink"):
        stage_module.build_stage_plan(world.options("symlink"))
    (site / "alias.py").unlink()
    (site / "sitecustomize.py").write_text("", encoding="utf-8")
    with pytest.raises(RuntimeAuthorityStageError, match="import hook"):
        stage_module.build_stage_plan(world.options("hook"))


def test_link_convention_prediction_matches_the_materialised_tree(tmp_path: Path) -> None:
    convention = publish_module.detect_directory_link_convention(tmp_path)
    layout = publish_module.GenerationLayout.build(
        {
            "a/one.txt": publish_module.StagedFile(0o444, payload=b"1"),
            "a/two.txt": publish_module.StagedFile(0o444, payload=b"2"),
            "a/b/three.txt": publish_module.StagedFile(0o444, payload=b"3"),
            "bin/python": publish_module.StagedFile(0o555, payload=b"#!"),
        },
        empty_directories=("cwd",),
    )
    predicted = publish_module.predict_manifest_entries(
        layout, owner_uid=UID, convention=convention
    )
    target = tmp_path / "tree"
    target.mkdir()
    publish_module.materialize_layout(layout, target)
    assert publish_module.scan_frozen_tree(target, owner_uid=UID) == predicted
    with pytest.raises(RuntimeAuthorityStageError, match="collide"):
        publish_module.GenerationLayout.build(
            {"A.py": publish_module.StagedFile(0o444, payload=b""),
             "a.py": publish_module.StagedFile(0o444, payload=b"")}
        )


def test_plan_parser_rejects_non_canonical_and_malformed_plans(world: World) -> None:
    plan = world.stage("plan")
    payload = plan.plan_payload
    assert publish_module.parse_plan(payload) == plan.plan
    with pytest.raises(RuntimeAuthorityStageError, match="not canonical"):
        publish_module.parse_plan(payload + b"\n")
    document = dict(plan.plan)
    escape = {"type": "file", "mode": 0o444, "size": 0, "sha256": "0" * 64}
    document["staged_files"] = {**document["staged_files"], "../escape": escape}
    with pytest.raises(RuntimeAuthorityStageError, match="invalid component"):
        publish_module.parse_plan(publish_module.plan_bytes(document))
    document = dict(plan.plan)
    document["sequence"] = 2
    with pytest.raises(RuntimeAuthorityStageError, match="disagrees"):
        publish_module.parse_plan(publish_module.plan_bytes(document))


def test_real_checkout_stages_in_a_dry_run(
    world: World, tmp_path: Path, capsysbinary: pytest.CaptureFixture[bytes]
) -> None:
    """A2 against the repository itself: a detached worktree of HEAD is always clean."""

    worktree = tmp_path / "real-checkout"
    subprocess.run(
        ["git", "-C", str(REPO_ROOT), "worktree", "add", "--detach", "-q", str(worktree), "HEAD"],
        check=True, capture_output=True,
    )
    try:
        commit = _git(worktree, "rev-parse", "HEAD").strip()
        plan = stage_module.build_stage_plan(
            world.options("real", checkout_root=worktree, commit=commit)
        )
        staged = plan.plan["staged_files"]
        assert staged["generation/src/rquant/runtime_authority.py"]["type"] == "file"
        assert staged["generation/src/rquant/runtime_service_main.py"]["type"] == "file"
        assert staged["generation/scripts/strict_json.py"]["type"] == "file"
        assert plan.plan["closure_summary"]["generation_entries"] > 300
        assert not (world.staging_root / "real").exists()
    finally:
        subprocess.run(
            ["git", "-C", str(REPO_ROOT), "worktree", "remove", "--force", str(worktree)],
            check=False, capture_output=True,
        )


# ---------------------------------------------------------------------------------------
# #198 BLK-1: the host's `platstdlib` is a directory the distribution never creates
# ---------------------------------------------------------------------------------------


def _facts(stdlib: Path, platstdlib: Path) -> dict[str, str]:
    return {"version": "3.11.6", "stdlib": str(stdlib), "platstdlib": str(platstdlib)}


def test_blk1_a_platstdlib_the_distribution_never_creates_is_skipped_and_recorded(
    tmp_path: Path,
) -> None:
    """The 2026-09-05 shape on 82.156.0.68: RHEL redirects `platstdlib` into `/usr/local`.

    `sysconfig` there answers `stdlib=/usr/lib64/python3.11` (present) and
    `platstdlib=/usr/local/lib64/python3.11` (absent, and the distribution never creates
    it). A directory that does not exist contributes no files, so the closure is exactly
    the one the walk would have produced anyway — but the skip has to be visible.
    """

    stdlib = tmp_path / "usr" / "lib64" / "python3.11"
    (stdlib / "json").mkdir(parents=True)
    (stdlib / "os.py").write_bytes(b"x")
    (stdlib / "json" / "__init__.py").write_bytes(b"x")
    absent = tmp_path / "usr" / "local" / "lib64" / "python3.11"

    walked, skipped = stage_module.stdlib_directories(_facts(stdlib, absent))

    assert walked == (stdlib,)
    assert skipped == (absent,)
    assert stage_module.stdlib_files(walked) == tuple(
        sorted([stdlib / "json" / "__init__.py", stdlib / "os.py"])
    )


def test_blk1_a_missing_stdlib_is_still_refused(tmp_path: Path) -> None:
    absent = tmp_path / "usr" / "lib64" / "python3.11"
    platstdlib = tmp_path / "usr" / "local" / "lib64" / "python3.11"
    platstdlib.mkdir(parents=True)

    with pytest.raises(RuntimeAuthorityStageError, match="standard library directory is missing"):
        stage_module.stdlib_directories(_facts(absent, platstdlib))


def test_blk1_two_existing_subtrees_are_both_walked_exactly_as_before(tmp_path: Path) -> None:
    stdlib = tmp_path / "usr" / "lib" / "python3.11"
    platstdlib = tmp_path / "usr" / "lib64" / "python3.11"
    stdlib.mkdir(parents=True)
    platstdlib.mkdir(parents=True)
    (stdlib / "os.py").write_bytes(b"x")
    (platstdlib / "_json.so").write_bytes(b"x")

    walked, skipped = stage_module.stdlib_directories(_facts(stdlib, platstdlib))

    assert walked == tuple(sorted([stdlib, platstdlib]))
    assert skipped == ()
    assert stage_module.stdlib_files(walked) == tuple(
        sorted([stdlib / "os.py", platstdlib / "_json.so"])
    )


def test_blk1_a_platstdlib_equal_to_the_stdlib_is_walked_once(tmp_path: Path) -> None:
    """The Debian shape, which is why the ubuntu CI runners never saw this."""

    stdlib = tmp_path / "usr" / "lib" / "python3.11"
    stdlib.mkdir(parents=True)

    assert stage_module.stdlib_directories(_facts(stdlib, stdlib)) == ((stdlib,), ())


def test_blk1_the_plan_records_the_subtrees_walked_and_the_ones_skipped(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    walked = Path("/usr/lib64/python3.11")
    absent = Path("/usr/local/lib64/python3.11")

    def closure(python: Path) -> stage_module.InterpreterClosure:
        base = _fake_closure(python)
        return stage_module.InterpreterClosure(
            version=base.version,
            elf_loader=base.elf_loader,
            stdlib=base.stdlib,
            shared_libraries=base.shared_libraries,
            stdlib_roots=(walked,),
            skipped_stdlib_roots=(absent,),
        )

    monkeypatch.setattr(stage_module, "discover_interpreter_closure", closure)

    summary = world.stage("blk1").plan["closure_summary"]

    assert summary["stdlib_roots"] == [str(walked)]
    assert summary["skipped_stdlib_roots"] == [str(absent)]


def test_blk1_the_real_closure_walk_records_the_skip_it_took(
    world: World,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`discover_interpreter_closure` itself, not a stand-in for it.

    Every other case in this file replaces this function with `_fake_closure`, which left
    the wiring between `stdlib_directories` and what the plan reports unguarded: dropping
    `skipped_stdlib_roots=` from the constructor, or the two log lines, kept the suite
    green while making the skip silent on the host — the one shape #198 BLK-1 was fixed to
    avoid, and the one the B-6' acceptance step reads.
    """

    stdlib = tmp_path / "usr" / "lib64" / "python3.11"
    stdlib.mkdir(parents=True)
    (stdlib / "os.py").write_bytes(b"os\n")
    absent = tmp_path / "usr" / "local" / "lib64" / "python3.11"
    loader = tmp_path / "ld-linux-x86-64.so.2"
    loader.write_bytes(b"loader")
    library = tmp_path / "libpython3.11.so.1.0"
    library.write_bytes(b"library")
    for path in (loader, library, stdlib / "os.py"):
        path.chmod(0o644)

    monkeypatch.setattr(
        stage_module,
        "interpreter_facts",
        lambda python: {"version": "3.11.6", "stdlib": str(stdlib), "platstdlib": str(absent)},
    )
    monkeypatch.setattr(stage_module.shutil, "which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(
        stage_module,
        "_run",
        lambda command, *, label: (
            f"      [Requesting program interpreter: {loader}]\n"
            if label == "readelf"
            else f"\tlibpython3.11.so.1.0 => {library} (0x00007f0000000000)\n"
        ),
    )

    closure = _REAL_INTERPRETER_CLOSURE(world.system_python)

    assert closure.stdlib_roots == (stdlib,)
    assert closure.skipped_stdlib_roots == (absent,)
    assert [item.path for item in closure.stdlib] == [stdlib / "os.py"]
    assert closure.elf_loader.path == Path(os.path.realpath(loader))
    assert [item.path for item in closure.shared_libraries] == [Path(os.path.realpath(library))]
    logged = capsys.readouterr().err
    assert f"stdlib subtrees {stdlib}\n" in logged
    assert f"stdlib subtrees absent on this host and skipped: {absent}\n" in logged

    # And the plan states the same two sets, so the whole chain is nailed end to end.
    monkeypatch.setattr(stage_module, "discover_interpreter_closure", lambda python: closure)
    summary = world.stage("real-closure").plan["closure_summary"]
    assert summary["stdlib_roots"] == [str(stdlib)]
    assert summary["skipped_stdlib_roots"] == [str(absent)]


# ---------------------------------------------------------------------------------------
# #198 BLK-2: an ancestor the distribution owns, at the mode the distribution ships
# ---------------------------------------------------------------------------------------


@contextmanager
def _mode(path: Path, mode: int) -> Iterator[None]:
    original = stat.S_IMODE(path.lstat().st_mode)
    path.chmod(mode)
    try:
        yield
    finally:
        path.chmod(original)


def _lock_world(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Path, Path, dict[Path, tuple[int, int | None]]]:
    """A stand-in `/` with the authority anchor under it, plus the policy that closes it.

    The returned dict is the very object the module now consults, so a test can relax or
    tighten one entry after the fact and the next walk sees it.
    """

    root = tmp_path / "root"
    anchor = root / "var" / "lib" / "rquant" / "runtime-authority"
    anchor.mkdir(parents=True)
    for path in (root, root / "var", root / "var" / "lib", anchor.parent, anchor):
        path.chmod(0o755)
    policy: dict[Path, tuple[int, int | None]] = dict(_directory_policy(anchor))
    monkeypatch.setattr(authority_module, "RUNTIME_AUTHORITY_ANCHOR", anchor)
    monkeypatch.setattr(authority_module, "RUNTIME_AUTHORITY_LOCK_PATH", anchor / "deployment.lock")
    monkeypatch.setattr(authority_module, "RUNTIME_AUTHORITY_OWNER_UID", UID)
    monkeypatch.setattr(authority_module, "_PRODUCTION_RUNTIME_DIRECTORY_POLICY", policy)
    return root, anchor, policy


def test_blk2_a_distribution_owned_ancestor_at_0555_is_accepted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """OpenCloudOS 9.2 ships `/` as `dr-xr-xr-x`; `rpm -V filesystem` is clean on that host."""

    root, _anchor, policy = _lock_world(tmp_path, monkeypatch)
    policy[root] = (UID, None)

    with _mode(root, 0o555), authority_module.acquire_runtime_deployment_lock() as lock:
        lock.assert_current()


@pytest.mark.parametrize("mode", (0o757, 0o775), ids=("other-writable", "group-writable"))
def test_blk2_a_relaxed_ancestor_that_anyone_can_write_is_still_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: int
) -> None:
    root, _anchor, policy = _lock_world(tmp_path, monkeypatch)
    policy[root] = (UID, None)

    with _mode(root, mode), pytest.raises(RuntimeAuthorityPublishError, match="ancestor"):
        authority_module.acquire_runtime_deployment_lock()


def test_blk2_a_relaxed_ancestor_owned_by_someone_else_is_still_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, _anchor, policy = _lock_world(tmp_path, monkeypatch)
    policy[root] = (UID + 1, None)

    with _mode(root, 0o555), pytest.raises(RuntimeAuthorityPublishError, match="ancestor"):
        authority_module.acquire_runtime_deployment_lock()


def test_blk2_a_directory_the_publisher_declares_a_mode_for_stays_exact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only the distribution's own directories were relaxed; rQuant's anchor was not."""

    root, anchor, policy = _lock_world(tmp_path, monkeypatch)
    policy[root] = (UID, None)
    assert policy[anchor] == (UID, 0o755)

    with (
        _mode(root, 0o555),
        _mode(anchor, 0o555),
        pytest.raises(RuntimeAuthorityPublishError, match="ancestor"),
    ):
        authority_module.acquire_runtime_deployment_lock()


def test_blk2_the_deployment_lock_file_keeps_its_exact_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, anchor, policy = _lock_world(tmp_path, monkeypatch)
    policy[root] = (UID, None)
    lock_path = anchor / "deployment.lock"
    lock_path.write_bytes(b"")
    lock_path.chmod(0o644)

    with (
        _mode(root, 0o555),
        pytest.raises(RuntimeAuthorityPublishError, match="deployment lock is unsafe"),
    ):
        authority_module.acquire_runtime_deployment_lock()


def test_blk2_only_the_ancestors_the_distribution_owns_are_relaxed() -> None:
    runtime = authority_module._PRODUCTION_RUNTIME_DIRECTORY_POLICY
    profile = authority_module._PRODUCTION_PROFILE_DIRECTORY_POLICY

    relaxed_runtime = {path for path, (_uid, mode) in runtime.items() if mode is None}
    relaxed_profile = {path for path, (_uid, mode) in profile.items() if mode is None}
    assert relaxed_runtime == {Path("/"), Path("/var"), Path("/var/lib")}
    assert relaxed_profile == {Path("/"), Path("/etc")}
    # Both tables and the quarantine walk read one list, so they cannot drift apart.
    assert relaxed_runtime | relaxed_profile == authority_module._DISTRIBUTION_OWNED_DIRECTORIES
    assert {path: mode for path, (_uid, mode) in runtime.items() if mode is not None} == {
        Path("/var/lib/rquant"): 0o755,
        authority_module.RUNTIME_AUTHORITY_ANCHOR: 0o755,
        authority_module.PRODUCTION_GENERATION_ROOT: 0o755,
    }
    assert {path: mode for path, (_uid, mode) in profile.items() if mode is not None} == {
        authority_module.PRODUCTION_PROFILE_ANCHOR: 0o755,
    }
    assert all(uid == 0 for uid, _mode in (*runtime.values(), *profile.values()))


def test_blk2_the_publisher_creates_only_the_directories_whose_mode_it_declares(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    declared = root / "rquant"
    undeclared = root / "distribution-owned"
    monkeypatch.setattr(
        authority_module,
        "_PRODUCTION_RUNTIME_DIRECTORY_POLICY",
        {undeclared: (UID, None), declared: (UID, 0o755)},
    )
    monkeypatch.setattr(authority_module, "_PRODUCTION_PROFILE_DIRECTORY_POLICY", {})
    monkeypatch.setattr(authority_module, "PRODUCTION_INBOX_ROOT", root / "inbox")
    monkeypatch.setattr(authority_module, "PRODUCTION_QUARANTINE_ROOT", root / "quarantine")
    monkeypatch.setattr(authority_module, "RUNTIME_AUTHORITY_OWNER_UID", UID)
    monkeypatch.setattr(publish_module, "PUBLISH_OWNER_GID", GID)

    publish_module._ensure_authority_directories()

    assert declared.is_dir() and stat.S_IMODE(declared.lstat().st_mode) == 0o755
    assert not undeclared.exists()


# ---------------------------------------------------------------------------------------
# BLK-3 (#200): the bootstrap manifests carry each role's real plane
# ---------------------------------------------------------------------------------------


def _staged_manifests(
    world: World, plan: stage_module.StagePlan
) -> dict[str, list[Any]]:
    """Every kind-backed manifest of a staged generation, parsed by the real contract."""

    from rquant.runtime_service_entrypoint import RuntimeServiceManifest

    generation = plan.options.staging / "generation"
    mapping = world.instances(plan)
    manifests: dict[str, list[Any]] = {}
    for role in KIND_BACKED_ROLES:
        for label in mapping[role]:
            payload = (generation / "manifests" / f"{label}.json").read_bytes()
            manifests.setdefault(role, []).append(
                RuntimeServiceManifest.model_validate_json(payload)
            )
    return manifests


def test_blk3_every_kind_backed_manifest_carries_the_role_plane(
    published: tuple[World, stage_module.StagePlan],
) -> None:
    """#200: route B wrote `plane: live` into all 28 manifests, so the seven serving and
    research builders refused their own manifest on the production host."""

    from rquant.runtime_deployment_bundle import _EXPECTED_PLANE
    from rquant.runtime_service_control import RuntimeServicePlane
    from rquant.runtime_service_entrypoint import RuntimeServiceKind

    world, plan = published
    manifests = _staged_manifests(world, plan)
    assert set(manifests) == set(KIND_BACKED_ROLES)
    observed: dict[str, RuntimeServicePlane] = {}
    for role, staged in manifests.items():
        expected = _EXPECTED_PLANE[RuntimeServiceKind(role)]
        for manifest in staged:
            assert manifest.plane is expected, role
        observed[role] = expected
    assert set(observed.values()) == {
        RuntimeServicePlane.LIVE,
        RuntimeServicePlane.SERVING,
        RuntimeServicePlane.RESEARCH,
    }
    assert observed["runtime_health_publisher"] is RuntimeServicePlane.SERVING
    assert observed["serving_publisher"] is RuntimeServicePlane.SERVING
    assert observed["daily_pipeline_orchestrator"] is RuntimeServicePlane.RESEARCH
    assert observed["paper_broker"] is RuntimeServicePlane.LIVE


def test_blk3_the_plane_comes_from_the_shared_table_not_a_second_copy(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The one authority is `runtime_deployment_bundle._EXPECTED_PLANE`; bend it and the
    staged manifest bends with it (the `_DISTRIBUTION_OWNED_DIRECTORIES` pattern of #198)."""

    import rquant.runtime_deployment_bundle as bundle
    from rquant.runtime_service_control import RuntimeServicePlane
    from rquant.runtime_service_entrypoint import RuntimeServiceKind

    bent = dict(bundle._EXPECTED_PLANE)
    bent[RuntimeServiceKind.PAPER_BROKER] = RuntimeServicePlane.RESEARCH
    monkeypatch.setattr(bundle, "_EXPECTED_PLANE", bent)
    plan = world.stage("blk3-plane-seam")
    manifests = _staged_manifests(world, plan)
    assert manifests["paper_broker"][0].plane is RuntimeServicePlane.RESEARCH


# ---------------------------------------------------------------------------------------
# BLK-3 (#200): the bootstrap manifests carry grounded settings
# ---------------------------------------------------------------------------------------

#: The settings model each builder validates its own manifest against, named by the module
#: that calls `model_validate(dict(manifest.settings))`. This is the acceptance's authority:
#: a staged manifest is only startable if its settings pass the model of its own builder.
SETTINGS_MODELS = {
    "artifact_retention": ("rquant.runtime_builder_retention", "ArtifactRetentionSettings"),
    "auction_match_source": ("rquant.runtime_service_builtin", "AuctionMatchSourceSettings"),
    "auction_universe_publisher": (
        "rquant.runtime_service_builtin",
        "AuctionUniversePublisherSettings",
    ),
    "candidate_publisher": (
        "rquant.runtime_builder_candidate",
        "CandidatePublisherRuntimeSettings",
    ),
    "daily_close_source": ("rquant.runtime_builder_daily", "DailyCloseSourceSettings"),
    "daily_pipeline_orchestrator": (
        "rquant.runtime_builder_daily_orchestrator",
        "DailyPipelineOrchestratorSettings",
    ),
    "feature_live": ("rquant.runtime_builder_feature", "FeatureLiveRuntimeSettings"),
    "lab_artifact_catalog": ("rquant.runtime_builder_artifact_catalog", "ArtifactCatalogSettings"),
    "lab_jobs_publisher": ("rquant.runtime_builder_authority", "LabJobsPublisherSettings"),
    "market_minute_source": ("rquant.runtime_service_builtin", "MarketMinuteSourceSettings"),
    "notifier": ("rquant.runtime_builder_signal", "NotifierSettings"),
    "paper_broker": ("rquant.runtime_builder_paper", "PaperBrokerSettings"),
    "paper_constraint_publisher": (
        "rquant.runtime_builder_authority",
        "PaperConstraintRuntimeSettings",
    ),
    "promotions_publisher": ("rquant.runtime_builder_authority", "PromotionsPublisherSettings"),
    "reference_slow_publisher": (
        "rquant.runtime_service_builtin",
        "ReferenceSlowPublisherSettings",
    ),
    "reference_slow_source": ("rquant.runtime_service_builtin", "ReferenceSlowSourceSettings"),
    "runtime_health_publisher": (
        "rquant.runtime_builder_authority",
        "RuntimeHealthPublisherSettings",
    ),
    "serving_publisher": ("rquant.runtime_builder_serving", "ServingRuntimeSettings"),
    "shadow_session": ("rquant.runtime_builder_shadow", "ShadowSessionSettings"),
    "signal_router": ("rquant.runtime_builder_signal", "SignalRouterSettings"),
    "strategy_live": ("rquant.runtime_builder_strategy", "StrategyLiveRuntimeSettings"),
    "watchlist_quote_source": ("rquant.runtime_service_builtin", "WatchlistQuoteSourceSettings"),
}
#: What a first installation cannot know, per role, stated as the validation error the
#: builder's own model raises. Every one of these is an operator fact that lives outside the
#: runtime owner root (`ProductionRuntimeProfileInputs.validate_complete_authority_set`
#: refuses an immutable input inside it) or a signer public key installed under
#: `/etc/rquant`: the market calendar generation, the sealed candidate documents, the
#: historical minute snapshot, the definition registry, the routing policy, the recovery and
#: retention authorities, the artifact location. Route B derives everything else; it does
#: not invent these, so the missing fact names itself instead of hiding behind a placeholder
#: (#200). An empty tuple means the role's settings are complete and it can start.
UNGROUNDED_BOOTSTRAP_FACTS = {
    "artifact_retention": (
        "missing:full_recovery_receipt_id",
        "missing:migration",
        "missing:recovery_profile_generation",
        "missing:recovery_publication_root",
        "missing:recovery_restore_root",
        "missing:recovery_target_manifest_id",
        "missing:schema_authority_path",
        "missing:schema_authority_root",
        "missing:schema_authority_sha256",
    ),
    "auction_match_source": (
        "missing:calendar_content_sha256",
        "missing:calendar_expected_commit",
        "missing:calendar_path",
    ),
    "auction_universe_publisher": (
        "missing:calendar_content_sha256",
        "missing:calendar_expected_commit",
        "missing:calendar_path",
    ),
    "candidate_publisher": ("value_error:candidate_input_path is required for sealed_document",),
    "daily_close_source": (
        "missing:calendar_content_sha256",
        "missing:calendar_expected_commit",
        "missing:calendar_path",
    ),
    "daily_pipeline_orchestrator": (),
    "feature_live": (
        "missing:historical_minutes_snapshot_path",
        "missing:historical_snapshot_id",
    ),
    "lab_artifact_catalog": ("missing:failure_domain", "missing:location_id"),
    "lab_jobs_publisher": (),
    "market_minute_source": (),
    "notifier": (),
    "paper_broker": (),
    "paper_constraint_publisher": (),
    "promotions_publisher": (),
    "reference_slow_publisher": (
        "missing:calendar_content_sha256",
        "missing:calendar_expected_commit",
        "missing:calendar_path",
    ),
    "reference_slow_source": (
        "missing:calendar_content_sha256",
        "missing:calendar_expected_commit",
        "missing:calendar_path",
    ),
    "runtime_health_publisher": (),
    "serving_publisher": (),
    "shadow_session": (
        "missing:calendar_content_sha256",
        "missing:calendar_expected_commit",
        "missing:calendar_path",
        "missing:completion_active_key_id",
        "missing:completion_active_public_key_pem",
        "missing:report_active_key_id",
        "missing:report_active_public_key_pem",
        "missing:runner_manifest_bindings",
    ),
    "signal_router": ("missing:routing_policy_fingerprint",),
    "strategy_live": ("missing:definition_registry_root",),
    "watchlist_quote_source": (),
}


def _settings_model(role: str) -> Any:
    import importlib

    module, name = SETTINGS_MODELS[role]
    return getattr(importlib.import_module(module), name)


def _validation_signatures(role: str, settings: Any) -> tuple[str, ...]:
    from pydantic import ValidationError

    try:
        _settings_model(role).model_validate(dict(settings))
    except ValidationError as exc:
        signatures = set()
        for error in exc.errors():
            location = ".".join(str(part) for part in error["loc"])
            signatures.add(
                f"missing:{location}" if error["type"] == "missing" else f"{error['type']}:"
                f"{error['msg'].removeprefix('Value error, ')}"
            )
        return tuple(sorted(signatures))
    return ()


@pytest.fixture
def bootstrap(
    world: World, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[World, stage_module.StagePlan]:
    """A staged generation whose manifests address a runtime root this host really has.

    The derived settings are the production topology; a test that hands them to the real
    models and the real builders needs that topology to exist somewhere writable, so the
    root seam moves and every path moves with it.
    """

    root = tmp_path / "runtime-root"
    root.mkdir()
    monkeypatch.setattr(stage_module, "PRODUCTION_RUNTIME_ROOT", root.resolve())
    return world, world.stage("blk3-bootstrap")


def test_blk3_every_kind_backed_manifest_validates_or_names_its_missing_facts(
    bootstrap: tuple[World, stage_module.StagePlan],
) -> None:
    """#200 acceptance: settings are validated by the real builder model, not a stub."""

    world, plan = bootstrap
    manifests = _staged_manifests(world, plan)
    assert set(manifests) == set(SETTINGS_MODELS) == set(UNGROUNDED_BOOTSTRAP_FACTS)
    for role, staged in sorted(manifests.items()):
        for manifest in staged:
            assert _validation_signatures(role, manifest.settings) == (
                UNGROUNDED_BOOTSTRAP_FACTS[role]
            ), role
    #: Settings-complete is not the same as startable: the default market-minute, watchlist
    #: and paper-broker builders additionally demand a market or trade calendar their own
    #: model calls optional, and the daily orchestrator loads a deployment profile document
    #: route B never writes. The end-to-end case below is what says who really starts.
    settings_complete = {role for role, gaps in UNGROUNDED_BOOTSTRAP_FACTS.items() if not gaps}
    assert settings_complete == {
        "daily_pipeline_orchestrator",
        "lab_jobs_publisher",
        "market_minute_source",
        "notifier",
        "paper_broker",
        "paper_constraint_publisher",
        "promotions_publisher",
        "runtime_health_publisher",
        "serving_publisher",
        "watchlist_quote_source",
    }


def test_blk3_derived_settings_stay_inside_the_frozen_production_topology(
    published: tuple[World, stage_module.StagePlan],
) -> None:
    """Every path a bootstrap manifest names is either under the runtime owner root, under
    its data parent, or one of six frozen constants of this repository. A derived value
    that escapes those is a guess about the host, not a derivation (#200)."""

    from rquant.runtime_artifact_terminal_lifecycle import operational_database_path

    world, plan = published
    root = stage_module.production_runtime_root()
    paths: set[str] = set()

    def walk(value: Any) -> None:
        if isinstance(value, str) and value.startswith("/"):
            paths.add(value)
        elif isinstance(value, Mapping):
            for item in value.values():
                walk(item)
        elif isinstance(value, (list, tuple)):
            for item in value:
                walk(item)

    manifests = _staged_manifests(world, plan)
    settings = {
        manifest.service_id: dict(manifest.settings)
        for staged in manifests.values()
        for manifest in staged
    }
    walk(settings)
    assert paths
    data = root.parent
    assert {path for path in paths if not path.startswith(f"{root}/")} == {
        #: the data parent: the operational authorities the runtime reads but does not own
        str(data / "rquant.duckdb"),
        str(data / "surge_live"),
        str(data / "legacy-shadow" / "monitor"),
        str(data / "legacy-shadow" / "surge"),
        str(data / "legacy-shadow" / "isolated-runners"),
        #: frozen constants of `runtime_deployment_profile` and the daily receipt authority
        "/etc/rquant/daily-receipt-trusted-keys.json",
        "/home/lighthouse/rquant",
        "/home/lighthouse/rquant/.venv/bin/python",
        "/run/rquant/daily-receipt-signer.sock",
        "/usr/bin/sudo",
        "/usr/local/libexec/rquant-shadow-report-signer",
    }
    assert settings["auction-universe.publisher.v1"]["database_path"] == str(
        operational_database_path(root)
    )
    assert settings["serving.publisher.v1"]["serving_root"] == str(root / "serving")
    assert settings["paper-constraint.market.v1"]["reference_registry_path"] == str(
        root / "authorities" / "reference-slow" / "reference.sqlite3"
    )


# ---------------------------------------------------------------------------------------
# BLK-3 (#200): a builder is constructed out of the packaged manifest, one per plane
# ---------------------------------------------------------------------------------------


def _builtin_registry(manifests: dict[str, list[Any]]) -> Any:
    """The production registry, with only the two capabilities a terminal-owner role needs.

    The lab jobs publisher opens its ledger reader through the registry's artifact terminal
    lifecycle; the reader is bound to the very path the staged manifest names, so a manifest
    that pointed somewhere else would fail the builder's own path check.
    """

    from rquant.lab_jobs import LabJobReader
    from rquant.runtime_artifact_terminal_lifecycle import ProductionArtifactTerminalLifecycle
    from rquant.runtime_service_builtin import build_builtin_registry

    lab_jobs_path = Path(str(manifests["lab_jobs_publisher"][0].settings["lab_jobs_path"]))

    def lifecycle() -> Any:
        return ProductionArtifactTerminalLifecycle(lab_job_reader=LabJobReader(lab_jobs_path))

    return build_builtin_registry(
        clock=lambda: datetime.now(UTC),
        artifact_terminal_lifecycle_factory=lifecycle,
    )


def test_blk3_builders_start_from_the_packaged_manifest_on_all_three_planes(
    bootstrap: tuple[World, stage_module.StagePlan],
) -> None:
    """The gap that let #200 reach the production host: the bootstrap probe only proved the
    wrapper could resolve a launch, never that a builder accepts the manifest the generation
    ships. This reads the staged manifest files, hands them to the real registry, and takes
    one role of each plane all the way to a runtime step.
    """

    from rquant.reference_data_registry import ReferenceRegistry
    from rquant.runtime_service_control import RuntimeServicePlane

    world, plan = bootstrap
    manifests = _staged_manifests(world, plan)
    registry = _builtin_registry(manifests)
    roles = (
        (RuntimeServicePlane.LIVE, "paper_constraint_publisher"),
        (RuntimeServicePlane.SERVING, "runtime_health_publisher"),
        (RuntimeServicePlane.SERVING, "serving_publisher"),
        (RuntimeServicePlane.RESEARCH, "lab_jobs_publisher"),
    )
    constraint = manifests["paper_constraint_publisher"][0]
    ReferenceRegistry(Path(str(constraint.settings["reference_registry_path"])))

    for plane, role in roles:
        manifest = manifests[role][0]
        assert manifest.plane is plane
        step = registry.build(manifest)
        assert callable(step)
        closer = getattr(step, "close", None)
        if closer is not None:
            closer()


def test_blk3_the_two_hardcoded_values_are_what_the_builders_refuse(
    bootstrap: tuple[World, stage_module.StagePlan],
) -> None:
    """The same three builders, given the manifest route B used to ship: `plane: live` for
    everyone and an empty settings object. Both halves of #200 must still be refusals, or
    the test above proves nothing."""

    import pytest as _pytest
    from pydantic import ValidationError

    from rquant.reference_data_registry import ReferenceRegistry
    from rquant.runtime_service_entrypoint import RuntimeServiceManifest

    world, plan = bootstrap
    manifests = _staged_manifests(world, plan)
    registry = _builtin_registry(manifests)
    constraint = manifests["paper_constraint_publisher"][0]
    ReferenceRegistry(Path(str(constraint.settings["reference_registry_path"])))

    for role, message in (
        ("runtime_health_publisher", "must run on the serving plane"),
        ("lab_jobs_publisher", "must run on the research plane"),
    ):
        document = strict_json_loads(manifests[role][0].model_dump_json().encode("utf-8"))
        assert isinstance(document, dict)
        document["plane"] = "live"
        with _pytest.raises(ValueError, match=message):
            registry.build(RuntimeServiceManifest.model_validate(document))

    for role in (
        "paper_constraint_publisher",
        "runtime_health_publisher",
        "lab_jobs_publisher",
        "serving_publisher",
    ):
        document = strict_json_loads(manifests[role][0].model_dump_json().encode("utf-8"))
        assert isinstance(document, dict)
        document["settings"] = {}
        with _pytest.raises(ValidationError):
            registry.build(RuntimeServiceManifest.model_validate(document))


def test_blk3_derived_settings_agree_with_the_production_profile_field_by_field(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`build_production_runtime_profile` is the authority for what a manifest says; route B
    cannot call it, because its inputs are the operator facts a first installation lacks.
    So the derivation restates the expressions, and this pins the restatement to the
    original the way `instance_label` is pinned to `_instance_name`: point both at one
    runtime root, build both, and demand agreement on every field route B derives.

    Five differences are the derivation itself, not drift:
      * `database_path` / `page_projection_*` come from
        `runtime_artifact_terminal_lifecycle.operational_database_path`, the frozen function
        of the root, instead of the profile's operator input;
      * `stage_commands` names the production interpreter, which the profile only does in
        `linux-production` mode;
      * the health publisher's `sources` carry route B's one staleness bound instead of the
        published profile's per-role cadence — the composition itself is compared below;
      * the three `receipt_*` keys come from the fixed `/etc/rquant` keyring, which is what
        the profile itself does in `linux-production` mode and not what this local-test
        fixture does.
    """

    from rquant.runtime_production_profile import build_production_runtime_profile
    from tests.unit.test_runtime_production_profile import _inputs

    inputs = _inputs(tmp_path)
    profile = build_production_runtime_profile(inputs)
    monkeypatch.setattr(stage_module, "PRODUCTION_RUNTIME_ROOT", inputs.runtime_root)
    monkeypatch.setattr(
        stage_module,
        "DAILY_RECEIPT_KEYRING_PATH",
        _write_daily_receipt_keyring(tmp_path / "daily-receipt-trusted-keys.json"),
    )
    monkeypatch.setattr(authority_module, "PRODUCTION_PROFILE_OWNER_UID", UID)
    derived = stage_module.bootstrap_settings(inputs.producer_commit)
    published = {
        manifest.service_id: json.loads(manifest.model_dump_json())["settings"]
        for manifest in profile.manifests
    }
    assert set(derived) == set(published)
    input_derived = {
        "database_path",
        "page_projection_database_path",
        "page_projection_surge_live_root",
        "stage_commands",
        "sources",
        #: hydrated from the fixed `/etc/rquant` keyring, which is what the profile does in
        #: `linux-production` mode; the fixture here leaves the operator input unset, so it
        #: falls back to the shadow completion key instead
        "receipt_active_key_id",
        "receipt_active_public_key_pem",
        "receipt_previous_public_key_pems",
    }
    for service_id, settings in sorted(derived.items()):
        for key, value in settings.items():
            assert key in published[service_id], (service_id, key)
            if key in input_derived:
                continue
            assert value == published[service_id][key], (service_id, key)

    def composition(sources: Any) -> set[tuple[str, ...]]:
        return {
            (
                str(source["control_root"]),
                str(source["service_id"]),
                str(source["plane"]),
                str(source["producer_commit"]),
            )
            for source in sources
        }

    health = "runtime-health.all.v1"
    assert composition(derived[health]["sources"]) == composition(published[health]["sources"])
    assert {source["stale_after_seconds"] for source in derived[health]["sources"]} == {
        stage_module.MANIFEST_STALE_AFTER_SECONDS
    }


# ---------------------------------------------------------------------------------------
# BLK-3 (#200): the daily receipt trusted keyring, the stage's one TCB read
# ---------------------------------------------------------------------------------------


def test_blk3_the_keyring_path_is_the_frozen_one_not_a_second_literal() -> None:
    """One constant names `/etc/rquant/daily-receipt-trusted-keys.json`: the one the
    production profile hydrates from. The stage seam defaults to it."""

    from rquant.runtime_deployment_profile import PRODUCTION_DAILY_RECEIPT_TRUSTED_KEYRING_PATH
    from rquant.runtime_production_profile import DAILY_RECEIPT_TRUSTED_KEYRING_PATH

    assert stage_module.DAILY_RECEIPT_KEYRING_PATH is None
    assert stage_module.daily_receipt_keyring_path() == (
        PRODUCTION_DAILY_RECEIPT_TRUSTED_KEYRING_PATH
    )
    assert PRODUCTION_DAILY_RECEIPT_TRUSTED_KEYRING_PATH == DAILY_RECEIPT_TRUSTED_KEYRING_PATH
    assert str(PRODUCTION_DAILY_RECEIPT_TRUSTED_KEYRING_PATH) == (
        "/etc/rquant/daily-receipt-trusted-keys.json"
    )


def test_blk3_the_daily_orchestrator_manifest_carries_the_keyring_authority(
    bootstrap: tuple[World, stage_module.StagePlan],
) -> None:
    """The read is what makes the orchestrator startable: its two receipt fields are the
    keyring's, and the manifest still names the keyring the runtime verifies against."""

    world, plan = bootstrap
    settings = dict(_staged_manifests(world, plan)["daily_pipeline_orchestrator"][0].settings)
    document = strict_json_loads(world.keyring.read_bytes())
    assert isinstance(document, dict)
    assert settings["receipt_active_key_id"] == document["active_key_id"]
    assert settings["receipt_active_public_key_pem"] == document["active_public_key"]
    assert settings["receipt_previous_public_key_pems"] == document["previous_public_keys"]
    assert settings["receipt_trusted_keyring_path"] == "/etc/rquant/daily-receipt-trusted-keys.json"


def test_blk3_an_absent_or_unsafe_keyring_refuses_and_says_why(world: World) -> None:
    """Never a silent empty authority: absent, foreign-owned or group/world writable each
    refuse with the observed fact in the message, and the stage as a whole fails closed."""

    world.keyring.chmod(0o644)
    world.keyring.unlink()
    with pytest.raises(RuntimeAuthorityStageError, match="keyring is unavailable"):
        stage_module.daily_receipt_authority()
    with pytest.raises(RuntimeAuthorityStageError, match="keyring is unavailable"):
        world.stage("blk3-keyring-absent")

    world.write_keyring()
    world.monkeypatch.setattr(authority_module, "PRODUCTION_PROFILE_OWNER_UID", UID + 1)
    with pytest.raises(RuntimeAuthorityStageError, match="keyring owner is unsafe") as owner:
        stage_module.daily_receipt_authority()
    assert f"owned by uid {UID}" in str(owner.value)
    world.monkeypatch.setattr(authority_module, "PRODUCTION_PROFILE_OWNER_UID", UID)

    for mode in (0o646, 0o464, 0o666):
        world.write_keyring(mode=mode)
        with pytest.raises(RuntimeAuthorityStageError, match="keyring mode is unsafe") as unsafe:
            stage_module.daily_receipt_authority()
        assert f"{mode:04o}" in str(unsafe.value)
    world.write_keyring(mode=0o440)
    assert stage_module.daily_receipt_authority()[0] == "daily-receipt-v1"


def test_blk3_a_malformed_keyring_refuses_rather_than_publishing_half_an_authority(
    world: World,
) -> None:
    """Shape is judged here too, because an empty or truncated key would reach the manifest
    as an authority the receipt signer cannot verify against."""

    world.write_keyring(active_key_id="")
    with pytest.raises(RuntimeAuthorityStageError, match="keyring shape is invalid"):
        stage_module.daily_receipt_authority()
    world.write_keyring(previous_public_keys={"retired": ""})
    with pytest.raises(RuntimeAuthorityStageError, match="keyring shape is invalid"):
        stage_module.daily_receipt_authority()
    world.keyring.chmod(0o644)
    world.keyring.write_bytes(b"not json\n")
    world.keyring.chmod(0o444)
    with pytest.raises(RuntimeAuthorityStageError, match="keyring is not canonical JSON"):
        stage_module.daily_receipt_authority()
    world.write_keyring()
    world.keyring.chmod(0o644)
    world.keyring.write_bytes(b"")
    world.keyring.chmod(0o444)
    with pytest.raises(RuntimeAuthorityStageError, match="keyring size is unsafe"):
        stage_module.daily_receipt_authority()
