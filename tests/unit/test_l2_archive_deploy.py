"""Offline gates for the independent archive component; no cloud or member inputs."""
from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from tests.unit import test_l2_component_deploy as existing

ROOT = Path(__file__).resolve().parents[2]
TARGET, TREE = "a" * 40, "b" * 40


def deployer():
    path = ROOT / "scripts/deploy-l2-archive.py"
    if not path.exists():
        pytest.fail("Independent archive deployer is not implemented")
    spec = importlib.util.spec_from_file_location("l2_archive_deploy", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def proof() -> dict:
    return {"schema": "rquant-l2-archive-acceptance-v1", "source_tree": TREE,
        "prefix": "l2/v1/_acceptance/" + "1" * 32, "real_r2": True, "synthetic_only": True,
        "cases": {name: True for name in ("put_head_get", "multi_date", "stateless_fetch",
            "original_sequences", "gap_preserved", "conditional_conflict")},
        "roundtrip": {"source_count": 4, "restored_count": 4,
            "received_dates": ["2026-10-12", "2026-10-13"], "source_sha256": "2" * 64,
            "restored_sha256": "2" * 64, "catalog_last_sequence": 4},
        "idempotence_puts": 0, "requests": {"put": 3, "head": 8, "get": 9, "precondition_failed": 1},
        "closed": True}


@pytest.mark.parametrize("bad", ["tree", "real", "prefix", "extra", "restore", "count", "case", "retry", "closed", "conflict"])
def test_real_r2_proof_is_strict_and_bound_to_archive_tree(bad: str) -> None:
    d = deployer(); value = proof()
    assert d.validate_proof(value, TREE)["source_tree"] == TREE
    if bad == "tree": value["source_tree"] = "c" * 40
    elif bad == "real": value["real_r2"] = False
    elif bad == "prefix": value["prefix"] = "l2/v1/rquant"
    elif bad == "extra": value["endpoint"] = "forbidden"
    elif bad == "restore": value["roundtrip"]["restored_sha256"] = "3" * 64
    elif bad == "count": value["roundtrip"]["source_count"] = True
    elif bad == "case": value["cases"]["conditional_conflict"] = False
    elif bad == "retry": value["idempotence_puts"] = 1
    elif bad == "closed": value["closed"] = False
    elif bad == "conflict": value["requests"]["precondition_failed"] = 0
    with pytest.raises(d.DeployError): d.validate_proof(value, TREE)


def test_cli_is_exact_and_has_no_credential_or_prefix_options(tmp_path: Path) -> None:
    d = deployer()
    argv = ["--target", TARGET, "--artifact", str(tmp_path / "release.tar.gz"),
        "--archive-proof", str(tmp_path / "proof.json"), "--archive-proof-sha256", "4" * 64]
    args = d.parse_cli(argv)
    assert args.target == TARGET and args.archive_proof_sha256 == "4" * 64
    for extra in (["--prefix", "other"], ["--endpoint", "other"], ["--rollback"], ["--target", "main"]):
        with pytest.raises(SystemExit): d.parse_cli(argv + extra)
    assert d.parse_cli(["--target", TARGET, "--rollback"]).rollback


def test_archive_units_and_dispatch_leave_core_authority_separate() -> None:
    d = deployer()
    assert d.core.UNITS == ("rquant-l2-api.service", "rquant-l2-collector.service")
    source = (ROOT / "scripts/deploy-production.sh").read_text()
    assert "deploy-l2-archive.py" in source and "l2-archive" in source
    service = (ROOT / "deploy/systemd/rquant-l2-archive.service").read_text()
    timer = (ROOT / "deploy/systemd/rquant-l2-archive.timer").read_text()
    assert "EnvironmentFile=/etc/rquant-r2.env" in service
    assert "/etc/rquant-web.env" not in service and "/usr/bin/env -i" not in service
    assert "ReadOnlyPaths=/home/lighthouse/rquant-l2-state/cache" in service
    assert "InaccessiblePaths=/home/lighthouse/rquant-l2-private" in service
    for setting in ("TimeoutStartSec=2h", "CPUQuota=20%", "MemoryMax=384M", "IOWeight=20", "Nice=15"):
        assert setting in service
    assert "Restart=" not in service and "ExecStartPre=" not in service
    assert "OnCalendar=Mon..Fri *-*-* 17:20:00 Asia/Shanghai" in timer and "Persistent=true" in timer


def test_transient_commands_are_fixed_and_never_read_credentials(tmp_path: Path) -> None:
    d = deployer(); paths = d.Paths(releases=tmp_path / "rel", state=tmp_path / "state")
    command = d.transient_argv(paths, TARGET, "init")
    assert command[:5] == ["sudo", "-n", "/usr/bin/systemd-run", "--wait", "--collect"]
    assert "--property=EnvironmentFile=/etc/rquant-r2.env" in command
    assert f"--unit=rquant-l2-archive-init-{TARGET[:8]}" in command
    assert command[-5:] == ["init", "--cache-root", str(paths.core.cache), "--state-root", str(paths.state)]
    check = d.transient_argv(paths, TARGET, "check")
    assert check[-1] == "check" and "--cache-root" not in check
    with pytest.raises(d.DeployError): d.transient_argv(paths, TARGET, "run")


def test_r2_f1_execute_keeps_logical_venv_python(tmp_path: Path, monkeypatch) -> None:
    d = deployer()
    logical = Path(f"/home/lighthouse/rquant-l2-archive-rel/{TARGET}/.venv/bin/python")
    base = tmp_path / "base-python"; base.write_bytes(b"synthetic base Python"); base.chmod(0o700)
    original = Path.resolve
    calls: list[list[str]] = []

    def resolve(path: Path, *args, **kwargs) -> Path:
        return base if path == logical else original(path, *args, **kwargs)

    def fake_run(argv: list[str], **kwargs) -> subprocess.CompletedProcess:
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, b"verified", b"")

    monkeypatch.setattr(Path, "resolve", resolve)
    monkeypatch.setattr(d.subprocess, "run", fake_run)
    assert d.execute([str(logical), "-I", "-c", "pass"]) == "verified"
    assert calls[0][0] == str(logical)


class FakeHost:
    def __init__(self, d, root: Path) -> None:
        core_root = root / "core"; core_root.mkdir(mode=0o700)
        self.cd, self.ch, ca, api = existing.deployment(core_root)
        existing.run_deploy(self.cd, self.ch, ca, api)
        self.d = d
        self.paths = d.Paths(root / "archive-rel", root / "archive-state", self.ch.paths)
        self.commands: list[list[str]] = []
        self.active = {name: False for name in d.UNITS}
        self.enabled = False
        self.dropins = ""
        self.bad_fragment = False
        self.catalog = False
        self.init_lost_reply = False
        self.fail_timer = False
        self.fail_uv = False
        self.service_active = False
        self.fail_stop_once = False
        self.on_timer_start = None
        self.bad_receipt = False
        self.receipt_prefix = "l2/v1/rquant"
        self.fail_timer_install = ""
        self.strict_missing_unit = False
        self.loaded_units: set[str] = set()

    def command(self, argv: list[str], cwd: Path | None = None) -> str:
        self.commands.append(list(argv))
        args = argv[2:] if argv[:2] == ["sudo", "-n"] else argv
        if args[0].endswith("/uv"):
            if self.fail_uv: raise self.d.DeployError("synthetic_build_failed")
            assert cwd
            path = cwd / ".venv/bin/python"; path.parent.mkdir(parents=True)
            path.write_bytes(b"synthetic archive python"); path.chmod(0o700)
        elif args[0] == "/usr/bin/install":
            dest = Path(args[-1])
            if dest.name == self.d.UNITS[1] and self.fail_timer_install == "before":
                raise self.d.DeployError("synthetic timer file install failed")
            dest.write_bytes(Path(args[-2]).read_bytes()); dest.chmod(0o644)
            if dest.name == self.d.UNITS[1] and self.fail_timer_install == "after":
                raise self.d.DeployError("synthetic timer install reply lost")
        elif args[0] == "/usr/bin/rm": Path(args[-1]).unlink()
        elif args[0] == "/usr/bin/systemd-run":
            operation = args[args.index("rquant.l2.archive") + 1]
            if operation == "init":
                assert (self.paths.state / "deploy-intent.json").is_file()
                intent = json.loads((self.paths.state / "deploy-intent.json").read_text())
                assert intent["initialization_attempted"] is True
                status = "already_initialized" if self.catalog else "initialized"
                self.catalog = True
                if self.init_lost_reply: raise self.d.DeployError("synthetic_reply_lost")
            elif operation == "check":
                if not self.catalog: raise self.d.DeployError("catalog_missing")
                status = "checked"
            else: pytest.fail("Unexpected archive operation")
            return json.dumps({"schema": "rquant-l2-archive-receipt-v1", "operation": operation,
                "prefix": self.receipt_prefix, "catalog_sha256": "9" * 64,
                "status": "failed" if self.bad_receipt else status})
        elif args[0] == "/usr/bin/systemctl":
            action = args[1]
            if action == "show":
                unit = args[2]
                if unit in self.d.core.UNITS: return self.ch.command(argv, cwd=cwd)
                if "--value" in args: return "enabled" if self.enabled else "disabled"
                file = self.paths.core.units / unit
                loaded = unit in self.loaded_units if self.strict_missing_unit else file.exists()
                fragment = str(file) if loaded else ""
                if self.bad_fragment and file.exists(): fragment = "/usr/lib/systemd/system/foreign.service"
                active = self.service_active if unit == self.d.UNITS[0] else self.active[unit]
                fields = [f"LoadState={'loaded' if loaded else 'not-found'}", f"FragmentPath={fragment}",
                    f"DropInPaths={self.dropins if file.exists() else ''}"]
                if unit == self.d.UNITS[0]: fields.append("MainPID=0")
                return "\n".join([*fields, f"ActiveState={'active' if active else 'inactive'}"])
            if action in {"stop", "disable"}:
                assert args[-1] in self.d.UNITS
                if self.strict_missing_unit and args[-1] not in self.loaded_units:
                    raise self.d.DeployError("synthetic absent unit stop refused")
                self.active[args[-1]] = False
                if args[-1] == self.d.UNITS[1] and action == "disable": self.enabled = False
                if action == "stop" and self.fail_stop_once:
                    self.fail_stop_once = False
                    raise self.d.DeployError("synthetic_stop_reply_lost")
            if action in {"start", "enable"}:
                assert args[-1] == self.d.UNITS[1], "Archive service must not be started by installer"
                if self.fail_timer: raise self.d.DeployError("synthetic_timer_failure")
                self.active[args[-1]] = True
                if action == "enable": self.enabled = True
                if action == "start" and self.on_timer_start: self.on_timer_start()
            if action == "daemon-reload":
                self.loaded_units = {unit for unit in self.d.UNITS if (self.paths.core.units / unit).exists()}
        return ""

    def mutations(self) -> list[list[str]]:
        return [args for args in self.commands if args[:2] == ["sudo", "-n"]]


def deployment(tmp_path: Path):
    tmp_path.chmod(0o700)
    d = deployer(); host = FakeHost(d, tmp_path)
    stage = tmp_path / "artifact-source"; stage.mkdir(mode=0o700)
    artifact, files, _ = existing.artifact(stage)
    files.pop("RELEASE.json"); files.pop("web/dist/index.html")
    files["src/rquant/l2/archive.py"] = b"# synthetic archive module\n"
    for unit in d.UNITS: files["deploy/systemd/" + unit] = (ROOT / "deploy/systemd" / unit).read_bytes()
    import io
    import tarfile
    tree = [{"path": name, "type": "blob", "mode": "100644",
        "sha": hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()} for name, data in files.items()]
    with tarfile.open(artifact, "w:gz") as archive:
        for name, data in {**files, "RELEASE.json": json.dumps({"commit": TARGET, "short": TARGET[:8]}).encode()}.items():
            entry = tarfile.TarInfo(name); entry.size = len(data); entry.mode = 0o644
            archive.addfile(entry, io.BytesIO(data))
    path = tmp_path / "r2-proof.json"; path.write_text(json.dumps(proof())); path.chmod(0o600)
    args = d.parse_cli(["--target", TARGET, "--artifact", str(artifact), "--archive-proof", str(path),
        "--archive-proof-sha256", hashlib.sha256(path.read_bytes()).hexdigest()])
    api = existing.github(artifact, tree)
    return d, host, args, api


def run(d, host: FakeHost, args, api: dict) -> dict:
    return d.deploy(args, paths=host.paths, api=api.__getitem__, command=host.command, health=host.ch.health)


def test_r2_f5_real_timer_view_has_no_pid(tmp_path: Path) -> None:
    d, host, _, _ = deployment(tmp_path)
    views = d.archive_units(host.paths, None, host.command)
    assert views[d.UNITS[1]] == {"LoadState": "not-found", "FragmentPath": "",
        "DropInPaths": "", "ActiveState": "inactive"}
    timer_command = next(argv for argv in host.commands if argv[:3] == [d.SYSTEMCTL, "show", d.UNITS[1]])
    assert timer_command[-1] == "--property=LoadState,FragmentPath,DropInPaths,ActiveState"
    assert not host.mutations()


@pytest.mark.parametrize("fault", ["missing", "extra", "duplicate", "malformed", "empty_state"])
def test_r2_f5_malformed_timer_schema_is_rejected(tmp_path: Path, fault: str) -> None:
    d, host, _, _ = deployment(tmp_path)

    def command(argv: list[str], cwd: Path | None = None) -> str:
        value = host.command(argv, cwd=cwd)
        if argv[:3] == [d.SYSTEMCTL, "show", d.UNITS[1]]:
            if fault == "missing": value = value.replace("DropInPaths=\n", "")
            elif fault == "extra": value += "\nMainPID=0"
            elif fault == "duplicate": value += "\nActiveState=inactive"
            elif fault == "malformed": value += "\nnot-a-property"
            else: value = value.replace("ActiveState=inactive", "ActiveState=")
        return value

    with pytest.raises(d.DeployError, match="^unit_inspection_failed$"):
        d.archive_units(host.paths, None, command)
    assert not host.mutations()


@pytest.mark.parametrize("pid", [None, "not-a-number"])
def test_r2_f5_archive_service_still_requires_numeric_pid(tmp_path: Path, pid: str | None) -> None:
    d, host, _, _ = deployment(tmp_path)

    def command(argv: list[str], cwd: Path | None = None) -> str:
        value = host.command(argv, cwd=cwd)
        if argv[:3] == [d.SYSTEMCTL, "show", d.UNITS[0]]:
            value = value.replace("MainPID=0\n", "" if pid is None else f"MainPID={pid}\n")
        return value

    with pytest.raises(d.DeployError, match="^unit_inspection_failed$"):
        d.archive_units(host.paths, None, command)
    assert not host.mutations()


@pytest.mark.parametrize("foreign", ["fragment", "dropin"])
def test_r2_f5_foreign_timer_is_not_taken_over(tmp_path: Path, foreign: str) -> None:
    d, host, args, api = deployment(tmp_path)
    run(d, host, args, api)
    newer = next_release(tmp_path, d, args, api)
    original = host.command

    def command(argv: list[str], cwd: Path | None = None) -> str:
        value = original(argv, cwd=cwd)
        if argv[:3] == [d.SYSTEMCTL, "show", d.UNITS[1]]:
            if foreign == "fragment":
                value = value.replace(f"FragmentPath={host.paths.core.units / d.UNITS[1]}",
                    "FragmentPath=/usr/lib/systemd/system/foreign.timer")
            else:
                value = value.replace("DropInPaths=", "DropInPaths=/etc/systemd/system/foreign.conf")
        return value

    host.commands.clear()
    host.command = command
    with pytest.raises(d.DeployError, match="^foreign_archive_unit$"):
        run(d, host, newer, api)
    assert not host.mutations() and host.enabled and host.active[d.UNITS[1]]


@pytest.mark.parametrize("bad", ["proof_hash", "tree", "ci", "core_missing", "core_inactive"])
def test_invalid_gates_precede_any_archive_change(tmp_path: Path, bad: str) -> None:
    d, host, args, api = deployment(tmp_path)
    if bad == "proof_hash": args.archive_proof_sha256 = "0" * 64
    elif bad == "tree": api[f"/commits/{TARGET}"]["commit"]["tree"]["sha"] = "f" * 40
    elif bad == "ci": api["/actions/runs/123/jobs?filter=latest&per_page=100"]["jobs"][0]["conclusion"] = "failure"
    elif bad == "core_missing": (host.paths.core.state / "installation.json").unlink()
    elif bad == "core_inactive": host.ch.active[host.d.core.UNITS[1]] = False
    with pytest.raises(d.DeployError): run(d, host, args, api)
    assert not host.mutations() and not host.paths.current.exists()


def test_first_install_uses_owned_init_and_preserves_all_core_files(tmp_path: Path, monkeypatch) -> None:
    d, host, args, api = deployment(tmp_path)
    core_before = {str(path): path.read_bytes() for path in tmp_path.joinpath("core").rglob("*") if path.is_file()}
    original = Path.open

    def guarded(path: Path, *args, **kwargs):
        if path.is_relative_to(host.paths.core.inputs): pytest.fail("Installer read member inputs")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded)
    result = run(d, host, args, api)
    assert result["status"] == "deployed" and host.catalog and host.enabled
    assert host.paths.current.resolve() == host.paths.releases / TARGET
    assert len([a for a in host.commands if "rquant.l2.archive" in a and "init" in a]) == 1
    assert not any(a[:2] == ["sudo", "-n"] and any(unit in a for unit in d.core.UNITS) for a in host.commands)
    monkeypatch.undo()
    assert core_before == {str(path): path.read_bytes() for path in tmp_path.joinpath("core").rglob("*")
        if path.is_file() and path.name not in d.UNITS}


def test_unknown_init_outcome_retries_check_and_never_initializes_again(tmp_path: Path) -> None:
    d, host, args, api = deployment(tmp_path); host.init_lost_reply = True
    with pytest.raises(d.DeployError): run(d, host, args, api)
    assert host.catalog and not host.paths.current.exists()
    host.init_lost_reply = False
    assert run(d, host, args, api)["status"] == "deployed"
    operations = [a[a.index("rquant.l2.archive") + 1] for a in host.commands if "rquant.l2.archive" in a]
    assert operations == ["init", "check"]


def test_first_activation_failure_removes_only_archive_units_and_retains_remote_catalog(tmp_path: Path) -> None:
    d, host, args, api = deployment(tmp_path); host.fail_timer = True
    with pytest.raises(d.DeployError): run(d, host, args, api)
    assert host.catalog and not host.paths.current.exists()
    assert not any((host.paths.core.units / unit).exists() for unit in d.UNITS)
    assert all(host.ch.active.values())


def test_r2_f2_pointer_replace_then_fsync_failure_cleans_owned_pointer(tmp_path: Path, monkeypatch) -> None:
    d, host, args, api = deployment(tmp_path)
    original = d.core.os.fsync
    failed = False

    def fail_releases_once(fd: int) -> None:
        nonlocal failed
        if not failed and host.paths.current.is_symlink() and os.fstat(fd).st_ino == host.paths.releases.stat().st_ino:
            failed = True
            raise OSError("synthetic post-replace fsync failure")
        original(fd)

    monkeypatch.setattr(d.core.os, "fsync", fail_releases_once)
    with pytest.raises(d.DeployError): run(d, host, args, api)
    assert failed and not host.paths.current.is_symlink()
    assert not (host.paths.state / "installation.json").exists()
    assert all(host.ch.active.values())


@pytest.mark.parametrize("when", ["before", "after"])
def test_r2_f2_partial_unit_install_cleans_files_even_when_units_not_loaded(tmp_path: Path, when: str) -> None:
    d, host, args, api = deployment(tmp_path)
    host.strict_missing_unit = True; host.fail_timer_install = when
    with pytest.raises(d.DeployError): run(d, host, args, api)
    assert not host.paths.current.is_symlink()
    assert not any((host.paths.core.units / unit).exists() for unit in d.UNITS)
    assert not (host.paths.state / "installation.json").exists()
    assert all(host.ch.active.values()) and host.catalog


def test_r2_f2_cleanup_does_not_stop_or_remove_changed_foreign_unit(tmp_path: Path) -> None:
    d, host, args, api = deployment(tmp_path)
    original = host.command
    foreign = host.paths.core.units / d.UNITS[0]

    def changed_unit(argv: list[str], cwd: Path | None = None) -> str:
        if "/usr/bin/install" in argv and argv[-1].endswith(d.UNITS[1]):
            foreign.write_bytes(b"foreign unit; ownership cannot be assumed")
            raise d.DeployError("synthetic second file failure")
        return original(argv, cwd=cwd)

    host.command = changed_unit
    with pytest.raises(d.DeployError): run(d, host, args, api)
    assert foreign.read_bytes() == b"foreign unit; ownership cannot be assumed"
    assert not any(("stop" in a or "/usr/bin/rm" in a) and a[-1] == d.UNITS[0] for a in host.commands)


def next_release(tmp_path: Path, d, args, api: dict) -> dict:
    import io
    import tarfile
    target, tree_sha = "5" * 40, "6" * 40
    with tarfile.open(args.artifact) as archive:
        files = {entry.name: archive.extractfile(entry).read() for entry in archive if entry.isfile()}
    files["src/rquant/l2/archive.py"] += b"# next version\n"
    files["RELEASE.json"] = json.dumps({"commit": target, "short": target[:8]}).encode()
    tree = [{"path": name, "type": "blob", "mode": "100644",
        "sha": hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()}
        for name, data in files.items() if name != "RELEASE.json"]
    artifact = tmp_path / "release-next.tar.gz"
    with tarfile.open(artifact, "w:gz") as archive:
        for name, data in files.items():
            entry = tarfile.TarInfo(name); entry.size = len(data); entry.mode = 0o644
            archive.addfile(entry, io.BytesIO(data))
    new_api = existing.github(artifact, tree, target)
    new_api[f"/commits/{target}"]["commit"]["tree"]["sha"] = tree_sha
    new_api.pop(f"/git/trees/{TREE}?recursive=1")
    new_api[f"/git/trees/{tree_sha}?recursive=1"] = {"sha": tree_sha, "truncated": False, "tree": tree}
    value = proof(); value["source_tree"] = tree_sha
    receipt = tmp_path / "next-proof.json"; receipt.write_text(json.dumps(value)); receipt.chmod(0o600)
    api.update(new_api)
    return d.parse_cli(["--target", target, "--artifact", str(artifact), "--archive-proof", str(receipt),
        "--archive-proof-sha256", hashlib.sha256(receipt.read_bytes()).hexdigest()])


def test_upgrade_checks_remote_catalog_and_rollback_never_starts_service(tmp_path: Path) -> None:
    d, host, args, api = deployment(tmp_path)
    run(d, host, args, api)
    newer = next_release(tmp_path, d, args, api)
    assert run(d, host, newer, api)["status"] == "deployed"
    assert host.paths.current.resolve() == host.paths.releases / newer.target
    rollback = d.parse_cli(["--target", TARGET, "--rollback"])
    assert run(d, host, rollback, api)["status"] == "rolled_back"
    operations = [a[a.index("rquant.l2.archive") + 1] for a in host.commands if "rquant.l2.archive" in a]
    assert operations == ["init", "check", "check"]
    assert host.enabled and host.active[d.UNITS[1]] and all(host.ch.active.values())


def test_upgrade_missing_catalog_does_not_stop_or_initialize(tmp_path: Path) -> None:
    d, host, args, api = deployment(tmp_path); run(d, host, args, api)
    newer = next_release(tmp_path, d, args, api); host.catalog = False; host.commands.clear()
    with pytest.raises(d.DeployError): run(d, host, newer, api)
    assert host.paths.current.resolve() == host.paths.releases / TARGET
    assert host.enabled and host.active[d.UNITS[1]]
    assert not any("init" in a or "stop" in a for a in host.commands)


def test_failed_upgrade_remote_check_can_resume_without_initialization(tmp_path: Path) -> None:
    d, host, args, api = deployment(tmp_path); run(d, host, args, api)
    newer = next_release(tmp_path, d, args, api); host.catalog = False
    with pytest.raises(d.DeployError): run(d, host, newer, api)
    host.catalog = True; host.commands.clear()
    assert run(d, host, newer, api)["status"] == "deployed"
    assert not any("init" in a for a in host.commands)


@pytest.mark.parametrize("foreign", ["dropin", "fragment", "busy_job"])
def test_foreign_or_running_archive_is_not_taken_over(tmp_path: Path, foreign: str) -> None:
    d, host, args, api = deployment(tmp_path); run(d, host, args, api)
    newer = next_release(tmp_path, d, args, api); host.commands.clear()
    if foreign == "dropin": host.dropins = "/etc/systemd/system/foreign.conf"
    elif foreign == "fragment": host.bad_fragment = True
    else: host.service_active = True
    with pytest.raises(d.DeployError): run(d, host, newer, api)
    assert host.mutations() == []


def test_upgrade_failed_stop_restores_previous_timer_state(tmp_path: Path) -> None:
    d, host, args, api = deployment(tmp_path); run(d, host, args, api)
    newer = next_release(tmp_path, d, args, api); host.fail_stop_once = True
    with pytest.raises(d.DeployError): run(d, host, newer, api)
    assert host.paths.current.resolve() == host.paths.releases / TARGET
    assert host.enabled and host.active[d.UNITS[1]] and all(host.ch.active.values())


def test_persistent_timer_can_launch_the_owned_archive_job_after_activation(tmp_path: Path) -> None:
    d, host, args, api = deployment(tmp_path)
    host.on_timer_start = lambda: setattr(host, "service_active", True)
    assert run(d, host, args, api)["status"] == "deployed"


def test_non_success_remote_receipt_never_activates(tmp_path: Path) -> None:
    d, host, args, api = deployment(tmp_path); host.bad_receipt = True
    with pytest.raises(d.DeployError): run(d, host, args, api)
    assert not host.enabled and not host.paths.current.exists()


def test_wrong_remote_prefix_never_activates(tmp_path: Path) -> None:
    d, host, args, api = deployment(tmp_path); host.receipt_prefix = "l2/v1/_acceptance/" + "1" * 32
    with pytest.raises(d.DeployError): run(d, host, args, api)
    assert not host.enabled and not host.paths.current.exists()


def test_dry_run_has_no_archive_mutation_or_remote_credential_job(tmp_path: Path) -> None:
    d, host, args, api = deployment(tmp_path); args.dry_run = True
    assert run(d, host, args, api)["status"] == "planned"
    assert host.mutations() == [] and not host.paths.state.exists()


def test_missing_previous_dry_rollback_is_a_fixed_refusal(tmp_path: Path) -> None:
    d, host, args, api = deployment(tmp_path)
    args = d.parse_cli(["--target", TARGET, "--rollback", "--dry-run"])
    with pytest.raises(d.DeployError): run(d, host, args, api)
    assert not host.paths.state.exists() and host.mutations() == []


@pytest.mark.parametrize("upgrade", [False, True])
def test_failed_final_receipt_restores_installation_record_together_with_units(tmp_path: Path, monkeypatch, upgrade: bool) -> None:
    d, host, args, api = deployment(tmp_path)
    if upgrade:
        run(d, host, args, api); args = next_release(tmp_path, d, args, api)
    original = d.core.save_json

    def fail_final(path: Path, value: dict) -> None:
        if path.name == "deploy-intent.json" and value.get("phase") == "complete" and value.get("result") == "deployed":
            raise OSError("synthetic final receipt failure")
        original(path, value)

    monkeypatch.setattr(d.core, "save_json", fail_final)
    with pytest.raises(d.DeployError): run(d, host, args, api)
    installed = host.paths.state / "installation.json"
    if upgrade:
        assert json.loads(installed.read_text())["current"]["target"] == TARGET
        assert host.paths.current.resolve() == host.paths.releases / TARGET
    else:
        assert not installed.exists() and not host.paths.current.exists()
