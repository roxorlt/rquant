#!/usr/bin/env python3
"""Deploy only the independent L2 archive job through deploy-production.sh."""
from __future__ import annotations

import argparse
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
from typing import Any

_spec = importlib.util.spec_from_file_location("_rquant_core_deployer", Path(__file__).with_name("deploy-l2-component.py"))
assert _spec and _spec.loader
core = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = core
_spec.loader.exec_module(core)
DeployError = core.DeployError
UNITS = ("rquant-l2-archive.service", "rquant-l2-archive.timer")
SYSTEMCTL = core.SYSTEMCTL
Command = Callable[..., str]


@dataclass(frozen=True)
class Paths:
    releases: Path = Path("/home/lighthouse/rquant-l2-archive-rel")
    state: Path = Path("/home/lighthouse/rquant-l2-archive-state")
    core: Any = field(default_factory=core.Paths)

    @property
    def current(self) -> Path:
        return self.releases / "current"


def validate_proof(value: dict[str, Any], tree: str) -> dict[str, Any]:
    try:
        if (set(value) != {"schema", "source_tree", "prefix", "real_r2", "synthetic_only", "cases", "roundtrip",
                "idempotence_puts", "requests", "closed"}
                or value["schema"] != "rquant-l2-archive-acceptance-v1" or value["source_tree"] != tree
                or not core.hex_value(tree, 40) or value["real_r2"] is not True or value["synthetic_only"] is not True
                or re.fullmatch(r"l2/v1/_acceptance/[0-9a-f]{32}", value["prefix"]) is None
                or value["closed"] is not True or type(value["idempotence_puts"]) is not int or value["idempotence_puts"] != 0):
            raise DeployError("archive_proof")
        cases = value["cases"]
        if (set(cases) != {"put_head_get", "multi_date", "stateless_fetch", "original_sequences", "gap_preserved", "conditional_conflict"}
                or any(flag is not True for flag in cases.values())):
            raise DeployError("archive_proof")
        result = value["roundtrip"]
        if (set(result) != {"source_count", "restored_count", "received_dates", "source_sha256", "restored_sha256", "catalog_last_sequence"}
                or any(type(result[name]) is not int or result[name] != 4 for name in ("source_count", "restored_count", "catalog_last_sequence"))
                or not core.hex_value(result["source_sha256"]) or result["restored_sha256"] != result["source_sha256"]
                or not isinstance(result["received_dates"], list) or len(result["received_dates"]) != 2
                or result["received_dates"] != sorted(set(result["received_dates"]))
                or any(date.fromisoformat(day).isoformat() != day for day in result["received_dates"])):
            raise DeployError("archive_proof")
        requests = value["requests"]
        if (set(requests) != {"put", "head", "get", "precondition_failed"}
                or any(type(requests[name]) is not int or requests[name] < 1 for name in ("put", "head", "get"))
                or type(requests["precondition_failed"]) is not int or requests["precondition_failed"] != 1):
            raise DeployError("archive_proof")
        return {"source_tree": tree, "prefix": value["prefix"], "source_count": 4,
                "source_sha256": result["source_sha256"]}
    except (KeyError, TypeError, ValueError, AttributeError):
        raise DeployError("archive_proof") from None


def parse_cli(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deploy only the L2 archive service and timer")
    parser.add_argument("--target", required=True)
    parser.add_argument("--artifact", type=Path)
    parser.add_argument("--archive-proof", type=Path)
    parser.add_argument("--archive-proof-sha256")
    parser.add_argument("--rollback", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if (not core.hex_value(args.target, 40)
            or args.rollback and any((args.artifact, args.archive_proof, args.archive_proof_sha256))
            or not args.rollback and (not args.artifact or not args.artifact.is_absolute()
                or not args.archive_proof or not args.archive_proof.is_absolute() or not core.hex_value(args.archive_proof_sha256))):
        parser.error("An exact commit and the archive's pinned inputs are required")
    return args


def transient_argv(paths: Paths, target: str, operation: str) -> list[str]:
    if not core.hex_value(target, 40) or operation not in {"init", "check"}:
        raise DeployError("invalid_archive_operation")
    release = paths.releases / target
    argv = ["sudo", "-n", "/usr/bin/systemd-run", "--wait", "--collect", "--quiet", "--pipe",
        f"--unit=rquant-l2-archive-{operation}-{target[:8]}", "--uid=lighthouse", "--gid=lighthouse",
        "--property=EnvironmentFile=/etc/rquant-r2.env", f"--property=WorkingDirectory={release}",
        "--property=UMask=0077", "--property=NoNewPrivileges=yes", "--property=PrivateTmp=yes",
        "--property=ProtectSystem=strict", f"--property=ReadOnlyPaths={paths.core.cache}",
        f"--property=ReadWritePaths={paths.state}", f"--property=InaccessiblePaths={paths.core.inputs}",
        "--property=RuntimeMaxSec=2h", "--setenv=RQUANT_DISABLE_DOTENV=1", "--setenv=TZ=Asia/Shanghai",
        str(release / ".venv/bin/python"), "-m", "rquant.l2.archive", operation]
    if operation == "init": argv += ["--cache-root", str(paths.core.cache), "--state-root", str(paths.state)]
    return argv


def execute(argv: list[str], cwd: Path | None = None) -> str:
    archive_python = re.fullmatch(r"/home/lighthouse/rquant-l2-archive-rel/[0-9a-f]{40}/\.venv/bin/python", argv[0])
    if not archive_python and not (argv[:3] == ["sudo", "-n", "/usr/bin/systemd-run"]):
        return core.execute(argv, cwd=cwd)
    binary = Path("/usr/bin/sudo") if argv[0] == "sudo" else Path(argv[0])
    info = binary.resolve(strict=True).stat() if archive_python else binary.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid not in ({0, os.getuid()} if archive_python else {0}) or info.st_mode & 0o022 or not info.st_mode & 0o111:
        raise DeployError("untrusted_executable")
    try:
        result = subprocess.run([str(binary), *argv[1:]], cwd=cwd, env=core.ENV, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=7205 if not archive_python else 35)
        if result.returncode or len(result.stdout) > 1 << 20:
            raise DeployError("archive_command_failed")
        return result.stdout.decode("utf-8", errors="strict")
    except (subprocess.SubprocessError, OSError, UnicodeError):
        raise DeployError("archive_command_failed") from None


def core_deployed(paths: Paths, command: Command, health: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    try:
        installed = core.json_object(core.file_bytes(paths.core.state / "installation.json", private=True))
        intent = core.json_object(core.file_bytes(paths.core.state / "deploy-intent.json", private=True))
        if installed.get("collector_running") is not True or intent.get("phase") != "complete" or intent.get("result") != "deployed":
            raise DeployError("core_not_deployed")
        manifest = core.load_release(paths.core, installed["current"])
        if not paths.core.current.is_symlink() or paths.core.current.resolve() != paths.core.releases / manifest["target"]:
            raise DeployError("core_not_deployed")
        accepted = core.validate_acceptance(manifest["acceptance"], manifest["tree"])
        views = core.managed_units(paths.core, manifest, command)
        if accepted["streams"] < 60 or any(v["ActiveState"] != "active" or int(v["MainPID"]) <= 0 for v in views.values()):
            raise DeployError("core_not_deployed")
        status = health()
        if (status.get("state") not in {"seeded_waiting", "listening_candidate"}
                or not isinstance(status.get("epoch_id"), str) or not status["epoch_id"]
                or type(status.get("last_sequence")) is not int or status["last_sequence"] < 60):
            raise DeployError("core_not_deployed")
        return {"target": manifest["target"], "tree": manifest["tree"],
                "manifest_sha256": installed["current"]["manifest_sha256"], "verified_streams": accepted["streams"]}
    except (OSError, ValueError, KeyError, TypeError):
        raise DeployError("core_not_deployed") from None


def archive_units(paths: Paths, manifest: dict[str, Any] | None, command: Command, *, idle: bool = True) -> dict[str, dict[str, str]]:
    views = {unit: core.system_view(unit, command) for unit in UNITS}
    for unit, view in views.items():
        file = paths.core.units / unit
        if manifest is None:
            if (view["LoadState"] != "not-found" or view["FragmentPath"] or view["DropInPaths"]
                    or file.exists() or file.is_symlink() or view["MainPID"] != "0"):
                raise DeployError("foreign_archive_unit")
        else:
            info = file.lstat()
            if (view["LoadState"] != "loaded" or view["FragmentPath"] != str(file) or view["DropInPaths"]
                    or not stat.S_ISREG(info.st_mode) or info.st_uid != paths.core.unit_uid
                    or stat.S_IMODE(info.st_mode) != 0o644 or core.sha256(file.read_bytes()) != manifest["unit_sha256"][unit]):
                raise DeployError("foreign_archive_unit")
    if manifest is not None:
        state = command([SYSTEMCTL, "show", UNITS[1], "--property=UnitFileState", "--value"]).strip()
        if state not in {"enabled", "disabled"}: raise DeployError("archive_enable_state")
        views[UNITS[1]]["enabled"] = state
        if idle and (views[UNITS[0]]["ActiveState"] not in {"inactive", "failed"} or views[UNITS[0]]["MainPID"] != "0"):
            raise DeployError("archive_job_active")
    return views


def stage_release(paths: Paths, origin: dict[str, Any], archive: dict[str, Any], artifact: Path,
                  proof_data: bytes, command: Command) -> dict[str, Any]:
    release = paths.releases / origin["target"]
    if release.exists() or release.is_symlink(): raise DeployError("archive_release_exists")
    required = {"src/rquant/l2/archive.py", *["deploy/systemd/" + unit for unit in UNITS]}
    if not required <= set(archive["files"]): raise DeployError("archive_source_required")
    release.mkdir(mode=0o700)
    for name, data in archive["files"].items():
        file = release / name; file.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with file.open("xb") as target: target.write(data)
        file.chmod(archive["modes"][name])
    core.private_write(release / ".archive-artifact.tar.gz", core.file_bytes(artifact))
    core.private_write(release / ".archive-proof.json", proof_data)
    command([core.UV, "--no-config", "sync", "--frozen", "--no-dev", "--python", core.PYTHON], cwd=release)
    digest = core.python_digest(release)
    command([str(release / ".venv/bin/python"), "-I", "-c",
        "import pathlib,rquant.l2.archive; assert pathlib.Path(rquant.l2.archive.__file__).resolve().is_relative_to(pathlib.Path.cwd()/'src')"], cwd=release)
    file_hashes = {name: core.sha256(data) for name, data in archive["files"].items()}
    if any(core.sha256(core.file_bytes(release / name)) != digest for name, digest in file_hashes.items()):
        raise DeployError("archive_release_changed")
    command(["/usr/bin/systemd-analyze", "verify", *[str(release / "deploy/systemd" / unit) for unit in UNITS]])
    manifest = {"target": origin["target"], "tree": origin["tree"], "artifact_sha256": origin["artifact_sha256"],
        "proof_sha256": core.sha256(proof_data), "python_sha256": digest, "file_sha256": file_hashes,
        "unit_sha256": {unit: file_hashes["deploy/systemd/" + unit] for unit in UNITS},
        "ci_run_id": origin["ci_run_id"], "release_id": origin["release_id"], "asset_id": origin["asset_id"]}
    core.save_json(release / ".l2-archive-deployment.json", manifest)
    return {"target": origin["target"], "manifest_sha256": core.sha256(core.file_bytes(release / ".l2-archive-deployment.json", private=True))}


def load_release(paths: Paths, record: dict[str, Any]) -> dict[str, Any]:
    target = record.get("target")
    if not core.hex_value(target, 40) or not core.hex_value(record.get("manifest_sha256")):
        raise DeployError("invalid_archive_record")
    release = paths.releases / target; core.directory(release)
    data = core.file_bytes(release / ".l2-archive-deployment.json", private=True)
    if core.sha256(data) != record["manifest_sha256"]: raise DeployError("archive_manifest_changed")
    value = core.json_object(data)
    try:
        if (value["target"] != target or not core.hex_value(value["tree"], 40)
                or set(value["unit_sha256"]) != set(UNITS)
                or core.python_digest(release) != value["python_sha256"]
                or core.sha256(core.file_bytes(release / ".archive-artifact.tar.gz")) != value["artifact_sha256"]):
            raise DeployError("invalid_archive_record")
        for name, digest in value["file_sha256"].items():
            if not core.safe_relative(name) or not core.hex_value(digest) or core.sha256(core.file_bytes(release / name)) != digest:
                raise DeployError("archive_release_changed")
        for unit in UNITS:
            if core.sha256(core.file_bytes(release / "deploy/systemd" / unit)) != value["unit_sha256"][unit]:
                raise DeployError("archive_release_changed")
        proof_data = core.file_bytes(release / ".archive-proof.json", private=True)
        if core.sha256(proof_data) != value["proof_sha256"]: raise DeployError("archive_proof_changed")
        validate_proof(core.json_object(proof_data), value["tree"])
    except (KeyError, TypeError, ValueError):
        raise DeployError("invalid_archive_record") from None
    return value


def deploy(args: argparse.Namespace, *, paths: Paths = Paths(), api: Callable[[str], dict[str, Any]] = core.github_json,
           command: Command = execute, health: Callable[[], dict[str, Any]] = core.health_status) -> dict[str, Any]:
    proof_data = None
    if not args.rollback:
        proof_data = core.file_bytes(args.archive_proof, private=True)
        if core.sha256(proof_data) != args.archive_proof_sha256: raise DeployError("archive_proof_hash")
        origin = core.verify_origin(args.target, args.artifact, api)
        archive = core.inspect_archive(args.artifact, args.target, origin["tree_entries"], origin["artifact_sha256"])
        accepted = validate_proof(core.json_object(proof_data), origin["tree"])
    core_record = core_deployed(paths, command, health)
    for root in (paths.releases, paths.state):
        if root.exists() or root.is_symlink(): core.directory(root)
    if not paths.state.exists() and args.rollback: raise DeployError("rollback_not_recorded_previous")
    if args.dry_run and not paths.state.exists(): return {"status": "planned", "target": args.target, "acceptance": accepted}
    for root in (paths.releases, paths.state): core.directory(root, create=True)
    with core.exclusive(paths.state / "deploy.lock"):
        intent_path, installed_path = paths.state / "deploy-intent.json", paths.state / "installation.json"
        prior = core.json_object(core.file_bytes(intent_path, private=True)) if intent_path.exists() else {}
        installed = core.json_object(core.file_bytes(installed_path, private=True)) if installed_path.exists() else None
        old_record = installed.get("current") if installed else None
        old = load_release(paths, old_record) if old_record else None
        if (paths.current.is_symlink() and (not old or paths.current.resolve() != paths.releases / old["target"])) or (
                not paths.current.is_symlink() and (paths.current.exists() or old)):
            raise DeployError("foreign_archive_current")
        previous_views = archive_units(paths, old, command)
        resumed = (prior.get("new") == args.target and prior.get("old") == (old["target"] if old else None)
            and isinstance(prior.get("candidate"), dict)
            and (old is not None or prior.get("initialization_attempted") is True))
        if prior and prior.get("phase") != "complete" and not resumed: raise DeployError("incomplete_archive_intent")
        if args.rollback:
            record = installed.get("previous") if installed else None
            if not record or record.get("target") != args.target: raise DeployError("rollback_not_recorded_previous")
            candidate = load_release(paths, record)
            artifact = paths.releases / args.target / ".archive-artifact.tar.gz"
            origin = core.verify_origin(args.target, artifact, api)
            archive = core.inspect_archive(artifact, args.target, origin["tree_entries"], origin["artifact_sha256"])
            accepted = validate_proof(core.json_object(core.file_bytes(paths.releases / args.target / ".archive-proof.json", private=True)), origin["tree"])
        elif old:
            if old["target"] == args.target: raise DeployError("already_current")
            comparison = api(f"/compare/{old['target']}...{args.target}")
            if comparison.get("merge_base_commit", {}).get("sha") != old["target"] or comparison.get("status") not in {"ahead", "identical"}:
                raise DeployError("upgrade_not_fast_forward")
        if args.dry_run: return {"status": "planned", "target": args.target, "acceptance": accepted}
        intent = {"old": old["target"] if old else None, "new": args.target, "phase": "prepared",
            "at": datetime.now(timezone.utc).isoformat(), "core": core_record,
            "initialization_attempted": prior.get("initialization_attempted") is True if resumed else False,
            "candidate": prior.get("candidate") if resumed else None}
        core.save_json(intent_path, intent)
        core.audit(paths, {**intent, "result": "prepared", "artifact_sha256": origin["artifact_sha256"]})

        def run(argv: list[str], cwd: Path | None = None) -> str:
            core.audit(paths, {"at": datetime.now(timezone.utc).isoformat(), "command": argv})
            return command(argv, cwd=cwd) if cwd is not None else command(argv)

        if not args.rollback:
            if resumed:
                record = intent["candidate"]; candidate = load_release(paths, record)
                if candidate["proof_sha256"] != args.archive_proof_sha256 or candidate["artifact_sha256"] != origin["artifact_sha256"]:
                    raise DeployError("archive_resume_changed")
            else:
                record = stage_release(paths, origin, archive, args.artifact, proof_data, run)
                candidate = load_release(paths, record)
        intent["candidate"] = record
        operation = "check" if old or resumed or args.rollback else "init"
        if operation == "init": intent["initialization_attempted"] = True
        intent["phase"] = "init_pending" if operation == "init" else "check_pending"
        core.save_json(intent_path, intent)
        receipt = core.json_object(run(transient_argv(paths, args.target, operation)).encode())
        if (receipt.get("schema") != "rquant-l2-archive-receipt-v1"
                or receipt.get("operation") != operation or receipt.get("prefix") != "l2/v1/rquant"
                or not core.hex_value(receipt.get("catalog_sha256"))
                or receipt.get("status") not in ({"initialized", "already_initialized"} if operation == "init" else {"checked"})):
            raise DeployError("archive_remote_check_failed")
        intent["phase"] = "remote_verified"; intent["remote_receipt_sha256"] = core.sha256(
            json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode())
        core.save_json(intent_path, intent)
        if core_deployed(paths, command, health) != core_record: raise DeployError("core_deployment_changed")
        archive_units(paths, old, command)
        touched: list[str] = []; switched = False; stop_attempted = False
        was_enabled = old is None or previous_views[UNITS[1]]["enabled"] == "enabled"
        was_active = old is None or previous_views[UNITS[1]]["ActiveState"] == "active"

        def install(target: str) -> None:
            for unit in UNITS:
                touched.append(unit)
                run(["sudo", "-n", "/usr/bin/install", "-o", "root", "-g", "root", "-m", "0644",
                     str(paths.releases / target / "deploy/systemd" / unit), str(paths.core.units / unit)])
            run(["sudo", "-n", SYSTEMCTL, "daemon-reload"])

        def restore_timer(enabled: bool, active: bool) -> None:
            run(["sudo", "-n", SYSTEMCTL, "enable" if enabled else "disable", UNITS[1]])
            if active: run(["sudo", "-n", SYSTEMCTL, "start", UNITS[1]])

        try:
            if old:
                stop_attempted = True
                run(["sudo", "-n", SYSTEMCTL, "stop", UNITS[1]])
                run(["sudo", "-n", SYSTEMCTL, "stop", UNITS[0]])
                archive_units(paths, old, command)
            switched = True
            core.change_current(paths, args.target)
            install(args.target)
            restore_timer(was_enabled, was_active)
            active = archive_units(paths, candidate, command, idle=False)
            if (active[UNITS[1]]["enabled"] == "enabled") != was_enabled or (active[UNITS[1]]["ActiveState"] == "active") != was_active:
                raise DeployError("archive_timer_not_active")
            core.save_json(installed_path, {"current": record, "previous": old_record})
            intent["phase"] = "complete"; intent["result"] = "rolled_back" if args.rollback else "deployed"
            core.save_json(intent_path, intent); core.audit(paths, intent)
            return {"status": intent["result"], "target": args.target, "acceptance": accepted}
        except Exception:
            try:
                if touched or switched or stop_attempted:
                    if old:
                        run(["sudo", "-n", SYSTEMCTL, "stop", UNITS[1]])
                        run(["sudo", "-n", SYSTEMCTL, "stop", UNITS[0]])
                        core.change_current(paths, old["target"])
                        install(old["target"]); restore_timer(was_enabled, was_active)
                    else:
                        for unit in reversed(UNITS):
                            file = paths.core.units / unit
                            view = core.system_view(unit, command)
                            absent = (view["LoadState"] == "not-found" and not view["FragmentPath"]
                                and not view["DropInPaths"] and view["MainPID"] == "0"
                                and view["ActiveState"] in {"inactive", "failed"})
                            if not file.exists() and not file.is_symlink():
                                if not absent: raise DeployError("archive_cleanup_changed")
                                continue
                            info = file.lstat()
                            if (not stat.S_ISREG(info.st_mode) or info.st_uid != paths.core.unit_uid
                                    or stat.S_IMODE(info.st_mode) != 0o644 or core.sha256(file.read_bytes()) != candidate["unit_sha256"][unit]):
                                raise DeployError("archive_cleanup_changed")
                            if not absent:
                                if view["LoadState"] != "loaded" or view["FragmentPath"] != str(file) or view["DropInPaths"]:
                                    raise DeployError("archive_cleanup_changed")
                                run(["sudo", "-n", SYSTEMCTL, "stop", unit])
                                if unit == UNITS[1]: run(["sudo", "-n", SYSTEMCTL, "disable", unit])
                            run(["sudo", "-n", "/usr/bin/rm", "--", str(file)])
                        for link in (paths.current, paths.releases / ".current.new"):
                            if not link.exists() and not link.is_symlink(): continue
                            if (not link.is_symlink() or link.lstat().st_uid != os.getuid()
                                    or link.resolve() != paths.releases / args.target):
                                raise DeployError("archive_cleanup_changed")
                            link.unlink()
                        descriptor = os.open(paths.releases, os.O_RDONLY | os.O_DIRECTORY)
                        try: os.fsync(descriptor)
                        finally: os.close(descriptor)
                        run(["sudo", "-n", SYSTEMCTL, "daemon-reload"])
                if installed is not None:
                    core.save_json(installed_path, installed)
                elif installed_path.exists() or installed_path.is_symlink():
                    if core.json_object(core.file_bytes(installed_path, private=True)) != {"current": record, "previous": old_record}:
                        raise DeployError("archive_installation_changed")
                    installed_path.unlink()
                    descriptor = os.open(paths.state, os.O_RDONLY | os.O_DIRECTORY)
                    try: os.fsync(descriptor)
                    finally: os.close(descriptor)
                intent["phase"] = "complete"; intent["result"] = "activation_failed"
                core.save_json(intent_path, intent); core.audit(paths, intent)
            except Exception:
                raise DeployError("archive_rollback_incomplete") from None
            raise DeployError("archive_activation_failed") from None


def main(argv: list[str] | None = None) -> int:
    try:
        if sys.platform != "linux" or os.getuid() == 0: raise DeployError("lighthouse_linux_required")
        import pwd
        if pwd.getpwuid(os.getuid()).pw_name != "lighthouse": raise DeployError("lighthouse_linux_required")
        print(json.dumps(deploy(parse_cli(argv)), sort_keys=True))
        return 0
    except DeployError as error:
        print(json.dumps({"status": "refused", "error_code": str(error)}), file=sys.stderr)
        return 2
    except (OSError, ValueError, KeyError, TypeError):
        print('{"status":"refused","error_code":"archive_preflight_failed"}', file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
