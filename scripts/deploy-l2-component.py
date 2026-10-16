#!/usr/bin/env python3
"""Narrow L2 component deployer, reached only through deploy-production.sh."""
from __future__ import annotations

import argparse
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import fcntl
import hashlib
import io
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import socket
import stat
import subprocess
import sys
import tarfile
import time
from typing import Any, Iterator
from urllib.request import Request, urlopen

API = "https://api.github.com/repos/roxorlt/rquant"
UNITS = ("rquant-l2-api.service", "rquant-l2-collector.service")
JOBS = {"L2 raw feed (Python 3.11)", "L2 raw feed (Python 3.12)", "web"}
EXPECTED = {(code, market, kind, side, base + offset)
    for code, market, base in (("603258", 17, 10117), ("300006", 33, 10127))
    for offset, (kind, side) in enumerate((("vendor4013", None), ("vendor4027", "buy"), ("vendor4027", "sell")))}


class DeployError(Exception):
    """Only fixed codes leave this process; command/API payloads are never printed."""


def hex_value(value: Any, size: int = 64) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{" + str(size) + "}", value) is not None


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name, value in items:
        if name in result:
            raise DeployError("duplicate_json_key")
        result[name] = value
    return result


def json_object(data: bytes) -> dict[str, Any]:
    try:
        result = json.loads(data, object_pairs_hook=pairs)
        if not isinstance(result, dict):
            raise ValueError
        return result
    except (ValueError, TypeError):
        raise DeployError("invalid_json") from None


def file_bytes(path: Path, *, private: bool = False, limit: int = 64 << 20) -> bytes:
    info = path.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
            or info.st_size > limit or (private and stat.S_IMODE(info.st_mode) != 0o600)):
        raise DeployError("unsafe_file")
    if private:
        directory(path.parent)
    with path.open("rb") as source:
        data = source.read(limit + 1)
    if len(data) > limit or path.stat().st_ino != info.st_ino:
        raise DeployError("unsafe_file")
    return data


def utc(value: Any) -> datetime:
    try:
        at = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if at.utcoffset() != timedelta(0):
            raise ValueError
        return at
    except (AttributeError, ValueError, TypeError):
        raise DeployError("invalid_receipt_time") from None


def validate_acceptance(proof: dict[str, Any], target_tree: str) -> dict[str, Any]:
    try:
        duration = proof["duration_seconds"]
        cache, http, cleanup = proof["cache"], proof["http"], proof["cleanup"]
        spacing = proof["minimum_query_spacing_seconds"]
        if (proof["schema"] != "rquant-l2-acceptance-v1" or not hex_value(proof["source_commit"], 40)
                or proof["source_tree"] != target_tree or not hex_value(target_tree, 40)
                or type(duration) is not int or duration not in (300, 1200)
                or any(type(proof[k]) is not int or proof[k] != n for k, n in
                       (("connections", 1), ("upstream_sends", 7), ("reconnect_attempts", 0)))
                or type(spacing) not in (int, float) or not math.isfinite(spacing) or spacing < 1
                or any(type(cache[k]) is not int for k in ("first_sequence", "last_sequence", "batch_count", "seed_batches"))
                or cache["first_sequence"] != 1 or cache["last_sequence"] != cache["batch_count"]
                or cache["batch_count"] < 6 or cache["seed_batches"] != 6
                or type(http["successful_requests"]) is not int or http["successful_requests"] < 6
                or type(http["failed_requests"]) is not int or http["failed_requests"] != 0
                or type(http["extra_upstream_sends"]) is not int or http["extra_upstream_sends"] != 0
                or any(type(http["windows"][w]) is not int or http["windows"][w] < 2 for w in ("early", "mid", "late"))
                or cleanup["worker_alive"] is not False or cleanup["http_worker_alive"] is not False
                or cleanup["all_upstream_sockets_closed"] is not True
                or not all(hex_value(v) for v in (cache["export_sha256"], cache["integrity_proof_sha256"],
                                                http["proof_sha256"], cleanup["proof_sha256"]))):
            raise DeployError("acceptance_gate")
        origin = utc(proof["collector_started_at"])
        streams, identities, epochs, sequences = proof["streams"], set(), set(), set()
        if len(streams) != 6:
            raise DeployError("acceptance_six_streams")
        for stream in streams:
            binding = stream["binding"]
            identity = tuple(binding[k] for k in ("code", "market", "kind", "side", "page_id"))
            if identity not in EXPECTED or identity in identities:
                raise DeployError("acceptance_binding")
            identities.add(identity)
            if not isinstance(stream["epoch_id"], str) or not stream["epoch_id"]:
                raise DeployError("acceptance_epoch")
            epochs.add(stream["epoch_id"])
            seen = set(stream["seed_ids"])
            if not seen or len(seen) != len(stream["seed_ids"]) or any(type(n) is not int or n < 0 for n in seen):
                raise DeployError("acceptance_seed")
            previous = stream["seed_sequence"]
            if type(previous) is not int or not 1 <= previous <= cache["last_sequence"] or previous in sequences:
                raise DeployError("acceptance_sequence")
            sequences.add(previous)
            high, arrivals, windows, late = max(seen), [], set(), set()
            for batch in stream["forward_batches"]:
                sequence, ids = batch["sequence"], batch["ids"]
                if (type(sequence) is not int or not previous < sequence <= cache["last_sequence"]
                        or sequence in sequences or not ids or len(set(ids)) != len(ids)
                        or any(type(n) is not int or n <= high or n in seen for n in ids)):
                    raise DeployError("acceptance_forward")
                previous = sequence; sequences.add(sequence); seen.update(ids); high = max(ids)
                at = utc(batch["received_at"]); elapsed = (at - origin).total_seconds()
                if not 0 <= elapsed <= duration:
                    raise DeployError("acceptance_time")
                arrivals.append(at); windows.add(int(at.timestamp()) // 30)
                if duration - 60 <= elapsed <= duration:
                    late.add(sequence)
            if not arrivals or (max(arrivals) - min(arrivals)).total_seconds() < 60 or len(windows) < 3 or len(late) < 2:
                raise DeployError("acceptance_sustained")
        if identities != EXPECTED or len(epochs) != 1:
            raise DeployError("acceptance_binding")
        return {"streams": 6, "source_tree": target_tree, "duration_seconds": duration,
                "late_window_seconds": [duration - 60, duration], "cache_batches": cache["batch_count"]}
    except (KeyError, TypeError, ValueError, OverflowError):
        raise DeployError("invalid_acceptance") from None


def github_json(path: str) -> dict[str, Any]:
    request = Request(API + path, headers={"Accept": "application/vnd.github+json", "User-Agent": "rquant-l2-deploy"})
    try:
        with urlopen(request, timeout=15) as response:
            if not response.geturl().startswith(API + "/"):
                raise DeployError("github_origin")
            body = response.read((8 << 20) + 1)
        if len(body) > 8 << 20:
            raise DeployError("github_size")
        return json_object(body)
    except Exception:
        raise DeployError("github_unavailable") from None


def verify_origin(target: str, artifact: Path, api: Callable[[str], dict[str, Any]]) -> dict[str, Any]:
    if not hex_value(target, 40):
        raise DeployError("exact_target_required")
    try:
        commit = api(f"/commits/{target}"); tree = commit["commit"]["tree"]["sha"]
        main = api("/git/ref/heads/main")["object"]["sha"]
        comparison = api(f"/compare/{target}...{main}")
        if (commit["sha"] != target or not hex_value(tree, 40) or not hex_value(main, 40)
                or comparison["merge_base_commit"]["sha"] != target or comparison["status"] not in {"ahead", "identical"}):
            raise DeployError("target_not_main")
        runs = api(f"/actions/workflows/web-mvp.yml/runs?event=push&head_sha={target}&per_page=100")["workflow_runs"]
        successful = [r for r in runs if r["head_sha"] == target and r["event"] == "push"
                      and r["head_branch"] == "main" and r["status"] == "completed" and r["conclusion"] == "success"]
        if not successful:
            raise DeployError("exact_ci_required")
        run = successful[0]
        if type(run["id"]) is not int or run["id"] <= 0:
            raise DeployError("exact_ci_required")
        jobs = api(f"/actions/runs/{run['id']}/jobs?filter=latest&per_page=100")["jobs"]
        if (len(jobs) != 3 or {j["name"] for j in jobs} != JOBS
                or any(j["status"] != "completed" or j["conclusion"] != "success" for j in jobs)):
            raise DeployError("ci_jobs_required")
        release = api(f"/releases/tags/web-{target[:8]}")
        assets = [a for a in release["assets"] if a["name"] == f"rquant-web-{target[:8]}.tar.gz"]
        if release["draft"] is not False or len(assets) != 1:
            raise DeployError("release_asset_required")
        asset = assets[0]; digest = asset.get("digest")
        if (not isinstance(digest, str) or not digest.startswith("sha256:") or not hex_value(digest[7:])
                or asset.get("state") != "uploaded" or asset["size"] != artifact.stat().st_size
                or sha256(file_bytes(artifact)) != digest[7:]):
            raise DeployError("artifact_digest")
        tree_record = api(f"/git/trees/{tree}?recursive=1")
        if tree_record.get("truncated") is not False or tree_record["sha"] != tree:
            raise DeployError("source_tree_required")
        return {"target": target, "tree": tree, "main": main, "ci_run_id": run["id"],
                "ci_jobs": sorted(JOBS), "release_id": release["id"], "asset_id": asset["id"],
                "artifact_sha256": digest[7:], "tree_entries": tree_record["tree"]}
    except (KeyError, TypeError, ValueError):
        raise DeployError("invalid_github_provenance") from None


def inspect_archive(artifact: Path, target: str, tree: list[dict[str, Any]], expected_sha256: str) -> dict[str, Any]:
    archive_bytes = file_bytes(artifact)
    if sha256(archive_bytes) != expected_sha256:
        raise DeployError("artifact_changed")
    files: dict[str, bytes] = {}; modes: dict[str, int] = {}; total = 0
    try:
        with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:gz") as archive:
            for member in archive:
                path = PurePosixPath(member.name)
                if path.is_absolute() or ".." in path.parts or "\\" in member.name or ".git" in path.parts:
                    raise DeployError("unsafe_archive_path")
                if member.isdir():
                    continue
                name = path.as_posix()
                if not member.isfile() or name in files or not 0 <= member.size <= 64 << 20:
                    raise DeployError("unsafe_archive_entry")
                total += member.size
                if total > 256 << 20:
                    raise DeployError("archive_size")
                source = archive.extractfile(member)
                if source is None:
                    raise DeployError("unsafe_archive_entry")
                files[name] = source.read(member.size + 1); modes[name] = 0o700 if member.mode & 0o111 else 0o600
                if len(files[name]) != member.size:
                    raise DeployError("unsafe_archive_entry")
    except (tarfile.TarError, EOFError, OSError):
        raise DeployError("invalid_archive") from None
    expected: dict[str, str] = {}
    for item in tree:
        if item["type"] == "tree":
            continue
        name = item["path"]
        if item["type"] != "blob" or item["mode"] not in {"100644", "100755"} or name in expected:
            raise DeployError("unsupported_source_tree")
        expected[name] = item["sha"]
        data = files.get(name)
        if (data is None or hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest() != item["sha"]
                or modes[name] != (0o700 if item["mode"] == "100755" else 0o600)):
            raise DeployError("source_tree_mismatch")
    if any(name not in expected and name != "RELEASE.json" and not name.startswith("web/dist/") for name in files):
        raise DeployError("unexpected_archive_file")
    metadata = json_object(files.get("RELEASE.json", b"{}"))
    if (metadata.get("commit") != target or metadata.get("short") != target[:8]
            or metadata.get("artifact_kind") == "isolated_pr_staging" or "uv.lock" not in expected):
        raise DeployError("release_metadata")
    unit_hashes = {}
    for unit in UNITS:
        name = "deploy/systemd/" + unit
        if name not in expected:
            raise DeployError("unit_missing")
        unit_hashes[unit] = sha256(files[name])
    return {"files": files, "modes": modes, "unit_sha256": unit_hashes,
            "source_blob_sha1": expected, "artifact_sha256": expected_sha256}


@dataclass(frozen=True)
class Paths:
    releases: Path = Path("/home/lighthouse/rquant-l2-rel")
    state: Path = Path("/home/lighthouse/rquant-l2-state")
    inputs: Path = Path("/home/lighthouse/rquant-l2-private")
    units: Path = Path("/etc/systemd/system")
    unit_uid: int = 0

    @property
    def current(self) -> Path:
        return self.releases / "current"

    @property
    def cache(self) -> Path:
        return self.state / "cache"


Command = Callable[..., str]
UV = "/home/lighthouse/.local/bin/uv"
PYTHON = "/usr/bin/python3.11"
SYSTEMCTL = "/usr/bin/systemctl"
ENV = {"PATH": "/usr/local/bin:/usr/bin:/bin", "HOME": "/home/lighthouse", "LANG": "C.UTF-8",
       "TZ": "Asia/Shanghai", "PYTHONDONTWRITEBYTECODE": "1", "RQUANT_DISABLE_DOTENV": "1",
       "RQUANT_WEB_WARM": "0"}


def directory(path: Path, *, create: bool = False) -> None:
    if create and not path.exists():
        path.mkdir(mode=0o700)
    info = path.lstat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o700 or path.resolve() != path.absolute()):
        raise DeployError("unsafe_directory")


def private_write(path: Path, data: bytes) -> None:
    directory(path.parent)
    temp = path.with_name("." + path.name + ".new")
    descriptor = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as target:
            target.write(data); target.flush(); os.fsync(target.fileno())
        os.replace(temp, path)
        descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try: os.fsync(descriptor)
        finally: os.close(descriptor)
    finally:
        if temp.exists(): temp.unlink()


def save_json(path: Path, value: dict[str, Any]) -> None:
    private_write(path, (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode())


@contextmanager
def exclusive(path: Path) -> Iterator[None]:
    directory(path.parent)
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o600:
            raise DeployError("unsafe_lock")
        try: fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError: raise DeployError("lock_busy") from None
        yield
    finally:
        os.close(descriptor)


def execute(argv: list[str], cwd: Path | None = None) -> str:
    binary = Path("/usr/bin/sudo") if argv[0] == "sudo" else Path(argv[0])
    release_python = re.fullmatch(r"/home/lighthouse/rquant-l2-rel/[0-9a-f]{40}/\.venv/bin/python", str(binary)) is not None
    info = binary.resolve(strict=True).stat() if release_python else binary.lstat()
    owners = {0, os.getuid()} if release_python else {os.getuid() if str(binary) == UV else 0}
    if (not stat.S_ISREG(info.st_mode) or info.st_uid not in owners
            or info.st_mode & 0o022 or not info.st_mode & 0o111):
        raise DeployError("untrusted_executable")
    command = [str(binary), *argv[1:]]
    try:
        result = subprocess.run(command, cwd=cwd, env=ENV, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=600 if str(binary) == UV else 35, check=False)
        if result.returncode or len(result.stdout) > 8 << 20:
            raise DeployError("command_failed")
        return result.stdout.decode("utf-8", errors="strict")
    except (subprocess.SubprocessError, OSError, UnicodeError):
        raise DeployError("command_failed") from None


def system_view(unit: str, command: Command) -> dict[str, str]:
    result = command([SYSTEMCTL, "show", unit,
                      "--property=LoadState,FragmentPath,DropInPaths,MainPID,ActiveState"])
    values = dict(line.split("=", 1) for line in result.splitlines() if "=" in line)
    if (set(values) != {"LoadState", "FragmentPath", "DropInPaths", "MainPID", "ActiveState"}
            or not values["MainPID"].isdigit()):
        raise DeployError("unit_inspection_failed")
    return values


def load_release(paths: Paths, record: dict[str, Any]) -> dict[str, Any]:
    target = record.get("target")
    if not hex_value(target, 40) or not hex_value(record.get("manifest_sha256")):
        raise DeployError("invalid_installation_record")
    release = paths.releases / target; directory(release)
    encoded = file_bytes(release / ".l2-deployment.json", private=True)
    if sha256(encoded) != record["manifest_sha256"]:
        raise DeployError("release_manifest_changed")
    manifest = json_object(encoded)
    try:
        if manifest["target"] != target or not hex_value(manifest["tree"], 40):
            raise DeployError("invalid_release_record")
        for name, digest in manifest["file_sha256"].items():
            if not hex_value(digest) or not safe_relative(name) or sha256(file_bytes(release / name)) != digest:
                raise DeployError("release_file_changed")
        if sha256(file_bytes(release / ".l2-artifact.tar.gz")) != manifest["artifact_sha256"]:
            raise DeployError("release_artifact_changed")
        if python_digest(release) != manifest["python_sha256"]:
            raise DeployError("release_python_changed")
        if set(manifest["unit_sha256"]) != set(UNITS):
            raise DeployError("invalid_release_units")
        for unit in UNITS:
            if sha256(file_bytes(release / "deploy/systemd" / unit)) != manifest["unit_sha256"][unit]:
                raise DeployError("release_unit_changed")
    except (KeyError, TypeError, ValueError):
        raise DeployError("invalid_release_record") from None
    return manifest


def safe_relative(name: str) -> bool:
    path = PurePosixPath(name)
    return bool(path.parts) and not path.is_absolute() and ".." not in path.parts and "\\" not in name


def managed_units(paths: Paths, manifest: dict[str, Any] | None, command: Command) -> dict[str, dict[str, str]]:
    views = {unit: system_view(unit, command) for unit in UNITS}
    for unit, view in views.items():
        file = paths.units / unit
        if manifest is None:
            if (view["LoadState"] != "not-found" or view["FragmentPath"] or view["DropInPaths"]
                    or file.exists() or file.is_symlink() or view["MainPID"] != "0"):
                raise DeployError("foreign_unit")
        else:
            info = file.lstat()
            if (view["LoadState"] != "loaded" or view["FragmentPath"] != str(file)
                    or view["DropInPaths"] or not stat.S_ISREG(info.st_mode) or info.st_uid != paths.unit_uid
                    or stat.S_IMODE(info.st_mode) != 0o644 or sha256(file.read_bytes()) != manifest["unit_sha256"][unit]):
                raise DeployError("foreign_unit")
    return views


def socket_writer_preflight(paths: Paths, views: dict[str, dict[str, str]], command: Command,
                            *, stopped: bool = False) -> None:
    api_pid, collector_pid = (int(views[unit]["MainPID"]) for unit in UNITS)
    for listening, argv in ((True, ["/usr/sbin/ss", "-H", "-ltnp"]), (False, ["/usr/sbin/ss", "-H", "-tnp"])):
        for line in command(argv).splitlines():
            fields = line.split()
            if len(fields) < 5:
                raise DeployError("socket_inspection_failed")
            relevant = fields[3].rsplit(":", 1)[-1] == "8771" if listening else fields[4] == "122.9.43.156:9528"
            if not relevant: continue
            pids = {int(n) for n in re.findall(r"pid=(\d+)", line)}
            expected = api_pid if listening else collector_pid
            if (not expected or pids != {expected} or (not listening and stopped)
                    or listening and fields[3] != "127.0.0.1:8771"):
                raise DeployError("foreign_socket")
    try:
        with exclusive(paths.cache / ".writer.lock"): pass
    except DeployError as error:
        if str(error) != "lock_busy" or stopped or not collector_pid:
            raise
    if stopped and (collector_pid or views[UNITS[1]]["ActiveState"] not in {"inactive", "failed"}):
        raise DeployError("collector_not_stopped")


def input_metadata(paths: Paths) -> None:
    directory(paths.inputs)
    for name in ("bootstrap.bin", "order-template.bin", "trade-template.bin"):
        info = (paths.inputs / name).lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o600:
            raise DeployError("unsafe_input_metadata")


def python_digest(release: Path) -> str:
    python = (release / ".venv/bin/python").resolve(strict=True)
    info = python.stat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid not in {0, os.getuid()} or info.st_mode & 0o022 or not info.st_mode & 0o111:
        raise DeployError("unsafe_release_python")
    return sha256(python.read_bytes())


def stage_release(paths: Paths, origin: dict[str, Any], archive: dict[str, Any], artifact: Path,
                  proof: dict[str, Any], proof_hash: str, command: Command) -> dict[str, Any]:
    release = paths.releases / origin["target"]
    if release.exists() or release.is_symlink():
        raise DeployError("release_already_exists")
    release.mkdir(mode=0o700)
    for name, data in archive["files"].items():
        file = release / name
        file.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with file.open("xb") as target: target.write(data)
        file.chmod(archive["modes"][name])
    private_write(release / ".l2-artifact.tar.gz", file_bytes(artifact))
    command([UV, "--no-config", "sync", "--frozen", "--no-dev", "--python", PYTHON], cwd=release)
    digest = python_digest(release)
    command([str(release / ".venv/bin/python"), "-I", "-c",
        "import pathlib,rquant.l2.api,rquant.l2.feed; p=pathlib.Path.cwd()/'src'; "
        "assert all(pathlib.Path(m.__file__).resolve().is_relative_to(p) for m in (rquant.l2.api,rquant.l2.feed))"], cwd=release)
    file_hashes = {name: sha256(data) for name, data in archive["files"].items()}
    if any(sha256(file_bytes(release / name)) != digest for name, digest in file_hashes.items()):
        raise DeployError("release_changed_during_build")
    command(["/usr/bin/systemd-analyze", "verify", *[str(release / "deploy/systemd" / u) for u in UNITS]])
    manifest = {"target": origin["target"], "tree": origin["tree"], "artifact_sha256": origin["artifact_sha256"],
        "unit_sha256": archive["unit_sha256"], "file_sha256": file_hashes, "python_sha256": digest,
        "acceptance": proof, "acceptance_sha256": proof_hash,
        "ci_run_id": origin["ci_run_id"], "release_id": origin["release_id"], "asset_id": origin["asset_id"]}
    save_json(release / ".l2-deployment.json", manifest)
    return {"target": origin["target"], "manifest_sha256": sha256(file_bytes(release / ".l2-deployment.json", private=True))}


def audit(paths: Paths, value: dict[str, Any]) -> None:
    path = paths.state / "deploy.jsonl"
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(descriptor)
        if info.st_uid != os.getuid() or not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600:
            raise DeployError("unsafe_audit")
        with os.fdopen(descriptor, "ab", closefd=False) as target:
            target.write((json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode())
            target.flush(); os.fsync(target.fileno())
    finally: os.close(descriptor)


def change_current(paths: Paths, target: str) -> None:
    temp = paths.releases / ".current.new"
    temp.symlink_to(paths.releases / target, target_is_directory=True)
    os.replace(temp, paths.current)
    descriptor = os.open(paths.releases, os.O_RDONLY | os.O_DIRECTORY)
    try: os.fsync(descriptor)
    finally: os.close(descriptor)


def health_status() -> dict[str, Any]:
    try:
        with urlopen("http://127.0.0.1:8771/api/v1/l2/status", timeout=2) as response:
            data = response.read((2 << 20) + 1)
        if len(data) > 2 << 20:
            raise DeployError("api_health_failed")
        return json_object(data)
    except Exception:
        raise DeployError("api_health_failed") from None


def wait_health(health: Callable[[], dict[str, Any]], sleep: Callable[[float], None], *, collector: bool,
                old_epoch: str | None = None) -> None:
    for _ in range(20 if collector else 5):
        try:
            value = health()
            if not isinstance(value.get("state"), str) or type(value.get("last_sequence")) is not int:
                raise DeployError("api_health_failed")
            if collector and (value.get("state") == "blocked" or value.get("error_code") not in {None, "restart_gap", "signal_stop", "duration_limit"}):
                raise DeployError("collector_health_failed")
            if not collector or (value.get("state") in {"seeded_waiting", "listening_candidate"}
                                 and isinstance(value.get("epoch_id"), str) and value["epoch_id"] != old_epoch
                                 and value.get("seed_queries") == 6 and value.get("market_batch_count", 0) >= 6
                                 and value.get("last_sequence", 0) >= 6):
                return
        except DeployError as error:
            if str(error) != "api_health_failed": raise
        sleep(1)
    raise DeployError("collector_health_failed" if collector else "api_health_failed")


def restart_window(now: datetime) -> bool:
    local = now.astimezone(timezone(timedelta(hours=8)))
    minute = local.hour * 60 + local.minute
    return local.weekday() < 5 and 9 * 60 + 15 <= minute < 15 * 60 + 10


def parse_cli(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deploy only the two fixed L2 system services")
    parser.add_argument("--target", required=True)
    parser.add_argument("--artifact", type=Path)
    parser.add_argument("--acceptance-proof", type=Path)
    parser.add_argument("--acceptance-sha256")
    parser.add_argument("--rollback", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if (not hex_value(args.target, 40) or args.rollback and any((args.artifact, args.acceptance_proof, args.acceptance_sha256))
            or not args.rollback and (not args.artifact or not args.artifact.is_absolute() or not args.acceptance_proof
                or not args.acceptance_proof.is_absolute() or not hex_value(args.acceptance_sha256))):
        parser.error("An exact commit and the component's pinned inputs are required")
    return args


def deploy(args: argparse.Namespace, *, paths: Paths = Paths(), api: Callable[[str], dict[str, Any]] = github_json,
           command: Command = execute, health: Callable[[], dict[str, Any]] = health_status,
           now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
           sleep: Callable[[float], None] = time.sleep) -> dict[str, Any]:
    proof_data = None
    if not args.rollback:
        proof_data = file_bytes(args.acceptance_proof, private=True)
        if sha256(proof_data) != args.acceptance_sha256:
            raise DeployError("acceptance_hash")
        proof_value = json_object(proof_data)
        origin = verify_origin(args.target, args.artifact, api)
        archive = inspect_archive(args.artifact, args.target, origin["tree_entries"], origin["artifact_sha256"])
        acceptance = validate_acceptance(proof_value, origin["tree"])
    for root in (paths.releases, paths.state, paths.cache): directory(root, create=True)
    with exclusive(paths.state / "deploy.lock"):
        intent_path, installed_path = paths.state / "deploy-intent.json", paths.state / "installation.json"
        prior_intent = json_object(file_bytes(intent_path, private=True)) if intent_path.exists() else {}
        if prior_intent and prior_intent.get("phase") != "complete":
            raise DeployError("incomplete_intent")
        installed = json_object(file_bytes(installed_path, private=True)) if installed_path.exists() else None
        current_record = installed.get("current") if installed else None
        old = load_release(paths, current_record) if current_record else None
        if (paths.current.is_symlink() and (not old or paths.current.resolve() != paths.releases / old["target"])) or (
                not paths.current.is_symlink() and (paths.current.exists() or old)):
            raise DeployError("foreign_current")
        views = managed_units(paths, old, command)
        socket_writer_preflight(paths, views, command)
        input_metadata(paths)
        if args.rollback:
            previous = installed.get("previous") if installed else None
            if not previous or previous.get("target") != args.target:
                raise DeployError("rollback_not_recorded_previous")
            candidate = load_release(paths, previous)
            artifact = paths.releases / args.target / ".l2-artifact.tar.gz"
            origin = verify_origin(args.target, artifact, api)
            archive = inspect_archive(artifact, args.target, origin["tree_entries"], origin["artifact_sha256"])
            acceptance = validate_acceptance(candidate["acceptance"], origin["tree"])
            candidate_record = previous
        else:
            if old:
                if old["target"] == args.target:
                    raise DeployError("already_current")
                comparison = api(f"/compare/{old['target']}...{args.target}")
                if comparison.get("merge_base_commit", {}).get("sha") != old["target"] or comparison.get("status") not in {"ahead", "identical"}:
                    raise DeployError("upgrade_not_fast_forward")
        at = now()
        if old and restart_window(at):
            raise DeployError("restart_window")
        last_start = prior_intent.get("last_collector_start")
        if not args.rollback and last_start and (at - utc(last_start)).total_seconds() < 60:
            raise DeployError("collector_cooldown")
        if args.dry_run:
            return {"status": "planned", "target": args.target, "acceptance": acceptance}
        intent = {"old": old["target"] if old else None, "new": args.target, "phase": "prepared",
                  "at": at.isoformat(), "last_collector_start": last_start, "rollback": args.rollback}
        save_json(intent_path, intent)
        audit(paths, {**intent, "result": "prepared", "artifact_sha256": origin["artifact_sha256"],
            "source_tree": origin["tree"], "ci_run_id": origin["ci_run_id"], "unit_sha256": archive["unit_sha256"],
            "previous_state": {unit: {k: views[unit][k] for k in ("ActiveState", "MainPID")} for unit in UNITS}})
        touched, switched, collector_started, old_stop_attempted = [], False, False, False

        def run(argv: list[str], cwd: Path | None = None) -> str:
            audit(paths, {"at": now().isoformat(), "command": argv})
            return command(argv, cwd=cwd) if cwd is not None else command(argv)

        def action(argv: list[str]) -> None:
            run(argv)

        def install_units(target: str) -> None:
            for unit in UNITS:
                touched.append(unit)
                action(["sudo", "-n", "/usr/bin/install", "-o", "root", "-g", "root", "-m", "0644",
                        str(paths.releases / target / "deploy/systemd" / unit), str(paths.units / unit)])
            action(["sudo", "-n", SYSTEMCTL, "daemon-reload"])

        try:
            if not args.rollback:
                candidate_record = stage_release(paths, origin, archive, args.artifact, proof_value, args.acceptance_sha256, run)
            candidate = load_release(paths, candidate_record)
            views = managed_units(paths, old, command)
            socket_writer_preflight(paths, views, command)
            if old:
                if restart_window(now()): raise DeployError("restart_window")
                old_stop_attempted = True
                action(["sudo", "-n", SYSTEMCTL, "stop", UNITS[1]])
                views = managed_units(paths, old, command)
            socket_writer_preflight(paths, views, command, stopped=True)
            change_current(paths, args.target); switched = True
            intent["phase"] = "activating"; save_json(intent_path, intent)
            install_units(args.target)
            action(["sudo", "-n", SYSTEMCTL, "restart" if old else "enable", *([] if old else ["--now"]), UNITS[0]])
            wait_health(health, sleep, collector=False)
            if not args.rollback:
                baseline_epoch = health().get("epoch_id")
                intent["last_collector_start"] = now().isoformat(); save_json(intent_path, intent)
                collector_started = True
                action(["sudo", "-n", SYSTEMCTL, "start" if old else "enable", *([] if old else ["--now"]), UNITS[1]])
                wait_health(health, sleep, collector=True, old_epoch=baseline_epoch)
            active = managed_units(paths, candidate, command)
            if any(active[u]["ActiveState"] != "active" or int(active[u]["MainPID"]) <= 0
                   for u in (UNITS[:1] if args.rollback else UNITS)):
                raise DeployError("service_not_active")
            socket_writer_preflight(paths, active, command, stopped=args.rollback)
            save_json(installed_path, {"current": candidate_record, "previous": current_record,
                                      "collector_running": not args.rollback})
            intent["phase"] = "complete"; intent["result"] = "rolled_back" if args.rollback else "deployed"
            save_json(intent_path, intent); audit(paths, intent)
            return {"status": intent["result"], "target": args.target, "acceptance": acceptance}
        except Exception as failure:
            try:
                if touched or switched:
                    if collector_started or old:
                        action(["sudo", "-n", SYSTEMCTL, "stop", UNITS[1]])
                    if old:
                        change_current(paths, old["target"])
                        install_units(old["target"])
                        action(["sudo", "-n", SYSTEMCTL, "restart", UNITS[0]])
                    else:
                        for unit in reversed(touched):
                            action(["sudo", "-n", SYSTEMCTL, "disable", "--now", unit])
                            file = paths.units / unit
                            if not file.exists() and not file.is_symlink():
                                continue
                            info = file.lstat()
                            if (stat.S_ISREG(info.st_mode) and info.st_uid == paths.unit_uid
                                    and stat.S_IMODE(info.st_mode) == 0o644
                                    and sha256(file.read_bytes()) == archive["unit_sha256"][unit]):
                                action(["sudo", "-n", "/usr/bin/rm", "--", str(file)])
                            else: raise DeployError("cleanup_unit_changed")
                        if paths.current.is_symlink() and paths.current.resolve() == paths.releases / args.target:
                            paths.current.unlink()
                        action(["sudo", "-n", SYSTEMCTL, "daemon-reload"])
                elif old_stop_attempted:
                    views = managed_units(paths, old, command)
                    socket_writer_preflight(paths, views, command, stopped=True)
                intent["phase"] = "complete"
                intent["result"] = ("failed_collector_stopped" if old_stop_attempted else "failed_removed" if touched
                                    else "failed_before_activation")
                save_json(intent_path, intent); audit(paths, intent)
            except Exception:
                raise DeployError("rollback_incomplete") from None
            if isinstance(failure, DeployError) and str(failure) == "restart_window" and not old_stop_attempted:
                raise DeployError("restart_window") from None
            raise DeployError("deployment_failed") from None


def main(argv: list[str] | None = None) -> int:
    try:
        if sys.platform != "linux" or os.getuid() == 0:
            raise DeployError("lighthouse_linux_required")
        import pwd
        if pwd.getpwuid(os.getuid()).pw_name != "lighthouse":
            raise DeployError("lighthouse_linux_required")
        print(json.dumps(deploy(parse_cli(argv)), sort_keys=True))
        return 0
    except DeployError as error:
        code = str(error)
        print(json.dumps({"status": "refused", "error_code": code}), file=sys.stderr)
        return 75 if code == "restart_window" else 2
    except (OSError, ValueError, KeyError, TypeError):
        print('{"status":"refused","error_code":"local_preflight_failed"}', file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
