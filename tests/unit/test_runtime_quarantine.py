from __future__ import annotations

import hashlib
import inspect
import os
import shutil
import socket
import stat
import sys
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType
from typing import Any

import pytest

import rquant.runtime_authority as authority_module
import rquant.runtime_quarantine as quarantine_module
from rquant.runtime_authority import (
    RuntimeAuthorityPublishError,
    RuntimeGenerationLifecycle,
    RuntimeGenerationSlot,
    parse_runtime_closure_profile,
    prepare_runtime_authority_publish,
    publish_runtime_authority,
)
from rquant.runtime_quarantine import (
    RuntimeQuarantineDurabilityError,
    RuntimeQuarantineError,
    RuntimeQuarantineRequest,
    RuntimeQuarantineStatus,
    cleanup_runtime_quarantine,
    parse_runtime_quarantine_request,
    publish_runtime_candidate,
)
from rquant.strict_json import canonical_json_bytes, strict_json_loads


class SimulatedCrash(BaseException):
    pass


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("ascii")).hexdigest()


def _file_policy(path: Path, *, mode: int = 0o444) -> dict[str, object]:
    return {
        "path": str(path),
        "sha256": _digest(str(path)),
        "owner_uid": 0,
        "mode": mode,
    }


def _profile_payload(
    *,
    inbox: Path,
    quarantine: Path,
    generations: Path,
) -> dict[str, object]:
    file_paths = (
        authority_module.PRODUCTION_SYSTEM_PYTHON,
        Path("/lib64/ld-linux-x86-64.so.2"),
        Path("/usr/lib/python3.11/os.py"),
        Path("/usr/lib64/libpython3.11.so.1.0"),
        authority_module.PRODUCTION_DEPLOY_PYZ,
        authority_module.PRODUCTION_RUNTIME_PYZ,
    )
    ancestors = sorted(
        {parent for file_path in file_paths for parent in file_path.parents},
        key=str,
    )
    body: dict[str, object] = {
        "schema_version": 1,
        "platform": "linux",
        "ancestors": [{"path": str(path), "owner_uid": 0, "mode": 0o755} for path in ancestors],
        "system_python": _file_policy(
            authority_module.PRODUCTION_SYSTEM_PYTHON,
            mode=0o555,
        ),
        "elf_loader": _file_policy(Path("/lib64/ld-linux-x86-64.so.2"), mode=0o555),
        "stdlib": [_file_policy(Path("/usr/lib/python3.11/os.py"))],
        "shared_libraries": [_file_policy(Path("/usr/lib64/libpython3.11.so.1.0"))],
        "deploy_pyz": _file_policy(authority_module.PRODUCTION_DEPLOY_PYZ, mode=0o555),
        "runtime_pyz": _file_policy(authority_module.PRODUCTION_RUNTIME_PYZ, mode=0o555),
        "inbox_root": str(inbox),
        "quarantine_root": str(quarantine),
        "generation_root": str(generations),
        "allowed_operations": ["publish", "rollback"],
        "roles": {
            "daily": {
                "module": "rquant.runtime_service_main",
                "environment_allowlist": ["LANG", "LC_ALL", "TZ"],
            }
        },
        "manifest_schema": {
            key: list(value) if isinstance(value, tuple) else value
            for key, value in authority_module.PRODUCTION_MANIFEST_SCHEMA.items()
        },
    }
    return {
        "profile_id": hashlib.sha256(canonical_json_bytes(body)).hexdigest(),
        **body,
    }


def _directory_policy(*roots: Path) -> MappingProxyType[Path, tuple[int, int]]:
    policy: dict[Path, tuple[int, int]] = {}
    for root in roots:
        current = Path("/")
        for component in root.parts[1:]:
            observed = os.stat(current, follow_symlinks=False)
            policy[current] = (observed.st_uid, stat.S_IMODE(observed.st_mode))
            current /= component
        observed = os.stat(current, follow_symlinks=False)
        policy[current] = (observed.st_uid, stat.S_IMODE(observed.st_mode))
    return MappingProxyType(policy)


def _set_tree_modes(root: Path) -> None:
    directories: list[Path] = []
    for directory, names, files in os.walk(root):
        current = Path(directory)
        directories.append(current)
        for name in files:
            target = current / name
            target.chmod(0o555 if target.name == "python" else 0o444)
        for name in names:
            (current / name).chmod(0o755)
    for directory in reversed(directories):
        directory.chmod(0o555)


def _make_candidate(
    inbox: Path,
    *,
    candidate_id: str,
    candidate_basename: str,
    extra_files: dict[str, bytes] | None = None,
) -> Path:
    candidate = inbox / candidate_id / candidate_basename
    candidate_parent = candidate.parent
    if candidate_parent.exists():
        candidate_parent.chmod(0o755)
    if candidate.exists() and not candidate.is_symlink():
        for directory, _names, files in os.walk(candidate):
            current = Path(directory)
            current.chmod(0o755)
            for name in files:
                (current / name).chmod(0o644)
        shutil.rmtree(candidate)
    elif candidate.is_symlink():
        candidate.unlink()
    files = {
        "pyvenv.cfg": b"include-system-site-packages = false\n",
        "release/src/rquant/__init__.py": b"",
        "release/src/rquant/runtime_service_main.py": b"def main():\n    return 0\n",
        "venv/bin/python": b"python executable bytes\n",
    }
    files.update(extra_files or {})
    for relative, payload in files.items():
        target = candidate / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
    (candidate / "venv/lib/python3.11/site-packages").mkdir(parents=True)
    _set_tree_modes(candidate)
    candidate.parent.chmod(0o555)
    return candidate


def _request(
    *,
    operation_id: str = "1" * 32,
    candidate_id: str = "a" * 64,
    candidate_basename: str = "candidate-v1",
    commit_claim: str = "untrusted-commit",
    manifest_claim: str = "b" * 64,
) -> RuntimeQuarantineRequest:
    return RuntimeQuarantineRequest(
        schema_version=1,
        operation_id=operation_id,
        candidate_id=candidate_id,
        candidate_basename=candidate_basename,
        untrusted_commit=commit_claim,
        untrusted_manifest_hash=manifest_claim,
    )


@pytest.fixture
def quarantine_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, Any]:
    authority = tmp_path / "runtime-authority"
    inbox = authority / "inbox"
    quarantine = authority / "quarantine"
    generations = authority / "generations"
    for path in (inbox, quarantine, generations):
        path.mkdir(parents=True, mode=0o755)
        path.chmod(0o755)
    monkeypatch.setattr(authority_module, "PRODUCTION_INBOX_ROOT", inbox)
    monkeypatch.setattr(authority_module, "PRODUCTION_QUARANTINE_ROOT", quarantine)
    monkeypatch.setattr(authority_module, "PRODUCTION_GENERATION_ROOT", generations)
    monkeypatch.setattr(authority_module, "RUNTIME_AUTHORITY_OWNER_UID", os.getuid())
    if sys.platform == "darwin":
        monkeypatch.setattr(authority_module, "GENERATION_DIRECTORY_MODE", 0o755)
    profile = parse_runtime_closure_profile(
        canonical_json_bytes(
            _profile_payload(inbox=inbox, quarantine=quarantine, generations=generations)
        )
    )
    monkeypatch.setattr(quarantine_module, "load_production_runtime_profile", lambda: profile)
    monkeypatch.setattr(
        quarantine_module,
        "_ANCHOR_DIRECTORY_POLICY",
        _directory_policy(inbox, quarantine, generations),
    )
    monkeypatch.setattr(authority_module, "RUNTIME_AUTHORITY_ANCHOR", authority)
    monkeypatch.setattr(authority_module, "RUNTIME_AUTHORITY_PATH", authority / "current.json")
    monkeypatch.setattr(
        authority_module,
        "RUNTIME_AUTHORITY_LOCK_PATH",
        authority / "deployment.lock",
    )
    monkeypatch.setattr(
        authority_module,
        "_PRODUCTION_RUNTIME_DIRECTORY_POLICY",
        _directory_policy(authority, generations),
    )
    monkeypatch.setattr(authority_module, "load_production_runtime_profile", lambda: profile)
    return {
        "inbox": inbox,
        "quarantine": quarantine,
        "generations": generations,
        "profile": profile,
    }


def test_request_parser_is_strict_bounded_and_has_no_capability_fields() -> None:
    assert tuple(inspect.signature(publish_runtime_candidate).parameters) == ("request",)
    payload = {
        "schema_version": 1,
        "operation_id": "1" * 32,
        "candidate_id": "a" * 64,
        "candidate_basename": "candidate-v1",
        "untrusted_commit": "audit-only",
        "untrusted_manifest_hash": "b" * 64,
    }
    parsed = parse_runtime_quarantine_request(canonical_json_bytes(payload))
    assert parsed == _request(commit_claim="audit-only")

    for forbidden in ("path", "command", "module", "environment", "fd", "shell"):
        with pytest.raises(RuntimeQuarantineError, match="fields"):
            parse_runtime_quarantine_request(
                canonical_json_bytes({**payload, forbidden: "candidate-controlled"})
            )
    with pytest.raises(RuntimeQuarantineError, match="duplicate"):
        parse_runtime_quarantine_request(
            b'{"schema_version":1,"operation_id":"11111111111111111111111111111111",'
            b'"operation_id":"22222222222222222222222222222222",'
            b'"candidate_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
            b'"candidate_basename":"candidate-v1","untrusted_commit":"audit-only",'
            b'"untrusted_manifest_hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}'
        )
    with pytest.raises(RuntimeQuarantineError, match="large"):
        parse_runtime_quarantine_request(b" " * (quarantine_module.MAX_REQUEST_BYTES + 1))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("operation_id", "../operation"),
        ("candidate_id", "../candidate"),
        ("candidate_basename", "../candidate"),
        ("candidate_basename", "."),
        ("candidate_basename", ""),
        ("candidate_basename", "candidate/name"),
        ("commit_claim", "bad\x00claim"),
        ("manifest_claim", "not-a-hash"),
    ],
)
def test_request_rejects_unsafe_or_unbounded_components(field: str, value: str) -> None:
    values = {
        "operation_id": "1" * 32,
        "candidate_id": "a" * 64,
        "candidate_basename": "candidate-v1",
        "commit_claim": "audit-only",
        "manifest_claim": "b" * 64,
    }
    values[field] = value
    with pytest.raises(RuntimeQuarantineError):
        _request(**values)


def test_publish_copies_closes_rehashes_and_returns_public_slot(
    quarantine_fixture: dict[str, Any],
) -> None:
    candidate = _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
    )
    result = publish_runtime_candidate(_request())

    assert result.status is RuntimeQuarantineStatus.PUBLISHED
    assert isinstance(result.slot, RuntimeGenerationSlot)
    assert result.slot.lifecycle is RuntimeGenerationLifecycle.ACTIVE
    assert result.slot.generation_id == result.slot.full_manifest_hash
    assert result.slot.generation_path == quarantine_fixture["generations"] / result.generation_id
    assert result.slot.commit == "untrusted-commit"
    assert result.untrusted_candidate_id == "a" * 64
    assert result.untrusted_manifest_matches is False
    assert candidate.exists()
    assert not (quarantine_fixture["quarantine"] / f".quarantine-{'1' * 32}").exists()

    manifest_path = result.slot.generation_path / authority_module.GENERATION_MANIFEST_NAME
    manifest = manifest_path.read_bytes()
    assert hashlib.sha256(manifest).hexdigest() == result.generation_id
    decoded = strict_json_loads(manifest)
    assert decoded["profile_id"] == quarantine_fixture["profile"].profile_id
    assert decoded["roles"]["daily"]["module"] == "rquant.runtime_service_main"
    assert all(entry["owner_uid"] == os.getuid() for entry in decoded["entries"])
    assert (
        stat.S_IMODE(result.slot.generation_path.stat().st_mode)
        == authority_module.GENERATION_DIRECTORY_MODE
    )


@pytest.mark.skipif(
    sys.platform != "linux" or os.geteuid() != 0,
    reason="requires Linux root to prove exact root-owned 0555 directory publication",
)
def test_linux_root_exact_generation_mode_gate() -> None:
    assert authority_module.GENERATION_DIRECTORY_MODE == 0o555


def test_untrusted_audit_claims_do_not_control_identity_behavior_or_permissions(
    quarantine_fixture: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = quarantine_fixture["inbox"].parent / "executed"
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
        extra_files={
            "release/src/rquant/hostile.py": (
                f"from pathlib import Path\nPath({str(marker)!r}).write_text('executed')\n"
            ).encode(),
        },
    )
    candidate_hash_claim = hashlib.sha256(b"candidate-controlled manifest").hexdigest()
    first = publish_runtime_candidate(
        _request(commit_claim="self-consistent-malicious", manifest_claim=candidate_hash_claim)
    )
    assert not marker.exists()
    assert first.generation_id != candidate_hash_claim
    assert first.slot.commit == "self-consistent-malicious"
    assert first.untrusted_manifest_matches is False
    assert all(
        stat.S_IMODE(path.stat().st_mode) in {0o444, 0o555}
        for path in first.slot.generation_path.rglob("*")
    )

    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
        extra_files={
            "release/src/rquant/hostile.py": (
                f"from pathlib import Path\nPath({str(marker)!r}).write_text('executed')\n"
            ).encode(),
        },
    )
    second = publish_runtime_candidate(
        _request(
            operation_id="2" * 32,
            commit_claim="different-audit-claim",
            manifest_claim=first.generation_id,
        )
    )
    assert second.status is RuntimeQuarantineStatus.IDEMPOTENT
    assert second.generation_id == first.generation_id
    assert second.untrusted_manifest_matches is True
    assert not marker.exists()


@pytest.mark.parametrize("mode", (0o644, 0o4755, 0o2755, 0o1755))
def test_publish_rejects_unsupported_or_privileged_candidate_modes(
    quarantine_fixture: dict[str, Any],
    mode: int,
) -> None:
    candidate = _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
    )
    candidate.chmod(0o755)
    (candidate / "venv/bin/python").chmod(mode)
    candidate.chmod(0o555)
    with pytest.raises(RuntimeQuarantineError, match="unsafe"):
        publish_runtime_candidate(_request())


@pytest.mark.parametrize("mutation", ("mode", "symlink"))
def test_publish_rejects_fixed_anchor_mutation(
    quarantine_fixture: dict[str, Any],
    mutation: str,
) -> None:
    inbox = quarantine_fixture["inbox"]
    _make_candidate(
        inbox,
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
    )
    if mutation == "mode":
        inbox.chmod(0o775)
    else:
        real_inbox = inbox.with_name("real-inbox")
        inbox.rename(real_inbox)
        inbox.symlink_to(real_inbox, target_is_directory=True)
    with pytest.raises(RuntimeQuarantineError, match="ancestor"):
        publish_runtime_candidate(_request())


@pytest.mark.parametrize("stage", ("revalidation", "rename"))
def test_quarantine_replacement_after_attestation_is_rejected_before_rename(
    quarantine_fixture: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
    )
    mutated = False

    def mutate(selected: str) -> None:
        nonlocal mutated
        if selected != stage or mutated:
            return
        mutated = True
        temporary = quarantine_fixture["quarantine"] / f".quarantine-{'1' * 32}"
        target = temporary / "pyvenv.cfg"
        target.chmod(0o644)
        target.write_bytes(b"include-system-site-packages = true\n")
        target.chmod(0o444)

    monkeypatch.setattr(quarantine_module, "_FAILPOINT", mutate)
    with pytest.raises(RuntimeQuarantineError, match="manifest|changed"):
        publish_runtime_candidate(_request())
    assert not any(quarantine_fixture["generations"].iterdir())


def test_runtime_authority_independently_revalidates_published_generation(
    quarantine_fixture: dict[str, Any],
) -> None:
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
    )
    result = publish_runtime_candidate(_request())
    record = prepare_runtime_authority_publish(
        None,
        result.slot,
        operation_id="9" * 32,
    )
    assert publish_runtime_authority(record).value == "committed"

    target = result.slot.generation_path / "pyvenv.cfg"
    target.chmod(0o644)
    target.write_bytes(b"include-system-site-packages = true\n")
    target.chmod(0o444)
    with pytest.raises(RuntimeAuthorityPublishError):
        publish_runtime_authority(record)


@pytest.mark.parametrize(
    "kind",
    ("candidate-symlink", "file-symlink", "hardlink", "fifo", "socket"),
)
def test_publish_rejects_hostile_filesystem_entries_without_side_effects(
    quarantine_fixture: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    candidate = _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
    )
    candidate.chmod(0o755)
    hostile = candidate / "hostile"
    if kind == "candidate-symlink":
        candidate.parent.chmod(0o755)
        candidate.rename(candidate.with_name("real-candidate"))
        candidate.symlink_to(candidate.with_name("real-candidate"), target_is_directory=True)
    elif kind == "file-symlink":
        hostile.symlink_to("pyvenv.cfg")
    elif kind == "hardlink":
        os.link(candidate / "pyvenv.cfg", hostile)
    elif kind == "fifo":
        os.mkfifo(hostile)
    else:
        listener = socket.socket(socket.AF_UNIX)
        monkeypatch.chdir(candidate)
        listener.bind("hostile")
        listener.close()
    if kind != "candidate-symlink":
        _set_tree_modes(candidate)

    with pytest.raises(RuntimeQuarantineError):
        publish_runtime_candidate(_request())
    assert not any(quarantine_fixture["generations"].iterdir())


def test_publish_rejects_normalized_duplicate_paths_and_reserved_manifest(
    quarantine_fixture: dict[str, Any],
) -> None:
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
        extra_files={"safe.py": b"safe", "ｓａｆｅ．ｐｙ": b"alias"},
    )
    with pytest.raises(RuntimeQuarantineError, match="duplicate"):
        publish_runtime_candidate(_request())
    assert not any(quarantine_fixture["generations"].iterdir())

    assert cleanup_runtime_quarantine("1" * 32) is True
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
        extra_files={authority_module.GENERATION_MANIFEST_NAME: b"candidate manifest"},
    )
    with pytest.raises(RuntimeQuarantineError, match="reserved"):
        publish_runtime_candidate(_request(operation_id="2" * 32))


@pytest.mark.parametrize(
    ("limit", "value", "extra_files", "match"),
    [
        ("max_entries", 8, {"extra.py": b"x"}, "entry"),
        ("max_total_bytes", 40, {"extra.py": b"x" * 64}, "total"),
        ("max_file_bytes", 8, {"extra.py": b"x" * 9}, "file"),
        ("max_depth", 3, {"a/b/c/deep.py": b"x"}, "depth"),
    ],
)
def test_publish_enforces_profile_manifest_budgets_while_streaming(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    limit: str,
    value: int,
    extra_files: dict[str, bytes],
    match: str,
) -> None:
    schema = dict(authority_module.PRODUCTION_MANIFEST_SCHEMA)
    schema[limit] = value
    monkeypatch.setattr(authority_module, "PRODUCTION_MANIFEST_SCHEMA", MappingProxyType(schema))
    fixture = quarantine_fixture.__wrapped__(tmp_path, monkeypatch)
    _make_candidate(
        fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
        extra_files=extra_files,
    )
    with pytest.raises(RuntimeQuarantineError, match=match):
        publish_runtime_candidate(_request())


def test_copy_is_chunked_and_source_identity_mutation_fails_closed(
    quarantine_fixture: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
        extra_files={"large.bin": b"x" * (quarantine_module.COPY_CHUNK_BYTES * 3)},
    )
    observed_reads: list[int] = []
    real_read = os.read

    def bounded_read(descriptor: int, size: int) -> bytes:
        observed_reads.append(size)
        return real_read(descriptor, size)

    mutated = False

    def mutate(stage: str) -> None:
        nonlocal mutated
        if stage != "copy" or mutated:
            return
        mutated = True
        candidate.chmod(0o755)
        original = candidate / "large.bin"
        original.rename(candidate / "large.original")
        original.write_bytes(b"replacement")
        original.chmod(0o444)
        candidate.chmod(0o555)

    monkeypatch.setattr(quarantine_module.os, "read", bounded_read)
    monkeypatch.setattr(quarantine_module, "_FAILPOINT", mutate)
    before = len(os.listdir("/dev/fd"))
    with pytest.raises(RuntimeQuarantineError, match="identity"):
        publish_runtime_candidate(_request())
    after = len(os.listdir("/dev/fd"))
    assert observed_reads
    assert max(observed_reads) <= quarantine_module.COPY_CHUNK_BYTES
    assert after == before


@pytest.mark.parametrize(
    "stage",
    (
        "creation",
        "traversal",
        "copy",
        "file_fsync",
        "directory_fsync",
        "close_reopen",
        "revalidation",
        "rename",
    ),
)
def test_crash_stages_leave_only_exact_operation_quarantine_for_safe_cleanup(
    quarantine_fixture: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
    )
    crashed = False

    def crash(selected: str) -> None:
        nonlocal crashed
        if selected == stage and not crashed:
            crashed = True
            raise SimulatedCrash(stage)

    monkeypatch.setattr(quarantine_module, "_FAILPOINT", crash)
    before = len(os.listdir("/dev/fd"))
    with pytest.raises(SimulatedCrash, match=stage):
        publish_runtime_candidate(_request())
    assert len(os.listdir("/dev/fd")) == before
    monkeypatch.setattr(quarantine_module, "_FAILPOINT", lambda _stage: None)
    assert cleanup_runtime_quarantine("1" * 32) is True
    assert cleanup_runtime_quarantine("1" * 32) is False
    assert not any(quarantine_fixture["generations"].iterdir())


def test_store_parent_fsync_crash_recovers_as_exact_idempotent_generation(
    quarantine_fixture: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
    )
    crashed = False

    def crash(stage: str) -> None:
        nonlocal crashed
        if stage == "store_parent_fsync" and not crashed:
            crashed = True
            raise SimulatedCrash(stage)

    monkeypatch.setattr(quarantine_module, "_FAILPOINT", crash)
    with pytest.raises(SimulatedCrash, match="store_parent_fsync"):
        publish_runtime_candidate(_request())
    published = tuple(quarantine_fixture["generations"].iterdir())
    assert len(published) == 1
    assert cleanup_runtime_quarantine("1" * 32) is False

    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
    )
    monkeypatch.setattr(quarantine_module, "_FAILPOINT", lambda _stage: None)
    result = publish_runtime_candidate(_request())
    assert result.status is RuntimeQuarantineStatus.IDEMPOTENT
    assert result.slot.generation_path == published[0]


def test_parent_fsync_one_failure_revalidates_and_reports_recovery(
    quarantine_fixture: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
    )
    real_fsync = quarantine_module.os.fsync
    failures = 0

    def fail_once(descriptor: int) -> None:
        nonlocal failures
        if quarantine_module._FSYNC_PHASE == "store_parent" and failures == 0:
            failures += 1
            raise OSError("injected parent fsync failure")
        real_fsync(descriptor)

    monkeypatch.setattr(quarantine_module.os, "fsync", fail_once)
    result = publish_runtime_candidate(_request())
    assert result.status is RuntimeQuarantineStatus.PUBLISHED_AFTER_RECOVERY
    assert failures == 1


def test_parent_fsync_persistent_failure_is_typed_and_retry_converges(
    quarantine_fixture: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
    )
    real_fsync = quarantine_module.os.fsync
    blocked = True

    def fail_parent(descriptor: int) -> None:
        if blocked and quarantine_module._FSYNC_PHASE == "store_parent":
            raise OSError("persistent parent fsync failure")
        real_fsync(descriptor)

    monkeypatch.setattr(quarantine_module.os, "fsync", fail_parent)
    with pytest.raises(RuntimeQuarantineDurabilityError):
        publish_runtime_candidate(_request())
    assert len(tuple(quarantine_fixture["generations"].iterdir())) == 1

    blocked = False
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
    )
    result = publish_runtime_candidate(_request())
    assert result.status is RuntimeQuarantineStatus.IDEMPOTENT


def test_cleanup_rejects_replaced_or_unsafe_operation_tree_and_never_generations(
    quarantine_fixture: dict[str, Any],
) -> None:
    operation_id = "1" * 32
    temporary = quarantine_fixture["quarantine"] / f".quarantine-{operation_id}"
    temporary.mkdir(mode=0o700)
    outside = quarantine_fixture["quarantine"].parent / "outside"
    outside.mkdir()
    temporary.rmdir()
    temporary.symlink_to(outside, target_is_directory=True)
    with pytest.raises(RuntimeQuarantineError):
        cleanup_runtime_quarantine(operation_id)

    generation = quarantine_fixture["generations"] / ("f" * 64)
    generation.mkdir(mode=0o555)
    assert generation.exists()
    assert cleanup_runtime_quarantine("2" * 32) is False
    assert generation.exists()


def test_existing_generation_conflict_is_rejected_without_overwrite(
    quarantine_fixture: dict[str, Any],
) -> None:
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
    )
    first = publish_runtime_candidate(_request())
    manifest = first.slot.generation_path / authority_module.GENERATION_MANIFEST_NAME
    manifest.chmod(0o644)
    manifest.write_bytes(b"tampered")
    manifest.chmod(0o444)
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
    )
    with pytest.raises(RuntimeQuarantineError, match="existing generation"):
        publish_runtime_candidate(_request(operation_id="2" * 32))
    assert manifest.read_bytes() == b"tampered"


def test_result_is_frozen_and_exposes_no_raw_descriptor_or_private_evidence(
    quarantine_fixture: dict[str, Any],
) -> None:
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
    )
    result = publish_runtime_candidate(_request())
    with pytest.raises((AttributeError, TypeError)):
        result.status = RuntimeQuarantineStatus.IDEMPOTENT  # type: ignore[misc]
    assert not any("fd" in name or "evidence" in name for name in result.__dataclass_fields__)
    assert replace(result.slot, commit="audit-only").generation_id == result.generation_id
