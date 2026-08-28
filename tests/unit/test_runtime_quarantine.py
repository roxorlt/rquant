from __future__ import annotations

import fcntl
import hashlib
import inspect
import multiprocessing
import os
import shutil
import socket
import stat
import time
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
            entry.name: {
                "module": entry.module,
                "environment_allowlist": list(entry.environment_allowlist),
                "instances": (
                    [f"svc-{hashlib.sha256(entry.name.encode()).hexdigest()}"]
                    if entry.instanced
                    else []
                ),
                "service_kind": entry.service_kind,
                "control_root": entry.control_root,
                "once": entry.once,
                "module_arguments": list(entry.module_arguments),
            }
            for entry in authority_module.PRODUCTION_ROLE_POLICY
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
        # Publication requires every allowlisted module to have a unique regular source in
        # the tree, and the policy maps its roles onto several modules. Deriving the sources
        # from the policy keeps a new role one line of data there and nothing here.
        **{
            f"release/src/{entry.module.replace('.', '/')}.py": b"def main():\n    return 0\n"
            for entry in authority_module.PRODUCTION_ROLE_POLICY
        },
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


def _fork_context() -> multiprocessing.context.ForkContext:
    if "fork" not in multiprocessing.get_all_start_methods():
        pytest.skip("requires fork to inherit the isolated filesystem policy fixture")
    return multiprocessing.get_context("fork")


def _publish_worker(request: RuntimeQuarantineRequest, result_queue: Any) -> None:
    try:
        result = publish_runtime_candidate(request)
    except BaseException as exc:
        result_queue.put(("error", type(exc).__name__, str(exc)))
    else:
        result_queue.put(("ok", result.status.value, result.generation_id))


def _cleanup_worker(operation_id: str, result_queue: Any) -> None:
    try:
        result_queue.put(("ok", cleanup_runtime_quarantine(operation_id)))
    except BaseException as exc:
        result_queue.put(("error", type(exc).__name__, str(exc)))


def _umask_publish_worker(
    request: RuntimeQuarantineRequest,
    mask: int,
    crash_stage: str | None,
    result_queue: Any,
) -> None:
    if crash_stage is not None:

        def crash(stage: str) -> None:
            if stage == crash_stage:
                raise SimulatedCrash(stage)

        quarantine_module._FAILPOINT = crash
    previous = os.umask(mask)
    try:
        _publish_worker(request, result_queue)
    finally:
        os.umask(previous)


def _join_processes(processes: tuple[multiprocessing.Process, ...]) -> None:
    for process in processes:
        process.join(5)
    for process in processes:
        if process.is_alive():
            process.terminate()
            process.join(2)
    assert all(not process.is_alive() for process in processes)


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
    if os.getuid() != 0:
        # Publication renames the sealed quarantine directory across parents,
        # and an unprivileged process cannot move a directory it has no write
        # bit on: the kernel has to rewrite the moved directory's "..", so both
        # Linux and macOS answer EACCES for the production 0555 mode. Root
        # (which is what publishes in production) bypasses that check. Give the
        # unprivileged test the same transaction with a mode it can move; the
        # published mode is still asserted against this value.
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


@pytest.mark.skip(
    reason=(
        "UNFULFILLED CLOUD GATE: authorized Linux-root harness must publish as UID 0, "
        "verify exact 0555/single-link metadata, then prove lighthouse cannot write, replace, "
        "or delete; production installation is separately authorized"
    )
)
def test_cloud_gate_linux_root_generation_publication_and_lighthouse_denial_unfulfilled() -> None:
    pytest.fail("cloud integration contract must never be represented by a constant assertion")


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


def test_quarantine_mutation_during_first_full_revalidation_is_rejected(
    quarantine_fixture: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
    )
    mutated = False

    def mutate(selected: str) -> None:
        nonlocal mutated
        if selected != "first_full_revalidation" or mutated:
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


def test_final_identity_check_rejects_quarantine_path_replacement(
    quarantine_fixture: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
    )
    temporary = quarantine_fixture["quarantine"] / f".quarantine-{'1' * 32}"
    displaced = quarantine_fixture["quarantine"] / "displaced-for-identity-check"
    real_assert_named_identity = quarantine_module._assert_named_identity
    mutated = False

    def replace_before_identity_check(
        parent_fd: int,
        name: str,
        descriptor: int,
        label: str,
    ) -> None:
        nonlocal mutated
        if label == "final operation quarantine" and not mutated:
            mutated = True
            temporary.rename(displaced)
            temporary.mkdir(mode=authority_module.GENERATION_DIRECTORY_MODE)
            temporary.chmod(authority_module.GENERATION_DIRECTORY_MODE)
        real_assert_named_identity(parent_fd, name, descriptor, label)

    monkeypatch.setattr(
        quarantine_module,
        "_assert_named_identity",
        replace_before_identity_check,
    )
    with pytest.raises(RuntimeQuarantineError, match="identity"):
        publish_runtime_candidate(_request())
    assert not any(quarantine_fixture["generations"].iterdir())
    assert cleanup_runtime_quarantine("1" * 32) is True
    displaced.rename(temporary)
    assert cleanup_runtime_quarantine("1" * 32) is True


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
        if stage != "candidate_copy:large.bin" or mutated:
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
    ("stage", "published_before_crash", "needs_parent_recovery"),
    (
        ("candidate_traversal:pyvenv.cfg", False, False),
        ("candidate_copy:pyvenv.cfg", False, False),
        ("content_file_fsync:pyvenv.cfg", False, False),
        ("manifest_write", False, False),
        ("manifest_fsync", False, False),
        ("completed_quarantine_directory_fsync", False, False),
        ("close_reopen", False, False),
        ("first_full_revalidation", False, False),
        ("final_identity_to_rename", False, False),
        ("atomic_rename", False, False),
        ("post_rename_pre_parent_fsync", True, False),
        ("generation_parent_fsync_recovery", True, True),
        ("generation_durable_before_authority", True, False),
    ),
)
def test_transition_exact_crash_matrix_preserves_authority_and_converges(
    quarantine_fixture: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
    published_before_crash: bool,
    needs_parent_recovery: bool,
) -> None:
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
    )
    active = publish_runtime_candidate(_request(operation_id="a" * 32))
    record = prepare_runtime_authority_publish(
        None,
        active.slot,
        operation_id="9" * 32,
    )
    assert publish_runtime_authority(record).value == "committed"
    authority_path = Path(authority_module.RUNTIME_AUTHORITY_PATH)
    authority_before = authority_path.read_bytes()
    baseline_generations = {path.name for path in quarantine_fixture["generations"].iterdir()}
    unrelated_operation_id = "2" * 32
    unrelated = quarantine_fixture["quarantine"] / f".quarantine-{unrelated_operation_id}"
    unrelated.mkdir(mode=0o700)
    unrelated_identity = (unrelated.stat().st_dev, unrelated.stat().st_ino)

    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
        extra_files={"release/src/rquant/review_fix.py": b"REVIEW_FIX = True\n"},
    )
    crashed = False

    def crash(selected: str) -> None:
        nonlocal crashed
        if selected == stage and not crashed:
            crashed = True
            raise SimulatedCrash(stage)

    monkeypatch.setattr(quarantine_module, "_FAILPOINT", crash)
    real_fsync = quarantine_module.os.fsync
    parent_failed = False

    def fail_first_generation_parent_fsync(descriptor: int) -> None:
        nonlocal parent_failed
        if (
            needs_parent_recovery
            and quarantine_module._FSYNC_PHASE == "store_parent"
            and not parent_failed
        ):
            parent_failed = True
            raise OSError("injected first generation parent fsync failure")
        real_fsync(descriptor)

    monkeypatch.setattr(quarantine_module.os, "fsync", fail_first_generation_parent_fsync)
    before = len(os.listdir("/dev/fd"))
    with pytest.raises(SimulatedCrash, match=stage):
        publish_runtime_candidate(_request())
    assert len(os.listdir("/dev/fd")) == before
    assert authority_path.read_bytes() == authority_before
    residue = quarantine_fixture["quarantine"] / f".quarantine-{'1' * 32}"
    generations_after_crash = {path.name for path in quarantine_fixture["generations"].iterdir()}
    assert residue.exists() is not published_before_crash
    assert (generations_after_crash != baseline_generations) is published_before_crash
    expected_residue = {unrelated.name}
    if not published_before_crash:
        expected_residue.add(residue.name)
    assert {path.name for path in quarantine_fixture["quarantine"].iterdir()} == expected_residue
    assert (unrelated.stat().st_dev, unrelated.stat().st_ino) == unrelated_identity

    monkeypatch.setattr(quarantine_module, "_FAILPOINT", lambda _stage: None)
    assert cleanup_runtime_quarantine("1" * 32) is (not published_before_crash)
    assert cleanup_runtime_quarantine("1" * 32) is False
    assert (unrelated.stat().st_dev, unrelated.stat().st_ino) == unrelated_identity
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
        extra_files={"release/src/rquant/review_fix.py": b"REVIEW_FIX = True\n"},
    )
    result = publish_runtime_candidate(_request())
    expected_status = (
        RuntimeQuarantineStatus.IDEMPOTENT
        if published_before_crash
        else RuntimeQuarantineStatus.PUBLISHED
    )
    assert result.status is expected_status
    assert authority_path.read_bytes() == authority_before
    assert (unrelated.stat().st_dev, unrelated.stat().st_ino) == unrelated_identity
    assert cleanup_runtime_quarantine(unrelated_operation_id) is True
    assert cleanup_runtime_quarantine(unrelated_operation_id) is False


@pytest.mark.parametrize(
    "stage",
    (
        "completed_quarantine_directory_fsync",
        "first_full_revalidation",
        "final_identity_to_rename",
    ),
)
def test_transition_hook_runs_after_its_named_operation(
    quarantine_fixture: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
    )
    state: dict[str, Any] = {
        "completed_directory_fsynced": False,
        "operation_quarantine_fd": -1,
        "first_revalidation_fd": -1,
        "final_verified": False,
        "final_identity_checked": False,
        "hook_calls": 0,
    }
    real_fsync_descriptor = quarantine_module._fsync_descriptor
    real_open_owned_directory = quarantine_module._open_owned_directory_at
    real_verify_closed_tree = quarantine_module._verify_closed_tree
    real_assert_named_identity = quarantine_module._assert_named_identity

    def observe_fsync(descriptor: int, phase: str) -> None:
        real_fsync_descriptor(descriptor, phase)
        if phase == "directory" and descriptor == state["operation_quarantine_fd"]:
            state["completed_directory_fsynced"] = True

    def observe_open_owned_directory(
        parent_fd: int,
        name: str,
        *,
        allowed_modes: set[int],
        label: str,
    ) -> int:
        descriptor = real_open_owned_directory(
            parent_fd,
            name,
            allowed_modes=allowed_modes,
            label=label,
        )
        if label == "operation quarantine":
            state["operation_quarantine_fd"] = descriptor
        elif label == "closed operation quarantine":
            state["first_revalidation_fd"] = descriptor
        return descriptor

    def observe_verify_closed_tree(
        tree_fd: int,
        profile: authority_module.RuntimeClosureProfile,
        *,
        label: str,
    ) -> bytes:
        result = real_verify_closed_tree(tree_fd, profile, label=label)
        if label == "final operation quarantine":
            state["final_verified"] = True
        return result

    def observe_assert_named_identity(
        parent_fd: int,
        name: str,
        descriptor: int,
        label: str,
    ) -> None:
        real_assert_named_identity(parent_fd, name, descriptor, label)
        if label == "final operation quarantine":
            state["final_identity_checked"] = True

    def inspect_transition(selected: str) -> None:
        if selected != stage:
            return
        state["hook_calls"] += 1
        if stage == "completed_quarantine_directory_fsync":
            assert state["completed_directory_fsynced"] is True
        elif stage == "first_full_revalidation":
            descriptor = state["first_revalidation_fd"]
            assert descriptor >= 0
            os.fstat(descriptor)
        else:
            assert state["final_verified"] is True
            assert state["final_identity_checked"] is True
        raise SimulatedCrash(stage)

    monkeypatch.setattr(quarantine_module, "_fsync_descriptor", observe_fsync)
    monkeypatch.setattr(
        quarantine_module,
        "_open_owned_directory_at",
        observe_open_owned_directory,
    )
    monkeypatch.setattr(quarantine_module, "_verify_closed_tree", observe_verify_closed_tree)
    monkeypatch.setattr(
        quarantine_module,
        "_assert_named_identity",
        observe_assert_named_identity,
    )
    monkeypatch.setattr(quarantine_module, "_FAILPOINT", inspect_transition)
    before = len(os.listdir("/dev/fd"))
    with pytest.raises(SimulatedCrash, match=stage):
        publish_runtime_candidate(_request())
    assert state["hook_calls"] == 1
    assert len(os.listdir("/dev/fd")) == before
    descriptor = state["first_revalidation_fd"]
    if descriptor >= 0:
        with pytest.raises(OSError):
            os.fstat(descriptor)
    monkeypatch.setattr(quarantine_module, "_FAILPOINT", lambda _stage: None)
    assert cleanup_runtime_quarantine("1" * 32) is True
    assert cleanup_runtime_quarantine("1" * 32) is False


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


def test_two_publishers_hold_one_lock_before_copy_without_concurrent_amplification(
    quarantine_fixture: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _fork_context()
    assert quarantine_fixture["profile"].manifest_schema.max_total_bytes == 4_294_967_296
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
        extra_files={"large.bin": b"x" * (quarantine_module.COPY_CHUNK_BYTES * 3)},
    )
    active = ctx.Value("i", 0)
    maximum = ctx.Value("i", 0)
    entered = ctx.Event()
    release = ctx.Event()
    seen: set[int] = set()

    def pause_first_copy(stage: str) -> None:
        if stage != "candidate_copy:large.bin" or os.getpid() in seen:
            return
        seen.add(os.getpid())
        with active.get_lock(), maximum.get_lock():
            active.value += 1
            maximum.value = max(maximum.value, active.value)
        entered.set()
        if not release.wait(5):
            raise RuntimeError("copy barrier timed out")
        with active.get_lock():
            active.value -= 1

    monkeypatch.setattr(quarantine_module, "_FAILPOINT", pause_first_copy)
    results = ctx.Queue()
    first = ctx.Process(target=_publish_worker, args=(_request(), results))
    second = ctx.Process(
        target=_publish_worker,
        args=(_request(operation_id="2" * 32), results),
    )
    first.start()
    assert entered.wait(3)
    second.start()
    try:
        time.sleep(0.25)
        assert active.value == 1
        assert maximum.value == 1
        assert second.is_alive()
    finally:
        release.set()
        _join_processes((first, second))

    outcomes = sorted(results.get(timeout=1) for _ in range(2))
    assert all(outcome[0] == "ok" for outcome in outcomes)
    assert {outcome[1] for outcome in outcomes} == {"published", "idempotent"}


def test_publish_and_cleanup_share_the_deployment_lock(
    quarantine_fixture: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _fork_context()
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
        extra_files={"large.bin": b"x" * (quarantine_module.COPY_CHUNK_BYTES * 2)},
    )
    cleanup_id = "2" * 32
    residue = quarantine_fixture["quarantine"] / f".quarantine-{cleanup_id}"
    residue.mkdir(mode=0o700)
    entered = ctx.Event()
    release = ctx.Event()
    seen = False

    def pause_copy(stage: str) -> None:
        nonlocal seen
        if stage != "candidate_copy:large.bin" or seen:
            return
        seen = True
        entered.set()
        if not release.wait(5):
            raise RuntimeError("publish barrier timed out")

    monkeypatch.setattr(quarantine_module, "_FAILPOINT", pause_copy)
    publish_results = ctx.Queue()
    cleanup_results = ctx.Queue()
    publisher = ctx.Process(target=_publish_worker, args=(_request(), publish_results))
    cleaner = ctx.Process(target=_cleanup_worker, args=(cleanup_id, cleanup_results))
    publisher.start()
    assert entered.wait(3)
    cleaner.start()
    try:
        time.sleep(0.25)
        assert cleaner.is_alive()
        assert residue.exists()
    finally:
        release.set()
        _join_processes((publisher, cleaner))
    assert publish_results.get(timeout=1)[0] == "ok"
    assert cleanup_results.get(timeout=1) == ("ok", True)


def test_double_cleanup_is_serialized_and_converges(
    quarantine_fixture: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _fork_context()
    operation_id = "3" * 32
    residue = quarantine_fixture["quarantine"] / f".quarantine-{operation_id}"
    residue.mkdir(mode=0o700)
    entered = ctx.Event()
    release = ctx.Event()
    seen = False

    def pause_cleanup(stage: str) -> None:
        nonlocal seen
        if stage != "cleanup_before_remove" or seen:
            return
        seen = True
        entered.set()
        if not release.wait(5):
            raise RuntimeError("cleanup barrier timed out")

    monkeypatch.setattr(quarantine_module, "_FAILPOINT", pause_cleanup)
    results = ctx.Queue()
    first = ctx.Process(target=_cleanup_worker, args=(operation_id, results))
    second = ctx.Process(target=_cleanup_worker, args=(operation_id, results))
    first.start()
    assert entered.wait(3)
    second.start()
    try:
        time.sleep(0.25)
        assert second.is_alive()
        assert residue.exists()
    finally:
        release.set()
        _join_processes((first, second))
    assert sorted(results.get(timeout=1) for _ in range(2)) == [("ok", False), ("ok", True)]


def test_foreign_generation_directory_flock_cannot_block_publication(
    quarantine_fixture: dict[str, Any],
) -> None:
    ctx = _fork_context()
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
    )
    holder = os.open(quarantine_fixture["generations"], os.O_RDONLY)
    fcntl.flock(holder, fcntl.LOCK_EX)
    results = ctx.Queue()
    publisher = ctx.Process(target=_publish_worker, args=(_request(), results))
    publisher.start()
    try:
        publisher.join(2)
        assert not publisher.is_alive()
        assert results.get(timeout=1)[0] == "ok"
    finally:
        fcntl.flock(holder, fcntl.LOCK_UN)
        os.close(holder)
        _join_processes((publisher,))


@pytest.mark.parametrize("mask", (0o000, 0o077, 0o777))
def test_operation_quarantine_mode_is_exact_under_process_umask(
    quarantine_fixture: dict[str, Any],
    mask: int,
) -> None:
    ctx = _fork_context()
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
    )
    results = ctx.Queue()
    process = ctx.Process(
        target=_umask_publish_worker,
        args=(
            _request(),
            mask,
            "candidate_copy:release/src/rquant/runtime_service_main.py",
            results,
        ),
    )
    process.start()
    _join_processes((process,))
    assert results.get(timeout=1)[0:2] == ("error", "SimulatedCrash")
    residue = quarantine_fixture["quarantine"] / f".quarantine-{'1' * 32}"
    unfinished_directories = (
        residue,
        residue / "release",
        residue / "release/src",
        residue / "release/src/rquant",
    )
    assert all(
        stat.S_IMODE(path.stat(follow_symlinks=False).st_mode) == 0o700
        for path in unfinished_directories
    )
    assert (
        stat.S_IMODE((quarantine_fixture["inbox"].parent / "deployment.lock").stat().st_mode)
        == 0o600
    )
    assert cleanup_runtime_quarantine("1" * 32) is True


def test_cleanup_repairs_only_exact_root_owned_mode_zero_operation_residue(
    quarantine_fixture: dict[str, Any],
) -> None:
    ctx = _fork_context()
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
    )
    results = ctx.Queue()
    process = ctx.Process(
        target=_umask_publish_worker,
        args=(_request(), 0o777, "temporary_created_before_mode_fix", results),
    )
    process.start()
    _join_processes((process,))
    assert results.get(timeout=1)[0:2] == ("error", "SimulatedCrash")
    residue = quarantine_fixture["quarantine"] / f".quarantine-{'1' * 32}"
    assert stat.S_IMODE(residue.stat(follow_symlinks=False).st_mode) == 0o000
    assert cleanup_runtime_quarantine("1" * 32) is True

    unsafe = quarantine_fixture["quarantine"] / f".quarantine-{'2' * 32}"
    unsafe.mkdir(mode=0o777)
    unsafe.chmod(0o777)
    with pytest.raises(RuntimeQuarantineError, match="unsafe"):
        cleanup_runtime_quarantine("2" * 32)


@pytest.mark.parametrize(
    "stage",
    ("first_full_revalidation", "atomic_rename"),
)
def test_pre_visibility_oserror_is_not_misreported_as_durability(
    quarantine_fixture: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
    )

    def fail(selected: str) -> None:
        if selected == stage:
            raise OSError(f"injected {stage}")

    monkeypatch.setattr(quarantine_module, "_FAILPOINT", fail)
    with pytest.raises(RuntimeQuarantineError) as rejected:
        publish_runtime_candidate(_request())
    assert type(rejected.value) is RuntimeQuarantineError
    assert not any(quarantine_fixture["generations"].iterdir())
    assert cleanup_runtime_quarantine("1" * 32) is True

    monkeypatch.setattr(quarantine_module, "_FAILPOINT", lambda _stage: None)
    assert publish_runtime_candidate(_request()).status is RuntimeQuarantineStatus.PUBLISHED


@pytest.mark.parametrize("fault", ("deployment-lock", "quarantine-root"))
def test_lock_and_root_open_failures_are_pre_visibility_and_leave_no_residue(
    quarantine_fixture: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
    )
    real_lock = authority_module.acquire_runtime_deployment_lock
    real_open_root = quarantine_module._open_fixed_root

    if fault == "deployment-lock":

        def reject_lock() -> authority_module.RuntimeDeploymentLock:
            raise RuntimeAuthorityPublishError("injected deployment lock failure")

        monkeypatch.setattr(authority_module, "acquire_runtime_deployment_lock", reject_lock)
    else:

        def reject_quarantine_root(
            path: Path,
            label: str,
        ) -> tuple[list[int], int]:
            if label == "quarantine root":
                raise OSError("injected quarantine root open failure")
            return real_open_root(path, label)

        monkeypatch.setattr(quarantine_module, "_open_fixed_root", reject_quarantine_root)

    with pytest.raises(RuntimeQuarantineError) as rejected:
        publish_runtime_candidate(_request())
    assert type(rejected.value) is RuntimeQuarantineError
    assert not any(quarantine_fixture["quarantine"].iterdir())
    assert not any(quarantine_fixture["generations"].iterdir())

    monkeypatch.setattr(authority_module, "acquire_runtime_deployment_lock", real_lock)
    monkeypatch.setattr(quarantine_module, "_open_fixed_root", real_open_root)
    assert publish_runtime_candidate(_request()).status is RuntimeQuarantineStatus.PUBLISHED


def test_post_rename_oserror_is_durability_typed_and_retry_is_idempotent(
    quarantine_fixture: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
    )

    def fail(stage: str) -> None:
        if stage == "post_rename_pre_parent_fsync":
            raise OSError("injected post-rename failure")

    monkeypatch.setattr(quarantine_module, "_FAILPOINT", fail)
    with pytest.raises(RuntimeQuarantineDurabilityError):
        publish_runtime_candidate(_request())
    assert len(tuple(quarantine_fixture["generations"].iterdir())) == 1
    assert cleanup_runtime_quarantine("1" * 32) is False

    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
    )
    monkeypatch.setattr(quarantine_module, "_FAILPOINT", lambda _stage: None)
    assert publish_runtime_candidate(_request()).status is RuntimeQuarantineStatus.IDEMPOTENT


def test_atomic_no_replace_preserves_target_created_in_identity_to_rename_window(
    quarantine_fixture: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
    )
    probe = publish_runtime_candidate(_request(operation_id="0" * 32))
    generation_id = probe.generation_id
    for directory, _names, files in os.walk(probe.slot.generation_path):
        current = Path(directory)
        current.chmod(0o755)
        for name in files:
            (current / name).chmod(0o644)
    shutil.rmtree(probe.slot.generation_path)
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
    )
    target = quarantine_fixture["generations"] / generation_id
    target_identity: tuple[int, int] | None = None

    def create_target(stage: str) -> None:
        nonlocal target_identity
        if stage != "atomic_rename":
            return
        target.mkdir(mode=0o755)
        target.chmod(authority_module.GENERATION_DIRECTORY_MODE)
        target_identity = (target.stat().st_dev, target.stat().st_ino)

    monkeypatch.setattr(quarantine_module, "_FAILPOINT", create_target)
    with pytest.raises(RuntimeQuarantineError):
        publish_runtime_candidate(_request())
    assert target_identity == (target.stat().st_dev, target.stat().st_ino)
    assert not any(target.iterdir())
    assert candidate.exists()
    residue = quarantine_fixture["quarantine"] / f".quarantine-{'1' * 32}"
    assert {path.name for path in quarantine_fixture["quarantine"].iterdir()} == {residue.name}
    assert stat.S_IMODE(residue.stat().st_mode) == authority_module.GENERATION_DIRECTORY_MODE
    assert cleanup_runtime_quarantine("1" * 32) is True
    assert cleanup_runtime_quarantine("1" * 32) is False
    target.chmod(0o755)
    target.rmdir()


def test_plain_rename_control_overwrites_an_empty_target(tmp_path: Path) -> None:
    source = tmp_path / "source"
    target = tmp_path / "target"
    source.mkdir()
    (source / "candidate").write_bytes(b"candidate")
    target.mkdir()
    target_identity = (target.stat().st_dev, target.stat().st_ino)

    parent_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.rename("source", "target", src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
    finally:
        os.close(parent_fd)

    assert (target.stat().st_dev, target.stat().st_ino) != target_identity
    assert (target / "candidate").read_bytes() == b"candidate"


def test_missing_atomic_no_replace_primitive_fails_closed_with_cleanup_residue(
    quarantine_fixture: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
    )
    monkeypatch.setattr(quarantine_module, "_ATOMIC_RENAME_NOREPLACE", None, raising=False)
    with pytest.raises(RuntimeQuarantineError, match="atomic no-replace"):
        publish_runtime_candidate(_request())
    assert not any(quarantine_fixture["generations"].iterdir())
    assert cleanup_runtime_quarantine("1" * 32) is True


@pytest.mark.parametrize(
    "relative",
    (
        "FULL-MANIFEST.JSON",
        "ｆｕｌｌ－ｍａｎｉｆｅｓｔ．ｊｓｏｎ",
        "．",
        "．．",
        "safe／escape.py",
    ),
)
def test_publisher_rejects_normalized_reserved_dot_and_separator_aliases(
    quarantine_fixture: dict[str, Any],
    relative: str,
) -> None:
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
        extra_files={relative: b"alias"},
    )
    with pytest.raises(RuntimeQuarantineError, match="normalized|reserved|component"):
        publish_runtime_candidate(_request())
    assert not any(quarantine_fixture["generations"].iterdir())


def test_publisher_accepts_legitimate_nonconfusable_unicode_component(
    quarantine_fixture: dict[str, Any],
) -> None:
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
        extra_files={"release/src/rquant/数据模型.py": b"VALUE = 1\n"},
    )
    result = publish_runtime_candidate(_request())
    assert (result.slot.generation_path / "release/src/rquant/数据模型.py").is_file()


def test_manifest_budget_rejects_during_traversal_before_all_long_paths_are_copied(
    quarantine_fixture: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    long_files = {f"release/src/rquant/{index:03d}_{'x' * 80}.py": b"" for index in range(100)}
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
        extra_files=long_files,
    )
    monkeypatch.setattr(quarantine_module, "MAX_GENERATION_MANIFEST_BYTES", 2_500)
    traversed: list[str] = []

    def observe(stage: str) -> None:
        if stage.startswith("candidate_traversal:"):
            traversed.append(stage)

    monkeypatch.setattr(quarantine_module, "_FAILPOINT", observe)
    with pytest.raises(RuntimeQuarantineError, match="manifest byte budget"):
        publish_runtime_candidate(_request())
    assert 0 < len(traversed) < len(long_files)
    residue = quarantine_fixture["quarantine"] / f".quarantine-{'1' * 32}"
    assert not (residue / authority_module.GENERATION_MANIFEST_NAME).exists()
    assert cleanup_runtime_quarantine("1" * 32) is True


def test_manifest_serializer_never_receives_a_duplicate_whole_tree_payload(
    quarantine_fixture: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _make_candidate(
        quarantine_fixture["inbox"],
        candidate_id="a" * 64,
        candidate_basename="candidate-v1",
    )
    real_canonical = quarantine_module.canonical_json_bytes

    def reject_aggregate_entries(
        value: Any,
        *,
        trailing_newline: bool = False,
    ) -> bytes:
        if type(value) is dict and type(value.get("entries")) is list:
            raise AssertionError("whole-tree entries payload was materialized")
        return real_canonical(value, trailing_newline=trailing_newline)

    monkeypatch.setattr(quarantine_module, "canonical_json_bytes", reject_aggregate_entries)
    assert publish_runtime_candidate(_request()).status is RuntimeQuarantineStatus.PUBLISHED
