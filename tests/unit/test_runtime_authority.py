from __future__ import annotations

import hashlib
import inspect
import multiprocessing
import os
import shutil
import socket
import stat
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType
from typing import Any

import pytest

import rquant.runtime_authority as authority_module
from rquant.runtime_authority import (
    ProductionRuntimeProfileError,
    RuntimeAuthorityDurabilityError,
    RuntimeAuthorityPublishError,
    RuntimeAuthorityRecord,
    RuntimeAuthorityRecordError,
    RuntimeAuthorityRollbackError,
    RuntimeAuthorityState,
    RuntimeGenerationSlot,
    canonical_runtime_authority_bytes,
    cleanup_runtime_authority_temp,
    load_production_runtime_profile,
    load_runtime_authority,
    parse_runtime_authority_record,
    parse_runtime_closure_profile,
    prepare_runtime_authority_publish,
    prepare_runtime_authority_rollback,
    publish_runtime_authority,
)
from rquant.strict_json import canonical_json_bytes


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("ascii")).hexdigest()


def _file(path: str, *, mode: int = 0o444) -> dict[str, object]:
    return {
        "path": path,
        "sha256": _digest(path),
        "owner_uid": 0,
        "mode": mode,
    }


def _profile_payload() -> dict[str, object]:
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
        "system_python": _file(str(authority_module.PRODUCTION_SYSTEM_PYTHON), mode=0o555),
        "elf_loader": _file("/lib64/ld-linux-x86-64.so.2", mode=0o555),
        "stdlib": [_file("/usr/lib/python3.11/os.py")],
        "shared_libraries": [_file("/usr/lib64/libpython3.11.so.1.0")],
        "deploy_pyz": _file(str(authority_module.PRODUCTION_DEPLOY_PYZ), mode=0o555),
        "runtime_pyz": _file(str(authority_module.PRODUCTION_RUNTIME_PYZ), mode=0o555),
        "inbox_root": str(authority_module.PRODUCTION_INBOX_ROOT),
        "quarantine_root": str(authority_module.PRODUCTION_QUARANTINE_ROOT),
        "generation_root": str(authority_module.PRODUCTION_GENERATION_ROOT),
        "allowed_operations": ["publish", "rollback"],
        "roles": {
            "daily": {
                "module": "rquant.runtime_service_main",
                "environment_allowlist": ["LANG", "LC_ALL", "TZ"],
            }
        },
        "manifest_schema": {
            "schema_id": "rquant-full-manifest/v1",
            "entry_types": ["directory", "file"],
            "directory_modes": [0o555],
            "file_modes": [0o444, 0o555],
            "max_entries": 100_000,
            "max_file_bytes": 1_073_741_824,
            "max_path_bytes": 4096,
        },
    }
    return {
        "profile_id": hashlib.sha256(canonical_json_bytes(body)).hexdigest(),
        **body,
    }


def _profile_bytes(payload: dict[str, object] | None = None) -> bytes:
    return canonical_json_bytes(_profile_payload() if payload is None else payload)


def _rehash_profile(payload: dict[str, object]) -> None:
    body = {key: value for key, value in payload.items() if key != "profile_id"}
    payload["profile_id"] = hashlib.sha256(canonical_json_bytes(body)).hexdigest()


def _directory_policy(*directories: Path) -> dict[Path, tuple[int, int]]:
    policy: dict[Path, tuple[int, int]] = {}
    for directory in directories:
        current = Path("/")
        for component in (None, *directory.parts[1:]):
            if component is not None:
                current /= component
            observed = os.stat(current, follow_symlinks=False)
            policy[current] = (observed.st_uid, stat.S_IMODE(observed.st_mode))
    return policy


def _role_payload(generation: Path) -> dict[str, object]:
    return {
        "python_path": str(generation / "venv" / "bin" / "python"),
        "module": "rquant.runtime_service_main",
        "working_directory": str(generation / "release"),
        "app_source": str(generation / "release" / "src"),
        "site_packages": [str(generation / "venv" / "lib" / "python3.11" / "site-packages")],
    }


def _slot_payload(
    generation_root: Path,
    marker: str,
    *,
    lifecycle: str = "active",
) -> dict[str, object]:
    generation_id = _digest(marker)
    generation = generation_root / generation_id
    return {
        "lifecycle": lifecycle,
        "generation_id": generation_id,
        "generation_path": str(generation),
        "commit": f"untrusted-{marker}",
        "full_manifest_hash": generation_id,
        "profile_id": _profile_payload()["profile_id"],
        "roles": {"daily": _role_payload(generation)},
    }


def _record_payload(
    generation_root: Path,
    *,
    operation: str = "1",
    current_marker: str = "current",
    prior_marker: str | None = None,
    state: str = "active",
    sequence: int = 1,
) -> dict[str, object]:
    current = _slot_payload(generation_root, current_marker)
    prior_lifecycle = "failed" if state == "rolled_back" else "rollback_ready"
    prior = (
        None
        if prior_marker is None
        else _slot_payload(generation_root, prior_marker, lifecycle=prior_lifecycle)
    )
    payload: dict[str, object] = {
        "schema_version": 1,
        "operation_id": operation * 32,
        "sequence": sequence,
        "state": state,
    }
    for prefix, slot in (("current", current), ("prior", prior)):
        for field in (
            "lifecycle",
            "generation_id",
            "generation_path",
            "commit",
            "full_manifest_hash",
            "profile_id",
            "roles",
        ):
            payload[f"{prefix}_{field}"] = None if slot is None else slot[field]
    return payload


def _record(
    generation_root: Path,
    *,
    operation: str = "1",
    current_marker: str = "current",
    prior_marker: str | None = None,
    state: str = "active",
    sequence: int = 1,
    durable: bool = True,
) -> RuntimeAuthorityRecord:
    if durable:
        prior_lifecycle = (
            authority_module.RuntimeGenerationLifecycle.FAILED
            if state == "rolled_back"
            else authority_module.RuntimeGenerationLifecycle.ROLLBACK_READY
        )
        return RuntimeAuthorityRecord(
            schema_version=1,
            operation_id=operation * 32,
            sequence=sequence,
            state=RuntimeAuthorityState(state),
            current=_materialize_generation_slot(
                generation_root,
                current_marker,
                authority_module.RuntimeGenerationLifecycle.ACTIVE,
            ),
            prior=(
                None
                if prior_marker is None
                else _materialize_generation_slot(
                    generation_root,
                    prior_marker,
                    prior_lifecycle,
                )
            ),
        )
    payload = _record_payload(
        generation_root,
        operation=operation,
        current_marker=current_marker,
        prior_marker=prior_marker,
        state=state,
        sequence=sequence,
    )
    prior_values = tuple(payload[f"prior_{field}"] for field in authority_module._SLOT_FIELDS)
    prior = (
        None
        if all(value is None for value in prior_values)
        else authority_module._parse_slot(payload, prefix="prior")
    )
    return RuntimeAuthorityRecord(
        schema_version=payload["schema_version"],
        operation_id=payload["operation_id"],
        sequence=payload["sequence"],
        state=RuntimeAuthorityState(payload["state"]),
        current=authority_module._parse_slot(payload, prefix="current"),
        prior=prior,
    )


def _materialize_generation_slot(
    generation_root: Path,
    marker: str,
    lifecycle: authority_module.RuntimeGenerationLifecycle,
    *,
    manifest_profile_id: str | None = None,
    manifest_module: str | None = None,
) -> RuntimeGenerationSlot:
    relative_role = {
        "python_path": "venv/bin/python",
        "module": "rquant.runtime_service_main",
        "working_directory": "release",
        "app_source": "release/src",
        "site_packages": ["venv/lib/python3.11/site-packages"],
    }
    manifest_role = dict(relative_role)
    if manifest_module is not None:
        manifest_role["module"] = manifest_module
    directory_paths = (
        "release",
        "release/src",
        "release/src/rquant",
        "venv",
        "venv/bin",
        "venv/lib",
        "venv/lib/python3.11",
        "venv/lib/python3.11/site-packages",
    )
    files = {
        "release/src/rquant/__init__.py": b"",
        "release/src/rquant/runtime_service_main.py": b"def main():\n    return 0\n",
        "venv/bin/python": marker.encode("utf-8"),
    }
    fixture_key = hashlib.sha256(
        f"{marker}:{manifest_profile_id}:{manifest_module}".encode()
    ).hexdigest()[:16]
    staging = generation_root / f".fixture-{fixture_key}"
    staging.mkdir(mode=0o700)
    for relative in directory_paths:
        (staging / relative).mkdir(mode=0o700)
    for relative, content in files.items():
        target = staging / relative
        target.write_bytes(content)
        target.chmod(0o555 if relative == "venv/bin/python" else 0o444)
    for relative in reversed(directory_paths):
        (staging / relative).chmod(0o555)
    entries = [
        {
            "path": path,
            "type": "directory",
            "owner_uid": os.getuid(),
            "mode": 0o555,
            "nlink": (staging / path).stat().st_nlink,
            "size": 0,
            "sha256": None,
        }
        for path in directory_paths
    ]
    entries.extend(
        {
            "path": path,
            "type": "file",
            "owner_uid": os.getuid(),
            "mode": 0o555 if path == "venv/bin/python" else 0o444,
            "nlink": (staging / path).stat().st_nlink,
            "size": len(content),
            "sha256": hashlib.sha256(content).hexdigest(),
        }
        for path, content in files.items()
    )
    manifest = canonical_json_bytes(
        {
            "schema_id": "rquant-full-manifest/v1",
            "profile_id": manifest_profile_id or _profile_payload()["profile_id"],
            "roles": {"daily": manifest_role},
            "entries": sorted(entries, key=lambda entry: entry["path"]),
        },
        trailing_newline=True,
    )
    generation_id = hashlib.sha256(manifest).hexdigest()
    generation = generation_root / generation_id
    manifest_path = staging / authority_module.GENERATION_MANIFEST_NAME
    manifest_path.write_bytes(manifest)
    manifest_path.chmod(0o444)
    if generation.exists():
        for directory, _subdirectories, _files in os.walk(staging):
            Path(directory).chmod(0o700)
        shutil.rmtree(staging)
    else:
        staging.rename(generation)
    return RuntimeGenerationSlot(
        lifecycle=lifecycle,
        generation_id=generation_id,
        generation_path=generation,
        commit=f"untrusted-{marker}",
        full_manifest_hash=generation_id,
        profile_id=_profile_payload()["profile_id"],
        roles={
            "daily": authority_module._parse_role(
                relative_role
                | {
                    "python_path": str(generation / relative_role["python_path"]),
                    "working_directory": str(generation / relative_role["working_directory"]),
                    "app_source": str(generation / relative_role["app_source"]),
                    "site_packages": [
                        str(generation / path) for path in relative_role["site_packages"]
                    ],
                }
            )
        },
    )


def _install_profile_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    anchor = tmp_path / "profile-root"
    anchor.mkdir(mode=0o700)
    profile_path = anchor / "production-runtime-profile.json"
    profile_path.write_bytes(_profile_bytes())
    profile_path.chmod(0o444)
    monkeypatch.setattr(authority_module, "PRODUCTION_PROFILE_ANCHOR", anchor)
    monkeypatch.setattr(authority_module, "PRODUCTION_PROFILE_PATH", profile_path)
    monkeypatch.setattr(authority_module, "PRODUCTION_PROFILE_OWNER_UID", os.getuid())
    monkeypatch.setattr(authority_module, "PRODUCTION_PROFILE_DIRECTORY_MODE", 0o700)
    monkeypatch.setattr(
        authority_module,
        "_PRODUCTION_PROFILE_DIRECTORY_POLICY",
        _directory_policy(anchor),
    )
    return profile_path


def _install_authority_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    record: RuntimeAuthorityRecord | None = None,
) -> tuple[Path, Path]:
    anchor = tmp_path / "authority"
    anchor.mkdir(mode=0o700)
    generations = anchor / "generations"
    generations.mkdir(mode=0o700)
    path = anchor / "current.json"
    lock_path = anchor / "deployment.lock"
    monkeypatch.setattr(authority_module, "RUNTIME_AUTHORITY_ANCHOR", anchor)
    monkeypatch.setattr(authority_module, "RUNTIME_AUTHORITY_PATH", path)
    monkeypatch.setattr(authority_module, "RUNTIME_AUTHORITY_LOCK_PATH", lock_path)
    monkeypatch.setattr(authority_module, "PRODUCTION_GENERATION_ROOT", generations)
    monkeypatch.setattr(authority_module, "RUNTIME_AUTHORITY_OWNER_UID", os.getuid())
    monkeypatch.setattr(authority_module, "RUNTIME_AUTHORITY_DIRECTORY_MODE", 0o700)
    monkeypatch.setattr(authority_module, "GENERATION_DIRECTORY_MODE", 0o700)
    monkeypatch.setattr(
        authority_module,
        "_PRODUCTION_RUNTIME_DIRECTORY_POLICY",
        _directory_policy(anchor, generations),
    )
    _install_profile_fixture(tmp_path, monkeypatch)
    if record is not None:
        path.write_bytes(canonical_runtime_authority_bytes(record))
        path.chmod(0o444)
    return path, generations


def _publish_worker(
    record: RuntimeAuthorityRecord,
    entered_read: Any,
    release_write: Any,
    result_queue: Any,
    pause_after_write: bool,
) -> None:
    original_read = authority_module._read_record_at
    original_write = authority_module._write_all

    def observed_read(*args: object, **kwargs: object) -> object:
        result = original_read(*args, **kwargs)
        if kwargs.get("label") == "existing runtime authority record":
            entered_read.set()
        return result

    def paused_write(descriptor: int, payload: bytes) -> None:
        original_write(descriptor, payload)
        if pause_after_write:
            entered_read.set()
            if not release_write.wait(5):
                raise RuntimeError("test publication barrier timed out")

    authority_module._read_record_at = observed_read
    authority_module._write_all = paused_write
    try:
        result = publish_runtime_authority(record)
    except Exception as exc:
        result_queue.put(("error", type(exc).__name__, str(exc)))
    else:
        result_queue.put(("ok", result.value))


def test_profile_v1_round_trips_and_self_checks_profile_id() -> None:
    profile = parse_runtime_closure_profile(_profile_bytes())

    assert profile.schema_version == 1
    assert profile.platform == "linux"
    assert profile.system_python.path == authority_module.PRODUCTION_SYSTEM_PYTHON
    assert profile.system_python.mode == 0o555
    assert profile.deploy_pyz.path == authority_module.PRODUCTION_DEPLOY_PYZ
    assert profile.runtime_pyz.path == authority_module.PRODUCTION_RUNTIME_PYZ
    assert len(profile.ancestors) == len({ancestor.path for ancestor in profile.ancestors})


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (lambda payload: payload.update(profile_id="0" * 64), "profile id"),
        (lambda payload: payload.update(schema_version=True), "schema"),
        (lambda payload: payload.update(platform=1), "platform"),
        (lambda payload: payload.update(extra="forbidden"), "fields"),
        (
            lambda payload: payload["system_python"].update(path="/usr/local/bin/python3.11"),
            "system Python",
        ),
        (lambda payload: payload["deploy_pyz"].update(mode=0o755), "deploy pyz"),
        (lambda payload: payload["stdlib"].append(payload["stdlib"][0]), "duplicate"),
        (lambda payload: payload.update(inbox_root="/tmp/inbox"), "inbox root"),
        (
            lambda payload: payload.update(allowed_operations=["publish"]),
            "allowed operations",
        ),
        (lambda payload: payload["roles"]["daily"].update(module="os"), "role module"),
        (
            lambda payload: payload["manifest_schema"].update(max_entries=1),
            "manifest schema",
        ),
    ],
)
def test_profile_rejects_tamper_and_non_native_schema(
    mutation: object,
    match: str,
) -> None:
    payload = _profile_payload()
    mutation(payload)
    if payload["profile_id"] != "0" * 64:
        _rehash_profile(payload)

    with pytest.raises(ProductionRuntimeProfileError, match=match):
        parse_runtime_closure_profile(canonical_json_bytes(payload))


def test_profile_rejects_duplicate_keys_float_constant_and_size_limit() -> None:
    encoded = _profile_bytes()
    duplicate = b'{"profile_id":"0",' + encoded[1:]
    with pytest.raises(ProductionRuntimeProfileError, match="strict JSON"):
        parse_runtime_closure_profile(duplicate)
    with pytest.raises(ProductionRuntimeProfileError, match="strict JSON"):
        parse_runtime_closure_profile(
            encoded.replace(b'"schema_version":1', b'"schema_version":1.0')
        )
    with pytest.raises(ProductionRuntimeProfileError, match="strict JSON"):
        parse_runtime_closure_profile(
            encoded.replace(b'"schema_version":1', b'"schema_version":NaN')
        )
    with pytest.raises(ProductionRuntimeProfileError, match="too large"):
        parse_runtime_closure_profile(b" " * (authority_module.MAX_PROFILE_BYTES + 1))


def test_profile_rejects_missing_or_unsafe_ancestor_policy() -> None:
    payload = _profile_payload()
    payload["ancestors"] = payload["ancestors"][1:]
    _rehash_profile(payload)
    with pytest.raises(ProductionRuntimeProfileError, match="ancestor"):
        parse_runtime_closure_profile(canonical_json_bytes(payload))

    payload = _profile_payload()
    payload["ancestors"][0]["mode"] = 0o777
    _rehash_profile(payload)
    with pytest.raises(ProductionRuntimeProfileError, match="ancestor"):
        parse_runtime_closure_profile(canonical_json_bytes(payload))


def test_production_profile_loader_has_no_override_and_reads_fixed_policy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_profile_fixture(tmp_path, monkeypatch)

    assert tuple(inspect.signature(load_production_runtime_profile).parameters) == ()
    assert load_production_runtime_profile().profile_id == _profile_payload()["profile_id"]


@pytest.mark.parametrize("fault", ("symlink", "hardlink", "mode", "owner", "ancestor-mode"))
def test_production_profile_loader_rejects_unsafe_file_and_ancestor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    profile_path = _install_profile_fixture(tmp_path, monkeypatch)
    if fault == "symlink":
        real = profile_path.with_name("real.json")
        profile_path.rename(real)
        profile_path.symlink_to(real.name)
    elif fault == "hardlink":
        os.link(profile_path, profile_path.with_name("alias.json"))
    elif fault == "mode":
        profile_path.chmod(0o644)
    elif fault == "owner":
        monkeypatch.setattr(authority_module, "PRODUCTION_PROFILE_OWNER_UID", os.getuid() + 1)
    else:
        profile_path.parent.chmod(0o770)

    with pytest.raises(ProductionRuntimeProfileError):
        load_production_runtime_profile()


@pytest.mark.parametrize(
    "fault",
    ("grandparent-symlink", "grandparent-mode", "grandparent-owner"),
)
def test_hyb1_p1_07_profile_walk_starts_at_root_and_rejects_real_ancestor_fault(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    real_root = tmp_path / "real-root"
    real_root.mkdir(mode=0o700)
    selected_root = real_root
    if fault == "grandparent-symlink":
        selected_root = tmp_path / "linked-root"
        selected_root.symlink_to(real_root, target_is_directory=True)
    elif fault == "grandparent-mode":
        real_root.chmod(0o770)
    anchor = selected_root / "profile-root"
    anchor.mkdir(mode=0o700)
    profile_path = anchor / "production-runtime-profile.json"
    profile_path.write_bytes(_profile_bytes())
    profile_path.chmod(0o444)
    monkeypatch.setattr(authority_module, "PRODUCTION_PROFILE_ANCHOR", anchor)
    monkeypatch.setattr(authority_module, "PRODUCTION_PROFILE_PATH", profile_path)
    monkeypatch.setattr(authority_module, "PRODUCTION_PROFILE_OWNER_UID", os.getuid())
    monkeypatch.setattr(authority_module, "PRODUCTION_PROFILE_DIRECTORY_MODE", 0o700)
    policy = _directory_policy(anchor)
    if fault == "grandparent-owner":
        _owner, mode = policy[real_root]
        policy[real_root] = (os.getuid() + 1, mode)
    monkeypatch.setattr(authority_module, "_PRODUCTION_PROFILE_DIRECTORY_POLICY", policy)

    with pytest.raises(ProductionRuntimeProfileError, match="ancestor"):
        load_production_runtime_profile()


def test_hyb1_p1_07_authority_record_walk_rejects_outer_ancestor_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, generation_root = _install_authority_fixture(tmp_path, monkeypatch)
    path.write_bytes(canonical_runtime_authority_bytes(_record(generation_root)))
    path.chmod(0o444)
    policy = dict(authority_module._PRODUCTION_RUNTIME_DIRECTORY_POLICY)
    owner, mode = policy[path.parent.parent]
    policy[path.parent.parent] = (owner + 1, mode)
    monkeypatch.setattr(authority_module, "_PRODUCTION_RUNTIME_DIRECTORY_POLICY", policy)

    with pytest.raises(RuntimeAuthorityRecordError, match="ancestor"):
        load_runtime_authority()


def test_hyb1_p1_07_generation_root_walk_rejects_group_writable_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, generation_root = _install_authority_fixture(tmp_path, monkeypatch)
    record = _record(generation_root)
    generation_root.chmod(0o770)

    with pytest.raises(RuntimeAuthorityPublishError, match="generation root ancestor"):
        publish_runtime_authority(record)
    assert not path.exists()


def test_record_v1_parses_complete_current_and_nullable_prior(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _path, generation_root = _install_authority_fixture(tmp_path, monkeypatch)
    record = parse_runtime_authority_record(canonical_json_bytes(_record_payload(generation_root)))

    assert record.state is RuntimeAuthorityState.ACTIVE
    assert record.prior is None
    assert record.current.generation_id == record.current.full_manifest_hash
    assert tuple(record.current.roles) == ("daily",)

    with_prior = parse_runtime_authority_record(
        canonical_json_bytes(_record_payload(generation_root, prior_marker="prior"))
    )
    assert with_prior.prior is not None
    assert with_prior.current != with_prior.prior


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (lambda payload, _root: payload.update(extra="forbidden"), "fields"),
        (lambda payload, _root: payload.update(operation_id=7), "operation"),
        (lambda payload, _root: payload.update(state="unknown"), "state"),
        (
            lambda payload, _root: payload.update(current_full_manifest_hash="0" * 64),
            "identity",
        ),
        (
            lambda payload, root: payload.update(
                current_generation_path=str(root.parent / "escape")
            ),
            "generation path",
        ),
        (
            lambda payload, _root: payload.update(current_generation_path="relative"),
            "generation path",
        ),
        (lambda payload, _root: payload.update(prior_generation_id="0" * 64), "prior slot"),
    ],
)
def test_record_rejects_strict_schema_and_path_escape(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: object,
    match: str,
) -> None:
    _path, generation_root = _install_authority_fixture(tmp_path, monkeypatch)
    payload = _record_payload(generation_root)
    mutation(payload, generation_root)

    with pytest.raises(RuntimeAuthorityRecordError, match=match):
        parse_runtime_authority_record(canonical_json_bytes(payload))


def test_record_rejects_duplicate_current_prior_and_role_escape(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _path, generation_root = _install_authority_fixture(tmp_path, monkeypatch)
    payload = _record_payload(generation_root, prior_marker="prior")
    for field in (
        "generation_id",
        "lifecycle",
        "generation_path",
        "commit",
        "full_manifest_hash",
        "profile_id",
        "roles",
    ):
        payload[f"prior_{field}"] = payload[f"current_{field}"]
    with pytest.raises(RuntimeAuthorityRecordError, match="same generation"):
        parse_runtime_authority_record(canonical_json_bytes(payload))

    payload = _record_payload(generation_root)
    payload["current_roles"]["daily"]["app_source"] = str(tmp_path / "outside")
    with pytest.raises(RuntimeAuthorityRecordError, match="role path"):
        parse_runtime_authority_record(canonical_json_bytes(payload))


def test_record_rejects_generation_symlink_escape(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _path, generation_root = _install_authority_fixture(tmp_path, monkeypatch)
    outside = tmp_path / "outside"
    outside.mkdir()
    marker = "current"
    generation_id = _digest(marker)
    (generation_root / generation_id).symlink_to(outside, target_is_directory=True)

    with pytest.raises(RuntimeAuthorityRecordError, match="symbolic"):
        parse_runtime_authority_record(
            canonical_json_bytes(_record_payload(generation_root, current_marker=marker))
        )


def test_publish_transition_moves_current_to_prior_and_requires_monotonic_operation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _path, generation_root = _install_authority_fixture(tmp_path, monkeypatch)
    old = _record(generation_root, operation="1")
    next_slot = _record(generation_root, operation="2", current_marker="next").current

    advanced = prepare_runtime_authority_publish(
        old,
        next_slot,
        operation_id="2" * 32,
    )
    assert advanced.state is RuntimeAuthorityState.ACTIVE
    assert advanced.current == next_slot
    assert advanced.prior is not None
    assert advanced.prior.generation_id == old.current.generation_id
    assert advanced.prior.lifecycle.value == "rollback_ready"

    with pytest.raises(RuntimeAuthorityRecordError, match="unique"):
        prepare_runtime_authority_publish(old, next_slot, operation_id=old.operation_id)
    with pytest.raises(RuntimeAuthorityRecordError, match="already recorded"):
        prepare_runtime_authority_publish(old, old.current, operation_id="2" * 32)


def test_first_publish_and_single_level_rollback_are_deterministic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _path, generation_root = _install_authority_fixture(tmp_path, monkeypatch)
    initial_slot = _record(generation_root).current
    initial = prepare_runtime_authority_publish(None, initial_slot, operation_id="1" * 32)
    assert initial.prior is None

    next_slot = _record(generation_root, current_marker="next").current
    advanced = prepare_runtime_authority_publish(initial, next_slot, operation_id="2" * 32)
    rolled_back = prepare_runtime_authority_rollback(advanced, operation_id="3" * 32)
    assert rolled_back.state is RuntimeAuthorityState.ROLLED_BACK
    assert rolled_back.current == initial.current
    assert rolled_back.prior is not None
    assert rolled_back.prior.generation_id == next_slot.generation_id
    assert rolled_back.prior.lifecycle.value == "failed"

    with pytest.raises(RuntimeAuthorityRollbackError, match="single-level"):
        prepare_runtime_authority_rollback(rolled_back, operation_id="4" * 32)
    with pytest.raises(RuntimeAuthorityRollbackError, match="prior"):
        prepare_runtime_authority_rollback(initial, operation_id="2" * 32)


def test_atomic_publish_and_fixed_loader_round_trip(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, generations = _install_authority_fixture(tmp_path, monkeypatch)
    record = _record(generations)

    assert tuple(inspect.signature(publish_runtime_authority).parameters) == ("record",)
    assert tuple(inspect.signature(load_runtime_authority).parameters) == ()
    publish_runtime_authority(record)

    assert path.stat().st_mode & 0o777 == 0o444
    assert path.stat().st_uid == os.getuid()
    assert path.stat().st_nlink == 1
    assert path.read_bytes() == canonical_runtime_authority_bytes(record)
    assert load_runtime_authority() == record


@pytest.mark.parametrize(
    ("phase", "new_visible"),
    (("write", False), ("temp_fsync", False), ("replace", False), ("parent_fsync", True)),
)
def test_atomic_publish_crash_exposes_only_complete_old_or_new(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
    new_visible: bool,
) -> None:
    initial_root = tmp_path / "placeholder"
    initial_root.mkdir()
    old = _record(initial_root, operation="1")
    path, generations = _install_authority_fixture(tmp_path, monkeypatch)
    old = _record(generations, operation="1")
    path.write_bytes(canonical_runtime_authority_bytes(old))
    path.chmod(0o444)
    new_slot = _record(generations, current_marker="next", operation="2").current
    new = prepare_runtime_authority_publish(old, new_slot, operation_id="2" * 32)

    original_write = authority_module._write_all
    original_fsync = authority_module._fsync_descriptor
    original_replace = authority_module._replace_record

    def fail_write(descriptor: int, payload: bytes) -> None:
        if phase == "write":
            raise OSError("injected write crash")
        original_write(descriptor, payload)

    def fail_fsync(descriptor: int, *, phase_name: str) -> None:
        if phase == phase_name:
            raise OSError(f"injected {phase_name} crash")
        original_fsync(descriptor, phase_name=phase_name)

    def fail_replace(parent_fd: int, temporary_name: str) -> None:
        if phase == "replace":
            raise OSError("injected replace crash")
        original_replace(parent_fd, temporary_name)

    monkeypatch.setattr(authority_module, "_write_all", fail_write)
    monkeypatch.setattr(authority_module, "_fsync_descriptor", fail_fsync)
    monkeypatch.setattr(authority_module, "_replace_record", fail_replace)

    with pytest.raises(RuntimeAuthorityPublishError):
        publish_runtime_authority(new)

    visible = parse_runtime_authority_record(path.read_bytes())
    assert visible == (new if new_visible else old)


@pytest.mark.parametrize("fault", ("symlink", "hardlink", "mode", "owner", "ancestor-mode"))
def test_atomic_publish_rejects_unsafe_existing_record_and_ancestor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    path, generations = _install_authority_fixture(tmp_path, monkeypatch)
    old = _record(generations)
    path.write_bytes(canonical_runtime_authority_bytes(old))
    path.chmod(0o444)
    if fault == "symlink":
        real = path.with_name("real.json")
        path.rename(real)
        path.symlink_to(real.name)
    elif fault == "hardlink":
        os.link(path, path.with_name("alias.json"))
    elif fault == "mode":
        path.chmod(0o644)
    elif fault == "owner":
        monkeypatch.setattr(authority_module, "RUNTIME_AUTHORITY_OWNER_UID", os.getuid() + 1)
    else:
        path.parent.chmod(0o770)

    with pytest.raises(RuntimeAuthorityPublishError):
        publish_runtime_authority(old)


def test_temp_recovery_cleans_only_exact_operation_without_directory_inference(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _path, generations = _install_authority_fixture(tmp_path, monkeypatch)
    record = _record(generations)
    operation_id = record.operation_id
    temp = authority_module.RUNTIME_AUTHORITY_ANCHOR / f".current.{operation_id}.tmp"
    unrelated = authority_module.RUNTIME_AUTHORITY_ANCHOR / ".current.unrelated.tmp"
    temp.write_bytes(b"partial")
    temp.chmod(0o600)
    unrelated.write_bytes(b"keep")

    assert cleanup_runtime_authority_temp(operation_id)
    assert not temp.exists()
    assert unrelated.read_bytes() == b"keep"
    assert not cleanup_runtime_authority_temp(operation_id)


def test_runtime_authority_public_models_are_frozen_stdlib_dataclasses() -> None:
    generation_root = Path("/var/lib/rquant/runtime-authority/generations")
    record = _record(generation_root, durable=False)

    with pytest.raises((AttributeError, TypeError)):
        record.operation_id = "f" * 32
    assert isinstance(record.current, RuntimeGenerationSlot)
    source = Path(authority_module.__file__).read_text(encoding="utf-8")
    assert "pydantic" not in source
    assert "os.getenv" not in source
    assert "os.environ" not in source


def test_hyb1_p1_01_profile_binds_roots_operations_roles_and_manifest() -> None:
    payload = _profile_payload()
    payload.update(
        inbox_root="/var/lib/rquant/runtime-authority/inbox",
        quarantine_root="/var/lib/rquant/runtime-authority/quarantine",
        generation_root="/var/lib/rquant/runtime-authority/generations",
        allowed_operations=["publish", "rollback"],
        roles={
            "daily": {
                "module": "rquant.runtime_service_main",
                "environment_allowlist": ["LANG", "LC_ALL", "TZ"],
            }
        },
        manifest_schema={
            "schema_id": "rquant-full-manifest/v1",
            "entry_types": ["directory", "file"],
            "directory_modes": [0o555],
            "file_modes": [0o444, 0o555],
            "max_entries": 100_000,
            "max_file_bytes": 1_073_741_824,
            "max_path_bytes": 4096,
        },
    )
    _rehash_profile(payload)

    profile = parse_runtime_closure_profile(canonical_json_bytes(payload))

    assert profile.generation_root == Path("/var/lib/rquant/runtime-authority/generations")
    assert profile.roles["daily"].module == "rquant.runtime_service_main"
    assert isinstance(authority_module.PRODUCTION_MANIFEST_SCHEMA, MappingProxyType)


def test_hyb1_p1_02_record_parser_has_no_generation_root_override() -> None:
    assert tuple(inspect.signature(parse_runtime_authority_record).parameters) == ("payload",)
    assert not inspect.signature(load_production_runtime_profile).parameters
    assert not inspect.signature(load_runtime_authority).parameters


def test_hyb1_p1_03_record_requires_sequence_and_slot_lifecycle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _path, generation_root = _install_authority_fixture(tmp_path, monkeypatch)
    payload = _record_payload(generation_root, prior_marker="prior")
    payload.update(sequence=7)

    record = parse_runtime_authority_record(canonical_json_bytes(payload))

    assert record.sequence == 7
    assert record.current.lifecycle.value == "active"
    assert record.prior is not None
    assert record.prior.lifecycle.value == "rollback_ready"


def test_hyb1_p1_05_operation_id_is_opaque_and_sequence_advances(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _path, generation_root = _install_authority_fixture(tmp_path, monkeypatch)
    previous = _record(generation_root, operation="f")
    next_slot = _record(generation_root, current_marker="next").current

    advanced = prepare_runtime_authority_publish(
        previous,
        next_slot,
        operation_id="0" * 32,
    )

    assert advanced.sequence == previous.sequence + 1


def test_hyb1_p1_06_rejects_unbounded_roles_and_noncanonical_temp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, generation_root = _install_authority_fixture(tmp_path, monkeypatch)
    payload = _record_payload(generation_root)
    payload["current_roles"] = {
        f"role-{index}": _role_payload(generation_root / payload["current_generation_id"])
        for index in range(65)
    }
    with pytest.raises(RuntimeAuthorityRecordError, match="roles"):
        parse_runtime_authority_record(canonical_json_bytes(payload))

    record = _record(generation_root)
    original_write = authority_module._write_all

    def write_noncanonical(descriptor: int, encoded: bytes) -> None:
        original_write(descriptor, encoded + b" ")

    monkeypatch.setattr(authority_module, "_write_all", write_noncanonical)
    with pytest.raises(RuntimeAuthorityPublishError, match="temporary"):
        publish_runtime_authority(record)
    assert not path.exists()


def test_hyb1_p1_01_record_roles_match_loaded_profile_exactly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _path, generation_root = _install_authority_fixture(tmp_path, monkeypatch)
    payload = _record_payload(generation_root)
    payload["current_roles"]["daily"]["module"] = "os"

    with pytest.raises(RuntimeAuthorityRecordError, match="loaded profile"):
        parse_runtime_authority_record(canonical_json_bytes(payload))


def test_hyb1_p1_03_publication_rejects_sequence_gap_and_invalid_lifecycle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, generation_root = _install_authority_fixture(tmp_path, monkeypatch)
    old = _record(generation_root)
    path.write_bytes(canonical_runtime_authority_bytes(old))
    path.chmod(0o444)
    next_slot = _record(generation_root, current_marker="next").current
    advanced = prepare_runtime_authority_publish(old, next_slot, operation_id="2" * 32)

    with pytest.raises(RuntimeAuthorityPublishError, match="sequence"):
        publish_runtime_authority(replace(advanced, sequence=old.sequence + 2))

    payload = _record_payload(
        generation_root,
        current_marker="next",
        prior_marker="current",
    )
    payload["prior_lifecycle"] = "failed"
    with pytest.raises(RuntimeAuthorityRecordError, match="lifecycles"):
        parse_runtime_authority_record(canonical_json_bytes(payload))

    failed_slot = replace(
        next_slot,
        lifecycle=authority_module.RuntimeGenerationLifecycle.FAILED,
    )
    with pytest.raises(RuntimeAuthorityRecordError, match="active"):
        prepare_runtime_authority_publish(old, failed_slot, operation_id="3" * 32)


def test_hyb1_p1_05_publish_is_idempotent_and_detects_operation_conflict(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _path, generation_root = _install_authority_fixture(tmp_path, monkeypatch)
    record = _record(generation_root)

    assert publish_runtime_authority(record).value == "committed"
    assert publish_runtime_authority(record).value == "idempotent"

    conflicting = _record(generation_root, current_marker="different")
    with pytest.raises(RuntimeAuthorityPublishError, match="conflict"):
        publish_runtime_authority(conflicting)


def test_hyb1_p1_05_parent_fsync_failure_recovers_committed_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, generation_root = _install_authority_fixture(tmp_path, monkeypatch)
    previous = _record(generation_root)
    path.write_bytes(canonical_runtime_authority_bytes(previous))
    path.chmod(0o444)
    next_slot = _record(generation_root, current_marker="next").current
    record = prepare_runtime_authority_publish(
        previous,
        next_slot,
        operation_id="2" * 32,
    )
    original_fsync = authority_module._fsync_descriptor
    parent_fsync_calls = 0

    def fail_parent_fsync(descriptor: int, *, phase_name: str) -> None:
        nonlocal parent_fsync_calls
        if phase_name == "parent_fsync":
            parent_fsync_calls += 1
        if phase_name == "parent_fsync" and parent_fsync_calls == 1:
            raise OSError("injected parent fsync failure")
        original_fsync(descriptor, phase_name=phase_name)

    monkeypatch.setattr(authority_module, "_fsync_descriptor", fail_parent_fsync)

    result = publish_runtime_authority(record)

    assert result.value == "committed_after_recovery"
    assert parent_fsync_calls == 2
    assert load_runtime_authority() == record


def test_hyb1_p1_05_persistent_parent_fsync_failure_is_typed_blocked(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, generation_root = _install_authority_fixture(tmp_path, monkeypatch)
    record = _record(generation_root)
    parent_fsync_calls = 0

    def fail_parent_fsync(_descriptor: int, *, phase_name: str) -> None:
        nonlocal parent_fsync_calls
        if phase_name == "parent_fsync":
            parent_fsync_calls += 1
            raise OSError("persistent parent fsync failure")

    monkeypatch.setattr(authority_module, "_fsync_descriptor", fail_parent_fsync)

    with pytest.raises(RuntimeAuthorityDurabilityError, match="durability"):
        publish_runtime_authority(record)

    assert parent_fsync_calls == 2
    assert path.read_bytes() == canonical_runtime_authority_bytes(record)


def test_hyb1_p1_05_blocked_same_operation_retry_fsyncs_and_converges(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _path, generation_root = _install_authority_fixture(tmp_path, monkeypatch)
    record = _record(generation_root)
    original_fsync = authority_module._fsync_descriptor
    parent_fsync_calls = 0

    def fail_twice_then_fsync(descriptor: int, *, phase_name: str) -> None:
        nonlocal parent_fsync_calls
        if phase_name == "parent_fsync":
            parent_fsync_calls += 1
            if parent_fsync_calls <= 2:
                raise OSError("blocked parent fsync")
        original_fsync(descriptor, phase_name=phase_name)

    monkeypatch.setattr(authority_module, "_fsync_descriptor", fail_twice_then_fsync)

    with pytest.raises(RuntimeAuthorityDurabilityError, match="durability"):
        publish_runtime_authority(record)
    result = publish_runtime_authority(record)

    assert result.value == "idempotent"
    assert parent_fsync_calls == 3


@pytest.mark.parametrize("tamper", ("operation", "sequence", "prior", "canonical"))
def test_hyb1_p1_05_parent_fsync_recovery_requires_exact_reopened_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    tamper: str,
) -> None:
    path, generation_root = _install_authority_fixture(tmp_path, monkeypatch)
    previous = _record(generation_root)
    path.write_bytes(canonical_runtime_authority_bytes(previous))
    path.chmod(0o444)
    record = prepare_runtime_authority_publish(
        previous,
        _record(generation_root, current_marker="next").current,
        operation_id="2" * 32,
    )

    def fail_after_tamper(_descriptor: int, *, phase_name: str) -> None:
        if phase_name != "parent_fsync":
            return
        if tamper == "operation":
            payload = canonical_runtime_authority_bytes(replace(record, operation_id="f" * 32))
        elif tamper == "sequence":
            payload = canonical_runtime_authority_bytes(
                replace(record, sequence=record.sequence + 1)
            )
        elif tamper == "prior":
            assert record.prior is not None
            payload = canonical_runtime_authority_bytes(
                replace(record, prior=replace(record.prior, commit="untrusted-tamper"))
            )
        else:
            payload = canonical_runtime_authority_bytes(record) + b" "
        path.chmod(0o644)
        path.write_bytes(payload)
        path.chmod(0o444)
        raise OSError("injected parent fsync failure")

    monkeypatch.setattr(authority_module, "_fsync_descriptor", fail_after_tamper)

    with pytest.raises(RuntimeAuthorityPublishError):
        publish_runtime_authority(record)


def test_hyb1_p1_04_deployment_lock_serializes_two_processes_from_predecessor_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, generation_root = _install_authority_fixture(tmp_path, monkeypatch)
    previous = _record(generation_root)
    path.write_bytes(canonical_runtime_authority_bytes(previous))
    path.chmod(0o444)
    first = prepare_runtime_authority_publish(
        previous,
        _record(generation_root, current_marker="first").current,
        operation_id="2" * 32,
    )
    stale_second = prepare_runtime_authority_publish(
        previous,
        _record(generation_root, current_marker="second").current,
        operation_id="3" * 32,
    )
    context = multiprocessing.get_context("fork")
    first_entered = context.Event()
    second_entered = context.Event()
    release_first = context.Event()
    unused_release = context.Event()
    results = context.Queue()
    first_process = context.Process(
        target=_publish_worker,
        args=(first, first_entered, release_first, results, True),
    )
    second_process = context.Process(
        target=_publish_worker,
        args=(stale_second, second_entered, unused_release, results, False),
    )
    try:
        first_process.start()
        assert first_entered.wait(5)
        second_process.start()
        assert not second_entered.wait(0.5)
        release_first.set()
        first_process.join(5)
        second_process.join(5)
        assert first_process.exitcode == 0
        assert second_process.exitcode == 0
        observed = sorted(results.get(timeout=1) for _ in range(2))
        assert observed == [
            (
                "error",
                "RuntimeAuthorityPublishError",
                "authority sequence must advance by one",
            ),
            ("ok", "committed"),
        ]
        assert load_runtime_authority() == first
    finally:
        release_first.set()
        for process in (first_process, second_process):
            if process.is_alive():
                process.terminate()
            process.join(1)


@pytest.mark.parametrize("fault", ("symlink", "hardlink", "mode", "owner"))
def test_hyb1_p1_04_deployment_lock_rejects_unsafe_inode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    path, generation_root = _install_authority_fixture(tmp_path, monkeypatch)
    lock_path = authority_module.RUNTIME_AUTHORITY_LOCK_PATH
    if fault == "symlink":
        real = lock_path.with_name("real.lock")
        real.write_bytes(b"")
        real.chmod(0o600)
        lock_path.symlink_to(real.name)
    else:
        lock_path.write_bytes(b"")
        lock_path.chmod(0o600)
        if fault == "hardlink":
            os.link(lock_path, lock_path.with_name("alias.lock"))
        elif fault == "mode":
            lock_path.chmod(0o644)
        else:
            monkeypatch.setattr(
                authority_module,
                "RUNTIME_AUTHORITY_OWNER_UID",
                os.getuid() + 1,
            )

    with pytest.raises(RuntimeAuthorityPublishError, match="deployment lock"):
        publish_runtime_authority(_record(generation_root, durable=False))
    assert not path.exists()


def test_hyb1_p1_04_deployment_lock_replacement_aborts_before_record_rename(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, generation_root = _install_authority_fixture(tmp_path, monkeypatch)
    lock_path = authority_module.RUNTIME_AUTHORITY_LOCK_PATH
    original_write = authority_module._write_all

    def replace_lock_after_write(descriptor: int, payload: bytes) -> None:
        original_write(descriptor, payload)
        displaced = lock_path.with_name("displaced.lock")
        lock_path.rename(displaced)
        lock_path.write_bytes(b"")
        lock_path.chmod(0o600)

    monkeypatch.setattr(authority_module, "_write_all", replace_lock_after_write)

    with pytest.raises(RuntimeAuthorityPublishError, match="lock identity"):
        publish_runtime_authority(_record(generation_root))
    assert not path.exists()


def test_hyb1_p1_08_publication_rejects_missing_generation_without_side_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, generation_root = _install_authority_fixture(tmp_path, monkeypatch)

    with pytest.raises(RuntimeAuthorityPublishError, match="generation"):
        publish_runtime_authority(_record(generation_root, durable=False))

    assert not path.exists()
    assert not list(path.parent.glob(".current.*.tmp"))


@pytest.mark.parametrize(
    "fault",
    ("missing", "symlink", "hardlink", "mode", "tampered"),
)
def test_hyb1_p1_08_publication_rejects_unsafe_or_tampered_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    path, generation_root = _install_authority_fixture(tmp_path, monkeypatch)
    record = _record(generation_root)
    manifest = record.current.generation_path / authority_module.GENERATION_MANIFEST_NAME
    if fault == "missing":
        manifest.unlink()
    elif fault == "symlink":
        real = manifest.with_name("real-manifest.json")
        manifest.rename(real)
        manifest.symlink_to(real.name)
    elif fault == "hardlink":
        os.link(manifest, manifest.with_name("manifest-alias.json"))
    elif fault == "mode":
        manifest.chmod(0o644)
    else:
        manifest.chmod(0o644)
        manifest.write_bytes(b"{}\n")
        manifest.chmod(0o444)

    with pytest.raises(RuntimeAuthorityPublishError, match="manifest"):
        publish_runtime_authority(record)
    assert not path.exists()
    assert not list(path.parent.glob(".current.*.tmp"))


def test_hyb1_p1_08_manifest_entries_exactly_cover_materialized_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, generation_root = _install_authority_fixture(tmp_path, monkeypatch)
    record = _record(generation_root)
    generation = record.current.generation_path

    assert (generation / "venv/bin/python").is_file()
    assert (generation / "release").is_dir()
    assert (generation / "release/src/rquant/runtime_service_main.py").is_file()
    assert (generation / "venv/lib/python3.11/site-packages").is_dir()
    assert publish_runtime_authority(record).value == "committed"
    assert path.exists()


@pytest.mark.parametrize(
    "fault",
    (
        "missing",
        "extra",
        "symlink",
        "hardlink",
        "mode",
        "bytes",
        "directory-mode",
        "fifo",
        "socket",
    ),
)
def test_hyb1_p1_08_generation_tree_must_exactly_match_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    path, generation_root = _install_authority_fixture(tmp_path, monkeypatch)
    record = _record(generation_root)
    generation = record.current.generation_path
    target = generation / "release/src/rquant/runtime_service_main.py"
    parent = target.parent
    opened_socket: socket.socket | None = None
    if fault == "missing":
        parent.chmod(0o755)
        target.unlink()
        parent.chmod(0o555)
    elif fault == "extra":
        parent.chmod(0o755)
        extra = parent / "extra.py"
        extra.write_bytes(b"pass\n")
        extra.chmod(0o444)
        parent.chmod(0o555)
    elif fault == "symlink":
        parent.chmod(0o755)
        target.unlink()
        target.symlink_to("__init__.py")
        parent.chmod(0o555)
    elif fault == "hardlink":
        parent.chmod(0o755)
        os.link(target, parent / "alias.py")
        parent.chmod(0o555)
    elif fault == "mode":
        target.chmod(0o644)
    elif fault == "bytes":
        target.chmod(0o644)
        target.write_bytes(b"def main():\n    return 1\n")
        target.chmod(0o444)
    elif fault == "directory-mode":
        (generation / "release/src").chmod(0o755)
    elif fault == "fifo":
        parent.chmod(0o755)
        os.mkfifo(parent / "injected.fifo", mode=0o444)
        parent.chmod(0o555)
    else:
        parent.chmod(0o755)
        opened_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        monkeypatch.chdir(parent)
        opened_socket.bind("injected.socket")
        parent.chmod(0o555)

    try:
        with pytest.raises(RuntimeAuthorityPublishError, match="generation"):
            publish_runtime_authority(record)
    finally:
        if opened_socket is not None:
            opened_socket.close()

    assert not path.exists()


@pytest.mark.parametrize("fault", ("profile", "roles"))
def test_hyb1_p1_08_manifest_must_match_loaded_profile_and_slot_roles(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    path, generation_root = _install_authority_fixture(tmp_path, monkeypatch)
    slot = _materialize_generation_slot(
        generation_root,
        fault,
        authority_module.RuntimeGenerationLifecycle.ACTIVE,
        manifest_profile_id="0" * 64 if fault == "profile" else None,
        manifest_module="rquant.wrong_module" if fault == "roles" else None,
    )
    record = RuntimeAuthorityRecord(
        schema_version=1,
        operation_id="1" * 32,
        sequence=1,
        state=RuntimeAuthorityState.ACTIVE,
        current=slot,
        prior=None,
    )

    with pytest.raises(RuntimeAuthorityPublishError, match="manifest"):
        publish_runtime_authority(record)
    assert not path.exists()


def test_hyb1_p1_08_forward_revalidates_prior_generation_before_activation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, generation_root = _install_authority_fixture(tmp_path, monkeypatch)
    previous = _record(generation_root)
    path.write_bytes(canonical_runtime_authority_bytes(previous))
    path.chmod(0o444)
    candidate = prepare_runtime_authority_publish(
        previous,
        _record(generation_root, current_marker="next").current,
        operation_id="2" * 32,
    )
    prior_manifest = previous.current.generation_path / authority_module.GENERATION_MANIFEST_NAME
    prior_manifest.unlink()

    with pytest.raises(RuntimeAuthorityPublishError, match="manifest"):
        publish_runtime_authority(candidate)
    assert path.read_bytes() == canonical_runtime_authority_bytes(previous)


@pytest.mark.parametrize("replaced", ("manifest", "generation"))
def test_hyb1_p1_08_expired_evidence_rejects_pre_rename_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    replaced: str,
) -> None:
    path, generation_root = _install_authority_fixture(tmp_path, monkeypatch)
    record = _record(generation_root)
    generation = record.current.generation_path
    manifest = generation / authority_module.GENERATION_MANIFEST_NAME
    original_write = authority_module._write_all

    def replace_after_evidence(descriptor: int, payload: bytes) -> None:
        original_write(descriptor, payload)
        if replaced == "manifest":
            original = manifest.with_name("old-manifest.json")
            manifest.rename(original)
            manifest.write_bytes(original.read_bytes())
            manifest.chmod(0o444)
        else:
            original = generation.with_name(f"{generation.name}.old")
            generation.rename(original)
            generation.mkdir(mode=0o700)
            replacement = generation / authority_module.GENERATION_MANIFEST_NAME
            replacement.write_bytes(
                (original / authority_module.GENERATION_MANIFEST_NAME).read_bytes()
            )
            replacement.chmod(0o444)

    monkeypatch.setattr(authority_module, "_write_all", replace_after_evidence)

    with pytest.raises(RuntimeAuthorityPublishError, match="evidence expired"):
        publish_runtime_authority(record)
    assert not path.exists()
    assert len(list(path.parent.glob(".current.*.tmp"))) == 1


@pytest.mark.parametrize("replaced", ("inode", "bytes", "extra"))
def test_hyb1_p1_08_evidence_detects_nested_tree_replacement_before_rename(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    replaced: str,
) -> None:
    path, generation_root = _install_authority_fixture(tmp_path, monkeypatch)
    record = _record(generation_root)
    target = record.current.generation_path / "release/src/rquant/runtime_service_main.py"
    parent = target.parent
    original_write = authority_module._write_all

    def replace_after_evidence(descriptor: int, payload: bytes) -> None:
        original_write(descriptor, payload)
        if replaced == "inode":
            parent.chmod(0o755)
            old = target.with_suffix(".old")
            target.rename(old)
            target.write_bytes(old.read_bytes())
            target.chmod(0o444)
            old.unlink()
            parent.chmod(0o555)
        elif replaced == "bytes":
            target.chmod(0o644)
            target.write_bytes(b"def main():\n    return 2\n")
            target.chmod(0o444)
        else:
            parent.chmod(0o755)
            extra = parent / "late.py"
            extra.write_bytes(b"pass\n")
            extra.chmod(0o444)
            parent.chmod(0o555)

    monkeypatch.setattr(authority_module, "_write_all", replace_after_evidence)

    with pytest.raises(RuntimeAuthorityPublishError, match="evidence|generation"):
        publish_runtime_authority(record)
    assert not path.exists()


def test_hyb1_p1_08_durable_evidence_is_private_sealed_and_not_exported(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _path, generation_root = _install_authority_fixture(tmp_path, monkeypatch)
    slot = _record(generation_root).current

    with pytest.raises(RuntimeAuthorityPublishError, match="cannot be constructed"):
        authority_module._DurableGenerationEvidence(
            seal=object(),
            slot=slot,
            generation_identity=(),
            manifest_identity=(),
            tree_identities=(),
        )
    assert "_DurableGenerationEvidence" not in authority_module.__all__


def test_hyb1_p1_06_serializer_and_role_collections_are_bounded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _path, generation_root = _install_authority_fixture(tmp_path, monkeypatch)
    payload = _record_payload(generation_root)
    payload["current_roles"]["daily"]["site_packages"] = [
        str(generation_root / payload["current_generation_id"] / "venv" / f"site-{index}")
        for index in range(authority_module.MAX_SITE_PACKAGES + 1)
    ]
    with pytest.raises(RuntimeAuthorityRecordError, match="site-packages"):
        parse_runtime_authority_record(canonical_json_bytes(payload))

    payload = _record_payload(generation_root)
    payload["current_roles"]["daily"]["module"] = "rquant." + "a" * (
        authority_module.MAX_MODULE_BYTES
    )
    with pytest.raises(RuntimeAuthorityRecordError, match="module is too long"):
        parse_runtime_authority_record(canonical_json_bytes(payload))

    payload = _record_payload(generation_root)
    payload["current_commit"] = "x" * (authority_module.MAX_COMMIT_BYTES + 1)
    with pytest.raises(RuntimeAuthorityRecordError, match="commit"):
        parse_runtime_authority_record(canonical_json_bytes(payload))

    record = _record(generation_root)
    encoded = canonical_runtime_authority_bytes(record)
    monkeypatch.setattr(authority_module, "MAX_RECORD_BYTES", len(encoded) - 1)
    with pytest.raises(RuntimeAuthorityRecordError, match="too large"):
        canonical_runtime_authority_bytes(record)
