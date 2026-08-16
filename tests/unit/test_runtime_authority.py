from __future__ import annotations

import hashlib
import inspect
import os
from pathlib import Path

import pytest

import rquant.runtime_authority as authority_module
from rquant.runtime_authority import (
    ProductionRuntimeProfileError,
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
) -> dict[str, object]:
    generation_id = _digest(marker)
    generation = generation_root / generation_id
    return {
        "generation_id": generation_id,
        "generation_path": str(generation),
        "commit": f"untrusted-{marker}",
        "full_manifest_hash": generation_id,
        "profile_id": _digest(f"profile-{marker}"),
        "roles": {"daily": _role_payload(generation)},
    }


def _record_payload(
    generation_root: Path,
    *,
    operation: str = "1",
    current_marker: str = "current",
    prior_marker: str | None = None,
    state: str = "active",
) -> dict[str, object]:
    current = _slot_payload(generation_root, current_marker)
    prior = None if prior_marker is None else _slot_payload(generation_root, prior_marker)
    payload: dict[str, object] = {
        "schema_version": 1,
        "operation_id": operation * 32,
        "state": state,
    }
    for prefix, slot in (("current", current), ("prior", prior)):
        for field in (
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
) -> RuntimeAuthorityRecord:
    return parse_runtime_authority_record(
        canonical_json_bytes(
            _record_payload(
                generation_root,
                operation=operation,
                current_marker=current_marker,
                prior_marker=prior_marker,
                state=state,
            )
        ),
        generation_root=generation_root,
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
    monkeypatch.setattr(authority_module, "RUNTIME_AUTHORITY_ANCHOR", anchor)
    monkeypatch.setattr(authority_module, "RUNTIME_AUTHORITY_PATH", path)
    monkeypatch.setattr(authority_module, "PRODUCTION_GENERATION_ROOT", generations)
    monkeypatch.setattr(authority_module, "RUNTIME_AUTHORITY_OWNER_UID", os.getuid())
    monkeypatch.setattr(authority_module, "RUNTIME_AUTHORITY_DIRECTORY_MODE", 0o700)
    if record is not None:
        path.write_bytes(canonical_runtime_authority_bytes(record))
        path.chmod(0o444)
    return path, generations


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


def test_record_v1_parses_complete_current_and_nullable_prior(tmp_path: Path) -> None:
    generation_root = tmp_path / "generations"
    generation_root.mkdir()
    record = _record(generation_root)

    assert record.state is RuntimeAuthorityState.ACTIVE
    assert record.prior is None
    assert record.current.generation_id == record.current.full_manifest_hash
    assert tuple(record.current.roles) == ("daily",)

    with_prior = _record(generation_root, prior_marker="prior")
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
    mutation: object,
    match: str,
) -> None:
    generation_root = tmp_path / "generations"
    generation_root.mkdir()
    payload = _record_payload(generation_root)
    mutation(payload, generation_root)

    with pytest.raises(RuntimeAuthorityRecordError, match=match):
        parse_runtime_authority_record(
            canonical_json_bytes(payload),
            generation_root=generation_root,
        )


def test_record_rejects_duplicate_current_prior_and_role_escape(tmp_path: Path) -> None:
    generation_root = tmp_path / "generations"
    generation_root.mkdir()
    payload = _record_payload(generation_root, prior_marker="prior")
    for field in (
        "generation_id",
        "generation_path",
        "commit",
        "full_manifest_hash",
        "profile_id",
        "roles",
    ):
        payload[f"prior_{field}"] = payload[f"current_{field}"]
    with pytest.raises(RuntimeAuthorityRecordError, match="same generation"):
        parse_runtime_authority_record(
            canonical_json_bytes(payload), generation_root=generation_root
        )

    payload = _record_payload(generation_root)
    payload["current_roles"]["daily"]["app_source"] = str(tmp_path / "outside")
    with pytest.raises(RuntimeAuthorityRecordError, match="role path"):
        parse_runtime_authority_record(
            canonical_json_bytes(payload), generation_root=generation_root
        )


def test_record_rejects_generation_symlink_escape(tmp_path: Path) -> None:
    generation_root = tmp_path / "generations"
    generation_root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    marker = "current"
    generation_id = _digest(marker)
    (generation_root / generation_id).symlink_to(outside, target_is_directory=True)

    with pytest.raises(RuntimeAuthorityRecordError, match="symbolic"):
        _record(generation_root, current_marker=marker)


def test_publish_transition_moves_current_to_prior_and_requires_monotonic_operation(
    tmp_path: Path,
) -> None:
    generation_root = tmp_path / "generations"
    generation_root.mkdir()
    old = _record(generation_root, operation="1")
    next_slot = _record(generation_root, operation="2", current_marker="next").current

    advanced = prepare_runtime_authority_publish(
        old,
        next_slot,
        operation_id="2" * 32,
    )
    assert advanced.state is RuntimeAuthorityState.ACTIVE
    assert advanced.current == next_slot
    assert advanced.prior == old.current

    with pytest.raises(RuntimeAuthorityRecordError, match="monotonic"):
        prepare_runtime_authority_publish(old, next_slot, operation_id="0" * 32)
    with pytest.raises(RuntimeAuthorityRecordError, match="already recorded"):
        prepare_runtime_authority_publish(old, old.current, operation_id="2" * 32)


def test_first_publish_and_single_level_rollback_are_deterministic(tmp_path: Path) -> None:
    generation_root = tmp_path / "generations"
    generation_root.mkdir()
    initial_slot = _record(generation_root).current
    initial = prepare_runtime_authority_publish(None, initial_slot, operation_id="1" * 32)
    assert initial.prior is None

    next_slot = _record(generation_root, current_marker="next").current
    advanced = prepare_runtime_authority_publish(initial, next_slot, operation_id="2" * 32)
    rolled_back = prepare_runtime_authority_rollback(advanced, operation_id="3" * 32)
    assert rolled_back.state is RuntimeAuthorityState.ROLLED_BACK
    assert rolled_back.current == initial.current
    assert rolled_back.prior == next_slot

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

    visible = parse_runtime_authority_record(path.read_bytes(), generation_root=generations)
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
    assert "listdir" not in Path(authority_module.__file__).read_text(encoding="utf-8")


def test_runtime_authority_public_models_are_frozen_stdlib_dataclasses() -> None:
    generation_root = Path("/var/lib/rquant/runtime-authority/generations")
    record = _record(generation_root)

    with pytest.raises((AttributeError, TypeError)):
        record.operation_id = "f" * 32
    assert isinstance(record.current, RuntimeGenerationSlot)
    source = Path(authority_module.__file__).read_text(encoding="utf-8")
    assert "pydantic" not in source
    assert "getenv" not in source
    assert "environ" not in source
