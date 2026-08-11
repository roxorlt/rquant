from __future__ import annotations

import base64
import json
import os
import runpy
import stat
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
HELPER = ROOT / "deploy" / "libexec" / "rquant-runtime-credential-sealer"
GENERATION = "b" * 64
SERVICE_ID = "source.market-minute"
SERVICE_KIND = "market_minute_source"


def _request(
    instances: tuple[str, ...],
    *,
    token: str = "secret",
    credential_instance: str | None = None,
    service_kind: str = SERVICE_KIND,
    generation: str = GENERATION,
) -> bytes:
    credentials: dict[str, str] = {}
    for instance in instances:
        credential = json.dumps(
            {
                "schema_version": 2,
                "service_id": SERVICE_ID,
                "service_kind": service_kind,
                "instance_name": credential_instance or instance,
                "bundle_generation": generation,
                "capabilities": {"TUSHARE_TOKEN_MAIN": token},
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        credentials[instance] = base64.b64encode(credential).decode()
    return json.dumps(
        {
            "schema_version": 2,
            "operation": "begin",
            "credentials": credentials,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _recover_request(
    instances: tuple[str, ...],
    *,
    generation: str,
    action: str,
) -> bytes:
    return json.dumps(
        {
            "schema_version": 2,
            "operation": "recover",
            "bundle_generation": generation,
            "instances": list(instances),
            "action": action,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def test_root_helper_seals_idempotent_generation_and_switches_scoped_pointers(
    tmp_path: Path,
) -> None:
    module = runpy.run_path(str(HELPER))
    process_request = module["process_request"]
    instances = ("svc-" + "a" * 64, "svc-" + "c" * 64)
    encrypted: list[bytes] = []

    def encrypt(payload: bytes) -> bytes:
        encrypted.append(payload)
        return b"encrypted:" + payload

    def decrypt(payload: bytes) -> bytes:
        return payload.removeprefix(b"encrypted:")

    first = process_request(
        _request(instances),
        store_root=tmp_path / "credstore",
        owner_uid=os.getuid(),
        encrypt=encrypt,
        decrypt=decrypt,
    )
    assert first["operation"] == "begin"
    assert tuple(first["sealed_instances"]) == instances
    assert len(encrypted) == 2

    for instance in instances:
        instance_root = tmp_path / "credstore" / "instances" / instance
        credential = instance_root / "generations" / f"{GENERATION}.cred"
        pointer = instance_root / "current.cred"
        assert stat.S_IMODE(credential.stat().st_mode) == 0o600
        assert pointer.is_symlink()
        assert os.readlink(pointer) == f"generations/{GENERATION}.cred"

    process_request(
        json.dumps(
            {
                "schema_version": 2,
                "operation": "commit",
                "transaction_id": first["transaction_id"],
            }
        ).encode(),
        store_root=tmp_path / "credstore",
        owner_uid=os.getuid(),
        encrypt=encrypt,
        decrypt=decrypt,
    )
    second = process_request(
        _request(instances),
        store_root=tmp_path / "credstore",
        owner_uid=os.getuid(),
        encrypt=encrypt,
        decrypt=decrypt,
    )
    process_request(
        json.dumps(
            {
                "schema_version": 2,
                "operation": "commit",
                "transaction_id": second["transaction_id"],
            }
        ).encode(),
        store_root=tmp_path / "credstore",
        owner_uid=os.getuid(),
        encrypt=encrypt,
        decrypt=decrypt,
    )
    assert len(encrypted) == 2


def test_root_helper_rejects_mixed_or_tampered_generation(tmp_path: Path) -> None:
    module = runpy.run_path(str(HELPER))
    process_request = module["process_request"]
    instance = "svc-" + "a" * 64
    root = tmp_path / "credstore"
    transaction = process_request(
        _request((instance,)),
        store_root=root,
        owner_uid=os.getuid(),
        encrypt=lambda payload: b"encrypted:" + payload,
        decrypt=lambda payload: payload.removeprefix(b"encrypted:"),
    )
    process_request(
        json.dumps(
            {
                "schema_version": 2,
                "operation": "commit",
                "transaction_id": transaction["transaction_id"],
            }
        ).encode(),
        store_root=root,
        owner_uid=os.getuid(),
    )
    target = root / "instances" / instance / "generations" / f"{GENERATION}.cred"
    target.write_bytes(b"tampered")
    target.chmod(0o600)

    with pytest.raises(ValueError, match="unsafe"):
        process_request(
            _request((instance,)),
            store_root=root,
            owner_uid=os.getuid(),
            encrypt=lambda payload: b"encrypted:" + payload,
            decrypt=lambda payload: payload.removeprefix(b"encrypted:"),
        )


def test_root_helper_rejects_credential_bound_to_another_instance(tmp_path: Path) -> None:
    module = runpy.run_path(str(HELPER))
    instance = "svc-" + "a" * 64

    with pytest.raises(ValueError, match="instance"):
        module["process_request"](
            _request((instance,), credential_instance="svc-" + "c" * 64),
            store_root=tmp_path / "credstore",
            owner_uid=os.getuid(),
            encrypt=lambda payload: b"encrypted:" + payload,
            decrypt=lambda payload: payload.removeprefix(b"encrypted:"),
        )


def test_root_helper_rejects_all_command_line_arguments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = runpy.run_path(str(HELPER))
    monkeypatch.setattr(sys, "argv", [str(HELPER), "--unexpected"])

    with pytest.raises(ValueError, match="does not accept arguments"):
        module["main"]()


def test_root_helper_rejects_retired_paper_consumer_credentials(tmp_path: Path) -> None:
    module = runpy.run_path(str(HELPER))
    instance = "svc-" + "a" * 64

    with pytest.raises(ValueError, match="service kind"):
        module["process_request"](
            _request((instance,), service_kind="paper_consumer"),
            store_root=tmp_path / "credstore",
            owner_uid=os.getuid(),
            encrypt=lambda payload: b"encrypted:" + payload,
            decrypt=lambda payload: payload.removeprefix(b"encrypted:"),
        )


@pytest.mark.parametrize(
    "service_kind",
    (
        "paper_constraint_publisher",
        "runtime_health_publisher",
        "lab_jobs_publisher",
        "promotions_publisher",
    ),
)
def test_root_helper_accepts_authority_publisher_credentials(
    tmp_path: Path,
    service_kind: str,
) -> None:
    module = runpy.run_path(str(HELPER))
    instance = "svc-" + "a" * 64

    receipt = module["process_request"](
        _request((instance,), service_kind=service_kind),
        store_root=tmp_path / "credstore",
        owner_uid=os.getuid(),
        encrypt=lambda payload: b"encrypted:" + payload,
        decrypt=lambda payload: payload.removeprefix(b"encrypted:"),
    )

    assert receipt["operation"] == "begin"
    assert receipt["sealed_instances"] == [instance]


def test_root_helper_rollback_restores_multiple_previous_pointers_and_fsyncs(
    tmp_path: Path,
) -> None:
    module = runpy.run_path(str(HELPER))
    process_request = module["process_request"]
    instances = ("svc-" + "a" * 64, "svc-" + "c" * 64)
    root = tmp_path / "credstore"
    fsynced: list[Path] = []
    fsync_directory = module["_fsync_directory"]

    def track_fsync(path: Path) -> None:
        fsynced.append(path)
        fsync_directory(path)

    process_request.__globals__["_fsync_directory"] = track_fsync
    old = process_request(
        _request(instances, generation="a" * 64),
        store_root=root,
        owner_uid=os.getuid(),
        encrypt=lambda payload: b"encrypted:" + payload,
        decrypt=lambda payload: payload.removeprefix(b"encrypted:"),
    )
    process_request(
        json.dumps(
            {
                "schema_version": 2,
                "operation": "commit",
                "transaction_id": old["transaction_id"],
            }
        ).encode(),
        store_root=root,
        owner_uid=os.getuid(),
    )
    new = process_request(
        _request(instances, generation="d" * 64),
        store_root=root,
        owner_uid=os.getuid(),
        encrypt=lambda payload: b"encrypted:" + payload,
        decrypt=lambda payload: payload.removeprefix(b"encrypted:"),
    )

    process_request(
        json.dumps(
            {
                "schema_version": 2,
                "operation": "rollback",
                "transaction_id": new["transaction_id"],
            }
        ).encode(),
        store_root=root,
        owner_uid=os.getuid(),
    )

    for instance in instances:
        instance_root = root / "instances" / instance
        assert os.readlink(instance_root / "current.cred") == ("generations/" + "a" * 64 + ".cred")
        assert instance_root in fsynced


def test_root_helper_partial_switch_failure_restores_every_pointer(
    tmp_path: Path,
) -> None:
    module = runpy.run_path(str(HELPER))
    process_request = module["process_request"]
    instances = ("svc-" + "a" * 64, "svc-" + "c" * 64)
    root = tmp_path / "credstore"
    old = process_request(
        _request(instances, generation="a" * 64),
        store_root=root,
        owner_uid=os.getuid(),
        encrypt=lambda payload: b"encrypted:" + payload,
        decrypt=lambda payload: payload.removeprefix(b"encrypted:"),
    )
    process_request(
        json.dumps(
            {
                "schema_version": 2,
                "operation": "commit",
                "transaction_id": old["transaction_id"],
            }
        ).encode(),
        store_root=root,
        owner_uid=os.getuid(),
    )
    replace_pointer = module["_replace_pointer"]
    switches = 0

    def fail_second_switch(pointer: Path, target: str) -> None:
        nonlocal switches
        if target.endswith(("d" * 64) + ".cred"):
            switches += 1
            if switches == 2:
                raise OSError("second pointer switch failed")
        replace_pointer(pointer, target)

    process_request.__globals__["_replace_pointer"] = fail_second_switch

    with pytest.raises(OSError, match="second pointer switch failed"):
        process_request(
            _request(instances, generation="d" * 64),
            store_root=root,
            owner_uid=os.getuid(),
            encrypt=lambda payload: b"encrypted:" + payload,
            decrypt=lambda payload: payload.removeprefix(b"encrypted:"),
        )

    for instance in instances:
        assert os.readlink(root / "instances" / instance / "current.cred") == (
            "generations/" + "a" * 64 + ".cred"
        )


def test_root_helper_rollback_failure_keeps_fail_closed_transaction_audit(
    tmp_path: Path,
) -> None:
    module = runpy.run_path(str(HELPER))
    process_request = module["process_request"]
    instance = "svc-" + "a" * 64
    root = tmp_path / "credstore"
    old = process_request(
        _request((instance,), generation="a" * 64),
        store_root=root,
        owner_uid=os.getuid(),
        encrypt=lambda payload: b"encrypted:" + payload,
        decrypt=lambda payload: payload.removeprefix(b"encrypted:"),
    )
    process_request(
        json.dumps(
            {
                "schema_version": 2,
                "operation": "commit",
                "transaction_id": old["transaction_id"],
            }
        ).encode(),
        store_root=root,
        owner_uid=os.getuid(),
    )
    transaction = process_request(
        _request((instance,), generation="d" * 64),
        store_root=root,
        owner_uid=os.getuid(),
        encrypt=lambda payload: b"encrypted:" + payload,
        decrypt=lambda payload: payload.removeprefix(b"encrypted:"),
    )
    process_request.__globals__["_replace_pointer"] = lambda *_args, **_kwargs: (
        _ for _ in ()
    ).throw(OSError("rollback pointer failure"))

    with pytest.raises(RuntimeError, match="fail.closed|rollback"):
        process_request(
            json.dumps(
                {
                    "schema_version": 2,
                    "operation": "rollback",
                    "transaction_id": transaction["transaction_id"],
                }
            ).encode(),
            store_root=root,
            owner_uid=os.getuid(),
        )

    audits = tuple((root / "transactions" / "active").glob("*.json"))
    assert len(audits) == 1
    assert json.loads(audits[0].read_text())["state"] == "rollback_failed"


def test_root_helper_recovers_crash_before_runtime_publish_by_exact_rollback(
    tmp_path: Path,
) -> None:
    module = runpy.run_path(str(HELPER))
    process_request = module["process_request"]
    instances = ("svc-" + "a" * 64, "svc-" + "c" * 64)
    root = tmp_path / "credstore"
    old = process_request(
        _request(instances, generation="a" * 64),
        store_root=root,
        owner_uid=os.getuid(),
        encrypt=lambda payload: b"encrypted:" + payload,
        decrypt=lambda payload: payload.removeprefix(b"encrypted:"),
    )
    process_request(
        json.dumps(
            {
                "schema_version": 2,
                "operation": "commit",
                "transaction_id": old["transaction_id"],
            }
        ).encode(),
        store_root=root,
        owner_uid=os.getuid(),
    )
    crashed = process_request(
        _request(instances, generation="d" * 64),
        store_root=root,
        owner_uid=os.getuid(),
        encrypt=lambda payload: b"encrypted:" + payload,
        decrypt=lambda payload: payload.removeprefix(b"encrypted:"),
    )

    recovered = process_request(
        _recover_request(instances, generation="d" * 64, action="rollback"),
        store_root=root,
        owner_uid=os.getuid(),
        decrypt=lambda payload: payload.removeprefix(b"encrypted:"),
    )

    assert recovered == {
        "schema_version": 2,
        "operation": "recover",
        "bundle_generation": "d" * 64,
        "sealed_instances": list(instances),
        "outcome": "rolled_back",
        "transaction_id": crashed["transaction_id"],
    }
    for instance in instances:
        assert os.readlink(root / "instances" / instance / "current.cred") == (
            "generations/" + "a" * 64 + ".cred"
        )
    assert not tuple((root / "transactions" / "active").glob("*.json"))


def test_root_helper_recovers_crash_after_runtime_publish_by_exact_commit(
    tmp_path: Path,
) -> None:
    module = runpy.run_path(str(HELPER))
    process_request = module["process_request"]
    instance = "svc-" + "a" * 64
    root = tmp_path / "credstore"
    crashed = process_request(
        _request((instance,), generation="d" * 64),
        store_root=root,
        owner_uid=os.getuid(),
        encrypt=lambda payload: b"encrypted:" + payload,
        decrypt=lambda payload: payload.removeprefix(b"encrypted:"),
    )

    recovered = process_request(
        _recover_request((instance,), generation="d" * 64, action="commit"),
        store_root=root,
        owner_uid=os.getuid(),
        decrypt=lambda payload: payload.removeprefix(b"encrypted:"),
    )

    assert recovered["outcome"] == "committed"
    assert recovered["transaction_id"] == crashed["transaction_id"]
    assert os.readlink(root / "instances" / instance / "current.cred") == (
        "generations/" + "d" * 64 + ".cred"
    )
    assert not tuple((root / "transactions" / "active").glob("*.json"))


def test_root_helper_recovery_rolls_back_a_prepared_partial_pointer_switch(
    tmp_path: Path,
) -> None:
    module = runpy.run_path(str(HELPER))
    process_request = module["process_request"]
    instances = ("svc-" + "a" * 64, "svc-" + "c" * 64)
    root = tmp_path / "credstore"
    old = process_request(
        _request(instances, generation="a" * 64),
        store_root=root,
        owner_uid=os.getuid(),
        encrypt=lambda payload: b"encrypted:" + payload,
        decrypt=lambda payload: payload.removeprefix(b"encrypted:"),
    )
    process_request(
        json.dumps(
            {
                "schema_version": 2,
                "operation": "commit",
                "transaction_id": old["transaction_id"],
            }
        ).encode(),
        store_root=root,
        owner_uid=os.getuid(),
    )
    process_request(
        _request(instances, generation="d" * 64),
        store_root=root,
        owner_uid=os.getuid(),
        encrypt=lambda payload: b"encrypted:" + payload,
        decrypt=lambda payload: payload.removeprefix(b"encrypted:"),
    )
    active = next((root / "transactions" / "active").glob("*.json"))
    record = json.loads(active.read_text())
    record["state"] = "prepared"
    active.write_text(json.dumps(record, separators=(",", ":"), sort_keys=True))
    active.chmod(0o600)
    partially_unswitched = root / "instances" / instances[0] / "current.cred"
    partially_unswitched.unlink()
    partially_unswitched.symlink_to("generations/" + "a" * 64 + ".cred")
    (root / "instances" / instances[0] / "generations" / ("d" * 64 + ".cred")).unlink()

    recovered = process_request(
        _recover_request(instances, generation="d" * 64, action="rollback"),
        store_root=root,
        owner_uid=os.getuid(),
        decrypt=lambda payload: payload.removeprefix(b"encrypted:"),
    )

    assert recovered["outcome"] == "rolled_back"
    for instance in instances:
        assert os.readlink(root / "instances" / instance / "current.cred") == (
            "generations/" + "a" * 64 + ".cred"
        )
        assert not (root / "instances" / instance / "generations" / ("d" * 64 + ".cred")).exists()


@pytest.mark.parametrize(
    "mutation",
    ("generation", "instances", "record", "credential", "ambiguous"),
)
def test_root_helper_recovery_fails_closed_on_non_exact_active_transaction(
    tmp_path: Path,
    mutation: str,
) -> None:
    module = runpy.run_path(str(HELPER))
    process_request = module["process_request"]
    instance = "svc-" + "a" * 64
    root = tmp_path / "credstore"
    process_request(
        _request((instance,), generation="d" * 64),
        store_root=root,
        owner_uid=os.getuid(),
        encrypt=lambda payload: b"encrypted:" + payload,
        decrypt=lambda payload: payload.removeprefix(b"encrypted:"),
    )
    active_root = root / "transactions" / "active"
    generation = "d" * 64
    instances = (instance,)
    if mutation == "generation":
        generation = "e" * 64
    elif mutation == "instances":
        instances = ("svc-" + "c" * 64,)
    elif mutation == "record":
        active = next(active_root.glob("*.json"))
        active.write_text("{}")
        active.chmod(0o600)
    elif mutation == "credential":
        target = root / "instances" / instance / "generations" / ("d" * 64 + ".cred")
        target.chmod(0o644)
    else:
        active = next(active_root.glob("*.json"))
        duplicate = active_root / ("f" * 64 + ".json")
        duplicate.write_bytes(active.read_bytes())
        duplicate.chmod(0o600)

    with pytest.raises((RuntimeError, ValueError), match="recover|transaction|ambiguous|invalid"):
        process_request(
            _recover_request(instances, generation=generation, action="rollback"),
            store_root=root,
            owner_uid=os.getuid(),
            decrypt=lambda payload: payload.removeprefix(b"encrypted:"),
        )

    assert tuple(active_root.glob("*.json"))
    assert (root / "instances" / instance / "current.cred").is_symlink()
