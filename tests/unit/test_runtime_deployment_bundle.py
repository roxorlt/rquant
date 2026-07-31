from __future__ import annotations

import hashlib
import json
import os
import stat
from pathlib import Path

import pytest

from rquant.runtime_deployment_bundle import install_runtime_deployment_bundle
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import (
    RuntimeServiceKind,
    RuntimeServiceManifest,
)

COMMIT = "a" * 40


def _manifest(
    root: Path,
    *,
    service_id: str,
    kind: RuntimeServiceKind,
    plane: RuntimeServicePlane,
    interval_seconds: float = 1,
) -> RuntimeServiceManifest:
    if kind is RuntimeServiceKind.MARKET_MINUTE_SOURCE:
        settings: dict[str, object] = {
            "spool_root": str(root / "live" / "market-minute"),
            "quota_path": str(root / "live" / "quota.sqlite"),
        }
    elif kind is RuntimeServiceKind.NOTIFIER:
        settings = {"signal_bus_path": str(root / "live" / "signal.sqlite")}
    elif kind is RuntimeServiceKind.SERVING_PUBLISHER:
        settings = {"serving_root": str(root / "serving")}
    else:
        settings = {}
    return RuntimeServiceManifest(
        service_id=service_id,
        service_kind=kind,
        plane=plane,
        interval_seconds=interval_seconds,
        stale_after_seconds=30,
        producer_commit=COMMIT,
        settings=settings,
    )


def _bundle_inputs(
    root: Path,
) -> tuple[
    tuple[RuntimeServiceManifest, ...],
    dict[str, dict[str, str]],
]:
    manifests = (
        _manifest(
            root,
            service_id="minute/source:primary",
            kind=RuntimeServiceKind.MARKET_MINUTE_SOURCE,
            plane=RuntimeServicePlane.LIVE,
        ),
        _manifest(
            root,
            service_id="notifier-admin",
            kind=RuntimeServiceKind.NOTIFIER,
            plane=RuntimeServicePlane.LIVE,
        ),
        _manifest(
            root,
            service_id="serving-publisher",
            kind=RuntimeServiceKind.SERVING_PUBLISHER,
            plane=RuntimeServicePlane.SERVING,
        ),
    )
    capabilities = {
        "minute/source:primary": {
            "TUSHARE_TOKEN_MAIN": "main-token",
            "TUSHARE_TOKEN_BACKUP": "backup-token",
        },
        "notifier-admin": {
            "PUSHDEER_KEYS": "pushdeer-key",
            "PUSHPLUS_TOKENS": "pushplus-token",
            "PUSHDEER_ENDPOINT": "https://pushdeer.invalid/send",
            "PUSHPLUS_ENDPOINT": "https://pushplus.invalid/send",
        },
        "serving-publisher": {},
    }
    return manifests, capabilities


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


def test_installs_canonical_generation_with_systemd_instance_mapping(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)

    receipt = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=manifests,
        capability_env=capabilities,
    )

    assert receipt.producer_commit == COMMIT
    assert len(receipt.generation_hash) == 64
    assert receipt.instance_mapping == {
        manifest.service_id: "svc-"
        + hashlib.sha256(manifest.service_id.encode("utf-8")).hexdigest()
        for manifest in manifests
    }
    current = root / "current"
    assert current.is_symlink()
    assert os.readlink(current) == f"generations/{receipt.generation_hash}"
    generation = current.resolve(strict=True)
    assert generation.parent == root / "generations"
    assert _mode(generation) == 0o700
    assert (generation / "runtime.env").read_text() == (
        f"RQUANT_RUNTIME_COMMIT={COMMIT}\n"
    )
    assert _mode(generation / "runtime.env") == 0o600

    for manifest in manifests:
        instance = receipt.instance_mapping[manifest.service_id]
        assert instance.startswith("svc-")
        assert len(instance) == 68
        manifest_path = generation / "manifests" / f"{instance}.json"
        assert _mode(manifest_path) == 0o600
        assert manifest_path.read_bytes() == json.dumps(
            manifest.model_dump(mode="json"),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

        env_path = generation / "secrets" / f"{instance}.env"
        if manifest.plane is RuntimeServicePlane.LIVE:
            assert _mode(env_path) == 0o600
            assert env_path.read_text().splitlines()[0] == (
                f"RQUANT_RUNTIME_COMMIT={COMMIT}"
            )
        else:
            assert not env_path.exists()

    manifest_payload = b"".join(
        path.read_bytes() for path in sorted((generation / "manifests").iterdir())
    )
    for secret in (b"main-token", b"pushdeer-key", b"pushplus-token"):
        assert secret not in manifest_payload


def test_same_inputs_are_deterministic_regardless_of_manifest_order(tmp_path: Path) -> None:
    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)

    first = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=manifests,
        capability_env=capabilities,
    )
    second = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=tuple(reversed(manifests)),
        capability_env=dict(reversed(tuple(capabilities.items()))),
    )

    assert second == first
    assert len(tuple((root / "generations").iterdir())) == 1


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("commit", "commit"),
        ("duplicate", "duplicate"),
        ("wrong_plane", "plane"),
        ("unknown_env", "unknown capability environment"),
        ("serving_env", "cannot receive capability environment"),
        ("missing_mapping", "exactly match"),
        ("secret_in_manifest", "plaintext capability value"),
        ("wrong_path_owner", "owned by the live plane"),
    ),
)
def test_rejects_invalid_or_overprivileged_bundle(
    tmp_path: Path,
    mutation: str,
    message: str,
) -> None:
    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)
    candidate = list(manifests)
    commit = COMMIT
    if mutation == "commit":
        candidate[0] = candidate[0].model_copy(update={"producer_commit": "b" * 40})
    elif mutation == "duplicate":
        candidate.append(candidate[0])
    elif mutation == "wrong_plane":
        candidate[0] = candidate[0].model_copy(
            update={"plane": RuntimeServicePlane.SERVING}
        )
    elif mutation == "unknown_env":
        capabilities["minute/source:primary"]["AWS_SECRET_ACCESS_KEY"] = "nope"
    elif mutation == "serving_env":
        capabilities["serving-publisher"]["PUSHDEER_KEYS"] = "nope"
    elif mutation == "missing_mapping":
        capabilities.pop("notifier-admin")
    elif mutation == "secret_in_manifest":
        candidate[0] = candidate[0].model_copy(
            update={"settings": {"label": "main-token"}}
        )
    elif mutation == "wrong_path_owner":
        candidate[0] = candidate[0].model_copy(
            update={
                "settings": {
                    "spool_root": str(root / "serving" / "raw"),
                    "quota_path": str(root / "live" / "quota.sqlite"),
                }
            }
        )

    with pytest.raises(ValueError, match=message):
        install_runtime_deployment_bundle(
            root,
            producer_commit=commit,
            manifests=tuple(candidate),
            capability_env=capabilities,
        )

    assert not (root / "current").exists()
    assert not list(root.glob(".staging-*"))


def test_failure_before_publish_preserves_previous_current_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import runtime_deployment_bundle as module

    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)
    first = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=manifests,
        capability_env=capabilities,
    )
    previous_target = os.readlink(root / "current")
    changed = manifests[0].model_copy(update={"interval_seconds": 2})
    write = module._write_secure_file
    calls = 0

    def fail_during_staging(path: Path, payload: bytes) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("simulated disk failure")
        write(path, payload)

    monkeypatch.setattr(module, "_write_secure_file", fail_during_staging)

    with pytest.raises(OSError, match="simulated disk failure"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(changed, *manifests[1:]),
            capability_env=capabilities,
        )

    assert os.readlink(root / "current") == previous_target
    assert (root / previous_target).is_dir()
    assert (root / previous_target).name == first.generation_hash
    assert not list(root.glob(".staging-*"))


def test_rejects_symlinked_runtime_parent_without_writing_outside(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    linked = tmp_path / "linked"
    linked.symlink_to(outside, target_is_directory=True)
    root = linked / "runtime"
    manifests, capabilities = _bundle_inputs(root)

    with pytest.raises(ValueError, match="symlink"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=manifests,
            capability_env=capabilities,
        )

    assert not (outside / "runtime").exists()


def test_rejects_symlinked_plane_path_that_escapes_its_owner(tmp_path: Path) -> None:
    root = tmp_path / "runtime"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (root / "live").symlink_to(outside, target_is_directory=True)
    manifests, capabilities = _bundle_inputs(root)

    with pytest.raises(ValueError, match="symlink"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=manifests,
            capability_env=capabilities,
        )

    assert not (root / "current").exists()
    assert not tuple(outside.iterdir())
