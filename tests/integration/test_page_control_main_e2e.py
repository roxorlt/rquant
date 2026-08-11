from __future__ import annotations

import json
import shutil
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

import pytest

from rquant.runtime_deployment_profile import (
    PageControlRuntimeProfile,
    RuntimeDeploymentProfile,
    install_runtime_deployment_profile,
)
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest

COMMIT = "a" * 40


def _openssl() -> str:
    executable = shutil.which("openssl")
    if executable is None:
        pytest.skip("openssl is required for PageControl main process E2E")
    return executable


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _manifest(runtime_root: Path) -> RuntimeServiceManifest:
    return RuntimeServiceManifest(
        service_id="lab-jobs.serving.v1",
        service_kind=RuntimeServiceKind.LAB_JOBS_PUBLISHER,
        plane=RuntimeServicePlane.RESEARCH,
        interval_seconds=30,
        stale_after_seconds=120,
        producer_commit=COMMIT,
        settings={
            "lab_jobs_path": str(runtime_root / "research" / "lab_jobs.sqlite3"),
            "authority_root": str(
                runtime_root / "research" / "serving-authorities" / "lab-jobs"
            ),
        },
    )


def _generate_signer(root: Path, *, key_id: str) -> tuple[Path, str]:
    root.mkdir(parents=True, exist_ok=True)
    private_key = root / f"{key_id}.private.pem"
    public_key = root / f"{key_id}.public.pem"
    subprocess.run(
        (_openssl(), "genpkey", "-algorithm", "ED25519", "-out", str(private_key)),
        check=True,
        capture_output=True,
    )
    subprocess.run(
        (
            _openssl(),
            "pkey",
            "-in",
            str(private_key),
            "-pubout",
            "-out",
            str(public_key),
        ),
        check=True,
        capture_output=True,
    )
    private_key.chmod(0o600)
    return private_key, public_key.read_text(encoding="utf-8")


def _write_signer_helper(root: Path, *, private_key: Path) -> Path:
    helper = root / "canvas-signer-helper.py"
    helper.write_text(
        """from __future__ import annotations
import base64
import json
import subprocess
import sys
import tempfile
from pathlib import Path

request = json.load(sys.stdin)
payload = base64.b64decode(request["payload_base64"], validate=True)
with tempfile.TemporaryDirectory() as raw_root:
    root = Path(raw_root)
    payload_path = root / "payload.bin"
    signature_path = root / "signature.bin"
    payload_path.write_bytes(payload)
    subprocess.run(
        [
            __OPENSSL__, "pkeyutl", "-sign", "-inkey", __PRIVATE_KEY__, "-rawin",
            "-in", str(payload_path), "-out", str(signature_path),
        ],
        check=True,
        capture_output=True,
    )
    signature = signature_path.read_bytes()
response = {
    "schema_version": 1,
    "operation": "sign",
    "request_id": request["request_id"],
    "key_id": request["key_id"],
    "namespace": request["namespace"],
    "payload_sha256": request["payload_sha256"],
    "signature": base64.b64encode(signature).decode("ascii"),
}
sys.stdout.write(json.dumps(response, sort_keys=True, separators=(",", ":")))
""".replace("__OPENSSL__", repr(_openssl())).replace(
            "__PRIVATE_KEY__", repr(str(private_key))
        ),
        encoding="utf-8",
    )
    helper.chmod(0o700)
    return helper


def _install_profile(
    runtime_root: Path,
    *,
    port: int,
    active_key_id: str,
    active_public_key: str,
    signer_command: tuple[str, ...],
    previous_public_keys: dict[str, str] | None = None,
) -> None:
    manifest = _manifest(runtime_root)
    profile = RuntimeDeploymentProfile(
        producer_commit=COMMIT,
        manifests=(manifest,),
        capability_environment={manifest.service_id: ()},
        page_control=PageControlRuntimeProfile.model_validate(
            {
                "endpoint": f"http://127.0.0.1:{port}/v1/commands",
                "outbox_path": runtime_root / "control" / "page-control.sqlite3",
                "data_dir": runtime_root / "serving" / "page-control",
                "log_dir": runtime_root / "control" / "page-control-logs",
                "page_projection_canvas_catalog_root": (
                    runtime_root / "serving" / "page-control" / "canvases"
                ),
                "canvas_publication": {
                    "active_key_id": active_key_id,
                    "active_public_key_pem": active_public_key,
                    "previous_public_key_pems": previous_public_keys or {},
                    "signer_command": signer_command,
                    "consumer_service_id": "page-control.production.v1",
                    "consumer_instance_id": "page-control-primary",
                    "timeout_seconds": 5,
                },
            }
        ),
    )
    install_runtime_deployment_profile(
        profile,
        runtime_root=runtime_root,
        environ={},
        schema_bootstrap_reason="PageControl clean-root E2E",
    )


def _start_main(runtime_root: Path) -> subprocess.Popen[bytes]:
    source_root = Path(__file__).resolve().parents[2] / "src"
    code = (
        "import sys; from pathlib import Path; "
        "from rquant.page_control_service import main; "
        "main(runtime_root=Path(sys.argv[1]), expected_commit=sys.argv[2])"
    )
    return subprocess.Popen(
        (sys.executable, "-c", code, str(runtime_root), COMMIT),
        cwd=Path(__file__).resolve().parents[2],
        env={
            "LANG": "C",
            "PATH": "/usr/bin:/bin",
            "PYTHONPATH": str(source_root),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def _wait_ready(process: subprocess.Popen[bytes], port: int) -> None:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if process.poll() is not None:
            _stdout, stderr = process.communicate()
            raise AssertionError(stderr.decode("utf-8", errors="replace"))
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.05)
    raise AssertionError("PageControl main did not bind before its deadline")


def _submit(port: int, payload: dict[str, object]) -> dict[str, object]:
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}/v1/commands",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        assert response.status == 200
        decoded = json.loads(response.read())
    assert isinstance(decoded, dict)
    return decoded


def _kill(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is None:
        process.kill()
    process.wait(timeout=10)


def test_main_clean_root_restart_is_exactly_once_with_immutable_receipt(
    tmp_path: Path,
) -> None:
    runtime_root = tmp_path / "runtime"
    private_key, public_key = _generate_signer(tmp_path / "signer", key_id="canvas-v1")
    helper = _write_signer_helper(tmp_path / "signer", private_key=private_key)
    port = _free_port()
    _install_profile(
        runtime_root,
        port=port,
        active_key_id="canvas-v1",
        active_public_key=public_key,
        signer_command=(sys.executable, str(helper)),
    )
    command = {
        "kind": "save_canvas",
        "command_id": "main-process-save-canvas",
        "requested_at": datetime.now(UTC).isoformat(),
        "name": "breakout",
        "description": "main process authority",
        "pool_refs": ["n-shape-pool1"],
        "source": "page_control",
    }

    first_process = _start_main(runtime_root)
    try:
        _wait_ready(first_process, port)
        first = _submit(port, command)
    finally:
        _kill(first_process)
    assert first["status"] == "succeeded"
    result = first["result"]
    assert isinstance(result, dict)
    publication_path = (
        runtime_root
        / "serving"
        / "page-control"
        / "canvas-publication-receipts"
        / f"{result['publication_receipt_id']}.json"
    )
    catalog_path = runtime_root / "serving" / "page-control" / "canvases" / "breakout.json"
    first_publication = publication_path.read_bytes()
    first_catalog = catalog_path.read_bytes()

    restarted = _start_main(runtime_root)
    try:
        _wait_ready(restarted, port)
        duplicate = _submit(port, command)
    finally:
        _kill(restarted)

    assert duplicate == first
    assert publication_path.read_bytes() == first_publication
    assert catalog_path.read_bytes() == first_catalog
    with sqlite3.connect(runtime_root / "control" / "page-control.sqlite3") as connection:
        effect_count = connection.execute(
            "SELECT COUNT(*) FROM page_control_effect WHERE command_id = ?",
            (command["command_id"],),
        ).fetchone()[0]
    assert effect_count == 1


def test_main_without_signer_capability_exits_nonzero_before_http(
    tmp_path: Path,
) -> None:
    runtime_root = tmp_path / "runtime"
    _private_key, public_key = _generate_signer(tmp_path / "keys", key_id="canvas-v1")
    port = _free_port()
    _install_profile(
        runtime_root,
        port=port,
        active_key_id="canvas-v1",
        active_public_key=public_key,
        signer_command=("/missing/rquant-canvas-publication-signer",),
    )

    process = _start_main(runtime_root)
    _stdout, stderr = process.communicate(timeout=10)

    assert process.returncode not in {None, 0}
    assert b"signer capability" in stderr
    with pytest.raises(OSError):
        socket.create_connection(("127.0.0.1", port), timeout=0.1)


def test_main_rejects_previous_only_signer_before_http(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    previous_private, previous_public = _generate_signer(
        tmp_path / "keys",
        key_id="canvas-v1",
    )
    _active_private, active_public = _generate_signer(
        tmp_path / "keys",
        key_id="canvas-v2",
    )
    helper = _write_signer_helper(tmp_path / "keys", private_key=previous_private)
    port = _free_port()
    _install_profile(
        runtime_root,
        port=port,
        active_key_id="canvas-v2",
        active_public_key=active_public,
        previous_public_keys={"canvas-v1": previous_public},
        signer_command=(sys.executable, str(helper)),
    )

    process = _start_main(runtime_root)
    _stdout, stderr = process.communicate(timeout=10)

    assert process.returncode not in {None, 0}
    assert b"active key" in stderr
    with pytest.raises(OSError):
        socket.create_connection(("127.0.0.1", port), timeout=0.1)


def test_main_rejects_polluted_immutable_profile_before_http(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    private_key, public_key = _generate_signer(tmp_path / "keys", key_id="canvas-v1")
    helper = _write_signer_helper(tmp_path / "keys", private_key=private_key)
    port = _free_port()
    _install_profile(
        runtime_root,
        port=port,
        active_key_id="canvas-v1",
        active_public_key=public_key,
        signer_command=(sys.executable, str(helper)),
    )
    current = (runtime_root / "current").readlink()
    profile_path = runtime_root / current / "deployment-profile.json"
    payload = json.loads(profile_path.read_text(encoding="utf-8"))
    payload["page_control"]["canvas_publication"]["consumer_instance_id"] = "polluted"
    profile_path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )

    process = _start_main(runtime_root)
    _stdout, stderr = process.communicate(timeout=10)

    assert process.returncode not in {None, 0}
    assert b"profile" in stderr.lower()
    with pytest.raises(OSError):
        socket.create_connection(("127.0.0.1", port), timeout=0.1)
