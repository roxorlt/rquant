from __future__ import annotations

import base64
import hashlib
import json
import shutil
import subprocess
import sys
import textwrap
import time
from dataclasses import dataclass
from pathlib import Path

import pytest

from rquant.runtime_builder_daily_orchestrator import (
    Ed25519DailyShadowStageReceiptKeyring,
    Ed25519DailyShadowStageReceiptSigner,
)


class OpenSslDailySigningClient:
    def __init__(self, private_key_path: Path) -> None:
        self._private_key_path = private_key_path

    def sign(self, *, namespace: str, payload: bytes) -> str:
        payload_path = self._private_key_path.parent / f"{namespace}.payload"
        signature_path = self._private_key_path.parent / f"{namespace}.signature"
        payload_path.write_bytes(payload)
        completed = subprocess.run(
            (
                _openssl(),
                "pkeyutl",
                "-sign",
                "-inkey",
                str(self._private_key_path),
                "-rawin",
                "-in",
                str(payload_path),
                "-out",
                str(signature_path),
            ),
            check=False,
            capture_output=True,
        )
        if completed.returncode != 0:
            raise RuntimeError(completed.stderr.decode("utf-8", errors="replace"))
        return base64.b64encode(signature_path.read_bytes()).decode("ascii")


@dataclass(frozen=True)
class DailyEd25519TestAuthority:
    key_id: str
    public_key_pem: str
    signer_command: tuple[str, ...]
    signer: Ed25519DailyShadowStageReceiptSigner
    keyring: Ed25519DailyShadowStageReceiptKeyring


@dataclass(frozen=True)
class RotatingDailyEd25519TestAuthority:
    active: DailyEd25519TestAuthority
    previous: DailyEd25519TestAuthority
    keyring: Ed25519DailyShadowStageReceiptKeyring


@dataclass(frozen=True)
class DailyEd25519SocketTestAuthority:
    """One real AF_UNIX authority process for rollout integration tests."""

    key_id: str
    public_key_pem: str
    endpoint: Path
    process: subprocess.Popen[bytes]

    def stop(self) -> None:
        if self.process.poll() is not None:
            if self.endpoint.exists():
                self.endpoint.unlink()
            return
        self.process.terminate()
        try:
            self.process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.communicate(timeout=5)
        if self.endpoint.exists():
            self.endpoint.unlink()


def _openssl() -> str:
    executable = shutil.which("openssl")
    if executable is None:
        pytest.skip("openssl is required for Daily Ed25519 tests")
    return executable


def _create_key_pair(root: Path, *, key_id: str) -> tuple[Path, bytes]:
    root.mkdir(parents=True, exist_ok=True)
    private_key = root / f"{key_id}.private.pem"
    public_key = root / f"{key_id}.public.pem"
    generated = subprocess.run(
        (_openssl(), "genpkey", "-algorithm", "ED25519", "-out", str(private_key)),
        check=False,
        capture_output=True,
    )
    if generated.returncode != 0:
        raise RuntimeError(generated.stderr.decode("utf-8", errors="replace"))
    exported = subprocess.run(
        (_openssl(), "pkey", "-in", str(private_key), "-pubout", "-out", str(public_key)),
        check=False,
        capture_output=True,
    )
    if exported.returncode != 0:
        raise RuntimeError(exported.stderr.decode("utf-8", errors="replace"))
    private_key.chmod(0o600)
    return private_key, public_key.read_bytes()


def _create_signer_command(root: Path, *, key_id: str, private_key: Path) -> tuple[str, ...]:
    script = root / f"{key_id}.signer.py"
    script.write_text(
        textwrap.dedent(
            f"""
            from __future__ import annotations

            import base64
            import hashlib
            import json
            import subprocess
            import sys
            from pathlib import Path

            openssl = {str(_openssl())!r}
            key_id = {key_id!r}
            private_key = Path({str(private_key)!r})
            request = json.loads(sys.stdin.buffer.read())
            payload = base64.b64decode(request["payload_base64"], validate=True)
            payload_sha256 = hashlib.sha256(payload).hexdigest()
            if request["key_id"] != key_id or request["payload_sha256"] != payload_sha256:
                raise SystemExit(2)
            payload_path = private_key.parent / f"{{request['request_id']}}.payload"
            signature_path = private_key.parent / f"{{request['request_id']}}.signature"
            payload_path.write_bytes(payload)
            completed = subprocess.run(
                (
                    openssl,
                    "pkeyutl",
                    "-sign",
                    "-inkey",
                    str(private_key),
                    "-rawin",
                    "-in",
                    str(payload_path),
                    "-out",
                    str(signature_path),
                ),
                check=False,
                capture_output=True,
            )
            if completed.returncode != 0:
                sys.stderr.buffer.write(completed.stderr)
                raise SystemExit(completed.returncode)
            response = {{
                "schema_version": 1,
                "operation": "sign",
                "request_id": request["request_id"],
                "key_id": request["key_id"],
                "namespace": request["namespace"],
                "payload_sha256": payload_sha256,
                "signature": base64.b64encode(signature_path.read_bytes()).decode("ascii"),
            }}
            sys.stdout.write(json.dumps(response, sort_keys=True, separators=(",", ":")))
            """
        ),
        encoding="utf-8",
    )
    script.chmod(0o700)
    return (sys.executable, str(script))


def create_daily_ed25519_test_authority(
    root: Path,
    *,
    key_id: str = "daily-stage-test-v1",
) -> DailyEd25519TestAuthority:
    private_key, public_key = _create_key_pair(root, key_id=key_id)
    signer = Ed25519DailyShadowStageReceiptSigner(
        key_id=key_id,
        client=OpenSslDailySigningClient(private_key),
    )
    keyring = Ed25519DailyShadowStageReceiptKeyring(
        active_key_id=key_id,
        active_public_key=public_key,
    )
    return DailyEd25519TestAuthority(
        key_id=key_id,
        public_key_pem=public_key.decode("utf-8"),
        signer_command=_create_signer_command(root, key_id=key_id, private_key=private_key),
        signer=signer,
        keyring=keyring,
    )


def start_daily_ed25519_socket_authority(
    root: Path,
    *,
    key_id: str = "daily-socket-test-v1",
) -> DailyEd25519SocketTestAuthority:
    """Start the actual socket authority, never a command-signing fixture.

    Linux uses the real ``SO_PEERCRED`` path.  macOS has no equivalent kernel
    option, so only the isolated test child supplies its own current UID/GID.
    The production authority does not expose that seam.
    """

    private_key, public_key = _create_key_pair(root / "keys", key_id=key_id)
    manifest = root / "daily-receipt-keys.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "generation": 1,
                "previous_manifest_hash": "0" * 64,
                "active_key_id": key_id,
                "active_private_key_path": str(private_key),
                "previous_public_keys": {},
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    manifest.chmod(0o600)
    endpoint = Path("/tmp") / (
        "rqdr-" + hashlib.sha256(str(root).encode("utf-8")).hexdigest()[:24] + ".sock"
    )
    if endpoint.exists():
        endpoint.unlink()
    server = root / "daily-receipt-server.py"
    server.write_text(
        textwrap.dedent(
            f"""
            import os
            import sys
            from pathlib import Path

            import rquant.daily_receipt_socket_authority as authority

            if not authority.daily_receipt_peer_credentials_supported():
                if sys.platform.startswith("linux"):
                    raise SystemExit("refusing to fake SO_PEERCRED on Linux")
                authority.daily_receipt_peer_credentials_supported = lambda: True
                authority._peer_credentials = lambda _connection: (os.getuid(), os.getgid())

            authority.serve_daily_receipt_socket_authority(
                socket_path=Path({str(endpoint)!r}),
                keys_path=Path({str(manifest)!r}),
                nonce_state_root=Path({str(root / "nonce-state")!r}),
                allowed_uid=os.getuid(),
                allowed_gid=os.getgid(),
            )
            """
        ),
        encoding="utf-8",
    )
    process = subprocess.Popen(
        (sys.executable, str(server)),
        cwd=Path(__file__).resolve().parents[1],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate(timeout=1)
            raise AssertionError(
                f"daily socket authority exited early: {process.returncode}\n{stdout!r}\n{stderr!r}"
            )
        if endpoint.exists():
            return DailyEd25519SocketTestAuthority(
                key_id=key_id,
                public_key_pem=public_key.decode("utf-8"),
                endpoint=endpoint,
                process=process,
            )
        time.sleep(0.02)
    process.kill()
    stdout, stderr = process.communicate(timeout=5)
    raise AssertionError(f"daily socket authority did not start\n{stdout!r}\n{stderr!r}")


def create_rotating_daily_ed25519_test_authority(
    root: Path,
) -> RotatingDailyEd25519TestAuthority:
    previous = create_daily_ed25519_test_authority(root / "previous", key_id="daily-stage-test-v1")
    active = create_daily_ed25519_test_authority(root / "active", key_id="daily-stage-test-v2")
    keyring = Ed25519DailyShadowStageReceiptKeyring(
        active_key_id=active.key_id,
        active_public_key=active.public_key_pem.encode("utf-8"),
        previous_public_keys={previous.key_id: previous.public_key_pem.encode("utf-8")},
    )
    return RotatingDailyEd25519TestAuthority(
        active=active,
        previous=previous,
        keyring=keyring,
    )
