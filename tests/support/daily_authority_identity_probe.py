#!/usr/bin/env python3
"""Small real Unix-socket identity probe fixture for installer fault tests."""

from __future__ import annotations

import argparse
import base64
import json
import socket
import subprocess
import tempfile
import time
from pathlib import Path


def canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def read_state(path: Path) -> str:
    for line in path.read_text(encoding="utf-8").splitlines():
        key, separator, value = line.partition("=")
        if (
            separator
            and key == "runtime_identity"
            and len(value) == 64
            and all(char in "0123456789abcdef" for char in value)
        ):
            return value
    raise RuntimeError("identity probe state has no valid runtime identity")


def recv_exact(connection: socket.socket, size: int) -> bytes:
    chunks: list[bytes] = []
    while size:
        chunk = connection.recv(size)
        if not chunk:
            raise RuntimeError("identity probe request is truncated")
        chunks.append(chunk)
        size -= len(chunk)
    return b"".join(chunks)


def sign(private_key: Path, payload: bytes) -> bytes:
    openssl = (
        "/opt/homebrew/bin/openssl"
        if Path("/opt/homebrew/bin/openssl").exists()
        else "/usr/bin/openssl"
    )
    with tempfile.TemporaryDirectory(prefix="rquant-identity-probe-", dir="/tmp") as raw:
        root = Path(raw)
        root.chmod(0o700)
        payload_path = root / "payload.bin"
        payload_path.write_bytes(payload)
        payload_path.chmod(0o600)
        result = subprocess.run(
            (
                openssl,
                "pkeyutl",
                "-sign",
                "-rawin",
                "-inkey",
                str(private_key),
                "-in",
                str(payload_path),
            ),
            stdin=subprocess.DEVNULL,
            capture_output=True,
            check=False,
            timeout=5,
        )
    if result.returncode != 0 or len(result.stdout) != 64:
        raise RuntimeError("identity probe Ed25519 signing failed")
    return result.stdout


def handle(
    connection: socket.socket,
    state_path: Path,
    *,
    private_key: Path,
    key_id: str,
    fault: str,
    fault_signing_key: Path | None,
    fault_key_id: str | None,
    fault_applied: bool,
) -> bool:
    connection.settimeout(10)
    size = int.from_bytes(recv_exact(connection, 4), "big")
    if not 0 < size <= 2 * 1024 * 1024:
        raise RuntimeError("identity probe request size is invalid")
    request = json.loads(recv_exact(connection, size))
    if request != {
        "version": 1,
        "operation": "identity",
        "protocol": "rquant-daily-receipt-authority.identity",
        "nonce": request.get("nonce"),
    }:
        raise RuntimeError("identity probe request protocol is invalid")
    identity = read_state(state_path)
    use_fault = bool(fault and not fault_applied)
    signed_key = private_key
    signed_key_id = key_id
    response_key_id = key_id
    response_nonce = request["nonce"]
    response_source = identity
    if use_fault and fault == "previous-key":
        if fault_signing_key is None or not fault_key_id:
            raise RuntimeError("identity probe previous key fixture is incomplete")
        signed_key = fault_signing_key
        signed_key_id = fault_key_id
        response_key_id = fault_key_id
    elif use_fault and fault == "wrong-key-id":
        response_key_id = fault_key_id or "daily-wrong-key"
    elif use_fault and fault == "nonce-tamper":
        response_nonce = ("0" if request["nonce"][0] != "0" else "1") + request["nonce"][1:]
    elif use_fault and fault in {"source-sha-tamper", "source-tamper"}:
        response_source = "0" * 64
    elif use_fault and fault not in {"bad-signature", ""}:
        raise RuntimeError(f"unknown identity probe fault: {fault}")

    signed_envelope = {
        "version": 1,
        "operation": "identity",
        "protocol": "rquant-daily-receipt-authority.identity",
        "nonce": response_nonce,
        "source_sha256": response_source,
        "key_id": response_key_id,
    }
    if use_fault and fault == "previous-key":
        signed_envelope["key_id"] = signed_key_id
    signature = sign(signed_key, canonical(signed_envelope))
    if use_fault and fault == "bad-signature":
        signature = bytes([signature[0] ^ 1]) + signature[1:]
    response = {
        **signed_envelope,
        "nonce": response_nonce,
        "source_sha256": response_source,
        "key_id": response_key_id,
        "signature": base64.b64encode(signature).decode("ascii"),
    }
    payload = canonical(response)
    connection.sendall(len(payload).to_bytes(4, "big") + payload)
    return fault_applied or use_fault


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--private-key", type=Path, required=True)
    parser.add_argument("--key-id", default="daily-v1")
    parser.add_argument(
        "--fault",
        choices=(
            "",
            "previous-key",
            "bad-signature",
            "wrong-key-id",
            "nonce-tamper",
            "source-sha-tamper",
            "source-tamper",
        ),
        default="",
    )
    parser.add_argument("--fault-signing-key", type=Path)
    parser.add_argument("--fault-key-id")
    parser.add_argument("--max-connections", type=int, default=16)
    args = parser.parse_args()
    args.endpoint.parent.mkdir(parents=True, exist_ok=True)
    args.endpoint.unlink(missing_ok=True)
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
        listener.bind(str(args.endpoint))
        listener.listen(8)
        args.endpoint.chmod(0o660)
        fault_applied = False
        for _ in range(args.max_connections):
            connection, _ = listener.accept()
            with connection:
                try:
                    fault_applied = handle(
                        connection,
                        args.state,
                        private_key=args.private_key,
                        key_id=args.key_id,
                        fault=args.fault,
                        fault_signing_key=args.fault_signing_key,
                        fault_key_id=args.fault_key_id,
                        fault_applied=fault_applied,
                    )
                except Exception as exc:  # noqa: BLE001
                    response = canonical({"ok": False, "error": str(exc)[:256]})
                    connection.sendall(len(response).to_bytes(4, "big") + response)
    time.sleep(0.01)
    args.endpoint.unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
