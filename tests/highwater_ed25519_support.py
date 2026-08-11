"""Real Ed25519 high-water key material for cross-process tests."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from rquant.strict_json import canonical_json_bytes

ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / "deploy" / "libexec" / "rquant-lab-highwater-authority"


def generate_key_pair(root: Path, key_id: str) -> tuple[Path, bytes]:
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    private_key = root / f"{key_id}.private.pem"
    public_key = root / f"{key_id}.public.pem"
    subprocess.run(
        [
            "/opt/homebrew/bin/openssl",
            "genpkey",
            "-algorithm",
            "ED25519",
            "-out",
            str(private_key),
        ],
        check=True,
        capture_output=True,
    )
    private_key.chmod(0o600)
    subprocess.run(
        [
            "/opt/homebrew/bin/openssl",
            "pkey",
            "-in",
            str(private_key),
            "-pubout",
            "-out",
            str(public_key),
        ],
        check=True,
        capture_output=True,
    )
    return private_key, public_key.read_bytes()


def write_private_manifest(
    path: Path,
    *,
    active_key_id: str,
    generation: int = 1,
    previous_manifest_hash: str = "0" * 64,
    previous_public_keys: dict[str, bytes] | None = None,
) -> tuple[Path, bytes]:
    active_private, active_public = generate_key_pair(path.parent / "key-material", active_key_id)
    path.write_bytes(
        canonical_json_bytes(
            {
                "schema_version": 3,
                "generation": generation,
                "previous_manifest_hash": previous_manifest_hash,
                "active_key_id": active_key_id,
                "active_private_key_path": str(active_private),
                "previous_public_keys": {
                    key_id: public_key.decode("utf-8")
                    for key_id, public_key in sorted((previous_public_keys or {}).items())
                },
            }
        )
    )
    path.chmod(0o600)
    return path, active_public


def export_public_keyring(
    private_manifest: Path,
    output: Path,
    *,
    current_keyring: Path | None = None,
    mode: int = 0o600,
) -> Path:
    command = [
        sys.executable,
        str(HELPER),
        "--keys-file",
        str(private_manifest),
        "--export-public-keyring",
    ]
    if current_keyring is not None:
        command.extend(["--current-keyring", str(current_keyring)])
    result = subprocess.run(command, check=False, capture_output=True)
    if result.returncode != 0:
        raise AssertionError(result.stderr.decode("utf-8", "replace"))
    output.write_bytes(result.stdout)
    output.chmod(mode)
    return output
