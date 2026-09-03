"""Contract tests for ``scripts/install-runtime-credential-keys.sh`` (TP2).

Every assertion is made against the *real* consumer loaders shipped in
``deploy/libexec`` (plus ``rquant.lab_highwater_authority`` for the only
in-tree reader), never against a re-implementation of the schemas.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "install-runtime-credential-keys.sh"
LIBEXEC = ROOT / "deploy" / "libexec"
SYSTEM_PYTHON = "/usr/bin/python3"

CANVAS_HELPER = LIBEXEC / "rquant-canvas-publication-signer"
SHADOW_HELPER = LIBEXEC / "rquant-shadow-report-signer"
HIGHWATER_HELPER = LIBEXEC / "rquant-lab-highwater-authority"
DAILY_HELPER = LIBEXEC / "rquant-daily-receipt-signer"

# The daily helper is zero-argument and pins ``/etc/rquant/daily-receipt-keys.json``
# at module scope, so the only non-root way to drive its real loader is the
# ``runpy`` seam already used by ``scripts/install-runtime-credential-infra.sh:1292``.
_DAILY_RUNPY = (
    "from pathlib import Path; import runpy, sys; "
    'module = runpy.run_path(sys.argv[1]); operation = module["_stdin_operation"]; '
    "operation(Path(sys.argv[2]))"
)


def _require_openssl() -> str:
    for candidate in ("/opt/homebrew/bin/openssl", "/usr/bin/openssl"):
        if Path(candidate).is_file():
            probe = subprocess.run(
                [candidate, "genpkey", "-algorithm", "ED25519", "-out", os.devnull],
                check=False,
                capture_output=True,
            )
            if probe.returncode == 0:
                return candidate
    resolved = shutil.which("openssl")
    if resolved is None:
        pytest.fail("openssl is required by the runtime credential key installer")
    return resolved


def _run(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["/bin/bash", str(SCRIPT), *arguments],
        cwd=str(ROOT),
        check=False,
        capture_output=True,
        text=True,
    )


@pytest.fixture()
def prefix(tmp_path: Path) -> Path:
    # macOS gives direct temporary children gid 0 even for an unprivileged owner;
    # test mode validates the invoking uid/effective gid, so normalize the root
    # before any descendant is created (same reason as the infra installer fixture).
    root = tmp_path / "root"
    root.mkdir(mode=0o755)
    os.chown(root, os.geteuid(), os.getegid())
    return root


def _expected_files(prefix: Path, suffix: str = "v1") -> dict[str, Path]:
    etc = prefix / "etc" / "rquant"
    return {
        "highwater_manifest": etc / "lab-highwater-keys.json",
        "highwater_private": etc / "lab-highwater" / f"hw-{suffix}.private.pem",
        "canvas_manifest": etc / "canvas-publication-keys.json",
        "canvas_private": etc / "canvas-publication" / f"canvas-{suffix}.private.pem",
        "shadow_manifest": etc / "shadow-report-keys.json",
        "shadow_private": etc / "shadow-report" / f"shadow-{suffix}.private.pem",
        "shadow_calendar": etc / "shadow-report" / "legacy-recovery-calendar.json",
        "daily_manifest": etc / "daily-receipt-keys.json",
        "daily_private": etc / "daily-receipt" / f"daily-{suffix}.private.pem",
    }


def _expected_directories(prefix: Path) -> dict[Path, int]:
    etc = prefix / "etc" / "rquant"
    return {
        etc: 0o755,
        etc / "lab-highwater": 0o700,
        etc / "canvas-publication": 0o700,
        etc / "shadow-report": 0o700,
        etc / "daily-receipt": 0o700,
    }


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _init(prefix: Path, *extra: str) -> subprocess.CompletedProcess[str]:
    _require_openssl()
    return _run("init", "--prefix", str(prefix), *extra)


# --------------------------------------------------------------------------- K-1


def test_init_creates_the_nine_files_with_the_expected_metadata(prefix: Path) -> None:
    result = _init(prefix)
    assert result.returncode == 0, result.stdout + result.stderr

    for directory, mode in _expected_directories(prefix).items():
        observed = directory.lstat()
        assert stat.S_ISDIR(observed.st_mode), directory
        assert stat.S_IMODE(observed.st_mode) == mode, directory
        assert observed.st_uid == os.geteuid(), directory

    files = _expected_files(prefix)
    assert len(files) == 9
    for name, path in files.items():
        observed = path.lstat()
        assert stat.S_ISREG(observed.st_mode), name
        assert stat.S_IMODE(observed.st_mode) == 0o600, name
        assert observed.st_nlink == 1, name
        assert observed.st_uid == os.geteuid(), name
        assert observed.st_gid == os.getegid(), name
        assert observed.st_size > 0, name

    for name in ("highwater_private", "canvas_private", "shadow_private", "daily_private"):
        text = files[name].read_text(encoding="utf-8")
        assert text.startswith("-----BEGIN PRIVATE KEY-----"), name
        assert "END PRIVATE KEY" in text, name


# --------------------------------------------------------------------------- K-2


def test_canvas_manifest_is_accepted_by_the_canvas_signer(prefix: Path) -> None:
    assert _init(prefix).returncode == 0
    files = _expected_files(prefix)
    result = subprocess.run(
        [
            SYSTEM_PYTHON,
            str(CANVAS_HELPER),
            "--keys-file",
            str(files["canvas_manifest"]),
            "--validate-key-material",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_shadow_manifest_and_calendar_are_accepted_by_the_shadow_signer(
    prefix: Path,
) -> None:
    assert _init(prefix).returncode == 0
    files = _expected_files(prefix)
    result = subprocess.run(
        [
            SYSTEM_PYTHON,
            str(SHADOW_HELPER),
            "--keys-file",
            str(files["shadow_manifest"]),
            "--validate-key-material",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_highwater_manifest_is_accepted_by_the_highwater_authority(prefix: Path) -> None:
    assert _init(prefix).returncode == 0
    files = _expected_files(prefix)
    result = subprocess.run(
        [
            SYSTEM_PYTHON,
            str(HIGHWATER_HELPER),
            "--keys-file",
            str(files["highwater_manifest"]),
            "--export-public-keyring",
        ],
        check=False,
        capture_output=True,
    )
    assert result.returncode == 0, result.stderr.decode("utf-8", "replace")
    keyring = json.loads(result.stdout)
    assert keyring["schema_version"] == 3
    assert keyring["generation"] == 1
    assert keyring["active_key_id"] == "hw-v1"
    assert keyring["previous_public_keys"] == {}


def test_daily_manifest_is_accepted_by_the_daily_receipt_loader(prefix: Path) -> None:
    assert _init(prefix).returncode == 0
    files = _expected_files(prefix)
    result = subprocess.run(
        [
            SYSTEM_PYTHON,
            "-I",
            "-S",
            "-c",
            _DAILY_RUNPY,
            str(DAILY_HELPER),
            str(files["daily_manifest"]),
        ],
        input='{"operation":"validate-key-material","schema_version":1}',
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


# --------------------------------------------------------------------------- K-3


def test_daily_manifest_is_byte_exact_canonical_json(prefix: Path) -> None:
    assert _init(prefix).returncode == 0
    files = _expected_files(prefix)
    payload = files["daily_manifest"].read_bytes()
    document = json.loads(payload)
    assert _canonical(document) == payload
    assert not payload.endswith(b"\n")
    assert payload.decode("ascii") == payload.decode("utf-8")
    assert set(document) == {
        "schema_version",
        "generation",
        "previous_manifest_hash",
        "active_key_id",
        "active_private_key_path",
        "previous_public_keys",
    }
    assert document["schema_version"] == 2
    assert document["active_private_key_path"] == str(files["daily_private"])


# --------------------------------------------------------------------------- K-4


def test_highwater_and_daily_genesis_bindings_hold(prefix: Path) -> None:
    assert _init(prefix).returncode == 0
    files = _expected_files(prefix)
    for name, schema_version, key_id in (
        ("highwater_manifest", 3, "hw-v1"),
        ("daily_manifest", 2, "daily-v1"),
    ):
        document = json.loads(files[name].read_text(encoding="utf-8"))
        assert document["schema_version"] == schema_version, name
        assert document["generation"] == 1, name
        assert document["previous_manifest_hash"] == "0" * 64, name
        assert document["previous_public_keys"] == {}, name
        assert document["active_key_id"] == key_id, name


# --------------------------------------------------------------------------- K-5


def test_shadow_recovery_calendar_is_self_consistent(prefix: Path) -> None:
    assert _init(prefix).returncode == 0
    files = _expected_files(prefix)
    payload = files["shadow_calendar"].read_bytes()
    document = json.loads(payload)
    assert set(document) == {
        "schema_version",
        "exchange",
        "coverage_start",
        "coverage_end",
        "open_dates",
        "content_sha256",
    }
    assert document["schema_version"] == 1
    assert document["exchange"] == "SSE"
    body = {key: value for key, value in document.items() if key != "content_sha256"}
    assert hashlib.sha256(_canonical(body)).hexdigest() == document["content_sha256"]
    assert document["coverage_start"] <= document["coverage_end"]
    open_dates = document["open_dates"]
    assert open_dates == sorted(set(open_dates))
    assert all(
        document["coverage_start"] <= value <= document["coverage_end"] for value in open_dates
    )
    manifest = json.loads(files["shadow_manifest"].read_text(encoding="utf-8"))
    assert manifest["legacy_recovery_calendar_path"] == str(files["shadow_calendar"])


# --------------------------------------------------------------------------- K-6


def test_init_is_idempotent_and_refuses_to_overwrite(prefix: Path) -> None:
    assert _init(prefix).returncode == 0
    files = _expected_files(prefix)
    before = {name: path.read_bytes() for name, path in files.items()}

    second = _init(prefix)
    assert second.returncode == 3, second.stdout + second.stderr
    after = {name: path.read_bytes() for name, path in files.items()}
    assert after == before


def test_dry_run_lists_the_plan_without_writing_anything(prefix: Path) -> None:
    result = _init(prefix, "--dry-run")
    assert result.returncode == 0, result.stdout + result.stderr
    assert not (prefix / "etc").exists()
    for path in _expected_files(prefix).values():
        assert str(path) in result.stdout
    for directory in _expected_directories(prefix):
        assert str(directory) in result.stdout
