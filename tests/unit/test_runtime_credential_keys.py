"""Contract tests for ``scripts/install-runtime-credential-keys.sh`` (TP2).

Every assertion is made against the *real* consumer loaders shipped in
``deploy/libexec`` (plus ``rquant.lab_highwater_authority`` for the only
in-tree reader), never against a re-implementation of the schemas.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
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
        "completion_manifest": etc / "shadow-completion-keys.json",
        "completion_private": etc / "shadow-completion" / f"completion-{suffix}.private.pem",
        "daily_manifest": etc / "daily-receipt-keys.json",
        "daily_private": etc / "daily-receipt" / f"daily-{suffix}.private.pem",
        "capability_manifest": etc / "runtime-capabilities-keys.json",
        "reference_source_private": etc / "runtime-capabilities" / f"reference-source-{suffix}",
    }


def _expected_directories(prefix: Path) -> dict[Path, int]:
    etc = prefix / "etc" / "rquant"
    return {
        etc: 0o755,
        etc / "lab-highwater": 0o700,
        etc / "canvas-publication": 0o700,
        etc / "shadow-report": 0o700,
        etc / "shadow-completion": 0o700,
        etc / "daily-receipt": 0o700,
        etc / "runtime-capabilities": 0o700,
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


def test_init_creates_the_thirteen_files_with_the_expected_metadata(prefix: Path) -> None:
    result = _init(prefix)
    assert result.returncode == 0, result.stdout + result.stderr

    for directory, mode in _expected_directories(prefix).items():
        observed = directory.lstat()
        assert stat.S_ISDIR(observed.st_mode), directory
        assert stat.S_IMODE(observed.st_mode) == mode, directory
        assert observed.st_uid == os.geteuid(), directory

    files = _expected_files(prefix)
    assert len(files) == 13
    for name, path in files.items():
        observed = path.lstat()
        assert stat.S_ISREG(observed.st_mode), name
        assert stat.S_IMODE(observed.st_mode) == 0o600, name
        assert observed.st_nlink == 1, name
        assert observed.st_uid == os.geteuid(), name
        assert observed.st_gid == os.getegid(), name
        assert observed.st_size > 0, name

    for name in (
        "highwater_private",
        "canvas_private",
        "shadow_private",
        "completion_private",
        "daily_private",
    ):
        text = files[name].read_text(encoding="utf-8")
        assert text.startswith("-----BEGIN PRIVATE KEY-----"), name
        assert "END PRIVATE KEY" in text, name

    # The reference source key is the one exception: `ssh-keygen -Y sign` needs an
    # OpenSSH key, so a PKCS#8 PEM here would fail only at the first real signature.
    reference = files["reference_source_private"].read_text(encoding="utf-8")
    assert reference.startswith("-----BEGIN OPENSSH PRIVATE KEY-----")


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


# --------------------------------------------------------------------------- K-7 / K-8

_DAILY_RUNPY_WITH_KEYRING = (
    "from pathlib import Path; import runpy, sys; "
    'module = runpy.run_path(sys.argv[1]); operation = module["_stdin_operation"]; '
    'operation.__globals__["PUBLIC_KEYS_FILE"] = Path(sys.argv[3]); '
    "operation(Path(sys.argv[2]))"
)


def _public_key(private_key: Path) -> str:
    result = subprocess.run(
        [_require_openssl(), "pkey", "-in", str(private_key), "-pubout"],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def _export_highwater_keyring(
    prefix: Path, current: Path | None = None
) -> subprocess.CompletedProcess[bytes]:
    arguments = [
        SYSTEM_PYTHON,
        str(HIGHWATER_HELPER),
        "--keys-file",
        str(_expected_files(prefix)["highwater_manifest"]),
        "--export-public-keyring",
    ]
    if current is not None:
        arguments += ["--current-keyring", str(current)]
    return subprocess.run(arguments, check=False, capture_output=True)


def _export_daily_keyring(
    prefix: Path, current: Path
) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        [
            SYSTEM_PYTHON,
            "-I",
            "-S",
            "-c",
            _DAILY_RUNPY_WITH_KEYRING,
            str(DAILY_HELPER),
            str(_expected_files(prefix)["daily_manifest"]),
            str(current),
        ],
        input=b'{"operation":"export-public-keyring","schema_version":1}',
        check=False,
        capture_output=True,
    )


def _publish_keyring(path: Path, payload: bytes) -> Path:
    path.write_bytes(payload)
    path.chmod(0o444)
    return path


def test_rotate_highwater_advances_the_chain_and_unlinks_the_retired_key(
    prefix: Path, tmp_path: Path
) -> None:
    assert _init(prefix).returncode == 0
    files = _expected_files(prefix)
    retired_public_key = _public_key(files["highwater_private"])
    exported = _export_highwater_keyring(prefix)
    assert exported.returncode == 0, exported.stderr.decode("utf-8", "replace")
    published = _publish_keyring(tmp_path / "lab-highwater-trusted-keys.json", exported.stdout)
    genesis_keyring = json.loads(exported.stdout)

    result = _run("rotate", "highwater", "--prefix", str(prefix), "--new-key-suffix", "v2")
    assert result.returncode == 0, result.stdout + result.stderr

    document = json.loads(files["highwater_manifest"].read_text(encoding="utf-8"))
    assert document["generation"] == 2
    assert document["previous_manifest_hash"] == genesis_keyring["manifest_hash"]
    assert document["previous_manifest_hash"] != "0" * 64
    assert document["active_key_id"] == "hw-v2"
    assert document["previous_public_keys"] == {"hw-v1": retired_public_key}
    rotated_private = _expected_files(prefix, "v2")["highwater_private"]
    assert document["active_private_key_path"] == str(rotated_private)
    assert not files["highwater_private"].exists()
    observed = rotated_private.lstat()
    assert stat.S_IMODE(observed.st_mode) == 0o600
    assert observed.st_nlink == 1

    chained = _export_highwater_keyring(prefix, current=published)
    assert chained.returncode == 0, chained.stderr.decode("utf-8", "replace")
    rotated_keyring = json.loads(chained.stdout)
    assert rotated_keyring["generation"] == 2
    assert rotated_keyring["active_key_id"] == "hw-v2"
    assert tuple(rotated_keyring["previous_public_keys"]) == ("hw-v1",)


def test_rotate_daily_keeps_the_manifest_canonical_and_chained(
    prefix: Path, tmp_path: Path
) -> None:
    assert _init(prefix).returncode == 0
    files = _expected_files(prefix)
    retired_public_key = _public_key(files["daily_private"])
    missing = tmp_path / "absent-trusted-keys.json"
    exported = _export_daily_keyring(prefix, missing)
    assert exported.returncode == 0, exported.stderr.decode("utf-8", "replace")
    published = _publish_keyring(tmp_path / "daily-receipt-trusted-keys.json", exported.stdout)
    genesis_keyring = json.loads(exported.stdout)

    result = _run("rotate", "daily", "--prefix", str(prefix), "--new-key-suffix", "v2")
    assert result.returncode == 0, result.stdout + result.stderr

    payload = files["daily_manifest"].read_bytes()
    document = json.loads(payload)
    assert _canonical(document) == payload
    assert not payload.endswith(b"\n")
    assert document["generation"] == 2
    assert document["previous_manifest_hash"] == genesis_keyring["manifest_hash"]
    assert document["active_key_id"] == "daily-v2"
    assert document["previous_public_keys"] == {"daily-v1": retired_public_key}
    assert not files["daily_private"].exists()

    chained = _export_daily_keyring(prefix, published)
    assert chained.returncode == 0, chained.stderr.decode("utf-8", "replace")
    rotated_keyring = json.loads(chained.stdout)
    assert rotated_keyring["generation"] == 2
    assert rotated_keyring["active_key_id"] == "daily-v2"
    assert tuple(rotated_keyring["previous_public_keys"]) == ("daily-v1",)

    accepted = subprocess.run(
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
    assert accepted.returncode == 0, accepted.stdout + accepted.stderr


def test_rotate_canvas_and_shadow_retain_history_without_a_chain(prefix: Path) -> None:
    assert _init(prefix).returncode == 0
    files = _expected_files(prefix)
    retired_canvas = _public_key(files["canvas_private"])
    retired_shadow = _public_key(files["shadow_private"])

    for target in ("canvas", "shadow"):
        result = _run("rotate", target, "--prefix", str(prefix), "--new-key-suffix", "v2")
        assert result.returncode == 0, result.stdout + result.stderr

    canvas = json.loads(files["canvas_manifest"].read_text(encoding="utf-8"))
    assert set(canvas) == {
        "schema_version",
        "active_key_id",
        "active_private_key_path",
        "previous_public_keys",
    }
    assert canvas["active_key_id"] == "canvas-v2"
    assert canvas["previous_public_keys"] == {"canvas-v1": retired_canvas}
    assert not files["canvas_private"].exists()

    shadow = json.loads(files["shadow_manifest"].read_text(encoding="utf-8"))
    assert shadow["active_key_id"] == "shadow-v2"
    assert shadow["previous_public_keys"] == {"shadow-v1": retired_shadow}
    assert shadow["legacy_recovery_calendar_path"] == str(files["shadow_calendar"])
    assert not files["shadow_private"].exists()

    for helper, manifest in (
        (CANVAS_HELPER, files["canvas_manifest"]),
        (SHADOW_HELPER, files["shadow_manifest"]),
    ):
        accepted = subprocess.run(
            [
                SYSTEM_PYTHON,
                str(helper),
                "--keys-file",
                str(manifest),
                "--validate-key-material",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        assert accepted.returncode == 0, accepted.stdout + accepted.stderr


def test_rotate_refuses_to_reuse_the_active_key_suffix(prefix: Path) -> None:
    assert _init(prefix).returncode == 0
    before = _expected_files(prefix)["canvas_manifest"].read_bytes()
    result = _run("rotate", "canvas", "--prefix", str(prefix), "--new-key-suffix", "v1")
    assert result.returncode != 0
    assert _expected_files(prefix)["canvas_manifest"].read_bytes() == before


def test_rotate_rejects_an_unknown_target(prefix: Path) -> None:
    assert _init(prefix).returncode == 0
    result = _run("rotate", "sealer", "--prefix", str(prefix))
    assert result.returncode == 2


# --------------------------------------------------------------------------- K-9

def _verify(prefix: Path) -> subprocess.CompletedProcess[str]:
    return _run("verify", "--prefix", str(prefix))


def _rewrite(path: Path, document: object) -> None:
    path.chmod(0o600)
    path.write_bytes(_canonical(document))
    path.chmod(0o600)


def test_verify_reports_thirteen_ok_lines_and_changes_nothing(prefix: Path) -> None:
    assert _init(prefix).returncode == 0
    files = _expected_files(prefix)
    before = {
        name: (path.read_bytes(), path.lstat().st_mtime_ns) for name, path in files.items()
    }

    result = _verify(prefix)
    assert result.returncode == 0, result.stdout + result.stderr
    lines = result.stdout.splitlines()
    assert len(lines) == 13, result.stdout
    assert all(line.startswith("OK ") for line in lines), result.stdout
    reported = [line.split(" ", 1)[1] for line in lines]
    assert sorted(reported) == sorted(str(path) for path in files.values())

    after = {
        name: (path.read_bytes(), path.lstat().st_mtime_ns) for name, path in files.items()
    }
    assert after == before


@pytest.mark.parametrize(
    "name",
    (
        "highwater_manifest",
        "canvas_manifest",
        "shadow_manifest",
        "daily_manifest",
        "shadow_calendar",
        "highwater_private",
    ),
)
def test_verify_rejects_a_world_readable_file(prefix: Path, name: str) -> None:
    assert _init(prefix).returncode == 0
    target = _expected_files(prefix)[name]
    target.chmod(0o644)

    result = _verify(prefix)
    assert result.returncode != 0, result.stdout + result.stderr
    assert str(target) in result.stderr
    assert "0644" in result.stderr


def test_verify_rejects_a_hardlinked_private_key(prefix: Path, tmp_path: Path) -> None:
    assert _init(prefix).returncode == 0
    private_key = _expected_files(prefix)["highwater_private"]
    os.link(private_key, tmp_path / "stolen.pem")

    result = _verify(prefix)
    assert result.returncode != 0
    assert "nlink" in result.stderr or "link" in result.stderr


# ------------------------------------------------------------------ mutations

def test_verify_rejects_a_broken_genesis_binding(prefix: Path) -> None:
    assert _init(prefix).returncode == 0
    manifest = _expected_files(prefix)["daily_manifest"]
    document = json.loads(manifest.read_text(encoding="utf-8"))
    document["previous_manifest_hash"] = "a" * 64
    _rewrite(manifest, document)

    result = _verify(prefix)
    assert result.returncode != 0
    assert "genesis" in result.stderr.lower()


def test_verify_rejects_a_duplicated_key_id(prefix: Path) -> None:
    assert _init(prefix).returncode == 0
    manifest = _expected_files(prefix)["canvas_manifest"]
    document = json.loads(manifest.read_text(encoding="utf-8"))
    document["previous_public_keys"] = {
        document["active_key_id"]: _public_key(_expected_files(prefix)["canvas_private"])
    }
    _rewrite(manifest, document)

    result = _verify(prefix)
    assert result.returncode != 0
    assert "active key" in result.stderr.lower()


def test_verify_rejects_a_daily_manifest_with_a_trailing_newline(prefix: Path) -> None:
    assert _init(prefix).returncode == 0
    manifest = _expected_files(prefix)["daily_manifest"]
    manifest.chmod(0o600)
    manifest.write_bytes(manifest.read_bytes() + b"\n")
    manifest.chmod(0o600)

    result = _verify(prefix)
    assert result.returncode != 0
    assert "canonical" in result.stderr.lower()


def test_verify_rejects_a_calendar_whose_digest_no_longer_matches(prefix: Path) -> None:
    assert _init(prefix).returncode == 0
    calendar = _expected_files(prefix)["shadow_calendar"]
    document = json.loads(calendar.read_text(encoding="utf-8"))
    document["coverage_end"] = "2099-12-31"
    _rewrite(calendar, document)

    result = _verify(prefix)
    assert result.returncode != 0
    assert "content_sha256" in result.stderr


def test_verify_rejects_a_manifest_pointing_outside_its_key_directory(
    prefix: Path, tmp_path: Path
) -> None:
    assert _init(prefix).returncode == 0
    stray = tmp_path / "stray.pem"
    stray.write_bytes(_expected_files(prefix)["canvas_private"].read_bytes())
    stray.chmod(0o600)
    manifest = _expected_files(prefix)["canvas_manifest"]
    document = json.loads(manifest.read_text(encoding="utf-8"))
    document["active_private_key_path"] = str(stray)
    _rewrite(manifest, document)

    result = _verify(prefix)
    assert result.returncode != 0
    assert str(stray) in result.stderr


# ------------------------------------- mutations proven against the consumers

def _daily_validate(manifest: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [SYSTEM_PYTHON, "-I", "-S", "-c", _DAILY_RUNPY, str(DAILY_HELPER), str(manifest)],
        input='{"operation":"validate-key-material","schema_version":1}',
        check=False,
        capture_output=True,
        text=True,
    )


def test_daily_helper_rejects_a_trailing_newline(prefix: Path) -> None:
    assert _init(prefix).returncode == 0
    manifest = _expected_files(prefix)["daily_manifest"]
    assert _daily_validate(manifest).returncode == 0
    manifest.write_bytes(manifest.read_bytes() + b"\n")
    manifest.chmod(0o600)

    rejected = _daily_validate(manifest)
    assert rejected.returncode != 0
    assert "not canonical" in rejected.stderr


def test_daily_helper_rejects_the_four_field_manifest_from_spec_v1(prefix: Path) -> None:
    # Regression guard for review finding M-6: the four-field manifest the first
    # spec draft implied is rejected outright, so the generator must emit six.
    assert _init(prefix).returncode == 0
    manifest = _expected_files(prefix)["daily_manifest"]
    document = json.loads(manifest.read_text(encoding="utf-8"))
    truncated = {
        key: value
        for key, value in document.items()
        if key not in {"generation", "previous_manifest_hash"}
    }
    manifest.write_bytes(_canonical(truncated))
    manifest.chmod(0o600)

    rejected = _daily_validate(manifest)
    assert rejected.returncode != 0
    assert "shape is invalid" in rejected.stderr


def test_canvas_helper_rejects_a_duplicated_active_key_id(prefix: Path) -> None:
    assert _init(prefix).returncode == 0
    files = _expected_files(prefix)
    manifest = files["canvas_manifest"]
    document = json.loads(manifest.read_text(encoding="utf-8"))
    document["previous_public_keys"] = {
        document["active_key_id"]: _public_key(files["canvas_private"])
    }
    manifest.write_bytes(_canonical(document))
    manifest.chmod(0o600)

    rejected = subprocess.run(
        [
            SYSTEM_PYTHON,
            str(CANVAS_HELPER),
            "--keys-file",
            str(manifest),
            "--validate-key-material",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert rejected.returncode != 0
    assert "previous key id is invalid" in rejected.stderr


def test_shadow_helper_rejects_a_world_readable_recovery_calendar(prefix: Path) -> None:
    assert _init(prefix).returncode == 0
    files = _expected_files(prefix)
    files["shadow_calendar"].chmod(0o444)

    rejected = subprocess.run(
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
    assert rejected.returncode != 0
    assert "Shadow recovery calendar is unsafe" in rejected.stderr


def test_highwater_helper_rejects_a_broken_rotation_binding(prefix: Path) -> None:
    assert _init(prefix).returncode == 0
    manifest = _expected_files(prefix)["highwater_manifest"]
    document = json.loads(manifest.read_text(encoding="utf-8"))
    document["generation"] = 2
    manifest.write_bytes(_canonical(document))
    manifest.chmod(0o600)

    rejected = _export_highwater_keyring(prefix)
    assert rejected.returncode != 0
    assert b"rotation binding is invalid" in rejected.stderr


# -------------------------------------------------------------------------- K-10

INFRA_INSTALLER = ROOT / "scripts" / "install-runtime-credential-infra.sh"

TRUSTED_KEYRINGS = (
    "lab-highwater-trusted-keys.json",
    "canvas-publication-trusted-keys.json",
    "shadow-report-trusted-keys.json",
    "daily-receipt-trusted-keys.json",
)


def _install_infra(prefix: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["/bin/bash", str(INFRA_INSTALLER), "--test-root", str(prefix)],
        cwd=str(ROOT),
        check=False,
        capture_output=True,
        text=True,
    )


def test_infra_installer_accepts_the_generated_tree(prefix: Path) -> None:
    from rquant.lab_highwater_authority import load_highwater_trusted_keys

    assert _init(prefix).returncode == 0
    result = _install_infra(prefix)
    assert result.returncode == 0, result.stdout + result.stderr

    etc = prefix / "etc" / "rquant"
    for name in TRUSTED_KEYRINGS:
        published = etc / name
        observed = published.lstat()
        assert stat.S_ISREG(observed.st_mode), name
        assert stat.S_IMODE(observed.st_mode) == 0o444, name
        assert "PRIVATE KEY" not in published.read_text(encoding="utf-8"), name

    keyring = load_highwater_trusted_keys(etc / "lab-highwater-trusted-keys.json")
    assert keyring.active_key_id == "hw-v1"
    assert keyring.previous_key_ids == ()


def test_infra_installer_chains_a_rotation_produced_by_this_script(prefix: Path) -> None:
    from rquant.lab_highwater_authority import load_highwater_trusted_keys

    assert _init(prefix).returncode == 0
    assert _install_infra(prefix).returncode == 0
    etc = prefix / "etc" / "rquant"
    highwater_keyring = etc / "lab-highwater-trusted-keys.json"
    daily_keyring = etc / "daily-receipt-trusted-keys.json"
    first_highwater = load_highwater_trusted_keys(highwater_keyring)
    first_daily = json.loads(daily_keyring.read_text(encoding="utf-8"))

    for target in ("highwater", "daily"):
        rotated = _run("rotate", target, "--prefix", str(prefix), "--new-key-suffix", "v2")
        assert rotated.returncode == 0, rotated.stdout + rotated.stderr

    result = _install_infra(prefix)
    assert result.returncode == 0, result.stdout + result.stderr

    rotated_highwater = load_highwater_trusted_keys(highwater_keyring)
    assert rotated_highwater.active_key_id == "hw-v2"
    assert rotated_highwater.previous_key_ids == ("hw-v1",)
    assert rotated_highwater.manifest_hash != first_highwater.manifest_hash

    rotated_daily = json.loads(daily_keyring.read_text(encoding="utf-8"))
    assert rotated_daily["generation"] == 2
    assert rotated_daily["active_key_id"] == "daily-v2"
    assert rotated_daily["previous_manifest_hash"] == first_daily["manifest_hash"]
    assert tuple(rotated_daily["previous_public_keys"]) == ("daily-v1",)


def test_rotate_refuses_when_the_published_keyring_is_stale(prefix: Path) -> None:
    assert _init(prefix).returncode == 0
    assert _install_infra(prefix).returncode == 0
    manifest = _expected_files(prefix)["highwater_manifest"]
    before = manifest.read_bytes()
    published = prefix / "etc" / "rquant" / "lab-highwater-trusted-keys.json"
    document = json.loads(published.read_text(encoding="utf-8"))
    document["manifest_hash"] = "b" * 64
    published.chmod(0o644)
    published.write_bytes(_canonical(document))
    published.chmod(0o444)

    result = _run("rotate", "highwater", "--prefix", str(prefix), "--new-key-suffix", "v2")
    assert result.returncode != 0
    assert "does not match the manifest" in result.stderr
    assert manifest.read_bytes() == before
    assert _expected_files(prefix)["highwater_private"].exists()
    assert not _expected_files(prefix, "v2")["highwater_private"].exists()


# --------------------------------------------------- verify after a published rotation

@pytest.mark.parametrize("target", ("highwater", "canvas", "shadow", "daily"))
def test_verify_succeeds_after_a_rotation_with_the_keyring_published(
    prefix: Path, target: str
) -> None:
    # Regression guard: verify --prefix's non-root high-water self-check degrades to
    # `--export-public-keyring` (the authority has no non-root --validate-key-material
    # at all).  Without `--current-keyring` that export always treats the manifest as a
    # fresh chain and rejects generation > 1, so `verify` would falsely fail after every
    # legitimate `rotate highwater` even though install-runtime-credential-infra.sh's own
    # export_highwater_public_keyring (:1302-1318) already appends --current-keyring
    # whenever a keyring has been published.  Parametrized over all four rotate targets,
    # not just the one that was broken, so a regression on any of them is caught here.
    assert _init(prefix).returncode == 0
    assert _install_infra(prefix).returncode == 0

    rotated = _run("rotate", target, "--prefix", str(prefix), "--new-key-suffix", "v2")
    assert rotated.returncode == 0, rotated.stdout + rotated.stderr

    result = _verify(prefix)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "consumer self-check passed" in result.stderr


# ----------------------------------------------------------------------- B8-B10

DOC = ROOT / "docs" / "operations" / "runtime-credential-keys.md"
PRIVATE_KEY_BLOCK = re.compile(
    rb"-----BEGIN (?:[A-Z0-9 ]+ )?PRIVATE KEY-----\s*\n[A-Za-z0-9+/=\s]{40,}-----END"
)


def test_operations_doc_records_rotation_backup_and_the_known_limits() -> None:
    text = DOC.read_text(encoding="utf-8")
    assert "私钥丢失不可恢复" in text
    assert "没有任何备份机制" in text
    assert "Canvas 与 Shadow 的轮换不成链" in text
    assert "没有 `generation` / `previous_manifest_hash` 字段" in text
    assert "rotate" in text and "verify" in text
    assert "离线加密" in text


def test_script_is_stdlib_only_and_never_touches_sudoers() -> None:
    text = SCRIPT.read_text(encoding="utf-8")
    assert "import rquant" not in text
    assert "from rquant" not in text
    assert "sudoers" not in text
    assert "uv run" not in text
    assert ".venv" not in text
    # The four stdin-only sudo aliases must stay zero-argument; this script is
    # invoked *under* sudo and never shells out to sudo itself.
    assert "sudo " not in text.replace("sudo bash", "")
    assert "/usr/local/libexec/rquant-lab-highwater-authority \"\"" not in text


def test_repository_carries_no_private_key_material() -> None:
    tracked = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=str(ROOT),
        check=True,
        capture_output=True,
    ).stdout.split(b"\0")
    paths = [Path(name.decode("utf-8")) for name in tracked if name]
    assert not [path for path in paths if path.suffix in {".pem", ".key", ".p8"}]
    for path in (SCRIPT, DOC, Path(__file__)):
        assert PRIVATE_KEY_BLOCK.search(path.read_bytes()) is None, path


def test_dry_run_reports_an_existing_tree_without_writing(prefix: Path) -> None:
    assert _init(prefix).returncode == 0
    files = _expected_files(prefix)
    before = {name: path.read_bytes() for name, path in files.items()}

    result = _init(prefix, "--dry-run")
    assert result.returncode == 3, result.stdout + result.stderr
    assert {name: path.read_bytes() for name, path in files.items()} == before


def test_verify_pins_the_four_root_call_shapes(prefix: Path) -> None:
    # B2: the root branch cannot be exercised unprivileged, so pin the exact
    # argument shapes.  Adding an argument to any of the four stdin-only sudo
    # aliases would turn the root boundary into a confused deputy.
    text = SCRIPT.read_text(encoding="utf-8")
    root_branch = text.split("Root form:", 1)[1].split("Non-root form", 1)[0]
    for helper in (
        "rquant-lab-highwater-authority",
        "rquant-canvas-publication-signer",
        "rquant-shadow-report-signer",
    ):
        assert f'"${{helper_dir}}/{helper}" \\\n            --validate-key-material' in root_branch
    assert '"${helper_dir}/rquant-daily-receipt-signer" >/dev/null' in root_branch
    assert "--keys-file" not in root_branch
    # ... and the reason the root branch is unreachable under --prefix.
    assert "must run unprivileged" in text


# --------------------------------------------------------------------------- K-13
# Route A: the completion keyring (ruling 3) and the six capability credentials
# (ruling 2). Every assertion below runs the value through the consumer that will
# read it in production, not through a restatement of its format.


def _capability_assignments(prefix: Path) -> dict[str, str]:
    result = _run("export-capabilities", "--prefix", str(prefix))
    assert result.returncode == 0, result.stdout + result.stderr
    return dict(line.split("=", 1) for line in result.stdout.splitlines())


def test_the_completion_keyring_is_separate_key_material_from_the_report_keyring(
    prefix: Path,
) -> None:
    """Ruling 3 is "one key, one use": the two manifests must not share a key."""

    assert _init(prefix).returncode == 0
    files = _expected_files(prefix)

    completion = json.loads(files["completion_manifest"].read_bytes())
    shadow = json.loads(files["shadow_manifest"].read_bytes())

    assert completion["active_key_id"] != shadow["active_key_id"]
    assert completion["active_private_key_path"] != shadow["active_private_key_path"]
    assert files["completion_private"].read_bytes() != files["shadow_private"].read_bytes()


_COMPLETION_RUNPY = (
    "from pathlib import Path; import runpy, sys; "
    'module = runpy.run_path(sys.argv[1]); operation = module["_stdin_operation"]; '
    'operation.__globals__["PUBLIC_KEYS_FILE"] = Path(sys.argv[3]); '
    "operation(Path(sys.argv[2]))"
)


def _drive_daily_helper(
    request: str,
    keys_file: Path,
    keyring_file: Path,
) -> subprocess.CompletedProcess[str]:
    """Run the daily helper against a caller-named manifest/keyring pair.

    The completion manifest is the daily shape, and the daily helper is zero-argument
    with module-level paths, so this is the seam both installer scripts use.
    """

    return subprocess.run(
        [
            SYSTEM_PYTHON,
            "-I",
            "-S",
            "-c",
            _COMPLETION_RUNPY,
            str(DAILY_HELPER),
            str(keys_file),
            str(keyring_file),
        ],
        input=request,
        check=False,
        capture_output=True,
        text=True,
    )


def test_the_completion_manifest_is_accepted_by_a_real_signer_loader(prefix: Path) -> None:
    """No completion signer helper exists yet, so the daily helper — the only loader of
    this manifest shape in the tree — is what proves the document is well formed."""

    assert _init(prefix).returncode == 0
    files = _expected_files(prefix)

    result = _drive_daily_helper(
        '{"operation":"validate-key-material","schema_version":1}',
        files["completion_manifest"],
        prefix / "etc" / "rquant" / "shadow-completion-trusted-keys.json",
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_the_completion_manifest_is_chained_like_the_daily_one(prefix: Path) -> None:
    """The published keyring carries `generation` and `previous_manifest_hash`, and those
    can only come from a chained manifest — the canvas shape has neither."""

    assert _init(prefix).returncode == 0

    document = json.loads(_expected_files(prefix)["completion_manifest"].read_bytes())

    assert document["schema_version"] == 2
    assert document["generation"] == 1
    assert document["previous_manifest_hash"] == "0" * 64


def test_rotating_the_completion_keyring_retires_the_old_public_key(prefix: Path) -> None:
    assert _init(prefix).returncode == 0
    before = json.loads(_expected_files(prefix)["completion_manifest"].read_bytes())

    result = _run("rotate", "completion", "--prefix", str(prefix), "--new-key-suffix", "v2")
    assert result.returncode == 0, result.stdout + result.stderr

    after = json.loads(_expected_files(prefix, "v2")["completion_manifest"].read_bytes())
    assert after["active_key_id"] == "completion-v2"
    assert set(after["previous_public_keys"]) == {before["active_key_id"]}
    assert not _expected_files(prefix)["completion_private"].exists()
    assert _verify(prefix).returncode == 0


def test_the_exported_capability_names_are_exactly_the_runtime_ones_without_an_owner() -> None:
    """The whitelist in the script must track `runtime_capabilities.CAPABILITY_KEYS`.

    Everything that mapping names is either an operator account credential the operator
    already holds (Tushare tokens, PushDeer/PushPlus) or one of the six this script now
    produces. A capability added to the runtime with neither owner would otherwise fail
    only at the deployer's `resolve_profile_capabilities`, on the host, at release time.
    """

    from rquant.runtime_capabilities import CAPABILITY_KEYS

    runtime_names = {name for names in CAPABILITY_KEYS.values() for name in names}
    operator_account_names = {
        "TUSHARE_TOKEN_MAIN",
        "TUSHARE_TOKEN_BACKUP",
        "PUSHDEER_KEYS",
        "PUSHDEER_ENDPOINT",
        "PUSHDEER_RECIPIENT_IDS",
        "PUSHPLUS_TOKENS",
        "PUSHPLUS_ENDPOINT",
        "PUSHPLUS_RECIPIENT_IDS",
    }
    text = SCRIPT.read_text(encoding="utf-8")
    block = text.split("CAPABILITY_ENVIRONMENT_NAMES = (", 1)[1].split(")", 1)[0]
    declared = set(re.findall(r'"([A-Z0-9_]+)"', block))

    assert declared == runtime_names - operator_account_names
    assert declared & operator_account_names == set()


def test_the_exported_capability_assignments_match_the_declared_names(prefix: Path) -> None:
    assert _init(prefix).returncode == 0
    text = SCRIPT.read_text(encoding="utf-8")
    block = text.split("CAPABILITY_ENVIRONMENT_NAMES = (", 1)[1].split(")", 1)[0]
    declared = set(re.findall(r'"([A-Z0-9_]+)"', block))

    assignments = _capability_assignments(prefix)

    assert set(assignments) == declared
    assert all(value for value in assignments.values())


def test_the_reference_source_credential_signs_and_verifies_a_real_payload(
    prefix: Path,
) -> None:
    """The end the runtime cares about: `ssh-keygen -Y sign` with the exported private
    key must verify against the exported public key under the exported key id."""

    import base64

    from rquant.live_spool import (
        ReferenceSourceBatchSigner,
        ReferenceSourceBatchVerifier,
    )

    assert _init(prefix).returncode == 0
    assignments = _capability_assignments(prefix)

    signer = ReferenceSourceBatchSigner(
        key_id=assignments["RQ_REFERENCE_SOURCE_SIGNING_KEY_ID"],
        private_key=base64.b64decode(
            assignments["RQ_REFERENCE_SOURCE_PRIVATE_KEY_BASE64"],
            validate=True,
        ).decode("ascii"),
    )
    verifier = ReferenceSourceBatchVerifier(
        key_id=assignments["RQ_REFERENCE_SOURCE_SIGNING_KEY_ID"],
        public_key=assignments["RQ_REFERENCE_SOURCE_PUBLIC_KEY"],
    )

    signature = signer.sign(b"route-a reference source payload")

    assert verifier.verify(b"route-a reference source payload", signature)
    assert not verifier.verify(b"a different payload", signature)


def test_the_reference_publication_credential_is_accepted_by_its_authenticator(
    prefix: Path,
) -> None:
    from rquant.reference_data_registry import ReferencePublicationAuthenticator

    assert _init(prefix).returncode == 0
    assignments = _capability_assignments(prefix)

    authenticator = ReferencePublicationAuthenticator(
        key_id=assignments["RQ_REFERENCE_PUBLICATION_HMAC_KEY_ID"],
        secret=bytes.fromhex(assignments["RQ_REFERENCE_PUBLICATION_HMAC_SECRET_HEX"]),
    )

    assert authenticator.key_id == assignments["RQ_REFERENCE_PUBLICATION_HMAC_KEY_ID"]


def test_the_retention_writer_credential_is_accepted_by_the_retention_builder(
    prefix: Path,
) -> None:
    from rquant.runtime_builder_retention import _writer_credential_from_capabilities

    assert _init(prefix).returncode == 0
    assignments = _capability_assignments(prefix)

    credential = _writer_credential_from_capabilities(
        {"RQ_ARTIFACT_RETENTION_WRITER_CREDENTIAL": assignments[
            "RQ_ARTIFACT_RETENTION_WRITER_CREDENTIAL"
        ]}
    )

    assert credential.sequence == 1
    assert credential.previous_secret_hex is None
    assert credential.expires_at > credential.not_before


def test_rotating_the_retention_writer_chains_the_retired_secret(prefix: Path) -> None:
    from rquant.runtime_builder_retention import _writer_credential_from_capabilities

    assert _init(prefix).returncode == 0
    before = _writer_credential_from_capabilities(
        {
            "RQ_ARTIFACT_RETENTION_WRITER_CREDENTIAL": _capability_assignments(prefix)[
                "RQ_ARTIFACT_RETENTION_WRITER_CREDENTIAL"
            ]
        }
    )

    result = _run(
        "rotate",
        "retention-writer",
        "--prefix",
        str(prefix),
        "--new-key-suffix",
        "v2",
    )
    assert result.returncode == 0, result.stdout + result.stderr

    after = _writer_credential_from_capabilities(
        {
            "RQ_ARTIFACT_RETENTION_WRITER_CREDENTIAL": _capability_assignments(prefix)[
                "RQ_ARTIFACT_RETENTION_WRITER_CREDENTIAL"
            ]
        }
    )
    assert after.key_id != before.key_id
    assert after.sequence == before.sequence + 1
    assert after.previous_secret_hex == before.secret_hex
    assert after.secret_hex != before.secret_hex
    assert _verify(prefix).returncode == 0


def test_rotating_the_reference_source_retires_its_public_key_and_unlinks_the_old_one(
    prefix: Path,
) -> None:
    assert _init(prefix).returncode == 0
    files = _expected_files(prefix)
    before = json.loads(files["capability_manifest"].read_bytes())

    result = _run(
        "rotate",
        "reference-source",
        "--prefix",
        str(prefix),
        "--new-key-suffix",
        "v2",
    )
    assert result.returncode == 0, result.stdout + result.stderr

    after = json.loads(files["capability_manifest"].read_bytes())
    assert after["reference_source_key_id"] == "reference-source-v2"
    assert set(after["reference_source_previous_public_keys"]) == {
        before["reference_source_key_id"]
    }
    assert after["reference_source_public_key"] != before["reference_source_public_key"]
    assert not files["reference_source_private"].exists()
    assert _expected_files(prefix, "v2")["reference_source_private"].exists()
    assert _verify(prefix).returncode == 0


def test_rotating_the_reference_publication_secret_changes_only_that_credential(
    prefix: Path,
) -> None:
    assert _init(prefix).returncode == 0
    before = json.loads(_expected_files(prefix)["capability_manifest"].read_bytes())

    result = _run(
        "rotate",
        "reference-publication",
        "--prefix",
        str(prefix),
        "--new-key-suffix",
        "v2",
    )
    assert result.returncode == 0, result.stdout + result.stderr

    after = json.loads(_expected_files(prefix)["capability_manifest"].read_bytes())
    assert after["reference_publication_key_id"] == "reference-publication-v2"
    assert (
        after["reference_publication_secret_hex"] != before["reference_publication_secret_hex"]
    )
    assert after["reference_source_key_id"] == before["reference_source_key_id"]
    assert after["retention_writer_credential"] == before["retention_writer_credential"]
    assert _verify(prefix).returncode == 0


def test_verify_rejects_a_reference_source_key_that_is_not_an_openssh_key(
    prefix: Path,
) -> None:
    assert _init(prefix).returncode == 0
    key = _expected_files(prefix)["reference_source_private"]
    key.chmod(0o600)
    key.write_bytes(b"-----BEGIN PRIVATE KEY-----\nnot an openssh key\n")
    key.chmod(0o600)

    result = _verify(prefix)

    assert result.returncode == 1
    assert "OpenSSH private key" in result.stderr


def test_verify_rejects_a_short_publication_secret(prefix: Path) -> None:
    assert _init(prefix).returncode == 0
    path = _expected_files(prefix)["capability_manifest"]
    document = json.loads(path.read_bytes())
    document["reference_publication_secret_hex"] = "ab" * 8

    _rewrite(path, document)

    result = _verify(prefix)
    assert result.returncode == 1
    assert "reference_publication_secret_hex is invalid" in result.stderr


def test_verify_rejects_a_capability_manifest_pointing_outside_its_directory(
    prefix: Path,
    tmp_path: Path,
) -> None:
    assert _init(prefix).returncode == 0
    path = _expected_files(prefix)["capability_manifest"]
    document = json.loads(path.read_bytes())
    document["reference_source_private_key_path"] = str(tmp_path / "elsewhere")

    _rewrite(path, document)

    result = _verify(prefix)
    assert result.returncode == 1
    assert "must be normalized inside" in result.stderr


def test_the_capability_manifest_never_appears_in_the_repository() -> None:
    """The secrets this script mints must exist only on the host that minted them."""

    tracked = subprocess.run(
        ["git", "ls-files"],
        cwd=str(ROOT),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()

    assert not [name for name in tracked if name.endswith("runtime-capabilities-keys.json")]
    assert not [name for name in tracked if "/runtime-capabilities/" in name]


# --------------------------------------------------------------------------- K-14
# M-1: adding a credential group to a host that already carries the other four.

BASE_COMMIT = "2db3846"


def _base_script(tmp_path: Path) -> Path:
    """The installer as it stood before this branch, to build a realistic starting state.

    The production host's `/etc/rquant` was written by that version, so a fixture that
    calls the *new* script to create the "already installed" four would be testing
    against a tree only the new script can produce.
    """

    script = tmp_path / "base-install-runtime-credential-keys.sh"
    script.write_bytes(
        subprocess.run(
            ["git", "show", f"{BASE_COMMIT}:scripts/install-runtime-credential-keys.sh"],
            cwd=str(ROOT),
            check=True,
            capture_output=True,
        ).stdout
    )
    return script


def _fingerprints(paths: dict[str, Path]) -> dict[str, tuple[bytes, int]]:
    return {
        name: (path.read_bytes(), path.lstat().st_mtime_ns)
        for name, path in paths.items()
        if path.exists()
    }


@pytest.fixture()
def four_group_prefix(prefix: Path, tmp_path: Path) -> Path:
    """A tree holding exactly what B-2 installed: the original nine files."""

    _require_openssl()
    result = subprocess.run(
        ["/bin/bash", str(_base_script(tmp_path)), "init", "--prefix", str(prefix)],
        cwd=str(ROOT),
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return prefix


def test_plain_init_still_refuses_a_tree_that_already_holds_the_four(
    four_group_prefix: Path,
) -> None:
    """Without the explicit mode the all-or-nothing refusal is exactly as it was."""

    before = _fingerprints(_expected_files(four_group_prefix))
    assert len(before) == 9

    result = _run("init", "--prefix", str(four_group_prefix), "--key-suffix", "v2")

    assert result.returncode == 3
    assert "refusing to overwrite existing key material" in result.stderr
    assert _fingerprints(_expected_files(four_group_prefix)) == before


def test_only_missing_adds_the_new_groups_without_touching_the_installed_ones(
    four_group_prefix: Path,
) -> None:
    """M-1: the point of the mode. Nine bytes-and-mtimes unchanged, four files added.

    Deleting the four to re-run `init` would rotate a receipt chain that is already
    running and a keyring that a published profile already references, so completing the
    tree in place is the only acceptable path on the host.
    """

    installed = _fingerprints(_expected_files(four_group_prefix))
    assert len(installed) == 9
    # The four key directories gain no entries, so a changed ctime here means the mode or
    # owner was rewritten — installed state touched for no reason.
    etc = four_group_prefix / "etc" / "rquant"
    directories = {
        name: etc.joinpath(name).lstat()
        for name in ("lab-highwater", "canvas-publication", "shadow-report", "daily-receipt")
    }

    result = _run("init", "--prefix", str(four_group_prefix), "--only-missing")
    assert result.returncode == 0, result.stdout + result.stderr

    for name, before in directories.items():
        after = etc.joinpath(name).lstat()
        assert (stat.S_IMODE(after.st_mode), after.st_uid, after.st_gid) == (
            stat.S_IMODE(before.st_mode),
            before.st_uid,
            before.st_gid,
        ), name
        assert after.st_ctime_ns == before.st_ctime_ns, name

    files = _expected_files(four_group_prefix)
    assert len(files) == 13
    for name, (payload, mtime) in installed.items():
        assert files[name].read_bytes() == payload, name
        assert files[name].lstat().st_mtime_ns == mtime, name
    for name in (
        "completion_manifest",
        "completion_private",
        "capability_manifest",
        "reference_source_private",
    ):
        assert files[name].exists(), name
        assert stat.S_IMODE(files[name].lstat().st_mode) == 0o600, name
    assert _verify(four_group_prefix).returncode == 0


def test_only_missing_leaves_the_installed_directory_mode_alone(
    four_group_prefix: Path,
) -> None:
    """Whatever the host's `/etc/rquant` mode is, adding a group is not the moment to
    change it. A mode the operator tightened by hand must survive the addition."""

    etc = four_group_prefix / "etc" / "rquant"
    etc.chmod(0o750)

    result = _run("init", "--prefix", str(four_group_prefix), "--only-missing")

    assert result.returncode == 0, result.stdout + result.stderr
    assert stat.S_IMODE(etc.lstat().st_mode) == 0o750


def test_only_missing_on_a_complete_tree_reports_that_and_writes_nothing(
    prefix: Path,
) -> None:
    assert _init(prefix).returncode == 0
    before = _fingerprints(_expected_files(prefix))

    result = _run("init", "--prefix", str(prefix), "--only-missing")

    assert result.returncode == 4
    assert "nothing to create" in result.stderr
    assert _fingerprints(_expected_files(prefix)) == before


def test_only_missing_refuses_a_half_installed_group(four_group_prefix: Path) -> None:
    """A manifest without its private key (or the reverse) is not completed silently:
    the manifest names a key path, so rebuilding either half changes what the other
    half means."""

    _expected_files(four_group_prefix)["shadow_private"].unlink()

    result = _run("init", "--prefix", str(four_group_prefix), "--only-missing")

    assert result.returncode == 3
    assert "half installed" in result.stderr
    assert not _expected_files(four_group_prefix)["completion_manifest"].exists()


def test_only_missing_dry_run_names_the_groups_without_writing(
    four_group_prefix: Path,
) -> None:
    before = _fingerprints(_expected_files(four_group_prefix))

    result = _run("init", "--prefix", str(four_group_prefix), "--only-missing", "--dry-run")

    assert result.returncode == 0, result.stdout + result.stderr
    assert "would create groups completion,capabilities" in result.stdout
    assert _fingerprints(_expected_files(four_group_prefix)) == before


# --------------------------------------------------------------------------- K-15
# M-2: the completion trusted keyring, published and read back.


def _infra(prefix: Path, *extra: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "/bin/bash",
            str(ROOT / "scripts" / "install-runtime-credential-infra.sh"),
            "--test-root",
            str(prefix),
            *extra,
        ],
        cwd=str(ROOT),
        check=False,
        capture_output=True,
        text=True,
    )


def _completion_keyring(prefix: Path) -> Path:
    return prefix / "etc" / "rquant" / "shadow-completion-trusted-keys.json"


def test_the_infra_installer_publishes_the_completion_trusted_keyring(
    prefix: Path,
) -> None:
    """M-2: before this, nothing in the tree wrote this file at all."""

    assert _init(prefix).returncode == 0

    result = _infra(prefix)
    assert result.returncode == 0, result.stdout + result.stderr

    keyring = _completion_keyring(prefix)
    assert keyring.is_file()
    assert stat.S_IMODE(keyring.lstat().st_mode) == 0o444
    document = json.loads(keyring.read_bytes())
    assert set(document) == {
        "schema_version",
        "generation",
        "previous_manifest_hash",
        "active_key_id",
        "active_public_key",
        "previous_public_keys",
        "manifest_hash",
        "signature",
    }
    assert document["active_key_id"] == "completion-v1"
    assert document["active_public_key"].startswith("-----BEGIN PUBLIC KEY-----")


def test_the_completion_keyring_is_not_the_report_keyring(prefix: Path) -> None:
    assert _init(prefix).returncode == 0
    assert _infra(prefix).returncode == 0

    completion = json.loads(_completion_keyring(prefix).read_bytes())
    report = json.loads(
        (prefix / "etc" / "rquant" / "shadow-report-trusted-keys.json").read_bytes()
    )

    assert completion["active_key_id"] != report["active_key_id"]
    assert completion["active_public_key"] != report["active_public_key"]


def test_only_missing_keyrings_publishes_just_the_absent_one(prefix: Path) -> None:
    """Route A adds a fifth keyring to a host whose other four are published and whose
    Daily authority is running; re-running the whole installer to add it would restart
    that authority and rewrite sudoers for no reason."""

    assert _init(prefix).returncode == 0
    assert _infra(prefix).returncode == 0
    published = {
        path.name: (path.read_bytes(), path.lstat().st_mtime_ns)
        for path in sorted((prefix / "etc" / "rquant").glob("*trusted-keys.json"))
    }
    assert len(published) == 5
    _completion_keyring(prefix).chmod(0o644)
    _completion_keyring(prefix).unlink()

    result = _infra(prefix, "--only-missing-keyrings")

    assert result.returncode == 0, result.stdout + result.stderr
    assert "published 1 missing keyring(s)" in result.stdout
    assert _completion_keyring(prefix).is_file()
    for name, (payload, mtime) in published.items():
        if name == _completion_keyring(prefix).name:
            continue
        path = prefix / "etc" / "rquant" / name
        assert path.read_bytes() == payload, name
        assert path.lstat().st_mtime_ns == mtime, name


def test_only_missing_keyrings_on_a_complete_tree_publishes_nothing(prefix: Path) -> None:
    assert _init(prefix).returncode == 0
    assert _infra(prefix).returncode == 0
    published = {
        path.name: (path.read_bytes(), path.lstat().st_mtime_ns)
        for path in sorted((prefix / "etc" / "rquant").glob("*trusted-keys.json"))
    }

    result = _infra(prefix, "--only-missing-keyrings")

    assert result.returncode == 0, result.stdout + result.stderr
    assert "published 0 missing keyring(s)" in result.stdout
    assert {
        path.name: (path.read_bytes(), path.lstat().st_mtime_ns)
        for path in sorted((prefix / "etc" / "rquant").glob("*trusted-keys.json"))
    } == published
