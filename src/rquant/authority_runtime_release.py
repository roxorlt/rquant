"""Signed immutable release verification for privileged authority services."""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import stat
import subprocess
import tempfile
from pathlib import Path, PurePosixPath
from typing import Literal, Self

from pydantic import Field, model_validator

from rquant.authority_path_security import (
    AuthorityPathSecurityError,
    read_secure_regular_file,
    secure_path_metadata,
)
from rquant.runtime_contracts import RuntimeContractModel
from rquant.strict_json import strict_model_validate_canonical_json

AUTHORITY_RUNTIME_ROOT = Path("/usr/local/libexec/rquant-authority-runtime")
AUTHORITY_RUNTIME_SIGNING_PUBLIC_KEY = Path("/etc/rquant/keys/authority-runtime/runtime.public.pem")
AUTHORITY_RUNTIME_PUBLISHER_VERSION = "rquant-authority-runtime-publisher/v2"
AUTHORITY_RUNTIME_PUBLISHER_SHA256 = (
    "e1b96190f56544a31306a43f417e101c7aa2e0463da78dde2134656b119a19df"
)
_RELEASE_SHA = re.compile(r"^[0-9a-f]{40}$")
_MAX_MANIFEST_BYTES = 16 * 1024 * 1024
_MAX_SIGNATURE_BYTES = 16 * 1024
_MAX_PUBLIC_KEY_BYTES = 64 * 1024
_MAX_RUNTIME_FILE_BYTES = 512 * 1024 * 1024


def _utf8_sort_key(value: str) -> bytes:
    return value.encode("utf-8", errors="strict")


class AuthorityRuntimeReleaseError(RuntimeError):
    """The selected authority runtime is not a signed immutable generation."""


class AuthorityRuntimeFile(RuntimeContractModel):
    path: str = Field(min_length=1, max_length=1_024)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size: int = Field(strict=True, ge=0, le=_MAX_RUNTIME_FILE_BYTES)
    mode: Literal[0o444, 0o555]

    @model_validator(mode="after")
    def validate_relative_path(self) -> Self:
        parsed = PurePosixPath(self.path)
        if (
            parsed.is_absolute()
            or self.path != parsed.as_posix()
            or any(part in {"", ".", ".."} for part in parsed.parts)
        ):
            raise ValueError("authority runtime file path is unsafe")
        return self


class AuthorityRuntimeManifest(RuntimeContractModel):
    schema_version: Literal[2] = 2
    contract: Literal["rquant-authority-runtime-manifest/v2"] = (
        "rquant-authority-runtime-manifest/v2"
    )
    release_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    publisher_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    publisher_version: Literal["rquant-authority-runtime-publisher/v2"]
    executable: str = Field(min_length=1, max_length=1_024)
    files: tuple[AuthorityRuntimeFile, ...] = Field(min_length=1, max_length=100_000)

    @model_validator(mode="after")
    def validate_closed_inventory(self) -> Self:
        paths = tuple(item.path for item in self.files)
        if paths != tuple(sorted(paths, key=_utf8_sort_key)) or len(paths) != len(set(paths)):
            raise ValueError("authority runtime inventory must be unique and sorted")
        folded: dict[str, str] = {}
        for path in paths:
            parts = PurePosixPath(path).parts
            for depth in range(1, len(parts) + 1):
                identity = "/".join(parts[:depth])
                previous = folded.setdefault(identity.casefold(), identity)
                if previous != identity:
                    raise ValueError("authority runtime inventory has a case conflict")
        if self.executable not in set(paths):
            raise ValueError("authority runtime executable is not inventoried")
        executable = next(item for item in self.files if item.path == self.executable)
        if executable.mode != 0o555:
            raise ValueError("authority runtime executable is not immutable executable content")
        return self


class VerifiedAuthorityRuntimeRelease(RuntimeContractModel):
    schema_version: Literal[2] = 2
    contract: Literal["rquant-verified-authority-runtime-release/v2"] = (
        "rquant-verified-authority-runtime-release/v2"
    )
    generation_id: str = Field(pattern=r"^[0-9a-f]{40}$")
    release_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    publisher_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    publisher_version: Literal["rquant-authority-runtime-publisher/v2"]
    executable_path: Path
    file_count: int = Field(strict=True, ge=1)


def _selected_generation_id(root: Path) -> str:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    descriptor = -1
    try:
        descriptor = os.open(root, flags)
        before = os.stat("current", dir_fd=descriptor, follow_symlinks=False)
        target = os.readlink("current", dir_fd=descriptor)
        after = os.stat("current", dir_fd=descriptor, follow_symlinks=False)
        if (
            not stat.S_ISLNK(before.st_mode)
            or (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino)
            or not target.startswith("generations/")
            or target.count("/") != 1
        ):
            raise AuthorityRuntimeReleaseError("authority runtime current pointer is unsafe")
        generation_id = target.removeprefix("generations/")
        if _RELEASE_SHA.fullmatch(generation_id) is None:
            raise AuthorityRuntimeReleaseError("authority runtime generation id is invalid")
        links = tuple(
            sorted(
                name
                for name in os.listdir(descriptor)
                if stat.S_ISLNK(os.stat(name, dir_fd=descriptor, follow_symlinks=False).st_mode)
            )
        )
        if links != ("current",):
            raise AuthorityRuntimeReleaseError(
                "authority runtime root must contain only the current pointer"
            )
        return generation_id
    except AuthorityRuntimeReleaseError:
        raise
    except OSError as exc:
        raise AuthorityRuntimeReleaseError(
            "authority runtime current pointer is unavailable"
        ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _verify_signature(*, public_key: bytes, manifest: bytes, signature: bytes) -> None:
    openssl = shutil.which("openssl")
    if openssl is None:
        raise AuthorityRuntimeReleaseError("authority runtime signature verifier is unavailable")
    try:
        with tempfile.TemporaryDirectory(prefix="rquant-authority-runtime-verify-") as temporary:
            directory = Path(temporary)
            public = directory / "public.pem"
            payload = directory / "manifest.json"
            signed = directory / "manifest.sig"
            public.write_bytes(public_key)
            payload.write_bytes(manifest)
            signed.write_bytes(signature)
            completed = subprocess.run(
                (
                    openssl,
                    "pkeyutl",
                    "-verify",
                    "-pubin",
                    "-rawin",
                    "-inkey",
                    str(public),
                    "-in",
                    str(payload),
                    "-sigfile",
                    str(signed),
                ),
                check=False,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                timeout=5,
            )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise AuthorityRuntimeReleaseError(
            "authority runtime signature verification failed"
        ) from exc
    if completed.returncode != 0:
        raise AuthorityRuntimeReleaseError("authority runtime manifest signature is invalid")


def verify_authority_runtime_release(
    *,
    root: Path = AUTHORITY_RUNTIME_ROOT,
    signing_public_key_path: Path = AUTHORITY_RUNTIME_SIGNING_PUBLIC_KEY,
    trusted_root: Path = Path("/"),
    expected_uid: int = 0,
    expected_gid: int = 0,
    signing_key_uid: int = 0,
    signing_key_gid: int = 0,
    expected_publisher_sha256: str,
    expected_publisher_version: str,
    expected_release_sha: str | None = None,
) -> VerifiedAuthorityRuntimeRelease:
    """Verify a signed content-addressed runtime and every payload byte."""

    if expected_release_sha is not None and _RELEASE_SHA.fullmatch(expected_release_sha) is None:
        raise AuthorityRuntimeReleaseError("expected authority runtime release SHA is invalid")
    if not re.fullmatch(r"[0-9a-f]{64}", expected_publisher_sha256) or (
        expected_publisher_version != AUTHORITY_RUNTIME_PUBLISHER_VERSION
    ):
        raise AuthorityRuntimeReleaseError("expected authority runtime publisher is invalid")
    try:
        secure_path_metadata(
            root,
            trusted_root=trusted_root,
            allowed_ancestor_uids=frozenset({0, expected_uid}),
            kind="directory",
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            expected_mode=0o755,
        )
        generations = root / "generations"
        secure_path_metadata(
            generations,
            trusted_root=trusted_root,
            allowed_ancestor_uids=frozenset({0, expected_uid}),
            kind="directory",
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            expected_mode=0o755,
        )
        generation_id = _selected_generation_id(root)
        generation = generations / generation_id
        secure_path_metadata(
            generation,
            trusted_root=trusted_root,
            allowed_ancestor_uids=frozenset({0, expected_uid}),
            kind="directory",
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            expected_mode=0o555,
        )
        manifest_bytes = read_secure_regular_file(
            generation / "manifest.json",
            trusted_root=trusted_root,
            allowed_ancestor_uids=frozenset({0, expected_uid}),
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            allowed_modes=frozenset({0o444}),
            max_bytes=_MAX_MANIFEST_BYTES,
        )
        manifest = strict_model_validate_canonical_json(
            AuthorityRuntimeManifest,
            manifest_bytes.decode("utf-8"),
        )
        if manifest.release_sha != generation_id:
            raise AuthorityRuntimeReleaseError(
                "authority runtime generation does not match release SHA"
            )
        manifest_hash_bytes = read_secure_regular_file(
            generation / "manifest.sha256",
            trusted_root=trusted_root,
            allowed_ancestor_uids=frozenset({0, expected_uid}),
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            allowed_modes=frozenset({0o444}),
            max_bytes=65,
        )
        expected_manifest_hash = hashlib.sha256(manifest_bytes).hexdigest()
        if manifest_hash_bytes != f"{expected_manifest_hash}\n".encode("ascii"):
            raise AuthorityRuntimeReleaseError("authority runtime manifest hash is invalid")
        if expected_release_sha is not None and manifest.release_sha != expected_release_sha:
            raise AuthorityRuntimeReleaseError("authority runtime release SHA mismatch")
        if (
            manifest.publisher_sha256 != expected_publisher_sha256
            or manifest.publisher_version != expected_publisher_version
        ):
            raise AuthorityRuntimeReleaseError("authority runtime publisher identity mismatch")
        signature = read_secure_regular_file(
            generation / "manifest.sig",
            trusted_root=trusted_root,
            allowed_ancestor_uids=frozenset({0, expected_uid}),
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            allowed_modes=frozenset({0o444}),
            max_bytes=_MAX_SIGNATURE_BYTES,
        )
        public_key = read_secure_regular_file(
            signing_public_key_path,
            trusted_root=trusted_root,
            allowed_ancestor_uids=frozenset({0, signing_key_uid}),
            expected_uid=signing_key_uid,
            expected_gid=signing_key_gid,
            allowed_modes=frozenset({0o440, 0o444}),
            max_bytes=_MAX_PUBLIC_KEY_BYTES,
        )
        _verify_signature(public_key=public_key, manifest=manifest_bytes, signature=signature)

        payload = generation / "payload"
        secure_path_metadata(
            payload,
            trusted_root=trusted_root,
            allowed_ancestor_uids=frozenset({0, expected_uid}),
            kind="directory",
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            expected_mode=0o555,
        )
        observed_paths: list[str] = []
        for directory, names, filenames in os.walk(payload, followlinks=False):
            directory_path = Path(directory)
            secure_path_metadata(
                directory_path,
                trusted_root=trusted_root,
                allowed_ancestor_uids=frozenset({0, expected_uid}),
                kind="directory",
                expected_uid=expected_uid,
                expected_gid=expected_gid,
                expected_mode=0o555,
            )
            for name in names:
                child = directory_path / name
                if child.is_symlink():
                    raise AuthorityRuntimeReleaseError("authority runtime contains a symlink")
            for filename in filenames:
                observed_paths.append((directory_path / filename).relative_to(payload).as_posix())
        inventory = {item.path: item for item in manifest.files}
        if tuple(sorted(observed_paths, key=_utf8_sort_key)) != tuple(
            sorted(inventory, key=_utf8_sort_key)
        ):
            raise AuthorityRuntimeReleaseError("authority runtime file inventory mismatch")
        for relative, item in inventory.items():
            file_path = payload / Path(*PurePosixPath(relative).parts)
            payload_bytes = read_secure_regular_file(
                file_path,
                trusted_root=trusted_root,
                allowed_ancestor_uids=frozenset({0, expected_uid}),
                expected_uid=expected_uid,
                expected_gid=expected_gid,
                allowed_modes=frozenset({item.mode}),
                max_bytes=max(1, item.size),
                min_bytes=0,
            )
            if len(payload_bytes) != item.size or hashlib.sha256(payload_bytes).hexdigest() != (
                item.sha256
            ):
                raise AuthorityRuntimeReleaseError("authority runtime file hash mismatch")
        executable = payload / Path(*PurePosixPath(manifest.executable).parts)
        return VerifiedAuthorityRuntimeRelease(
            generation_id=generation_id,
            release_sha=manifest.release_sha,
            publisher_sha256=manifest.publisher_sha256,
            publisher_version=manifest.publisher_version,
            executable_path=executable,
            file_count=len(manifest.files),
        )
    except AuthorityRuntimeReleaseError:
        raise
    except (AuthorityPathSecurityError, OSError, UnicodeError, ValueError) as exc:
        raise AuthorityRuntimeReleaseError("authority runtime path or content is unsafe") from exc


__all__ = [
    "AUTHORITY_RUNTIME_ROOT",
    "AUTHORITY_RUNTIME_PUBLISHER_SHA256",
    "AUTHORITY_RUNTIME_PUBLISHER_VERSION",
    "AUTHORITY_RUNTIME_SIGNING_PUBLIC_KEY",
    "AuthorityRuntimeFile",
    "AuthorityRuntimeManifest",
    "AuthorityRuntimeReleaseError",
    "VerifiedAuthorityRuntimeRelease",
    "verify_authority_runtime_release",
]
