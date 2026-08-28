"""Immutable file outbox from the artifact catalog to the retention writer.

The catalog may verify a sealed bundle, but it never mutates retention metadata
directly.  It writes one immutable request here; the retention service is the
only process that can apply the request to ``references.sqlite3``.
"""

from __future__ import annotations

import os
import stat
from contextlib import suppress
from pathlib import Path

from pydantic import Field, model_validator

from rquant.artifact_retention import (
    ArtifactBundleRegistration,
    ArtifactRegistrationCounts,
    ObjectReference,
    OwnerTerminalReleaseReceipt,
)
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256

_MAX_REQUEST_BYTES = 16 * 1024 * 1024
_REQUEST_ID_PATTERN = r"^[0-9a-f]{64}$"


class ArtifactCatalogRegistrationOutboxError(RuntimeError):
    """A catalog-to-retention registration request is unsafe or inconsistent."""


class ArtifactCatalogRegistrationRequest(RuntimeContractModel):
    """One catalog-verified bundle plus its already-verified Job terminal evidence."""

    request_id: str | None = Field(default=None, pattern=_REQUEST_ID_PATTERN)
    registration: ArtifactBundleRegistration
    job_terminal_receipt: OwnerTerminalReleaseReceipt
    enqueued_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_request_identity(self) -> ArtifactCatalogRegistrationRequest:
        job_reference = next(
            (item for item in self.registration.references if item.owner_type == "job"),
            None,
        )
        receipt = self.job_terminal_receipt
        if job_reference is None or (
            receipt.reference_id,
            receipt.owner_type,
            receipt.owner_id,
            receipt.content_sha256,
        ) != (
            job_reference.reference_id,
            "job",
            job_reference.owner_id,
            job_reference.content_sha256,
        ):
            raise ValueError("catalog registration Job terminal receipt conflicts with references")
        if receipt.released_at < job_reference.created_at:
            raise ValueError("catalog Job terminal receipt predates its reference")
        payload = self.model_dump(mode="python", exclude={"request_id"})
        expected = canonical_sha256(payload)
        if self.request_id is None:
            object.__setattr__(self, "request_id", expected)
        elif self.request_id != expected:
            raise ValueError("catalog registration request identity is invalid")
        return self


class ArtifactCatalogRegistrationOutbox:
    """Crash-recoverable filesystem IPC with immutable request identities."""

    def __init__(self, root: Path) -> None:
        normalized = Path(os.path.abspath(root))
        if not root.is_absolute() or root != normalized:
            raise ValueError("catalog registration outbox root must be absolute and normalized")
        self.root = normalized
        self._ensure_directory(self.root)
        self._ensure_directory(self._directory("queued"))
        self._ensure_directory(self._directory("claimed"))
        self._ensure_directory(self._directory("completed"))

    def enqueue(self, request: ArtifactCatalogRegistrationRequest) -> bool:
        """Durably add one request, returning false for an exact replay."""

        request = ArtifactCatalogRegistrationRequest.model_validate(
            request.model_dump(mode="python")
        )
        for state in ("completed", "claimed", "queued"):
            existing = self._path(state, request)
            if not existing.exists():
                continue
            if self._load(existing) != request:
                raise ArtifactCatalogRegistrationOutboxError(
                    "catalog registration request conflicts with existing immutable request"
                )
            return False
        destination = self._path("queued", request)
        payload = request.model_dump_json().encode("utf-8")
        if len(payload) > _MAX_REQUEST_BYTES:
            raise ArtifactCatalogRegistrationOutboxError(
                "catalog registration request exceeds size"
            )
        try:
            descriptor = os.open(
                destination,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
        except FileExistsError:
            if self._load(destination) != request:
                raise ArtifactCatalogRegistrationOutboxError(
                    "catalog registration request conflicts with existing immutable request"
                ) from None
            return False
        try:
            written = 0
            while written < len(payload):
                written += os.write(descriptor, payload[written:])
            os.fsync(descriptor)
        except BaseException:
            with suppress(FileNotFoundError):
                os.unlink(destination)
            raise
        finally:
            os.close(descriptor)
        self._sync_directory(destination.parent)
        return True

    def recover_claims(self) -> int:
        """Return uncompleted claims after a retention process restart."""

        recovered = 0
        for path in self._request_paths("claimed"):
            destination = self._directory("queued") / path.name
            self._move(path, destination)
            recovered += 1
        return recovered

    def claim_next(self, *, limit: int) -> tuple[ArtifactCatalogRegistrationRequest, ...]:
        if limit < 1 or limit > 10_000:
            raise ValueError("catalog registration claim limit is out of bounds")
        claimed: list[ArtifactCatalogRegistrationRequest] = []
        for queued in self._request_paths("queued"):
            if len(claimed) >= limit:
                break
            destination = self._directory("claimed") / queued.name
            try:
                self._move(queued, destination)
            except FileNotFoundError:
                continue
            try:
                claimed.append(self._load(destination))
            except BaseException:
                self._move(destination, queued)
                raise
        return tuple(claimed)

    def complete(self, request: ArtifactCatalogRegistrationRequest) -> None:
        request = ArtifactCatalogRegistrationRequest.model_validate(
            request.model_dump(mode="python")
        )
        claimed = self._path("claimed", request)
        completed = self._path("completed", request)
        if completed.exists():
            if self._load(completed) != request:
                raise ArtifactCatalogRegistrationOutboxError(
                    "completed catalog registration request conflicts with immutable request"
                )
            if claimed.exists():
                if self._load(claimed) != request:
                    raise ArtifactCatalogRegistrationOutboxError(
                        "claimed catalog registration request conflicts with immutable request"
                    )
                os.unlink(claimed)
                self._sync_directory(claimed.parent)
            return
        if self._load(claimed) != request:
            raise ArtifactCatalogRegistrationOutboxError(
                "claimed catalog registration request conflicts with immutable request"
            )
        self._move(claimed, completed)

    def pending_count(self) -> int:
        return len(self._request_paths("queued")) + len(self._request_paths("claimed"))

    def _directory(self, name: str) -> Path:
        return self.root / name

    def _path(self, state: str, request: ArtifactCatalogRegistrationRequest) -> Path:
        assert request.request_id is not None
        return self._directory(state) / f"{request.request_id}.json"

    @staticmethod
    def _ensure_directory(path: Path) -> None:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        observed = os.lstat(path)
        if (
            stat.S_ISLNK(observed.st_mode)
            or not stat.S_ISDIR(observed.st_mode)
            or observed.st_uid != os.geteuid()
            or stat.S_IMODE(observed.st_mode) != 0o700
        ):
            raise ArtifactCatalogRegistrationOutboxError(
                "catalog registration outbox directory is unsafe"
            )

    def _request_paths(self, state: str) -> tuple[Path, ...]:
        directory = self._directory(state)
        self._ensure_directory(directory)
        paths: list[Path] = []
        for path in sorted(directory.glob("*.json")):
            if not path.is_file() or path.is_symlink():
                raise ArtifactCatalogRegistrationOutboxError(
                    "catalog registration outbox contains an unsafe request path"
                )
            if path.stem != path.name.removesuffix(".json") or len(path.stem) != 64:
                raise ArtifactCatalogRegistrationOutboxError(
                    "catalog registration outbox request name is invalid"
                )
            paths.append(path)
        return tuple(paths)

    @staticmethod
    def _sync_directory(path: Path) -> None:
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        descriptor = os.open(path, flags | getattr(os, "O_NOFOLLOW", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _load(self, path: Path) -> ArtifactCatalogRegistrationRequest:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            observed = os.fstat(descriptor)
            if (
                not stat.S_ISREG(observed.st_mode)
                or observed.st_uid != os.geteuid()
                or stat.S_IMODE(observed.st_mode) != 0o600
                or observed.st_nlink != 1
                or observed.st_size > _MAX_REQUEST_BYTES
            ):
                raise ArtifactCatalogRegistrationOutboxError(
                    "catalog registration request file is unsafe"
                )
            chunks: list[bytes] = []
            remaining = _MAX_REQUEST_BYTES + 1
            while remaining:
                chunk = os.read(descriptor, min(remaining, 1024 * 1024))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            payload = b"".join(chunks)
            if len(payload) > _MAX_REQUEST_BYTES:
                raise ArtifactCatalogRegistrationOutboxError(
                    "catalog registration request exceeds size"
                )
        finally:
            os.close(descriptor)
        try:
            request = ArtifactCatalogRegistrationRequest.model_validate_json(payload)
        except (TypeError, ValueError) as exc:
            raise ArtifactCatalogRegistrationOutboxError(
                "catalog registration request payload is invalid"
            ) from exc
        expected_name = f"{request.request_id}.json"
        if path.name != expected_name:
            raise ArtifactCatalogRegistrationOutboxError(
                "catalog registration request filename conflicts with payload identity"
            )
        return request

    def _move(self, source: Path, destination: Path) -> None:
        self._ensure_directory(source.parent)
        self._ensure_directory(destination.parent)
        os.replace(source, destination)
        self._sync_directory(source.parent)
        if destination.parent != source.parent:
            self._sync_directory(destination.parent)


class ArtifactCatalogRegistrationSink:
    """Narrow catalog capability that stages metadata for the retention writer.

    It intentionally implements only the three operations required by the Lab
    catalog's registrar.  ``register_bundle_atomic`` records no metadata itself;
    a matching Job terminal receipt is required before one immutable IPC request
    can be emitted.
    """

    def __init__(self, outbox: ArtifactCatalogRegistrationOutbox) -> None:
        self.outbox = outbox
        self.path = outbox.root / "catalog-registration.ipc"
        self._registrations: dict[str, ArtifactBundleRegistration] = {}

    def register_bundle_atomic(
        self,
        registration: ArtifactBundleRegistration,
        *,
        identity_guard: object | None = None,
    ) -> ArtifactRegistrationCounts:
        registration = ArtifactBundleRegistration.model_validate(
            registration.model_dump(mode="python")
        )
        if callable(identity_guard):
            identity_guard()
        content_sha256 = registration.object_identity.content_sha256
        existing = self._registrations.get(content_sha256)
        if existing is not None:
            if existing != registration:
                raise ArtifactCatalogRegistrationOutboxError(
                    "catalog registration conflicts with staged immutable bundle"
                )
            return ArtifactRegistrationCounts(
                registered_objects=0,
                registered_copies=0,
                registered_references=0,
            )
        self._registrations[content_sha256] = registration
        return ArtifactRegistrationCounts(
            registered_objects=1,
            registered_copies=1,
            registered_references=len(registration.references),
        )

    def list_active_references(self, content_sha256: str) -> tuple[ObjectReference, ...]:
        registration = self._registrations.get(content_sha256)
        if registration is None:
            return ()
        return registration.references

    def release_owner_terminal(self, receipt: OwnerTerminalReleaseReceipt) -> bool:
        receipt = OwnerTerminalReleaseReceipt.model_validate(receipt.model_dump(mode="python"))
        if receipt.owner_type != "job":
            raise ArtifactCatalogRegistrationOutboxError(
                "catalog IPC accepts only the verified Job terminal receipt"
            )
        registration = self._registrations.get(receipt.content_sha256)
        if registration is None:
            raise ArtifactCatalogRegistrationOutboxError(
                "catalog Job terminal receipt has no staged bundle registration"
            )
        if not any(
            (
                reference.reference_id,
                reference.owner_type,
                reference.owner_id,
                reference.content_sha256,
            )
            == (
                receipt.reference_id,
                receipt.owner_type,
                receipt.owner_id,
                receipt.content_sha256,
            )
            for reference in registration.references
        ):
            raise ArtifactCatalogRegistrationOutboxError(
                "catalog Job terminal receipt conflicts with staged bundle registration"
            )
        request = ArtifactCatalogRegistrationRequest(
            registration=registration,
            job_terminal_receipt=receipt,
            enqueued_at=receipt.released_at,
        )
        created = self.outbox.enqueue(request)
        self._registrations.pop(receipt.content_sha256, None)
        return created


__all__ = [
    "ArtifactCatalogRegistrationOutbox",
    "ArtifactCatalogRegistrationOutboxError",
    "ArtifactCatalogRegistrationRequest",
    "ArtifactCatalogRegistrationSink",
]
