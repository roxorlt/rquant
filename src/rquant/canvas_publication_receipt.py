"""Signed immutable receipts for Canvas publications consumed by Serving."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import stat
import subprocess
from collections.abc import Mapping
from contextlib import suppress
from pathlib import Path
from typing import Annotated, Literal, Self
from uuid import uuid4

from pydantic import Field, StringConstraints, field_validator, model_validator

from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
)
from rquant.runtime_shadow_validation import (
    CompletionAttestationSigningClient,
    Ed25519CompletionAttestationKeyring,
    Ed25519CompletionAttestationSigner,
    _ed25519_signing_payload,
    _valid_ed25519_signature,
    _verify_ed25519_signature,
)
from rquant.strict_json import (
    canonical_json_bytes,
    strict_model_validate_canonical_json,
    strict_model_validate_json,
)

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
PoolRef = Annotated[str, StringConstraints(min_length=1, max_length=128)]

CANVAS_PUBLICATION_NAMESPACE = "rquant-serving-canvas-publication-receipt"
CANVAS_PUBLICATION_PROBE_NAMESPACE = "rquant-serving-canvas-publication-probe"
CANVAS_CATALOG_SCHEMA_VERSION = 1
CANVAS_PUBLICATION_EFFECT_KIND = "save_canvas_publication"
PRIVATE_DIRECTORY_MODE = 0o700
PRIVATE_FILE_MODE = 0o600
MAX_CANVAS_CATALOG_RECORD_BYTES = 64 * 1024
MAX_RECEIPT_BYTES = 1024 * 1024


class CanvasPublicationCommand(RuntimeContractModel):
    kind: Literal["save_canvas"] = "save_canvas"
    command_id: str = Field(min_length=1, max_length=128)
    requested_at: AwareUtcDatetime
    name: str = Field(min_length=1, max_length=128, pattern=r"^[\w\u4e00-\u9fff-]+$")
    description: str = Field(default="", max_length=8_192)
    pool_refs: tuple[PoolRef, ...] = Field(default_factory=tuple, max_length=256)
    source: str = Field(default="page_control", min_length=1, max_length=128)


class CanvasPublicationCatalogRecord(RuntimeContractModel):
    schema_version: Literal[1] = CANVAS_CATALOG_SCHEMA_VERSION
    name: str = Field(min_length=1, max_length=128, pattern=r"^[\w\u4e00-\u9fff-]+$")
    description: str = Field(default="", max_length=8_192)
    pool_refs: tuple[PoolRef, ...] = Field(default_factory=tuple, max_length=256)
    created_at: AwareUtcDatetime
    updated_at: AwareUtcDatetime
    source: str = Field(min_length=1, max_length=128)
    command_id: str = Field(min_length=1, max_length=128)
    command_hash: Sha256
    source_identity_hash: Sha256
    publication_generation_id: Sha256
    publication_receipt_id: Sha256
    record_hash: Sha256

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if self.updated_at < self.created_at:
            raise ValueError("canvas publication catalog updated_at precedes created_at")
        expected_source_identity_hash = canvas_source_identity_hash(
            command_id=self.command_id,
            command_hash=self.command_hash,
            source=self.source,
        )
        if self.source_identity_hash != expected_source_identity_hash:
            raise ValueError("canvas publication source identity hash mismatch")
        expected_record_hash = canvas_catalog_record_hash(self)
        if self.record_hash != expected_record_hash:
            raise ValueError("canvas publication catalog record hash mismatch")
        if len(_canvas_catalog_publication_bytes(self)) > MAX_CANVAS_CATALOG_RECORD_BYTES:
            raise ValueError("canvas publication catalog record exceeds size bound")
        return self


class CanvasPublicationClaims(RuntimeContractModel):
    contract: Literal["serving-canvas-publication-claims/v1"] = (
        "serving-canvas-publication-claims/v1"
    )
    command: CanvasPublicationCommand
    command_hash: Sha256
    catalog_record: CanvasPublicationCatalogRecord
    catalog_record_hash: Sha256
    source_identity_hash: Sha256
    consumer_service_id: str = Field(min_length=1, max_length=128)
    consumer_instance_id: str = Field(min_length=1, max_length=256)
    effect_kind: Literal["save_canvas_publication"] = CANVAS_PUBLICATION_EFFECT_KIND
    effect_id: Sha256
    generation_id: Sha256
    receipt_id: Sha256
    created_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_claims(self) -> Self:
        expected_command_hash = canvas_command_hash(self.command)
        if self.command_hash != expected_command_hash:
            raise ValueError("canvas publication command hash mismatch")
        expected_source_identity_hash = canvas_source_identity_hash(
            command_id=self.command.command_id,
            command_hash=self.command_hash,
            source=self.command.source,
        )
        if self.source_identity_hash != expected_source_identity_hash:
            raise ValueError("canvas publication source identity mismatch")
        expected_effect_id = canvas_publication_effect_id(
            command_hash=self.command_hash,
            source_identity_hash=self.source_identity_hash,
            consumer_service_id=self.consumer_service_id,
            consumer_instance_id=self.consumer_instance_id,
        )
        if self.effect_id != expected_effect_id:
            raise ValueError("canvas publication effect identity mismatch")
        expected_generation_id = canvas_publication_generation_id(
            command_hash=self.command_hash,
            source_identity_hash=self.source_identity_hash,
            effect_id=self.effect_id,
        )
        if self.generation_id != expected_generation_id:
            raise ValueError("canvas publication generation identity mismatch")
        expected_receipt_id = canvas_publication_receipt_id(
            command_hash=self.command_hash,
            source_identity_hash=self.source_identity_hash,
            effect_id=self.effect_id,
            generation_id=self.generation_id,
        )
        if self.receipt_id != expected_receipt_id:
            raise ValueError("canvas publication receipt identity mismatch")
        catalog = self.catalog_record
        expected_catalog_fields: Mapping[str, object] = {
            "name": self.command.name,
            "description": self.command.description,
            "pool_refs": self.command.pool_refs,
            "source": self.command.source,
            "command_id": self.command.command_id,
            "command_hash": self.command_hash,
            "source_identity_hash": self.source_identity_hash,
            "publication_generation_id": self.generation_id,
            "publication_receipt_id": self.receipt_id,
        }
        for field_name, expected in expected_catalog_fields.items():
            if getattr(catalog, field_name) != expected:
                raise ValueError(f"canvas publication catalog mismatch: {field_name}")
        if self.catalog_record_hash != catalog.record_hash:
            raise ValueError("canvas publication catalog record hash claim mismatch")
        return self


class CanvasPublicationReceipt(RuntimeContractModel):
    contract: Literal["serving-canvas-publication-receipt/v1"] = (
        "serving-canvas-publication-receipt/v1"
    )
    receipt_id: Sha256
    receipt_hash: Sha256 | None = None
    key_id: str = Field(min_length=1, max_length=128)
    claims: CanvasPublicationClaims
    signature_algorithm: Literal["ed25519"] = "ed25519"
    signature: str = Field(min_length=1, max_length=16_384)

    @field_validator("key_id")
    @classmethod
    def validate_key_id(cls, value: str) -> str:
        if any(character.isspace() for character in value):
            raise ValueError("canvas publication key_id cannot contain whitespace")
        return value

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if self.receipt_id != self.claims.receipt_id:
            raise ValueError("canvas publication receipt id does not match claims")
        if self.claims.catalog_record.publication_receipt_id != self.receipt_id:
            raise ValueError("canvas publication catalog does not bind receipt id")
        if not _valid_ed25519_signature(self.signature):
            raise ValueError("canvas publication signature is not an Ed25519 signature")
        expected_hash = canvas_publication_receipt_hash(self)
        if self.receipt_hash is None:
            object.__setattr__(self, "receipt_hash", expected_hash)
        elif self.receipt_hash != expected_hash:
            raise ValueError("canvas publication receipt hash mismatch")
        return self

    def canonical_json_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json", round_trip=True))


class CanvasPublicationSigner:
    def issue_publication(
        self,
        claims: CanvasPublicationClaims,
    ) -> CanvasPublicationReceipt:
        raise NotImplementedError


class CanvasPublicationKeyring:
    active_key_id: str

    def verify_publication_receipt(
        self,
        receipt: CanvasPublicationReceipt,
        *,
        require_active: bool = False,
    ) -> bool:
        raise NotImplementedError


class CanvasPublicationSigningRequest(RuntimeContractModel):
    schema_version: Literal[1] = 1
    operation: Literal["sign"] = "sign"
    request_id: Sha256
    key_id: str = Field(min_length=1, max_length=128)
    namespace: str = Field(min_length=1, max_length=256)
    payload_base64: str = Field(min_length=1, max_length=2 * 1024 * 1024)
    payload_sha256: Sha256


class CanvasPublicationSigningResponse(RuntimeContractModel):
    schema_version: Literal[1] = 1
    operation: Literal["sign"] = "sign"
    request_id: Sha256
    key_id: str = Field(min_length=1, max_length=128)
    namespace: str = Field(min_length=1, max_length=256)
    payload_sha256: Sha256
    signature: str = Field(min_length=1, max_length=16_384)


class SecureCanvasPublicationSigningClient:
    """Invoke one protected signer capability without exposing private key material."""

    def __init__(
        self,
        *,
        command: tuple[str, ...],
        key_id: str,
        timeout_seconds: float,
    ) -> None:
        if not command or any(not item for item in command):
            raise ValueError("Canvas publication signer capability command is invalid")
        self._command = tuple(command)
        self.key_id = key_id
        self.timeout_seconds = timeout_seconds

    def sign(self, *, namespace: str, payload: bytes) -> str:
        if namespace not in {
            CANVAS_PUBLICATION_NAMESPACE,
            CANVAS_PUBLICATION_PROBE_NAMESPACE,
        }:
            raise ValueError("Canvas publication signer namespace is not allowed")
        if not payload or len(payload) > MAX_RECEIPT_BYTES:
            raise ValueError("Canvas publication signer payload is invalid")
        payload_sha256 = hashlib.sha256(payload).hexdigest()
        request_id = canonical_sha256(
            {
                "contract": "serving-canvas-publication-signing-request/v1",
                "key_id": self.key_id,
                "namespace": namespace,
                "payload_sha256": payload_sha256,
            }
        )
        request = CanvasPublicationSigningRequest(
            request_id=request_id,
            key_id=self.key_id,
            namespace=namespace,
            payload_base64=base64.b64encode(payload).decode("ascii"),
            payload_sha256=payload_sha256,
        )
        try:
            completed = subprocess.run(
                self._command,
                input=canonical_json_bytes(request.model_dump(mode="json")),
                check=False,
                capture_output=True,
                timeout=self.timeout_seconds,
                env={"LANG": "C", "PATH": "/usr/bin:/bin"},
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise RuntimeError("Canvas publication signer capability is unavailable") from exc
        if completed.returncode != 0 or not completed.stdout:
            raise RuntimeError("Canvas publication signer capability failed")
        try:
            response = strict_model_validate_json(
                CanvasPublicationSigningResponse,
                completed.stdout,
            )
        except ValueError as exc:
            raise RuntimeError(
                "Canvas publication signer capability returned an invalid response"
            ) from exc
        expected = (
            response.request_id == request.request_id
            and response.key_id == request.key_id
            and response.namespace == request.namespace
            and response.payload_sha256 == request.payload_sha256
            and _valid_ed25519_signature(response.signature)
        )
        if not expected:
            raise RuntimeError("Canvas publication signer capability response does not match")
        return response.signature


class Ed25519CanvasPublicationSigner(Ed25519CompletionAttestationSigner):
    """Opaque Ed25519 signer adapter; private key material stays behind the client."""

    def __init__(self, *, key_id: str, client: CompletionAttestationSigningClient) -> None:
        super().__init__(key_id=key_id, client=client)

    def issue_publication(
        self,
        claims: CanvasPublicationClaims,
    ) -> CanvasPublicationReceipt:
        verified = CanvasPublicationClaims.model_validate(claims)
        signature = self._client.sign(
            namespace=CANVAS_PUBLICATION_NAMESPACE,
            payload=_ed25519_signing_payload(
                namespace=CANVAS_PUBLICATION_NAMESPACE,
                payload=canvas_publication_claims_payload(verified),
            ),
        )
        if not _valid_ed25519_signature(signature):
            raise ValueError("canvas publication signing client returned an invalid signature")
        unsigned = CanvasPublicationReceipt(
            receipt_id=verified.receipt_id,
            key_id=self.key_id,
            claims=verified,
            signature=signature,
        )
        return CanvasPublicationReceipt(
            **unsigned.model_dump(mode="python", exclude={"receipt_hash"}),
            receipt_hash=canvas_publication_receipt_hash(unsigned),
        )


class Ed25519CanvasPublicationKeyring(Ed25519CompletionAttestationKeyring):
    """Public-only active plus previous-key verifier for Canvas receipts."""

    def verify_publication_receipt(
        self,
        receipt: CanvasPublicationReceipt,
        *,
        require_active: bool = False,
    ) -> bool:
        try:
            verified = CanvasPublicationReceipt.model_validate(receipt)
        except (TypeError, ValueError):
            return False
        return self.verify_detached_payload(
            key_id=verified.key_id,
            payload=_ed25519_signing_payload(
                namespace=CANVAS_PUBLICATION_NAMESPACE,
                payload=canvas_publication_claims_payload(verified.claims),
            ),
            signature=verified.signature,
            require_active=require_active,
        )

    def verify_detached_payload(
        self,
        *,
        key_id: str,
        payload: bytes,
        signature: str,
        require_active: bool = False,
    ) -> bool:
        if require_active and key_id != self.active_key_id:
            return False
        public_key = self._keys.get(key_id)
        if public_key is None:
            return False
        return _verify_ed25519_signature(
            public_key=public_key,
            payload=payload,
            signature=signature,
        )


class CanvasPublicationReceiptStore:
    """Safe no-follow, no-overwrite store for immutable Canvas publication receipts."""

    def __init__(self, root: Path, *, directory_descriptor: int | None = None) -> None:
        self.root = Path(os.path.abspath(root))
        self._directory_descriptor = directory_descriptor

    def path_for(self, receipt_id: str) -> Path:
        _validate_sha256(receipt_id, label="canvas publication receipt id")
        return self.root / f"{receipt_id}.json"

    def write_immutable(self, receipt: CanvasPublicationReceipt) -> Path:
        verified = CanvasPublicationReceipt.model_validate(receipt)
        payload = verified.canonical_json_bytes()
        if len(payload) > MAX_RECEIPT_BYTES:
            raise ValueError("canvas publication receipt exceeds size bound")
        directory = self._open_directory(create=True)
        file_name = f"{verified.receipt_id}.json"
        staging_name = f".{file_name}.{uuid4().hex}.tmp"
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor: int | None = None
        try:
            _verify_open_directory_matches_path(self.root, directory)
            try:
                descriptor = os.open(staging_name, flags, PRIVATE_FILE_MODE, dir_fd=directory)
            except OSError as exc:
                raise ValueError(
                    "canvas publication receipt staging cannot be opened safely"
                ) from exc
            os.fchmod(descriptor, PRIVATE_FILE_MODE)
            _write_all(descriptor, payload)
            os.fsync(descriptor)
            opened = os.fstat(descriptor)
            if not stat.S_ISREG(opened.st_mode):
                raise ValueError("canvas publication receipt staging is not a regular file")
            _verify_open_file_matches_entry(
                directory,
                staging_name,
                opened,
                label="canvas publication receipt staging",
                expected_mode=PRIVATE_FILE_MODE,
            )
            _verify_open_directory_matches_path(self.root, directory)
            try:
                os.link(
                    staging_name,
                    file_name,
                    src_dir_fd=directory,
                    dst_dir_fd=directory,
                    follow_symlinks=False,
                )
            except FileExistsError:
                existing, opened_existing = _read_receipt_bytes_from_directory(
                    directory,
                    file_name,
                    label="canvas publication receipt",
                )
                _verify_open_directory_matches_path(self.root, directory)
                _verify_open_file_matches_entry(
                    directory,
                    file_name,
                    opened_existing,
                    label="canvas publication receipt",
                )
                _verify_open_directory_matches_path(self.root, directory)
                if existing != payload:
                    raise ValueError(
                        "canvas publication receipt already exists with different bytes"
                    ) from None
                return self.path_for(verified.receipt_id)
            except OSError as exc:
                raise ValueError("canvas publication receipt cannot be published safely") from exc
            _verify_open_file_matches_entry(
                directory,
                file_name,
                opened,
                label="canvas publication receipt",
                expected_mode=PRIVATE_FILE_MODE,
            )
            _verify_open_directory_matches_path(self.root, directory)
            os.fsync(directory)
            with suppress(FileNotFoundError):
                os.unlink(staging_name, dir_fd=directory)
            _verify_open_directory_matches_path(self.root, directory)
            os.fsync(directory)
            _verify_open_directory_matches_path(self.root, directory)
        finally:
            if descriptor is not None:
                os.close(descriptor)
            with suppress(FileNotFoundError):
                os.unlink(staging_name, dir_fd=directory)
            os.close(directory)
        return self.path_for(verified.receipt_id)

    def read(self, receipt_id: str) -> CanvasPublicationReceipt:
        payload = self.read_bytes(receipt_id)
        try:
            receipt = strict_model_validate_canonical_json(
                CanvasPublicationReceipt,
                payload,
            )
        except ValueError as exc:
            raise ValueError("canvas publication receipt is not canonical JSON") from exc
        if receipt.receipt_id != receipt_id:
            raise ValueError("canvas publication receipt path identity mismatch")
        return receipt

    def read_bytes(self, receipt_id: str) -> bytes:
        path = self.path_for(receipt_id)
        directory = self._open_directory(create=False)
        try:
            _verify_open_directory_matches_path(self.root, directory)
            payload, opened = _read_receipt_bytes_from_directory(
                directory,
                path.name,
                label="canvas publication receipt",
            )
            _verify_open_directory_matches_path(self.root, directory)
            _verify_open_file_matches_entry(
                directory,
                path.name,
                opened,
                label="canvas publication receipt",
            )
            _verify_open_directory_matches_path(self.root, directory)
            return payload
        finally:
            os.close(directory)

    def _open_directory(self, *, create: bool) -> int:
        if self._directory_descriptor is None:
            return (
                _open_or_create_receipt_directory(self.root)
                if create
                else _open_existing_receipt_directory(self.root)
            )
        descriptor = os.dup(self._directory_descriptor)
        try:
            observed = os.fstat(descriptor)
            if not stat.S_ISDIR(observed.st_mode):
                raise ValueError(
                    "canvas publication receipt bound descriptor is not a directory"
                )
            _verify_open_directory_matches_path(self.root, descriptor)
            return descriptor
        except Exception:
            os.close(descriptor)
            raise


def canvas_command_hash(command: CanvasPublicationCommand) -> str:
    return canonical_sha256(command.model_dump(mode="json"))


def canvas_source_identity_hash(
    *,
    command_id: str,
    command_hash: str,
    source: str,
) -> str:
    return canonical_sha256(
        {
            "schema_version": CANVAS_CATALOG_SCHEMA_VERSION,
            "command_id": command_id,
            "command_hash": command_hash,
            "source": source,
        }
    )


def canvas_publication_effect_id(
    *,
    command_hash: str,
    source_identity_hash: str,
    consumer_service_id: str,
    consumer_instance_id: str,
) -> str:
    return canonical_sha256(
        {
            "contract": "serving-canvas-publication-effect/v1",
            "effect_kind": CANVAS_PUBLICATION_EFFECT_KIND,
            "command_hash": command_hash,
            "source_identity_hash": source_identity_hash,
            "consumer_service_id": consumer_service_id,
            "consumer_instance_id": consumer_instance_id,
        }
    )


def canvas_publication_generation_id(
    *,
    command_hash: str,
    source_identity_hash: str,
    effect_id: str,
) -> str:
    return canonical_sha256(
        {
            "contract": "serving-canvas-publication-generation/v1",
            "command_hash": command_hash,
            "source_identity_hash": source_identity_hash,
            "effect_id": effect_id,
        }
    )


def canvas_publication_receipt_id(
    *,
    command_hash: str,
    source_identity_hash: str,
    effect_id: str,
    generation_id: str,
) -> str:
    return canonical_sha256(
        {
            "contract": "serving-canvas-publication-receipt-id/v1",
            "command_hash": command_hash,
            "source_identity_hash": source_identity_hash,
            "effect_id": effect_id,
            "generation_id": generation_id,
        }
    )


def canvas_catalog_record_hash(record: CanvasPublicationCatalogRecord) -> str:
    return canonical_sha256(record.model_dump(mode="json", exclude={"record_hash"}))


def canvas_publication_claims_payload(claims: CanvasPublicationClaims) -> bytes:
    return canonical_json_bytes(claims.model_dump(mode="json", round_trip=True))


def canvas_publication_receipt_hash(receipt: CanvasPublicationReceipt) -> str:
    return canonical_sha256(
        {
            "contract": "serving-canvas-publication-receipt-envelope/v1",
            "receipt_id": receipt.receipt_id,
            "key_id": receipt.key_id,
            "claims": receipt.claims,
            "signature_algorithm": receipt.signature_algorithm,
            "signature": receipt.signature,
        }
    )


def build_canvas_publication_claims(
    *,
    command: CanvasPublicationCommand,
    catalog_created_at: AwareUtcDatetime,
    catalog_updated_at: AwareUtcDatetime,
    consumer_service_id: str,
    consumer_instance_id: str,
) -> CanvasPublicationClaims:
    command_hash = canvas_command_hash(command)
    source_identity_hash = canvas_source_identity_hash(
        command_id=command.command_id,
        command_hash=command_hash,
        source=command.source,
    )
    effect_id = canvas_publication_effect_id(
        command_hash=command_hash,
        source_identity_hash=source_identity_hash,
        consumer_service_id=consumer_service_id,
        consumer_instance_id=consumer_instance_id,
    )
    generation_id = canvas_publication_generation_id(
        command_hash=command_hash,
        source_identity_hash=source_identity_hash,
        effect_id=effect_id,
    )
    receipt_id = canvas_publication_receipt_id(
        command_hash=command_hash,
        source_identity_hash=source_identity_hash,
        effect_id=effect_id,
        generation_id=generation_id,
    )
    catalog_without_hash = {
        "schema_version": CANVAS_CATALOG_SCHEMA_VERSION,
        "name": command.name,
        "description": command.description,
        "pool_refs": command.pool_refs,
        "created_at": catalog_created_at,
        "updated_at": catalog_updated_at,
        "source": command.source,
        "command_id": command.command_id,
        "command_hash": command_hash,
        "source_identity_hash": source_identity_hash,
        "publication_generation_id": generation_id,
        "publication_receipt_id": receipt_id,
    }
    record_hash_payload = CanvasPublicationCatalogRecord.model_construct(
        **catalog_without_hash,
        record_hash="0" * 64,
    ).model_dump(mode="json", exclude={"record_hash"})
    catalog_record = CanvasPublicationCatalogRecord(
        **catalog_without_hash,
        record_hash=canonical_sha256(record_hash_payload),
    )
    return CanvasPublicationClaims(
        command=command,
        command_hash=command_hash,
        catalog_record=catalog_record,
        catalog_record_hash=catalog_record.record_hash,
        source_identity_hash=source_identity_hash,
        consumer_service_id=consumer_service_id,
        consumer_instance_id=consumer_instance_id,
        effect_id=effect_id,
        generation_id=generation_id,
        receipt_id=receipt_id,
        created_at=catalog_updated_at,
    )


def _validate_sha256(value: str, *, label: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{label} is invalid")


def _write_all(descriptor: int, payload: bytes) -> None:
    written = 0
    while written < len(payload):
        count = os.write(descriptor, payload[written:])
        if count <= 0:
            raise OSError("short write while writing canvas publication receipt")
        written += count


def _read_receipt_bytes_from_directory(
    directory_descriptor: int,
    file_name: str,
    *,
    label: str,
) -> tuple[bytes, os.stat_result]:
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        item = os.stat(file_name, dir_fd=directory_descriptor, follow_symlinks=False)
    except FileNotFoundError:
        raise
    if stat.S_ISLNK(item.st_mode) or not stat.S_ISREG(item.st_mode):
        raise ValueError(f"{label} must be a regular non-symlink file")
    if item.st_size > MAX_RECEIPT_BYTES:
        raise ValueError(f"{label} exceeds size bound")
    descriptor: int | None = None
    try:
        try:
            descriptor = os.open(file_name, flags, dir_fd=directory_descriptor)
        except OSError as exc:
            raise ValueError(f"{label} cannot be opened safely") from exc
        opened = os.fstat(descriptor)
        if _file_identity(opened) != _file_identity(item):
            raise ValueError(f"{label} rotated while open")
        chunks: list[bytes] = []
        total = 0
        while chunk := os.read(descriptor, 1024 * 1024):
            total += len(chunk)
            if total > MAX_RECEIPT_BYTES:
                raise ValueError(f"{label} exceeds size bound")
            chunks.append(chunk)
        after = os.fstat(descriptor)
        if _file_identity(after) != _file_identity(opened):
            raise ValueError(f"{label} rotated while read")
        _verify_open_file_matches_entry(
            directory_descriptor,
            file_name,
            after,
            label=label,
        )
        return b"".join(chunks), after
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _open_or_create_receipt_directory(path: Path) -> int:
    return _open_receipt_directory_chain(path, create=True)


def _canvas_catalog_publication_bytes(record: CanvasPublicationCatalogRecord) -> bytes:
    return (
        json.dumps(record.model_dump(mode="json"), ensure_ascii=False, indent=2) + "\n"
    ).encode("utf-8")


def _open_existing_receipt_directory(path: Path) -> int:
    return _open_receipt_directory_chain(path, create=False)


def _open_receipt_directory_chain(path: Path, *, create: bool) -> int:
    normalized = Path(os.path.abspath(path))
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptors: list[int] = []
    try:
        descriptors.append(os.open(normalized.anchor, flags))
        components = normalized.parts[1:]
        for index, component in enumerate(components):
            parent = descriptors[-1]
            try:
                entry = os.stat(component, dir_fd=parent, follow_symlinks=False)
            except FileNotFoundError:
                if not create:
                    raise
                os.mkdir(component, PRIVATE_DIRECTORY_MODE, dir_fd=parent)
                entry = os.stat(component, dir_fd=parent, follow_symlinks=False)
            if stat.S_ISLNK(entry.st_mode):
                raise ValueError(
                    "canvas publication receipt directory ancestor cannot be a symlink: "
                    f"{normalized}"
                )
            if not stat.S_ISDIR(entry.st_mode):
                raise ValueError(
                    "canvas publication receipt directory ancestor is not a directory: "
                    f"{normalized}"
                )
            descriptor = os.open(component, flags, dir_fd=parent)
            opened = os.fstat(descriptor)
            if _file_node(opened) != _file_node(entry):
                os.close(descriptor)
                raise ValueError(
                    "canvas publication receipt directory ancestor changed while open: "
                    f"{normalized}"
                )
            descriptors.append(descriptor)
            if index == len(components) - 1:
                os.fchmod(descriptor, PRIVATE_DIRECTORY_MODE)
        result = descriptors.pop()
        _verify_open_directory_matches_path(normalized, result)
        return result
    except OSError as exc:
        raise ValueError(
            f"canvas publication receipt directory cannot be opened: {normalized}"
        ) from exc
    except Exception:
        raise
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _verify_open_directory_matches_path(path: Path, descriptor: int) -> None:
    try:
        entry = os.stat(path, follow_symlinks=False)
    except FileNotFoundError as exc:
        raise ValueError(f"canvas publication receipt directory changed: {path}") from exc
    if stat.S_ISLNK(entry.st_mode):
        raise ValueError(f"canvas publication receipt directory cannot be a symlink: {path}")
    if not stat.S_ISDIR(entry.st_mode):
        raise ValueError(f"canvas publication receipt path is not a directory: {path}")
    opened = os.fstat(descriptor)
    if _file_node(entry) != _file_node(opened):
        raise ValueError(f"canvas publication receipt directory changed: {path}")


def _verify_open_file_matches_entry(
    directory_descriptor: int,
    file_name: str,
    opened: os.stat_result,
    *,
    label: str,
    expected_mode: int | None = None,
) -> None:
    try:
        entry = os.stat(file_name, dir_fd=directory_descriptor, follow_symlinks=False)
    except FileNotFoundError as exc:
        raise ValueError(f"{label} changed while open") from exc
    if stat.S_ISLNK(entry.st_mode):
        raise ValueError(f"{label} cannot be a symlink")
    if not stat.S_ISREG(entry.st_mode):
        raise ValueError(f"{label} is not a regular file")
    if _file_node(entry) != _file_node(opened):
        raise ValueError(f"{label} changed while open")
    if expected_mode is not None and stat.S_IMODE(entry.st_mode) != expected_mode:
        raise ValueError(f"{label} has unsafe permissions")


def _file_node(value: os.stat_result) -> tuple[int, int]:
    return value.st_dev, value.st_ino


def _file_identity(value: os.stat_result) -> tuple[int, int, int, int]:
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns


__all__ = [
    "CANVAS_CATALOG_SCHEMA_VERSION",
    "CANVAS_PUBLICATION_EFFECT_KIND",
    "CANVAS_PUBLICATION_PROBE_NAMESPACE",
    "CanvasPublicationSigningRequest",
    "CanvasPublicationSigningResponse",
    "CanvasPublicationCatalogRecord",
    "CanvasPublicationClaims",
    "CanvasPublicationCommand",
    "CanvasPublicationKeyring",
    "CanvasPublicationReceipt",
    "CanvasPublicationReceiptStore",
    "CanvasPublicationSigner",
    "Ed25519CanvasPublicationKeyring",
    "Ed25519CanvasPublicationSigner",
    "SecureCanvasPublicationSigningClient",
    "build_canvas_publication_claims",
    "canvas_catalog_record_hash",
    "canvas_command_hash",
    "canvas_publication_effect_id",
    "canvas_publication_generation_id",
    "canvas_publication_receipt_hash",
    "canvas_publication_receipt_id",
    "canvas_source_identity_hash",
]
